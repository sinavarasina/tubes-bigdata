#!/usr/bin/env python3
import argparse
import json
import os
import statistics
import sys
import time
from datetime import datetime
from pathlib import Path

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_NAME      = os.environ.get("KAFKA_TOPIC", "student-data")
GROUP_ID        = os.environ.get("KAFKA_GROUP_ID", "dropout-consumer-python")
PARQUET_OUT_DIR = os.environ.get("PARQUET_OUT_DIR",
                    os.path.join(os.path.dirname(__file__), "..", "..", "data", "preprocessed"))
LOG_DIR         = os.environ.get("LOG_DIR",
                    os.path.join(os.path.dirname(__file__), "..", "logs"))


def preprocess_batch(records: list) -> list:
    """
    Preprocessing per batch:
    - Validasi range nilai
    - Feature engineering (pass_rate, grade_delta, financial_stability_index)
    - Encode label
    """
    clean = []
    for r in records:
        try:
            age = int(r.get("Age at enrollment", r.get("age_at_enrollment", 0)))
            if not (17 <= age <= 70):
                continue

            g1 = float(r.get("Curricular units 1st sem (grade)",
                              r.get("curricular_units_1st_sem_grade", 0)))
            g2 = float(r.get("Curricular units 2nd sem (grade)",
                              r.get("curricular_units_2nd_sem_grade", 0)))
            if g1 < 0 or g2 < 0:
                continue

            target = str(r.get("Target", r.get("target", ""))).strip()
            label_map = {"Dropout": 0, "Enrolled": 1, "Graduate": 2}
            label = label_map.get(target, -1)
            if label == -1:
                continue

            enrolled_1 = int(r.get("Curricular units 1st sem (enrolled)",
                                    r.get("curricular_units_1st_sem_enrolled", 0)))
            approved_1 = int(r.get("Curricular units 1st sem (approved)",
                                    r.get("curricular_units_1st_sem_approved", 0)))
            enrolled_2 = int(r.get("Curricular units 2nd sem (enrolled)",
                                    r.get("curricular_units_2nd_sem_enrolled", 0)))
            approved_2 = int(r.get("Curricular units 2nd sem (approved)",
                                    r.get("curricular_units_2nd_sem_approved", 0)))
            scholarship = int(r.get("Scholarship holder",
                                     r.get("scholarship_holder", 0)))
            debtor      = int(r.get("Debtor", r.get("debtor", 0)))
            tuition     = int(r.get("Tuition fees up to date",
                                     r.get("tuition_fees_up_to_date", 0)))

            pass_rate_1 = approved_1 / enrolled_1 if enrolled_1 > 0 else 0.0
            pass_rate_2 = approved_2 / enrolled_2 if enrolled_2 > 0 else 0.0
            grade_delta = g2 - g1
            fsi         = float(tuition) + float(scholarship) - float(debtor)

            row = {
                "marital_status":             int(r.get("Marital status", r.get("marital_status", 0))),
                "gender":                     int(r.get("Gender", r.get("gender", 0))),
                "age_at_enrollment":          age,
                "international":              int(r.get("International", r.get("international", 0))),
                "displaced":                  int(r.get("Displaced", r.get("displaced", 0))),
                "educational_special_needs":  int(r.get("Educational special needs", r.get("educational_special_needs", 0))),
                "nacionality":                int(r.get("Nacionality", r.get("nacionality", 0))),
                "mothers_qualification":      int(r.get("Mother's qualification", r.get("mothers_qualification", 0))),
                "fathers_qualification":      int(r.get("Father's qualification", r.get("fathers_qualification", 0))),
                "mothers_occupation":         int(r.get("Mother's occupation", r.get("mothers_occupation", 0))),
                "fathers_occupation":         int(r.get("Father's occupation", r.get("fathers_occupation", 0))),
                "scholarship_holder":         scholarship,
                "debtor":                     debtor,
                "tuition_fees_up_to_date":    tuition,
                "application_mode":           int(r.get("Application mode", r.get("application_mode", 0))),
                "application_order":          int(r.get("Application order", r.get("application_order", 0))),
                "course":                     int(r.get("Course", r.get("course", 0))),
                "daytime_evening_attendance": int(r.get("Daytime/evening attendance", r.get("daytime_evening_attendance", 0))),
                "previous_qualification":     int(r.get("Previous qualification", r.get("previous_qualification", 0))),
                "previous_qualification_grade": float(r.get("Previous qualification (grade)", r.get("previous_qualification_grade", 0))),
                "admission_grade":            float(r.get("Admission grade", r.get("admission_grade", 0))),
                "curricular_units_1st_sem_credited":            int(r.get("Curricular units 1st sem (credited)", r.get("curricular_units_1st_sem_credited", 0))),
                "curricular_units_1st_sem_enrolled":            enrolled_1,
                "curricular_units_1st_sem_evaluations":         int(r.get("Curricular units 1st sem (evaluations)", r.get("curricular_units_1st_sem_evaluations", 0))),
                "curricular_units_1st_sem_approved":            approved_1,
                "curricular_units_1st_sem_grade":               g1,
                "curricular_units_2nd_sem_credited":            int(r.get("Curricular units 2nd sem (credited)", r.get("curricular_units_2nd_sem_credited", 0))),
                "curricular_units_2nd_sem_enrolled":            enrolled_2,
                "curricular_units_2nd_sem_evaluations":         int(r.get("Curricular units 2nd sem (evaluations)", r.get("curricular_units_2nd_sem_evaluations", 0))),
                "curricular_units_2nd_sem_approved":            approved_2,
                "curricular_units_2nd_sem_grade":               g2,
                "unemployment_rate":          float(r.get("Unemployment rate", r.get("unemployment_rate", 0))),
                "inflation_rate":             float(r.get("Inflation rate", r.get("inflation_rate", 0))),
                "gdp":                        float(r.get("GDP", r.get("gdp", 0))),
                "pass_rate_1st_sem":          pass_rate_1,
                "pass_rate_2nd_sem":          pass_rate_2,
                "grade_delta":                grade_delta,
                "financial_stability_index":  fsi,
                "target":                     target,
                "label":                      label,
            }
            clean.append(row)
        except (ValueError, TypeError, KeyError):
            continue

    return clean


def flush_parquet(rows: list, out_dir: str, batch_idx: int):
    import pandas as pd
    df   = pd.DataFrame(rows)
    path = os.path.join(out_dir, f"batch_{batch_idx:04d}.parquet")
    df.to_parquet(path, index=False, compression="snappy")
    return path


def consume(batch_size: int, idle_timeout: int):
    from kafka import KafkaConsumer

    os.makedirs(PARQUET_OUT_DIR, exist_ok=True)
    os.makedirs(LOG_DIR, exist_ok=True)

    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=idle_timeout * 1000,
    )

    print(f"[INFO] Consumer started. Topic: {TOPIC_NAME} | Group: {GROUP_ID}")
    print(f"[INFO] Output: {PARQUET_OUT_DIR}")

    batch        = []
    batch_idx    = 0
    total_recv   = 0
    total_inv    = 0
    latencies_us = []
    start        = time.time()

    try:
        for msg in consumer:
            t_recv    = time.perf_counter()
            total_recv += 1
            record    = msg.value

            clean = preprocess_batch([record])
            if not clean:
                total_inv += 1
            else:
                batch.extend(clean)

            lat_us = (time.perf_counter() - t_recv) * 1_000_000
            latencies_us.append(lat_us)

            if len(batch) >= batch_size:
                path = flush_parquet(batch, PARQUET_OUT_DIR, batch_idx)
                print(f"  [batch {batch_idx:04d}] {len(batch)} rows → {os.path.basename(path)}")
                batch     = []
                batch_idx += 1

            if total_recv % 500 == 0:
                elapsed = time.time() - start
                print(f"  [{datetime.now().strftime('%H:%M:%S')}] "
                      f"Recv: {total_recv:>5,} | "
                      f"Invalid: {total_inv} | "
                      f"Rate: {total_recv/elapsed:.1f} rec/s")

    except Exception as e:
        pass
    finally:
        if batch:
            path = flush_parquet(batch, PARQUET_OUT_DIR, batch_idx)
            print(f"  [batch {batch_idx:04d}] {len(batch)} rows (final) → {os.path.basename(path)}")
        consumer.close()

    elapsed  = time.time() - start
    tput     = total_recv / elapsed if elapsed > 0 else 0

    p99 = sorted(latencies_us)[int(len(latencies_us) * 0.99)] if latencies_us else 0

    print(f"\n{'='*60}")
    print("RINGKASAN CONSUMER (Python / kafka-python)")
    print(f"{'='*60}")
    print(f"  Total received : {total_recv:,}")
    print(f"  Total invalid  : {total_inv}")
    print(f"  Batches written: {batch_idx + 1}")
    print(f"  Elapsed        : {elapsed:.2f} s")
    print(f"  Throughput     : {tput:.2f} rec/s")
    print(f"  Proc P99       : {p99:.1f} µs")
    print(f"{'='*60}")

    ts   = int(time.time())
    path = os.path.join(LOG_DIR, f"consumer_python_{ts}.json")
    with open(path, "w") as f:
        json.dump({
            "impl"           : "python",
            "total_received" : total_recv,
            "total_invalid"  : total_inv,
            "total_batches"  : batch_idx + 1,
            "total_time_s"   : round(elapsed, 3),
            "throughput_recs": round(tput, 2),
        }, f, indent=2)
    print(f"[INFO] Result saved: {path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-size",   type=int, default=2000)
    parser.add_argument("--idle-timeout", type=int, default=10,
                        help="Seconds to wait for messages before stopping")
    args = parser.parse_args()
    consume(args.batch_size, args.idle_timeout)
