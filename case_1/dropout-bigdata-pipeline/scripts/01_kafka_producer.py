#!/usr/bin/env python3
import argparse
import json
import os
import statistics
import sys
import time
from datetime import datetime

import pandas as pd
from kafka import KafkaProducer
from kafka.errors import KafkaError

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_NAME      = "student-data"
DATA_PATH       = os.environ.get("DATASET_PATH", os.path.join(os.path.dirname(__file__), "..", "..", "data", "dataset_clean.csv"))
LOG_DIR         = os.environ.get("LOG_DIR", os.path.join(os.path.dirname(__file__), "..", "logs"))


def create_producer():
    for attempt in range(1, 6):
        try:
            p = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: str(k).encode("utf-8"),
                acks="1",
                retries=3,
                linger_ms=5,
                batch_size=16384,
            )
            print(f"[OK] Connected to Kafka: {KAFKA_BOOTSTRAP}")
            return p
        except Exception as e:
            print(f"[WARN] Attempt {attempt}/5 failed: {e}")
            if attempt < 5:
                time.sleep(5)
    print("[ERROR] Cannot connect to Kafka.")
    sys.exit(1)


def load_data():
    if not os.path.exists(DATA_PATH):
        print(f"[ERROR] Dataset not found: {DATA_PATH}")
        print("        Run: python3 scripts/00_download_dataset.py")
        sys.exit(1)
    df = pd.read_csv(DATA_PATH)
    print(f"[INFO] Dataset loaded: {len(df):,} records, {df.shape[1]} columns")
    return df


def percentile(data, q):
    """Hitung percentile dari list float. q dalam 0–100."""
    if not data:
        return 0
    s = sorted(data)
    idx = int(len(s) * q / 100)
    return s[min(idx, len(s) - 1)]


def produce(rate: int, loop: bool = False):
    producer = create_producer()
    df       = load_data()
    records  = df.to_dict(orient="records")

    total_sent    = 0
    total_errors  = 0
    latencies_us  = [] 
    interval      = 1.0 / rate if rate > 0 else 0
    start         = time.time()
    iteration     = 0

    print(f"\n[INFO] Sending to topic '{TOPIC_NAME}'")
    print(f"       Rate: {'BURST' if rate == 0 else f'{rate} rec/s'}")
    print(f"       Loop: {'yes' if loop else 'no'}")
    print("-" * 60)

    try:
        while True:
            for idx, record in enumerate(records):
                record = dict(record)
                record["_event_id"]  = f"{iteration}_{idx}"
                record["_timestamp"] = datetime.utcnow().isoformat()

                t_send = time.perf_counter()

                future = producer.send(TOPIC_NAME, key=str(total_sent), value=record)
                try:
                    future.get(timeout=10)
                    total_sent += 1
                except KafkaError as e:
                    total_errors += 1

                lat_us = (time.perf_counter() - t_send) * 1_000_000
                latencies_us.append(lat_us)

                if rate > 0:
                    elapsed_send = time.perf_counter() - t_send
                    sleep_time   = interval - elapsed_send
                    if sleep_time > 0:
                        time.sleep(sleep_time)

                if total_sent % 500 == 0 and total_sent > 0:
                    elapsed = time.time() - start
                    print(f"  [{datetime.now().strftime('%H:%M:%S')}] "
                          f"Sent: {total_sent:>5,} | "
                          f"Rate: {total_sent/elapsed:>7.1f} rec/s | "
                          f"Errors: {total_errors}")

            iteration += 1
            if not loop:
                break

    except KeyboardInterrupt:
        print("\n[INFO] Stopped by user.")
    finally:
        producer.flush()
        producer.close()

    elapsed    = time.time() - start
    avg_rate   = total_sent / elapsed if elapsed > 0 else 0

    p50  = percentile(latencies_us, 50)
    p95  = percentile(latencies_us, 95)
    p99  = percentile(latencies_us, 99)
    p999 = percentile(latencies_us, 99.9)
    mean_lat = statistics.mean(latencies_us) if latencies_us else 0

    print(f"\n{'='*60}")
    print("RINGKASAN PRODUCER (Python / kafka-python)")
    print(f"{'='*60}")
    print(f"  Total sent     : {total_sent:,}")
    print(f"  Total errors   : {total_errors}")
    print(f"  Elapsed        : {elapsed:.2f} s")
    print(f"  Throughput     : {avg_rate:.2f} rec/s")
    print(f"  Latency P50    : {p50:,.1f} µs")
    print(f"  Latency P95    : {p95:,.1f} µs")
    print(f"  Latency P99    : {p99:,.1f} µs")
    print(f"  Latency P99.9  : {p999:,.1f} µs")
    print(f"{'='*60}")

    os.makedirs(LOG_DIR, exist_ok=True)
    ts   = int(time.time())
    path = os.path.join(LOG_DIR, f"producer_rate{rate}_{ts}.json")
    result = {
        "impl"        : "python",
        "target_rate" : rate,
        "total_sent"  : total_sent,
        "total_errors": total_errors,
        "total_time_s": round(elapsed, 3),
        "avg_rate"    : round(avg_rate, 2),
        "latency_us"  : {
            "p50"  : round(p50,  1),
            "p95"  : round(p95,  1),
            "p99"  : round(p99,  1),
            "p99_9": round(p999, 1),
            "mean" : round(mean_lat, 1),
            "max"  : round(max(latencies_us), 1) if latencies_us else 0,
        }
    }
    with open(path, "w") as f:
        json.dump(result, f, indent=2)
    print(f"[INFO] Result saved: {path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--rate", type=int, default=100,
                        help="Records per second. 0 = burst. Default: 100")
    parser.add_argument("--loop", action="store_true",
                        help="Loop dataset until Ctrl+C")
    args = parser.parse_args()
    produce(rate=args.rate, loop=args.loop)
