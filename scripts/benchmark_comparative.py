#!/usr/bin/env python3
import argparse
import os
import subprocess
import sys
import threading
import time
from datetime import datetime

CASE2_RUST = os.environ.get(
    "CASE2_RUST_DIR",
    os.path.join(os.path.dirname(__file__), "..",
                 "case_2", "dropout-bigdata-pipeline-rust")
)


def stream_output(proc, label: str, lines_out: list):
    for line in iter(proc.stdout.readline, b""):
        text = line.decode("utf-8", errors="replace").rstrip()
        if text:
            ts = datetime.now().strftime("%H:%M:%S")
            print(f"  [{ts}] [{label}] {text}", flush=True)
            lines_out.append(text)
    proc.stdout.close()


def run_concurrent_pipeline(rate: int, batch_size: int, idle_timeout: int):
    rust_bin = os.path.join(CASE2_RUST, "target", "release")
    producer_bin = os.path.join(rust_bin, "producer")
    consumer_bin = os.path.join(rust_bin, "consumer")
    dataset_path = os.path.join(
        CASE2_RUST, "..", "..", "data", "dataset_clean.csv")
    parquet_out = os.environ.get("PARQUET_OUT_DIR", os.path.join(
        CASE2_RUST, "..", "..", "data", "preprocessed"))

    for b in [producer_bin, consumer_bin]:
        if not os.path.exists(b):
            print(f"[ERROR] Binary not found: {b}")
            print("        Please run: make build-case2")
            sys.exit(1)

    print("=" * 60)
    print("  CONCURRENT PIPELINE — Case 2 (Rust)")
    print("=" * 60)
    print(f"  Rate     : {'BURST' if rate == 0 else f'{rate} rec/s'}")
    print(f"  Output   : {parquet_out}")
    print(f"  Batch sz : {batch_size}")
    print()

    os.makedirs(parquet_out, exist_ok=True)

    consumer_cmd = [
        consumer_bin,
        "--output", parquet_out,
        "--batch-size", str(batch_size),
        "--idle-timeout", str(idle_timeout),
    ]
    print(f"[START] Consumer: {' '.join(consumer_cmd)}")
    consumer_proc = subprocess.Popen(
        consumer_cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        env={**os.environ, "RUST_LOG": "info"},
    )

    time.sleep(2)

    producer_cmd = [
        producer_bin,
        "--rate", str(rate),
        "--dataset", os.path.abspath(dataset_path),
    ]
    print(f"[START] Producer: {' '.join(producer_cmd)}")
    producer_proc = subprocess.Popen(
        producer_cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        env={**os.environ, "RUST_LOG": "info"},
    )

    prod_lines, cons_lines = [], []

    prod_thread = threading.Thread(
        target=stream_output, args=(producer_proc, "PRODUCER", prod_lines), daemon=True
    )
    cons_thread = threading.Thread(
        target=stream_output, args=(consumer_proc, "CONSUMER", cons_lines), daemon=True
    )
    prod_thread.start()
    cons_thread.start()

    start = time.time()
    try:
        while True:
            prod_alive = producer_proc.poll() is None
            cons_alive = consumer_proc.poll() is None

            elapsed = time.time() - start
            print(
                f"\r  [{elapsed:6.1f}s] "
                f"Producer: {'RUNNING' if prod_alive else 'DONE ':6s} | "
                f"Consumer: {'RUNNING' if cons_alive else 'DONE ':6s}",
                end="", flush=True
            )

            if not prod_alive and not cons_alive:
                break

            time.sleep(0.5)

    except KeyboardInterrupt:
        print("\n[INFO] Interrupted by user. Cleaning up processes...")
        producer_proc.terminate()
        consumer_proc.terminate()

    prod_thread.join(timeout=5)
    cons_thread.join(timeout=5)

    prod_rc = producer_proc.wait()
    cons_rc = consumer_proc.wait()

    print(f"\n\n[DONE] Producer exit: {prod_rc} | Consumer exit: {cons_rc}")

    if prod_rc != 0 or cons_rc != 0:
        print("[WARNING] One of the processes exited with an error. Check the logs above.")
        return False

    return True


def main():
    parser = argparse.ArgumentParser(
        description="Run Rust producer and consumer concurrently (real pipeline behavior)"
    )
    parser.add_argument("--rate", type=int,
                        default=0, help="Records per second, 0 = burst mode")
    parser.add_argument("--batch-size", type=int,
                        default=5000, help="Parquet batch size")
    parser.add_argument("--idle-timeout", type=int, default=10,
                        help="Seconds before the consumer stops when idle")
    args = parser.parse_args()

    success = run_concurrent_pipeline(
        args.rate, args.batch_size, args.idle_timeout)

    if success:
        print("\n[NEXT] Run PySpark training:")
        print("       python3 case_2/spark_training_parquet.py")
    else:
        sys.exit(1)


if __name__ == "__main__":
    main()
