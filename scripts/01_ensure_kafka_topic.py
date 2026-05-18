#!/usr/bin/env python3

import argparse
import sys
import time

from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import KafkaError, NoBrokersAvailable, TopicAlreadyExistsError


def wait_admin_client(bootstrap_server: str, timeout_s: int) -> KafkaAdminClient:
    deadline = time.time() + timeout_s
    last_error: Exception | None = None

    while time.time() < deadline:
        try:
            return KafkaAdminClient(
                bootstrap_servers=bootstrap_server,
                client_id="dropout-topic-init",
            )
        except NoBrokersAvailable as error:
            last_error = error
            print("[kafka] Broker not ready yet...")
            time.sleep(1)

    raise RuntimeError(
        f"Kafka broker is not ready after {timeout_s} seconds: {last_error}"
    )


def ensure_topic(
    bootstrap_server: str,
    topic: str,
    partitions: int,
    replication_factor: int,
    timeout_s: int,
) -> None:
    admin = wait_admin_client(bootstrap_server, timeout_s)

    try:
        existing_topics = set(admin.list_topics())

        if topic in existing_topics:
            print(f"[kafka] Topic already exists: {topic}")
            return

        print(f"[kafka] Creating topic: {topic}")
        admin.create_topics(
            new_topics=[
                NewTopic(
                    name=topic,
                    num_partitions=partitions,
                    replication_factor=replication_factor,
                )
            ],
            validate_only=False,
        )
        print(f"[kafka] Topic created: {topic}")

    except TopicAlreadyExistsError:
        print(f"[kafka] Topic already exists: {topic}")
    except KafkaError as error:
        raise RuntimeError(f"Failed to ensure Kafka topic: {error}") from error
    finally:
        admin.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-server", default="127.0.0.1:9092")
    parser.add_argument("--topic", default="student-data-rust")
    parser.add_argument("--partitions", type=int, default=1)
    parser.add_argument("--replication-factor", type=int, default=1)
    parser.add_argument("--timeout", type=int, default=20)
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    try:
        ensure_topic(
            bootstrap_server=args.bootstrap_server,
            topic=args.topic,
            partitions=args.partitions,
            replication_factor=args.replication_factor,
            timeout_s=args.timeout,
        )
    except Exception as error:
        print(f"[ERROR] {error}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
