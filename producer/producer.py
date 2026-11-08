import csv
import glob
import json
import os
import re
import sys
import time
from pathlib import Path

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "mock_data")
DATA_DIR = os.getenv("DATA_DIR", "/data")
SEND_DELAY = float(os.getenv("SEND_DELAY_SECONDS", "0.001"))


def file_order(path: str) -> int:
    name = Path(path).name
    match = re.search(r"\((\d+)\)", name)
    return int(match.group(1)) if match else 10


def connect_producer() -> KafkaProducer:
    last_error = None
    for attempt in range(1, 61):
        try:
            producer = KafkaProducer(
                bootstrap_servers=BOOTSTRAP.split(","),
                key_serializer=lambda value: value.encode("utf-8"),
                value_serializer=lambda value: json.dumps(
                    value, ensure_ascii=False, separators=(",", ":")
                ).encode("utf-8"),
                acks="all",
                retries=10,
                max_in_flight_requests_per_connection=1,
                request_timeout_ms=30000,
                api_version_auto_timeout_ms=10000,
            )
            producer.partitions_for(TOPIC)
            print(f"Connected to Kafka {BOOTSTRAP}; topic={TOPIC}", flush=True)
            return producer
        except NoBrokersAvailable as exc:
            last_error = exc
            print(f"Kafka is not ready ({attempt}/60), retrying...", flush=True)
            time.sleep(2)
    raise RuntimeError(f"Could not connect to Kafka: {last_error}")


def main() -> None:
    paths = sorted(
        glob.glob(os.path.join(DATA_DIR, "MOCK_DATA*.csv")),
        key=file_order,
    )
    if len(paths) != 10:
        raise RuntimeError(
            f"Expected exactly 10 MOCK_DATA CSV files in {DATA_DIR}, found {len(paths)}"
        )

    producer = connect_producer()
    total_sent = 0
    expected_headers = None

    try:
        for file_index, path in enumerate(paths, start=1):
            file_name = Path(path).name
            file_rows = 0
            with open(path, "r", encoding="utf-8-sig", newline="") as csv_file:
                reader = csv.DictReader(csv_file)
                if expected_headers is None:
                    expected_headers = reader.fieldnames
                    if not expected_headers or len(expected_headers) != 50:
                        raise RuntimeError(
                            f"{file_name}: expected 50 CSV columns, got "
                            f"{0 if not expected_headers else len(expected_headers)}"
                        )
                elif reader.fieldnames != expected_headers:
                    raise RuntimeError(f"CSV header mismatch in {file_name}")

                for row_number, row in enumerate(reader, start=1):
                    if row_number > 1000:
                        raise RuntimeError(f"{file_name}: more than 1000 rows")

                    event_id = f"file{file_index:02d}-row{row_number:04d}"
                    payload = {
                        key: (value if value is not None else "")
                        for key, value in row.items()
                    }
                    payload["event_id"] = event_id
                    payload["source_file"] = file_name
                    payload["source_row"] = str(row_number)

                    producer.send(TOPIC, key=event_id, value=payload)
                    file_rows += 1
                    total_sent += 1

                    if total_sent % 1000 == 0:
                        producer.flush()
                        print(f"Produced {total_sent}/10000 JSON messages", flush=True)

                    if SEND_DELAY > 0:
                        time.sleep(SEND_DELAY)

            if file_rows != 1000:
                raise RuntimeError(f"{file_name}: expected 1000 rows, got {file_rows}")
            print(f"Sent {file_name}: {file_rows} rows", flush=True)

        producer.flush()
        if total_sent != 10000:
            raise RuntimeError(f"Expected 10000 messages, sent {total_sent}")
        print(f"SUCCESS: produced {total_sent} JSON messages to topic '{TOPIC}'", flush=True)
    finally:
        producer.close()


if __name__ == "__main__":
    main()
