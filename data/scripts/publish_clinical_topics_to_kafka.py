import argparse
import csv
import json
import os
from datetime import datetime, timezone
from pathlib import Path

import yaml
from kafka import KafkaProducer


def load_config(path):
    with open(path, "r", encoding="utf-8") as file:
        return yaml.safe_load(file)


def is_embedded_header(row):
    return any(value == key for key, value in row.items())


def validate_row(row, required_fields):
    if is_embedded_header(row):
        return "embedded_csv_header"
    missing = [field for field in required_fields if not str(row.get(field, "")).strip()]
    return f"missing_required_fields:{','.join(missing)}" if missing else ""


def envelope(domain, row, row_number, producer_run_id):
    return {
        "domain": domain,
        "producer_run_id": producer_run_id,
        "producer_row_number": row_number,
        "producer_timestamp": datetime.now(timezone.utc).isoformat(),
        "payload": row,
    }


def load_cursor(path):
    cursor_path = Path(path)
    if not cursor_path.exists():
        return {}
    cursor = json.loads(cursor_path.read_text(encoding="utf-8"))
    if not isinstance(cursor, dict):
        raise ValueError(f"Kafka cursor must be a JSON object: {cursor_path}")
    return {str(domain): max(0, int(row_number)) for domain, row_number in cursor.items()}


def write_cursor(path, cursor):
    cursor_path = Path(path)
    cursor_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = cursor_path.with_suffix(f"{cursor_path.suffix}.tmp")
    temporary_path.write_text(json.dumps(cursor, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary_path.replace(cursor_path)


def publish_domain(producer, domain, domain_config, dlq_topic, start_row, max_rows, producer_run_id):
    sent = 0
    rejected = 0
    scanned = 0
    next_row = start_row
    source_path = domain_config["source"]
    topic = domain_config["topic"]
    required_fields = domain_config.get("required_fields", [])
    with open(source_path, newline="", encoding="utf-8") as csv_file:
        for row_number, row in enumerate(csv.DictReader(csv_file)):
            if row_number < start_row:
                continue
            if max_rows and scanned >= max_rows:
                break
            scanned += 1
            next_row = row_number + 1
            error = validate_row(row, required_fields)
            message = envelope(domain, row, row_number, producer_run_id)
            key = str(row.get("ID") or row.get("ENCOUNTER") or row.get("PATIENT") or row_number)
            if error:
                message["error_reason"] = error
                producer.send(dlq_topic, key=key, value=message).get(timeout=30)
                rejected += 1
                continue
            producer.send(topic, key=key, value=message).get(timeout=30)
            sent += 1
    print(
        f"[KAFKA PRODUCER] {domain:13} topic={topic:25} "
        f"rows={scanned:,} sent={sent:,} dlq={rejected:,} next_row={next_row:,}"
    )
    return sent, rejected, next_row


def main():
    parser = argparse.ArgumentParser(description="Publish Clinical 360 CSV domains to Kafka.")
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--domain", action="append", help="Publish only the selected domain. Repeat as needed.")
    parser.add_argument("--start-row", type=int, default=int(os.getenv("KAFKA_START_ROW", "0")))
    parser.add_argument("--max-rows", type=int, default=int(os.getenv("KAFKA_MAX_ROWS", "5000")))
    parser.add_argument("--use-cursor", action="store_true", help="Resume each domain from its persisted source row.")
    parser.add_argument("--cursor-path", help="Override the persisted per-domain cursor JSON path.")
    parser.add_argument("--producer-run-id", default=os.getenv("KAFKA_PRODUCER_RUN_ID"))
    args = parser.parse_args()

    config = load_config(args.config)
    kafka_config = config["kafka"]
    selected_domains = args.domain or list(kafka_config["domains"])
    unknown = sorted(set(selected_domains) - set(kafka_config["domains"]))
    if unknown:
        raise ValueError(f"Unknown Kafka domains: {', '.join(unknown)}")
    producer_run_id = args.producer_run_id or datetime.now(timezone.utc).strftime("clinical-%Y%m%d%H%M%S")
    cursor_path = args.cursor_path or kafka_config.get("cursor_path", "lakehouse/checkpoints/kafka_publish_cursor.json")
    cursor = load_cursor(cursor_path) if args.use_cursor else {}
    producer = KafkaProducer(
        bootstrap_servers=kafka_config["bootstrap_servers"],
        key_serializer=lambda value: value.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        linger_ms=25,
        acks="all",
    )
    try:
        for domain in selected_domains:
            start_row = cursor.get(domain, args.start_row) if args.use_cursor else args.start_row
            _, _, next_row = publish_domain(
                producer,
                domain,
                kafka_config["domains"][domain],
                kafka_config["dlq_topic"],
                start_row,
                args.max_rows,
                producer_run_id,
            )
            if args.use_cursor:
                cursor[domain] = next_row
                write_cursor(cursor_path, cursor)
        producer.flush()
    finally:
        producer.close()


if __name__ == "__main__":
    main()
