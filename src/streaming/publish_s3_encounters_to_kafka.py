import argparse
import json
import os
from datetime import datetime, timezone

import pandas as pd
from confluent_kafka import Producer
from pyspark.sql import SparkSession


DEFAULT_SOURCE = (
    "/Volumes/workspace/default/clinical_s3_landing/"
    "encounters/encounters_combined.csv"
)
DEFAULT_OFFSET_TABLE = "workspace.default.kafka_publish_offsets"
DEFAULT_TOPIC = "clinical-encounters"
DEFAULT_DLQ_TOPIC = "clinical-dlq"


def get_secret(scope, key, env_name):
    if env_name in os.environ:
        return os.environ[env_name]
    try:
        return dbutils.secrets.get(scope=scope, key=key)
    except NameError as error:
        raise RuntimeError(
            f"Missing {env_name}. Set it locally or configure Databricks secret "
            f"{scope}/{key}."
        ) from error


def kafka_producer():
    bootstrap_server = get_secret(
        "clinical360-kafka",
        "bootstrap-server",
        "AIVEN_KAFKA_BOOTSTRAP_SERVER",
    ).strip()
    username = get_secret(
        "clinical360-kafka",
        "username",
        "AIVEN_KAFKA_USERNAME",
    )
    password = get_secret(
        "clinical360-kafka",
        "password",
        "AIVEN_KAFKA_PASSWORD",
    )
    ca_file = os.getenv(
        "AIVEN_KAFKA_CA_FILE",
        "/Volumes/workspace/default/healthcare_lakehouse/kafka/ca.pem",
    )
    return Producer(
        {
            "bootstrap.servers": bootstrap_server,
            "security.protocol": "SASL_SSL",
            "sasl.mechanism": "SCRAM-SHA-256",
            "sasl.username": username,
            "sasl.password": password,
            "ssl.ca.location": ca_file,
            "client.id": "clinical360-s3-encounter-replay",
            "enable.idempotence": True,
            "acks": "all",
        }
    )


def ensure_offset_table(spark, table_name):
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            source_name STRING,
            last_published_row BIGINT,
            updated_at TIMESTAMP
        )
        USING DELTA
        """
    )


def read_offset(spark, table_name, source_name):
    ensure_offset_table(spark, table_name)
    rows = spark.sql(
        f"""
        SELECT COALESCE(MAX(last_published_row), 0) AS last_published_row
        FROM {table_name}
        WHERE source_name = '{source_name}'
        """
    ).collect()
    return int(rows[0]["last_published_row"]) if rows else 0


def write_offset(spark, table_name, source_name, next_row):
    spark.sql(
        f"""
        MERGE INTO {table_name} AS target
        USING (
            SELECT
                '{source_name}' AS source_name,
                CAST({int(next_row)} AS BIGINT) AS last_published_row,
                current_timestamp() AS updated_at
        ) AS source
        ON target.source_name = source.source_name
        WHEN MATCHED THEN UPDATE SET
            last_published_row = source.last_published_row,
            updated_at = source.updated_at
        WHEN NOT MATCHED THEN INSERT *
        """
    )


def normalize_timestamp(value):
    timestamp = pd.to_datetime(value, errors="coerce", utc=True)
    if pd.isna(timestamp):
        return ""
    return timestamp.isoformat()


def encounter_type(description):
    text = str(description or "").lower()
    if "emergency" in text:
        return "emergency"
    if "inpatient" in text or "hospital" in text:
        return "inpatient"
    if "urgent" in text:
        return "urgentcare"
    if "wellness" in text:
        return "wellness"
    return "ambulatory"


def length_of_stay_hours(description):
    text = str(description or "").lower()
    if "inpatient" in text or "hospital" in text:
        return 72.0
    if "emergency" in text:
        return 8.0
    return 1.0


def build_event(row, source_row):
    encounter_id = str(row.get("ID", "")).strip()
    patient_id = str(row.get("PATIENT", "")).strip()
    description = str(row.get("DESCRIPTION", "")).strip()
    reason_description = str(row.get("REASONDESCRIPTION", "")).strip()
    return {
        "event_id": f"s3-encounter-{encounter_id}-{source_row}",
        "encounter_id": encounter_id,
        "patient_id": patient_id,
        "event_type": "encounter_replay",
        "event_timestamp": normalize_timestamp(row.get("DATE", "")),
        "encounter_type": encounter_type(description),
        "diagnosis_code": str(row.get("CODE", "")).strip(),
        "diagnosis_description": reason_description or description,
        "facility": "S3 Historical Replay",
        "provider_id": "s3-replay",
        "length_of_stay_hours": length_of_stay_hours(description),
        "source": "s3_clinical_encounters",
        "source_row": int(source_row),
    }


def validation_error(event):
    if not event["encounter_id"]:
        return "MISSING_ENCOUNTER_ID"
    if not event["patient_id"]:
        return "MISSING_PATIENT_ID"
    if not event["event_timestamp"]:
        return "INVALID_EVENT_TIMESTAMP"
    if not event["diagnosis_code"]:
        return "MISSING_DIAGNOSIS_CODE"
    return ""


def publish(source, max_rows, source_name, offset_table, topic, dlq_topic):
    spark = SparkSession.builder.getOrCreate()
    start_row = read_offset(spark, offset_table, source_name)
    producer = kafka_producer()
    producer_run_id = datetime.now(timezone.utc).strftime("s3-kafka-%Y%m%d%H%M%S")

    sent = 0
    rejected = 0
    scanned = 0
    next_row = start_row

    reader = pd.read_csv(
        source,
        chunksize=max_rows,
        skiprows=range(1, start_row + 1) if start_row else None,
    )
    try:
        chunk = next(reader)
    except StopIteration:
        print(f"[S3->KAFKA] No rows remaining for {source_name}; offset={start_row:,}")
        return

    for index, row in chunk.iterrows():
        source_row = start_row + scanned
        scanned += 1
        next_row = source_row + 1
        event = build_event(row, source_row)
        event["producer_run_id"] = producer_run_id
        event["producer_row_number"] = int(source_row)
        error = validation_error(event)
        if error:
            dlq_event = {
                "domain": "encounters",
                "source_topic": topic,
                "error_reason": error,
                "raw_payload": row.to_dict(),
                "producer_run_id": producer_run_id,
                "producer_row_number": int(source_row),
                "dlq_published_at": datetime.now(timezone.utc).isoformat(),
            }
            producer.produce(
                topic=dlq_topic,
                key=event["patient_id"] or str(source_row),
                value=json.dumps(dlq_event),
            )
            rejected += 1
            continue
        producer.produce(
            topic=topic,
            key=event["patient_id"],
            value=json.dumps(event),
        )
        sent += 1
        producer.poll(0)

    producer.flush()
    write_offset(spark, offset_table, source_name, next_row)
    print(
        f"[S3->KAFKA] source={source_name} topic={topic} "
        f"start_row={start_row:,} next_row={next_row:,} "
        f"scanned={scanned:,} sent={sent:,} dlq={rejected:,} "
        f"producer_run_id={producer_run_id}"
    )


def main():
    parser = argparse.ArgumentParser(description="Publish the next S3 encounter batch to Kafka.")
    parser.add_argument("--source", default=DEFAULT_SOURCE)
    parser.add_argument("--max-rows", type=int, default=5000)
    parser.add_argument("--source-name", default="s3_encounters")
    parser.add_argument("--offset-table", default=DEFAULT_OFFSET_TABLE)
    parser.add_argument("--topic", default=DEFAULT_TOPIC)
    parser.add_argument("--dlq-topic", default=DEFAULT_DLQ_TOPIC)
    args = parser.parse_args()
    publish(
        args.source,
        args.max_rows,
        args.source_name,
        args.offset_table,
        args.topic,
        args.dlq_topic,
    )


if __name__ == "__main__":
    main()
