import json
from datetime import datetime, timezone

import pytest
from pyspark.sql import SparkSession

from scripts.ingest_kafka_bronze_domains import validate_envelopes
from scripts.refresh_kafka_silver_domains import latest_by_natural_key


@pytest.fixture(scope="module")
def spark():
    session = SparkSession.builder.master("local[1]").appName("kafka-hardening-tests").getOrCreate()
    yield session
    session.stop()


def test_bronze_consumer_rejects_missing_required_fields_after_receipt(spark):
    domains = {
        "encounters": {
            "required_fields": ["ID", "PATIENT", "DATE"],
        },
    }
    rows = [
        ("key-1", json.dumps({"domain": "encounters", "payload": {"ID": "enc-1", "DATE": "2026-01-01"}})),
        (
            "key-2",
            json.dumps(
                {
                    "domain": "encounters",
                    "payload": {"ID": "enc-2", "PATIENT": "pat-2", "DATE": "2026-01-01"},
                }
            ),
        ),
    ]
    batch = spark.createDataFrame(rows, ["kafka_key", "raw_value"])

    errors = {
        row.kafka_key: row._validation_error
        for row in validate_envelopes(batch, domains).select("kafka_key", "_validation_error").collect()
    }

    assert errors == {
        "key-1": "missing_required_payload_fields",
        "key-2": "",
    }


def test_silver_keeps_latest_payload_for_repeated_natural_key(spark):
    rows = [
        (
            {"ID": "enc-1", "PATIENT": "pat-1", "DESCRIPTION": "old"},
            datetime(2026, 1, 1, tzinfo=timezone.utc),
            0,
            1,
            "old",
        ),
        (
            {"ID": "enc-1", "PATIENT": "pat-1", "DESCRIPTION": "corrected"},
            datetime(2026, 1, 2, tzinfo=timezone.utc),
            0,
            2,
            "corrected",
        ),
        (
            {"ID": "enc-2", "PATIENT": "pat-2", "DESCRIPTION": "second encounter"},
            datetime(2026, 1, 1, tzinfo=timezone.utc),
            0,
            3,
            "second encounter",
        ),
    ]
    source = spark.createDataFrame(rows, ["payload", "kafka_timestamp", "kafka_partition", "kafka_offset", "raw_value"])

    latest = latest_by_natural_key(source, ["ID"]).collect()
    descriptions = {row.payload["ID"]: row.payload["DESCRIPTION"] for row in latest}

    assert descriptions == {
        "enc-1": "corrected",
        "enc-2": "second encounter",
    }
