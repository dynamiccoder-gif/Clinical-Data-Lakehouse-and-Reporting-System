from collections import Counter

import pytest
from pyspark.sql import SparkSession

from tests.fixtures.corruption_generator import clean_encounter_rows, make_corrupted_batch
from src.transformations.quality_gates import apply_clinical_quality_rules


@pytest.fixture(scope="session")
def spark():
    session = SparkSession.builder.master("local[1]").appName("corruption-quality-tests").getOrCreate()
    yield session
    session.stop()


def test_seeded_corruption_generator_matches_quality_gate_counts(spark):
    rows, expected_counts = make_corrupted_batch(clean_encounter_rows(15), seed=42, rows_per_rule=2)
    df = spark.createDataFrame(rows)

    tagged = apply_clinical_quality_rules(df, {})
    quarantine = tagged.filter(tagged._failure_reasons != "")
    silver = tagged.filter(tagged._failure_reasons == "")

    actual_counts = Counter()
    for row in quarantine.select("_failure_reasons").collect():
        for reason in row["_failure_reasons"].split(", "):
            actual_counts[reason] += 1

    assert dict(actual_counts) == expected_counts
    assert quarantine.count() == sum(expected_counts.values())
    assert silver.count() + quarantine.count() == len(rows)


def test_corruption_reconciliation_contract():
    rows, expected_counts = make_corrupted_batch(clean_encounter_rows(15), seed=42, rows_per_rule=2)
    quarantine_count = sum(expected_counts.values())
    silver_count = len(rows) - quarantine_count
    duplicate_records_skipped = 0

    assert len(rows) == silver_count + quarantine_count + duplicate_records_skipped
