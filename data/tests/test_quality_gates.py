import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

from src.transformations.quality_gates import apply_clinical_quality_rules


@pytest.fixture(scope="session")
def spark():
    session = SparkSession.builder.master("local[1]").appName("quality-gate-tests").getOrCreate()
    yield session
    session.stop()


def test_quality_gate_marks_clean_rows(spark):
    df = spark.createDataFrame(
        [("enc-1", "pat-1", "2026-01-01", "2026-01-02", "A123", 28.0, 120.0, 80.0, "M", "white", 2, 1, 1)],
        [
            "encounter_id",
            "patient_id",
            "admission_date",
            "discharge_date",
            "diagnosis_code",
            "bmi",
            "systolic_bp",
            "diastolic_bp",
            "gender",
            "race",
            "unique_medications",
            "condition_count",
            "observation_count",
        ],
    ).selectExpr(
        "encounter_id",
        "patient_id",
        "to_timestamp(admission_date) as admission_date",
        "to_timestamp(discharge_date) as discharge_date",
        "diagnosis_code",
        "bmi",
        "systolic_bp",
        "diastolic_bp",
        "gender",
        "race",
        "unique_medications",
        "condition_count",
        "observation_count",
    )

    result = apply_clinical_quality_rules(df, {}).collect()[0]

    assert result["_failure_reasons"] == ""
    assert result["_warning_reasons"] == ""


def test_quality_gate_collects_failure_reasons(spark):
    schema = StructType([
        StructField("encounter_id", StringType(), True),
        StructField("patient_id", StringType(), True),
        StructField("admission_date", StringType(), True),
        StructField("discharge_date", StringType(), True),
        StructField("diagnosis_code", StringType(), True),
        StructField("bmi", DoubleType(), True),
        StructField("systolic_bp", DoubleType(), True),
        StructField("diastolic_bp", DoubleType(), True),
        StructField("gender", StringType(), True),
        StructField("race", StringType(), True),
        StructField("unique_medications", IntegerType(), True),
        StructField("condition_count", IntegerType(), True),
        StructField("observation_count", IntegerType(), True),
    ])
    df = spark.createDataFrame(
        [("", "", "2026-01-03", "2026-01-02", "", None, None, None, "UNKNOWN", "UNKNOWN", 0, 0, 0)],
        schema,
    ).selectExpr(
        "encounter_id",
        "patient_id",
        "to_timestamp(admission_date) as admission_date",
        "to_timestamp(discharge_date) as discharge_date",
        "diagnosis_code",
        "bmi",
        "systolic_bp",
        "diastolic_bp",
        "gender",
        "race",
        "unique_medications",
        "condition_count",
        "observation_count",
    )

    result = apply_clinical_quality_rules(df, {}).collect()[0]

    assert "ERR_MISSING_ENCOUNTER_ID" in result["_failure_reasons"]
    assert "ERR_MISSING_PATIENT_ID" in result["_failure_reasons"]
    assert "ERR_CHRONO_FLOW" in result["_failure_reasons"]
    assert "ERR_MISSING_ICD10" in result["_failure_reasons"]
    assert "WARN_MISSING_BMI" in result["_warning_reasons"]
    assert "WARN_MISSING_BP" in result["_warning_reasons"]
    assert "WARN_MISSING_DEMOGRAPHICS" in result["_warning_reasons"]
