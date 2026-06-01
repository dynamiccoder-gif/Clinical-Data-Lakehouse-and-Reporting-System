import argparse
import inspect
import os
import sys
from datetime import datetime
from pathlib import Path

import yaml
from pyspark.sql.functions import (
    array_join,
    col,
    collect_set,
    count,
    countDistinct,
    current_timestamp,
    datediff,
    expr,
    lit,
    lower,
    max as spark_max,
    sum as spark_sum,
    trim,
    when,
)
from pyspark.sql.types import StructField, StringType, StructType

CURRENT_FILE = Path(globals().get("__file__") or inspect.getfile(inspect.currentframe())).resolve()
sys.path.insert(0, str(CURRENT_FILE.parents[1]))

from src.utils.spark_manager import get_optimized_spark_session
from src.transformations.clinical_rules import (
    ANTIMICROBIAL_PATTERN,
    CHRONIC_CAREPLAN_PATTERN,
    HIGH_RISK_MEDICATION_PATTERN,
    MAJOR_PROCEDURE_PATTERN,
    OBSERVATION_CODES,
    PREVENTIVE_IMMUNIZATION_PATTERN,
    SEVERE_ALLERGY_PATTERN,
)


def read_csv_uppercase(spark, path, mode="PERMISSIVE", schema=None):
    if not os.path.exists(path):
        return None

    reader = spark.read.option("header", "true").option("mode", mode)
    if schema is not None:
        reader = reader.schema(schema)

    df = reader.csv(path)
    for column in df.columns:
        df = df.withColumnRenamed(column, column.upper())
    return df


def patient_schema():
    return StructType([
        StructField("ID", StringType(), True),
        StructField("BIRTHDATE", StringType(), True),
        StructField("DEATHDATE", StringType(), True),
        StructField("SSN", StringType(), True),
        StructField("DRIVERS", StringType(), True),
        StructField("PASSPORT", StringType(), True),
        StructField("PREFIX", StringType(), True),
        StructField("FIRST", StringType(), True),
        StructField("LAST", StringType(), True),
        StructField("SUFFIX", StringType(), True),
        StructField("MAIDEN", StringType(), True),
        StructField("MARITAL", StringType(), True),
        StructField("RACE", StringType(), True),
        StructField("ETHNICITY", StringType(), True),
        StructField("GENDER", StringType(), True),
        StructField("BIRTHPLACE", StringType(), True),
        StructField("ADDRESS", StringType(), True),
        StructField("_corrupt_record", StringType(), True),
    ])


def write_delta(df, path):
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)
    print(f"[FEATURES] Wrote {path}")


def build_completeness_scorecard(spark, paths):
    table_specs = [
        ("patients", paths["raw_patients"], ["ID", "BIRTHDATE"], "BIRTHDATE", patient_schema()),
        ("encounters", paths["source_encounters"], ["ID", "PATIENT", "DATE"], "DATE", None),
        ("conditions", paths["raw_conditions"], ["PATIENT", "ENCOUNTER", "DESCRIPTION", "START"], "START", None),
        ("medications", paths["raw_medications"], ["PATIENT", "DESCRIPTION", "START"], "START", None),
        ("observations", paths["raw_observations"], ["PATIENT", "ENCOUNTER", "DESCRIPTION", "DATE"], "DATE", None),
        ("procedures", paths["raw_procedures"], ["PATIENT", "ENCOUNTER", "DESCRIPTION", "DATE"], "DATE", None),
        ("careplans", paths["raw_careplans"], ["PATIENT", "ENCOUNTER", "DESCRIPTION", "START"], "START", None),
        ("allergies", paths["raw_allergies"], ["PATIENT", "DESCRIPTION", "START"], "START", None),
        ("immunizations", paths["raw_immunizations"], ["PATIENT", "DESCRIPTION", "DATE"], "DATE", None),
    ]
    scorecard_rows = []
    for table_name, path, required_columns, date_column, schema in table_specs:
        raw_df = read_csv_uppercase(spark, path, schema=schema)
        if raw_df is None:
            continue
        required_present = lit(True)
        for column_name in required_columns:
            required_present = required_present & col(column_name).isNotNull() & (trim(col(column_name)) != "")
        date_valid = expr(f"try_cast({date_column} as date)").isNotNull()
        if "_CORRUPT_RECORD" in raw_df.columns:
            required_present = required_present & col("_CORRUPT_RECORD").isNull()
        metrics = raw_df.agg(
            count("*").alias("total_records"),
            spark_sum(when(required_present, 1).otherwise(0)).alias("complete_key_records"),
            spark_sum(when(date_valid, 1).otherwise(0)).alias("valid_date_records"),
        ).collect()[0]
        total_records = int(metrics["total_records"] or 0)
        complete_key_records = int(metrics["complete_key_records"] or 0)
        valid_date_records = int(metrics["valid_date_records"] or 0)
        valid_records = min(complete_key_records, valid_date_records)
        scorecard_rows.append((
            table_name,
            total_records,
            complete_key_records,
            valid_date_records,
            round((complete_key_records / total_records) * 100, 2) if total_records else 0.0,
            round((valid_date_records / total_records) * 100, 2) if total_records else 0.0,
            round((valid_records / total_records) * 100, 2) if total_records else 0.0,
        ))
    scorecard = spark.createDataFrame(
        scorecard_rows,
        "table_name string, total_records long, complete_key_records long, valid_date_records long, "
        "completeness_pct double, date_validity_pct double, trust_score_pct double",
    )
    write_delta(scorecard, paths["gold_completeness_scorecard"])


def build_features(spark, config):
    paths = config["paths"]
    feature_paths = paths.get("features", {})
    if not feature_paths:
        raise ValueError("Missing paths.features section in config.yaml")

    raw_obs = read_csv_uppercase(spark, paths["raw_observations"])
    if raw_obs is not None:
        obs_features = raw_obs.groupBy(col("ENCOUNTER").alias("encounter_id")).agg(
            count("*").alias("observation_count"),
            *[
                spark_max(when(col("CODE") == code, col("VALUE").cast("double"))).alias(name)
                for name, code in OBSERVATION_CODES.items()
            ],
        ).withColumn(
            "abnormal_vital_count",
            when(col("bmi") >= 30, lit(1)).otherwise(lit(0))
            + when(col("systolic_bp") >= 140, lit(1)).otherwise(lit(0))
            + when(col("diastolic_bp") >= 90, lit(1)).otherwise(lit(0))
        )
        write_delta(obs_features, feature_paths["observations_by_encounter"])

    raw_pat = (
    spark.read.option("header", "true")
    .option("mode", "PERMISSIVE")
    .option("columnNameOfCorruptRecord", "_corrupt_record")
    .schema(patient_schema())
    .csv(paths["raw_patients"])
)

    corrupt_patients = raw_pat.filter(col("_corrupt_record").isNotNull())
    if corrupt_patients.count() > 0:
        corrupt_patients.withColumn("quarantine_timestamp", current_timestamp()) \
            .write.format("delta").mode("append").option("mergeSchema", "true").save(paths.get("patient_quarantine", "lakehouse/quarantine/patients"))
    patient_features = raw_pat.filter(col("_corrupt_record").isNull()).select(
        col("ID").alias("patient_id"),
        col("GENDER").alias("gender"),
        col("RACE").alias("race"),
        expr("try_cast(BIRTHDATE as date)").alias("birthdate"),
        expr("try_cast(DEATHDATE as date)").alias("deathdate"),
        col("SSN").alias("ssn"),
    ).withColumn(
        "birthdate",
        when(col("birthdate").isNotNull(), col("birthdate")).otherwise(expr("to_date('1980-01-01')"))
    ).dropDuplicates(["patient_id"])
    write_delta(patient_features, feature_paths["patients"])

    raw_cond = read_csv_uppercase(spark, paths["raw_conditions"])
    if raw_cond is not None:
        cond_detail = raw_cond.select(
            col("ENCOUNTER").alias("encounter_id"),
            col("DESCRIPTION").alias("condition_name"),
            expr("try_cast(START as date)").alias("condition_start"),
            expr("try_cast(STOP as date)").alias("condition_stop"),
            datediff(expr("try_cast(STOP as date)"), expr("try_cast(START as date)")).alias("condition_duration_days"),
        ).filter(col("encounter_id").isNotNull())
        condition_features = cond_detail.groupBy("encounter_id").agg(
            array_join(collect_set("condition_name"), " | ").alias("condition_names"),
            spark_max("condition_name").alias("primary_condition_name"),
            count("*").alias("condition_count"),
            spark_sum(when(col("condition_stop").isNull(), lit(1)).otherwise(lit(0))).alias("chronic_condition_count"),
            spark_max("condition_duration_days").alias("max_condition_duration_days"),
        ).withColumn(
            "los_days",
            expr("greatest(1, least(coalesce(cast(max_condition_duration_days as int), 1), 30))")
        )
        write_delta(condition_features, feature_paths["conditions_by_encounter"])

        patient_conditions = raw_cond.select(
            col("PATIENT").alias("patient_id"),
            trim(col("DESCRIPTION")).alias("condition_name"),
            expr("try_cast(START as date)").alias("condition_start"),
        ).filter(
            col("patient_id").isNotNull()
            & col("condition_name").isNotNull()
            & (col("condition_name") != "")
        ).groupBy("patient_id", "condition_name").agg(
            expr("min(condition_start)").alias("condition_start")
        )
        write_delta(patient_conditions, feature_paths["conditions_by_patient"])

    raw_med = read_csv_uppercase(spark, paths["raw_medications"])
    if raw_med is not None:
        raw_encounters = read_csv_uppercase(spark, paths["source_encounters"])
        encounter_dates = raw_encounters.select(
            col("ID").alias("encounter_id"),
            col("PATIENT").alias("patient_id"),
            expr("try_cast(DATE as date)").alias("encounter_date"),
        )
        medication_windows = raw_med.select(
            col("PATIENT").alias("med_patient_id"),
            col("CODE").alias("medication_code"),
            col("DESCRIPTION").alias("medication_name"),
            expr("try_cast(START as date)").alias("medication_start"),
            expr("try_cast(STOP as date)").alias("medication_stop"),
        )
        active_medications = encounter_dates.join(
            medication_windows,
            (col("patient_id") == col("med_patient_id"))
            & (col("medication_start") <= col("encounter_date"))
            & (col("medication_stop").isNull() | (col("medication_stop") >= col("encounter_date"))),
            "left",
        )
        medication_features = active_medications.groupBy("encounter_id").agg(
            count("medication_code").alias("medication_events"),
            countDistinct("medication_code").alias("unique_medications"),
            countDistinct(when(lower(col("medication_name")).rlike(HIGH_RISK_MEDICATION_PATTERN), col("medication_code"))).alias("high_risk_med_count"),
            countDistinct(when(lower(col("medication_name")).rlike(ANTIMICROBIAL_PATTERN), col("medication_code"))).alias("antimicrobial_count"),
            spark_max(datediff(col("medication_stop"), col("medication_start"))).alias("max_medication_days"),
        )
        write_delta(medication_features, feature_paths["medications_by_encounter"])

    raw_proc = read_csv_uppercase(spark, paths.get("raw_procedures", ""))
    if raw_proc is not None:
        procedure_features = raw_proc.groupBy(col("ENCOUNTER").alias("encounter_id")).agg(
            count("*").alias("procedure_count"),
            countDistinct("CODE").alias("unique_procedures"),
            spark_sum(when(lower(col("DESCRIPTION")).rlike(MAJOR_PROCEDURE_PATTERN), lit(1)).otherwise(lit(0))).alias("major_procedure_count"),
        )
        write_delta(procedure_features, feature_paths["procedures_by_encounter"])

    raw_careplan = read_csv_uppercase(spark, paths.get("raw_careplans", ""))
    if raw_careplan is not None:
        careplan_features = raw_careplan.groupBy(col("ENCOUNTER").alias("encounter_id")).agg(
            count("*").alias("careplan_count"),
            countDistinct("CODE").alias("unique_careplans"),
            spark_sum(when(lower(col("DESCRIPTION")).rlike(CHRONIC_CAREPLAN_PATTERN), lit(1)).otherwise(lit(0))).alias("chronic_careplan_count"),
        )
        write_delta(careplan_features, feature_paths["careplans_by_encounter"])

    raw_allergy = read_csv_uppercase(spark, paths.get("raw_allergies", ""))
    if raw_allergy is not None:
        allergy_features = raw_allergy.groupBy(col("PATIENT").alias("patient_id")).agg(
            countDistinct("CODE").alias("allergy_count"),
            spark_sum(when(lower(col("DESCRIPTION")).rlike(SEVERE_ALLERGY_PATTERN), lit(1)).otherwise(lit(0))).alias("severe_allergy_count"),
        )
        write_delta(allergy_features, feature_paths["allergies_by_patient"])

    raw_immunization = read_csv_uppercase(spark, paths.get("raw_immunizations", ""))
    if raw_immunization is not None:
        immunization_features = raw_immunization.groupBy(col("PATIENT").alias("patient_id")).agg(
            countDistinct("CODE").alias("immunization_count"),
            spark_sum(when(lower(col("DESCRIPTION")).rlike(PREVENTIVE_IMMUNIZATION_PATTERN), lit(1)).otherwise(lit(0))).alias("preventive_immunization_count"),
        )
        write_delta(immunization_features, feature_paths["immunizations_by_patient"])

    build_completeness_scorecard(spark, paths)
    print(f"[FEATURES] Clinical 360 feature preparation finished at {datetime.now().isoformat()}")


def main():
    parser = argparse.ArgumentParser(description="Prepare reusable Clinical 360 Delta feature tables from raw Synthea files.")
    parser.add_argument("--config", default="config.yaml")
    args = parser.parse_args()

    with open(args.config, "r", encoding="utf-8") as file:
        config = yaml.safe_load(file)

    spark = get_optimized_spark_session(config["spark"])
    build_features(spark, config)


if __name__ == "__main__":
    main()
