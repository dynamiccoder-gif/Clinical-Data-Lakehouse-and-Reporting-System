import argparse
import os

import yaml
from delta.tables import DeltaTable
from pyspark.sql.functions import (
    array_join,
    col,
    collect_set,
    count,
    countDistinct,
    datediff,
    expr,
    lit,
    lower,
    max as spark_max,
    sum as spark_sum,
    trim,
    when,
)

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


def load_config(path):
    with open(path, "r", encoding="utf-8") as file:
        return yaml.safe_load(file)


def read_domain(spark, silver_root, domain, fields):
    path = os.path.join(silver_root, domain)
    if not os.path.exists(path):
        return None
    payload = spark.read.format("delta").load(path)
    return payload.select(*[col("payload").getItem(field).alias(field) for field in fields])


def upsert_delta(df, path, keys):
    if df is None:
        return
    source = df.dropDuplicates(keys)
    if os.path.exists(path):
        condition = " AND ".join(f"target.{key} <=> source.{key}" for key in keys)
        DeltaTable.forPath(df.sparkSession, path).alias("target").merge(
            source.alias("source"),
            condition,
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    else:
        source.write.format("delta").mode("overwrite").save(path)
    print(f"[KAFKA FEATURES] upserted {path}")


def refresh_features(spark, config):
    kafka_config = config["kafka"]
    silver_root = kafka_config.get("silver_root", "lakehouse/silver/kafka")
    feature_paths = config["paths"]["features"]

    patients = read_domain(spark, silver_root, "patients", ["ID", "GENDER", "RACE", "BIRTHDATE", "DEATHDATE", "SSN"])
    if patients is not None:
        patient_features = patients.select(
            col("ID").alias("patient_id"),
            col("GENDER").alias("gender"),
            col("RACE").alias("race"),
            expr("try_cast(BIRTHDATE as date)").alias("birthdate"),
            expr("try_cast(DEATHDATE as date)").alias("deathdate"),
            col("SSN").alias("ssn"),
        ).filter(
            col("patient_id").isNotNull()
        ).withColumn(
            "birthdate",
            when(col("birthdate").isNotNull(), col("birthdate")).otherwise(expr("to_date('1980-01-01')")),
        )
        upsert_delta(patient_features, feature_paths["patients"], ["patient_id"])

    observations = read_domain(spark, silver_root, "observations", ["ENCOUNTER", "CODE", "VALUE"])
    if observations is not None:
        observation_features = observations.filter(col("ENCOUNTER").isNotNull()).groupBy(
            col("ENCOUNTER").alias("encounter_id")
        ).agg(
            count("*").alias("observation_count"),
            *[
                spark_max(when(col("CODE") == code, col("VALUE").cast("double"))).alias(name)
                for name, code in OBSERVATION_CODES.items()
            ],
        ).withColumn(
            "abnormal_vital_count",
            when(col("bmi") >= 30, lit(1)).otherwise(lit(0))
            + when(col("systolic_bp") >= 140, lit(1)).otherwise(lit(0))
            + when(col("diastolic_bp") >= 90, lit(1)).otherwise(lit(0)),
        )
        upsert_delta(observation_features, feature_paths["observations_by_encounter"], ["encounter_id"])

    conditions = read_domain(spark, silver_root, "conditions", ["PATIENT", "ENCOUNTER", "DESCRIPTION", "START", "STOP"])
    if conditions is not None:
        condition_detail = conditions.select(
            col("PATIENT").alias("patient_id"),
            col("ENCOUNTER").alias("encounter_id"),
            trim(col("DESCRIPTION")).alias("condition_name"),
            expr("try_cast(START as date)").alias("condition_start"),
            expr("try_cast(STOP as date)").alias("condition_stop"),
            datediff(expr("try_cast(STOP as date)"), expr("try_cast(START as date)")).alias("condition_duration_days"),
        ).filter(col("encounter_id").isNotNull())
        condition_features = condition_detail.groupBy("encounter_id").agg(
            array_join(collect_set("condition_name"), " | ").alias("condition_names"),
            spark_max("condition_name").alias("primary_condition_name"),
            count("*").alias("condition_count"),
            spark_sum(when(col("condition_stop").isNull(), lit(1)).otherwise(lit(0))).alias("chronic_condition_count"),
            spark_max("condition_duration_days").alias("max_condition_duration_days"),
        ).withColumn(
            "los_days",
            expr("greatest(1, least(coalesce(cast(max_condition_duration_days as int), 1), 30))"),
        )
        upsert_delta(condition_features, feature_paths["conditions_by_encounter"], ["encounter_id"])
        patient_conditions = condition_detail.filter(
            col("patient_id").isNotNull() & col("condition_name").isNotNull() & (col("condition_name") != "")
        ).groupBy("patient_id", "condition_name").agg(
            expr("min(condition_start)").alias("condition_start")
        )
        upsert_delta(patient_conditions, feature_paths["conditions_by_patient"], ["patient_id", "condition_name"])

    encounters = read_domain(spark, silver_root, "encounters", ["ID", "PATIENT", "DATE"])
    medications = read_domain(spark, silver_root, "medications", ["PATIENT", "CODE", "DESCRIPTION", "START", "STOP"])
    if encounters is not None and medications is not None:
        encounter_dates = encounters.select(
            col("ID").alias("encounter_id"),
            col("PATIENT").alias("patient_id"),
            expr("try_cast(DATE as date)").alias("encounter_date"),
        )
        medication_windows = medications.select(
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
        upsert_delta(medication_features, feature_paths["medications_by_encounter"], ["encounter_id"])

    simple_specs = [
        ("procedures", ["ENCOUNTER", "CODE", "DESCRIPTION"], "procedures_by_encounter"),
        ("careplans", ["ENCOUNTER", "CODE", "DESCRIPTION"], "careplans_by_encounter"),
        ("allergies", ["PATIENT", "CODE", "DESCRIPTION"], "allergies_by_patient"),
        ("immunizations", ["PATIENT", "CODE", "DESCRIPTION"], "immunizations_by_patient"),
    ]
    for domain, fields, feature_name in simple_specs:
        raw = read_domain(spark, silver_root, domain, fields)
        if raw is None:
            continue
        if domain == "procedures":
            features = raw.groupBy(col("ENCOUNTER").alias("encounter_id")).agg(
                count("*").alias("procedure_count"),
                countDistinct("CODE").alias("unique_procedures"),
                spark_sum(when(lower(col("DESCRIPTION")).rlike(MAJOR_PROCEDURE_PATTERN), 1).otherwise(0)).alias("major_procedure_count"),
            )
            keys = ["encounter_id"]
        elif domain == "careplans":
            features = raw.groupBy(col("ENCOUNTER").alias("encounter_id")).agg(
                count("*").alias("careplan_count"),
                countDistinct("CODE").alias("unique_careplans"),
                spark_sum(when(lower(col("DESCRIPTION")).rlike(CHRONIC_CAREPLAN_PATTERN), 1).otherwise(0)).alias("chronic_careplan_count"),
            )
            keys = ["encounter_id"]
        elif domain == "allergies":
            features = raw.groupBy(col("PATIENT").alias("patient_id")).agg(
                countDistinct("CODE").alias("allergy_count"),
                spark_sum(when(lower(col("DESCRIPTION")).rlike(SEVERE_ALLERGY_PATTERN), 1).otherwise(0)).alias("severe_allergy_count"),
            )
            keys = ["patient_id"]
        else:
            features = raw.groupBy(col("PATIENT").alias("patient_id")).agg(
                countDistinct("CODE").alias("immunization_count"),
                spark_sum(when(lower(col("DESCRIPTION")).rlike(PREVENTIVE_IMMUNIZATION_PATTERN), 1).otherwise(0)).alias("preventive_immunization_count"),
            )
            keys = ["patient_id"]
        upsert_delta(features, feature_paths[feature_name], keys)


def main():
    parser = argparse.ArgumentParser(description="Upsert Kafka Silver state into reusable Clinical 360 feature tables.")
    parser.add_argument("--config", default="config.yaml")
    args = parser.parse_args()

    config = load_config(args.config)
    spark = get_optimized_spark_session(config["spark"])
    refresh_features(spark, config)
    spark.stop()


if __name__ == "__main__":
    main()
