from pyspark.sql import SparkSession
from pyspark.sql import functions as F


spark = SparkSession.builder.getOrCreate()

BRONZE = "workspace.default.bronze_kafka_vitals"
SILVER = "workspace.default.silver_kafka_vitals"
QUARANTINE = "workspace.default.quarantine_kafka_vitals"
GOLD_SUMMARY = "workspace.default.gold_kafka_vitals_summary"
GOLD_PATIENT = "workspace.default.gold_kafka_vitals_by_patient"

results = []


def add_check(name, passed, details):
    results.append((name, bool(passed), str(details)))


bronze_count = spark.table(BRONZE).count()
silver_count = spark.table(SILVER).count()
quarantine_count = spark.table(QUARANTINE).count()
gold_patient_count = spark.table(GOLD_PATIENT).count()

add_check(
    "bronze_not_empty",
    bronze_count > 0,
    f"bronze_count={bronze_count}",
)

add_check(
    "silver_plus_quarantine_not_greater_than_bronze",
    silver_count + quarantine_count <= bronze_count,
    (
        f"bronze={bronze_count}, silver={silver_count}, "
        f"quarantine={quarantine_count}"
    ),
)

duplicate_events = (
    spark.table(SILVER)
    .groupBy("event_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

add_check(
    "silver_event_id_unique",
    duplicate_events == 0,
    f"duplicate_event_ids={duplicate_events}",
)

missing_patient_ids = (
    spark.table(SILVER)
    .filter(
        F.col("patient_id").isNull() |
        (F.trim(F.col("patient_id")) == "")
    )
    .count()
)

add_check(
    "silver_patient_id_complete",
    missing_patient_ids == 0,
    f"missing_patient_ids={missing_patient_ids}",
)

invalid_heart_rates = (
    spark.table(SILVER)
    .filter(
        F.col("heart_rate").isNotNull() &
        ~F.col("heart_rate").between(20, 250)
    )
    .count()
)

add_check(
    "silver_heart_rate_valid",
    invalid_heart_rates == 0,
    f"invalid_heart_rates={invalid_heart_rates}",
)

gold_summary_count = spark.table(GOLD_SUMMARY).count()

add_check(
    "gold_summary_single_row",
    gold_summary_count == 1,
    f"gold_summary_rows={gold_summary_count}",
)

add_check(
    "gold_patient_not_empty",
    gold_patient_count > 0,
    f"gold_patient_rows={gold_patient_count}",
)

quality_df = spark.createDataFrame(
    results,
    ["check_name", "passed", "details"],
).withColumn(
    "checked_at",
    F.current_timestamp(),
)

quality_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.default.kafka_vitals_quality_results")

failed_checks = quality_df.filter(~F.col("passed")).count()

quality_df.orderBy("check_name").show(truncate=False)

if failed_checks > 0:
    raise RuntimeError(
        f"Kafka vitals quality checks failed: {failed_checks}"
    )

print("All Kafka vitals quality checks passed.")
