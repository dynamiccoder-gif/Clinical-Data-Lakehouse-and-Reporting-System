from pyspark.sql import SparkSession
from pyspark.sql import functions as F


spark = SparkSession.builder.getOrCreate()

SILVER_TABLE = "workspace.default.silver_kafka_encounters"
QUARANTINE_TABLE = "workspace.default.quarantine_kafka_encounters"
GOLD_SUMMARY_TABLE = "workspace.default.gold_kafka_encounters_summary"
GOLD_TYPE_TABLE = "workspace.default.gold_kafka_encounters_by_type"
GOLD_PATIENT_TABLE = "workspace.default.gold_kafka_encounters_by_patient"
AUDIT_TABLE = "workspace.default.batch_audit"

silver_df = spark.table(SILVER_TABLE)

summary_df = (
    silver_df
    .agg(
        F.count("*").alias("total_encounters"),
        F.sum(F.when(F.col("is_emergency"), 1).otherwise(0)).alias("emergency_encounters"),
        F.sum(F.when(F.col("is_long_stay"), 1).otherwise(0)).alias("long_stay_encounters"),
        F.sum(F.when(F.col("encounter_risk_flag"), 1).otherwise(0)).alias("risk_flagged_encounters"),
        F.countDistinct("patient_id").alias("unique_patients"),
        F.round(F.avg("length_of_stay_hours"), 2).alias("avg_length_of_stay_hours"),
        F.max("event_timestamp").alias("latest_event_time"),
    )
    .withColumn(
        "risk_flag_rate_pct",
        F.round(
            F.when(
                F.col("total_encounters") > 0,
                (F.col("risk_flagged_encounters") / F.col("total_encounters")) * 100,
            ).otherwise(F.lit(0.0)),
            2,
        ),
    )
    .withColumn("gold_processed_at", F.current_timestamp())
)

type_df = (
    silver_df
    .groupBy("encounter_type")
    .agg(
        F.count("*").alias("encounters"),
        F.sum(F.when(F.col("is_emergency"), 1).otherwise(0)).alias("emergency_encounters"),
        F.sum(F.when(F.col("is_long_stay"), 1).otherwise(0)).alias("long_stay_encounters"),
        F.sum(F.when(F.col("encounter_risk_flag"), 1).otherwise(0)).alias("risk_flagged_encounters"),
        F.round(F.avg("length_of_stay_hours"), 2).alias("avg_length_of_stay_hours"),
        F.countDistinct("patient_id").alias("unique_patients"),
    )
    .withColumn(
        "risk_flag_rate_pct",
        F.round(
            F.when(
                F.col("encounters") > 0,
                (F.col("risk_flagged_encounters") / F.col("encounters")) * 100,
            ).otherwise(F.lit(0.0)),
            2,
        ),
    )
    .withColumn("gold_processed_at", F.current_timestamp())
)

patient_df = (
    silver_df
    .groupBy("patient_id")
    .agg(
        F.count("*").alias("encounters"),
        F.sum(F.when(F.col("is_emergency"), 1).otherwise(0)).alias("emergency_encounters"),
        F.sum(F.when(F.col("is_long_stay"), 1).otherwise(0)).alias("long_stay_encounters"),
        F.sum(F.when(F.col("encounter_risk_flag"), 1).otherwise(0)).alias("risk_flagged_encounters"),
        F.round(F.avg("length_of_stay_hours"), 2).alias("avg_length_of_stay_hours"),
        F.max("event_timestamp").alias("latest_event_time"),
    )
    .withColumn("has_emergency_encounter", F.col("emergency_encounters") > 0)
    .withColumn("has_long_stay", F.col("long_stay_encounters") > 0)
    .withColumn("has_encounter_risk_flag", F.col("risk_flagged_encounters") > 0)
    .withColumn(
        "risk_flag_rate_pct",
        F.round(
            F.when(
                F.col("encounters") > 0,
                (F.col("risk_flagged_encounters") / F.col("encounters")) * 100,
            ).otherwise(F.lit(0.0)),
            2,
        ),
    )
    .withColumn("gold_processed_at", F.current_timestamp())
)

(
    summary_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(GOLD_SUMMARY_TABLE)
)

(
    type_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(GOLD_TYPE_TABLE)
)

(
    patient_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(GOLD_PATIENT_TABLE)
)

silver_count = silver_df.count()
quarantine_count = spark.table(QUARANTINE_TABLE).count()
source_count = silver_count + quarantine_count
pass_rate = round((silver_count / source_count) * 100, 2) if source_count else 0.0

existing_batch = spark.table(AUDIT_TABLE).agg(F.max("batch_id").alias("batch_id")).collect()
next_batch_id = int(existing_batch[0]["batch_id"] or 0) + 1

audit_df = spark.createDataFrame(
    [
        {
            "batch_id": next_batch_id,
            "execution_timestamp": None,
            "pass_rate_pct": float(pass_rate),
            "pipeline_name": "Kafka-Encounter-Stream",
            "quarantine_records_written": int(quarantine_count),
            "silver_records_written": int(silver_count),
            "source_records_read": int(source_count),
            "status": "SUCCESS" if quarantine_count == 0 else "ANOMALIES_ISOLATED",
            "duplicate_records_skipped": 0,
            "duration_seconds": 0.0,
            "input_file_count": 0,
            "reconciliation_status": "PASS",
            "records_inserted": int(silver_count),
            "records_updated": 0,
            "throughput_rows_per_second": 0.0,
            "batch_type": "KAFKA_ENCOUNTERS",
            "hard_failure_records": int(quarantine_count),
            "validation_pass_rate_pct": float(pass_rate),
            "warning_rate_pct": 0.0,
            "warning_records": 0,
        }
    ]
).withColumn("execution_timestamp", F.current_timestamp())

(
    audit_df.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(AUDIT_TABLE)
)

print(f"Gold encounter summary rows: {summary_df.count()}")
print(f"Gold encounter type rows: {type_df.count()}")
print(f"Gold encounter patient rows: {patient_df.count()}")
print(f"Kafka encounter audit batch_id: {next_batch_id}")
print(f"Created table: {GOLD_SUMMARY_TABLE}")
print(f"Created table: {GOLD_TYPE_TABLE}")
print(f"Created table: {GOLD_PATIENT_TABLE}")
