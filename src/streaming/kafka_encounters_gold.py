from pyspark.sql import SparkSession
from pyspark.sql import functions as F


spark = SparkSession.builder.getOrCreate()

SILVER_TABLE = "workspace.default.silver_kafka_encounters"
GOLD_SUMMARY_TABLE = "workspace.default.gold_kafka_encounters_summary"
GOLD_TYPE_TABLE = "workspace.default.gold_kafka_encounters_by_type"
GOLD_PATIENT_TABLE = "workspace.default.gold_kafka_encounters_by_patient"

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

print(f"Gold encounter summary rows: {summary_df.count()}")
print(f"Gold encounter type rows: {type_df.count()}")
print(f"Gold encounter patient rows: {patient_df.count()}")
print(f"Created table: {GOLD_SUMMARY_TABLE}")
print(f"Created table: {GOLD_TYPE_TABLE}")
print(f"Created table: {GOLD_PATIENT_TABLE}")
