from pyspark.sql import SparkSession
from pyspark.sql import functions as F


spark = SparkSession.builder.getOrCreate()

SILVER_TABLE = "workspace.default.silver_kafka_vitals"
GOLD_SUMMARY_TABLE = "workspace.default.gold_kafka_vitals_summary"
GOLD_PATIENT_TABLE = "workspace.default.gold_kafka_vitals_by_patient"

silver_df = spark.table(SILVER_TABLE)

summary_df = (
    silver_df
    .agg(
        F.count("*").alias("total_vitals_events"),
        F.sum(F.when(F.col("has_abnormal_vitals"), 1).otherwise(0)).alias("abnormal_vitals_count"),
        F.max("event_timestamp").alias("latest_event_time"),
        F.round(F.avg("heart_rate"), 2).alias("avg_heart_rate"),
        F.round(F.avg("systolic_bp"), 2).alias("avg_systolic_bp"),
        F.round(F.avg("diastolic_bp"), 2).alias("avg_diastolic_bp"),
        F.round(F.avg("temperature"), 2).alias("avg_temperature"),
        F.countDistinct("patient_id").alias("patients_monitored"),
    )
    .withColumn(
        "abnormal_rate_pct",
        F.round(
            F.when(
                F.col("total_vitals_events") > 0,
                (F.col("abnormal_vitals_count") / F.col("total_vitals_events")) * 100,
            ).otherwise(F.lit(0.0)),
            2,
        ),
    )
    .withColumn("gold_processed_at", F.current_timestamp())
)

patient_summary_df = (
    silver_df
    .groupBy("patient_id")
    .agg(
        F.count("*").alias("vitals_events"),
        F.sum(F.when(F.col("has_abnormal_vitals"), 1).otherwise(0)).alias("abnormal_vitals_events"),
        F.max("event_timestamp").alias("latest_event_time"),
        F.round(F.avg("heart_rate"), 2).alias("avg_heart_rate"),
        F.round(F.avg("systolic_bp"), 2).alias("avg_systolic_bp"),
        F.round(F.avg("diastolic_bp"), 2).alias("avg_diastolic_bp"),
        F.round(F.avg("temperature"), 2).alias("avg_temperature"),
    )
    .withColumn("has_abnormal_vitals", F.col("abnormal_vitals_events") > 0)
    .withColumn(
        "abnormal_rate_pct",
        F.round(
            F.when(
                F.col("vitals_events") > 0,
                (F.col("abnormal_vitals_events") / F.col("vitals_events")) * 100,
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
    patient_summary_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(GOLD_PATIENT_TABLE)
)

print(f"Gold summary rows: {summary_df.count()}")
print(f"Gold patient rows: {patient_summary_df.count()}")
print(f"Created table: {GOLD_SUMMARY_TABLE}")
print(f"Created table: {GOLD_PATIENT_TABLE}")
