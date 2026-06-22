from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder.getOrCreate()

BRONZE_TABLE = "workspace.default.bronze_kafka_vitals"
SILVER_TABLE = "workspace.default.silver_kafka_vitals"
QUARANTINE_TABLE = "workspace.default.quarantine_kafka_vitals"
DLQ_TOPIC = "clinical-dlq"


def kafka_options():
    bootstrap_server = dbutils.secrets.get(
        scope="clinical360-kafka",
        key="bootstrap-server",
    ).strip()
    username = dbutils.secrets.get(
        scope="clinical360-kafka",
        key="username",
    )
    password = dbutils.secrets.get(
        scope="clinical360-kafka",
        key="password",
    )
    jaas_config = (
        "kafkashaded.org.apache.kafka.common.security.scram."
        "ScramLoginModule required "
        f'username="{username}" '
        f'password="{password}";'
    )
    return {
        "kafka.bootstrap.servers": bootstrap_server,
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.mechanism": "SCRAM-SHA-256",
        "kafka.sasl.jaas.config": jaas_config,
        "kafka.ssl.truststore.type": "PEM",
        "kafka.ssl.truststore.location": "/Volumes/workspace/default/healthcare_lakehouse/kafka/ca.pem",
        "topic": DLQ_TOPIC,
    }

bronze_df = spark.table(BRONZE_TABLE)

# Keep the latest Kafka record for each event_id.
dedup_window = (
    Window
    .partitionBy("event_id")
    .orderBy(
        F.col("kafka_timestamp").desc(),
        F.col("kafka_offset").desc()
    )
)

deduplicated_df = (
    bronze_df
    .withColumn("row_number", F.row_number().over(dedup_window))
    .filter(F.col("row_number") == 1)
    .drop("row_number")
)

validated_df = (
    deduplicated_df
    .withColumn(
        "validation_error",
        F.when(
            F.col("event_id").isNull() |
            (F.trim(F.col("event_id")) == ""),
            F.lit("MISSING_EVENT_ID")
        )
        .when(
            F.col("patient_id").isNull() |
            (F.trim(F.col("patient_id")) == ""),
            F.lit("MISSING_PATIENT_ID")
        )
        .when(
            F.col("event_timestamp").isNull(),
            F.lit("INVALID_EVENT_TIMESTAMP")
        )
        .when(
            F.col("event_timestamp") > F.current_timestamp(),
            F.lit("FUTURE_EVENT_TIMESTAMP")
        )
        .when(
            F.col("heart_rate").isNotNull() &
            ~F.col("heart_rate").between(20, 250),
            F.lit("INVALID_HEART_RATE")
        )
        .when(
            F.col("systolic_bp").isNotNull() &
            ~F.col("systolic_bp").between(50, 260),
            F.lit("INVALID_SYSTOLIC_BP")
        )
        .when(
            F.col("diastolic_bp").isNotNull() &
            ~F.col("diastolic_bp").between(30, 180),
            F.lit("INVALID_DIASTOLIC_BP")
        )
        .when(
            F.col("temperature").isNotNull() &
            ~F.col("temperature").between(30, 45),
            F.lit("INVALID_TEMPERATURE")
        )
    )
    .withColumn(
        "abnormal_heart_rate",
        F.when(
            F.col("heart_rate").isNotNull(),
            ~F.col("heart_rate").between(60, 100)
        ).otherwise(F.lit(False))
    )
    .withColumn(
        "abnormal_blood_pressure",
        F.when(
            F.col("systolic_bp").isNotNull() &
            F.col("diastolic_bp").isNotNull(),
            (F.col("systolic_bp") < 90) |
            (F.col("systolic_bp") > 140) |
            (F.col("diastolic_bp") < 60) |
            (F.col("diastolic_bp") > 90)
        ).otherwise(F.lit(False))
    )
    .withColumn(
        "abnormal_temperature",
        F.when(
            F.col("temperature").isNotNull(),
            ~F.col("temperature").between(36.0, 37.5)
        ).otherwise(F.lit(False))
    )
    .withColumn(
        "has_abnormal_vitals",
        F.col("abnormal_heart_rate") |
        F.col("abnormal_blood_pressure") |
        F.col("abnormal_temperature")
    )
    .withColumn("silver_processed_at", F.current_timestamp())
)

valid_df = (
    validated_df
    .filter(F.col("validation_error").isNull())
    .drop("validation_error")
)

quarantine_df = (
    validated_df
    .filter(F.col("validation_error").isNotNull())
    .withColumnRenamed("validation_error", "quarantine_reason")
    .withColumn("quarantined_at", F.current_timestamp())
)

(
    valid_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_TABLE)
)

(
    quarantine_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(QUARANTINE_TABLE)
)

quarantine_count = quarantine_df.count()
if quarantine_count:
    dlq_df = quarantine_df.select(
        F.col("patient_id").cast("string").alias("key"),
        F.to_json(
            F.struct(
                F.lit("clinical-vitals").alias("source_topic"),
                F.lit("vitals").alias("domain"),
                F.col("quarantine_reason").alias("error_reason"),
                F.col("raw_payload"),
                F.col("kafka_topic"),
                F.col("kafka_partition"),
                F.col("kafka_offset"),
                F.col("kafka_timestamp").cast("string").alias("kafka_timestamp"),
                F.col("quarantined_at").cast("string").alias("dlq_published_at"),
            )
        ).alias("value"),
    )
    writer = dlq_df.write.format("kafka")
    for option, value in kafka_options().items():
        writer = writer.option(option, value)
    writer.save()
    print(f"Published vitals DLQ records: {quarantine_count}")

print(f"Silver records: {valid_df.count()}")
print(f"Quarantined records: {quarantine_count}")
print(f"Created table: {SILVER_TABLE}")
print(f"Created table: {QUARANTINE_TABLE}")
