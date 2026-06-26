from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


spark = SparkSession.builder.getOrCreate()

BRONZE_TABLE = "workspace.default.bronze_kafka_encounters"
SILVER_TABLE = "workspace.default.silver_kafka_encounters"
QUARANTINE_TABLE = "workspace.default.quarantine_kafka_encounters"
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

dedup_window = (
    Window
    .partitionBy("event_id")
    .orderBy(
        F.col("kafka_timestamp").desc(),
        F.col("kafka_offset").desc(),
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
            F.lit("MISSING_EVENT_ID"),
        )
        .when(
            F.col("encounter_id").isNull() |
            (F.trim(F.col("encounter_id")) == ""),
            F.lit("MISSING_ENCOUNTER_ID"),
        )
        .when(
            F.col("patient_id").isNull() |
            (F.trim(F.col("patient_id")) == ""),
            F.lit("MISSING_PATIENT_ID"),
        )
        .when(
            F.col("encounter_type").isNull() |
            (F.trim(F.col("encounter_type")) == ""),
            F.lit("MISSING_ENCOUNTER_TYPE"),
        )
        .when(
            F.col("event_timestamp").isNull(),
            F.lit("INVALID_EVENT_TIMESTAMP"),
        )
        .when(
            F.col("event_timestamp") > F.current_timestamp(),
            F.lit("FUTURE_EVENT_TIMESTAMP"),
        )
        .when(
            F.col("length_of_stay_hours").isNotNull() &
            (F.col("length_of_stay_hours") < 0),
            F.lit("NEGATIVE_LENGTH_OF_STAY"),
        )
    )
    .withColumn(
        "is_long_stay",
        F.when(
            F.col("length_of_stay_hours").isNotNull(),
            F.col("length_of_stay_hours") >= 24,
        ).otherwise(F.lit(False)),
    )
    .withColumn(
        "is_emergency",
        F.lower(F.col("encounter_type")) == F.lit("emergency"),
    )
    .withColumn(
        "has_diagnosis",
        F.col("diagnosis_code").isNotNull() &
        (F.trim(F.col("diagnosis_code")) != ""),
    )
    .withColumn(
        "encounter_risk_flag",
        F.col("is_emergency") |
        F.col("is_long_stay"),
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
                F.lit("clinical-encounters").alias("source_topic"),
                F.lit("encounters").alias("domain"),
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
    print(f"Published encounter DLQ records: {quarantine_count}")

print(f"Silver encounter records: {valid_df.count()}")
print(f"Quarantined encounter records: {quarantine_count}")
print(f"Created table: {SILVER_TABLE}")
print(f"Created table: {QUARANTINE_TABLE}")
