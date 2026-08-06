from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
)


bootstrap_server = dbutils.secrets.get(
    scope="clinical360-kafka",
    key="bootstrap-server",
)
bootstrap_server = bootstrap_server.strip()

if "://" in bootstrap_server:
    raise ValueError(
        "bootstrap-server must be HOST:PORT only, without a protocol"
    )

if ":" not in bootstrap_server:
    raise ValueError(
        "bootstrap-server must contain both hostname and port"
    )

print(
    "Kafka bootstrap server loaded:",
    bootstrap_server.split(":")[0],
    "port:",
    bootstrap_server.rsplit(":", 1)[1],
)

username = dbutils.secrets.get(
    scope="clinical360-kafka",
    key="username",
)

password = dbutils.secrets.get(
    scope="clinical360-kafka",
    key="password",
)

ca_path = (
    "/Volumes/workspace/default/"
    "healthcare_lakehouse/kafka/ca.pem"
)

encounters_schema = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("encounter_id", StringType(), False),
        StructField("patient_id", StringType(), False),
        StructField("event_type", StringType(), False),
        StructField("event_timestamp", StringType(), False),
        StructField("encounter_type", StringType(), True),
        StructField("diagnosis_code", StringType(), True),
        StructField("diagnosis_description", StringType(), True),
        StructField("facility", StringType(), True),
        StructField("provider_id", StringType(), True),
        StructField("length_of_stay_hours", DoubleType(), True),
        StructField("source", StringType(), True),
    ]
)

jaas_config = (
    "kafkashaded.org.apache.kafka.common.security.scram."
    "ScramLoginModule required "
    f'username="{username}" '
    f'password="{password}";'
)

raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", bootstrap_server)
    .option("subscribe", "clinical-encounters")
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", 5000)
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.mechanism", "SCRAM-SHA-256")
    .option("kafka.sasl.jaas.config", jaas_config)
    .option("kafka.ssl.truststore.type", "PEM")
    .option("kafka.ssl.truststore.location", ca_path)
    .option("failOnDataLoss", "false")
    .load()
)

bronze_df = (
    raw_df
    .select(
        F.col("key").cast("string").alias("message_key"),
        F.col("value").cast("string").alias("raw_payload"),
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
    )
    .withColumn(
        "parsed",
        F.from_json("raw_payload", encounters_schema),
    )
    .select(
        "*",
        "parsed.*",
    )
    .drop("parsed")
    .withColumn(
        "event_timestamp",
        F.to_timestamp("event_timestamp"),
    )
    .withColumn(
        "ingestion_timestamp",
        F.current_timestamp(),
    )
)

query = (
    bronze_df.writeStream
    .format("delta")
    .option(
        "checkpointLocation",
        "/Volumes/workspace/default/"
        "healthcare_lakehouse/checkpoints/kafka/encounters_v2",
    )
    .trigger(availableNow=True)
    .toTable("workspace.default.bronze_kafka_encounters")
)

query.awaitTermination()
