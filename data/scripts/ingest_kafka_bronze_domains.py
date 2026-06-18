import argparse
import os

import yaml
from pyspark.sql.functions import coalesce, col, current_timestamp, from_json, lit, struct, to_json, trim, when
from pyspark.sql.types import LongType, MapType, StringType, StructField, StructType

from src.utils.spark_manager import get_optimized_spark_session


def load_config(path):
    with open(path, "r", encoding="utf-8") as file:
        return yaml.safe_load(file)


def envelope_schema():
    return StructType([
        StructField("domain", StringType(), True),
        StructField("producer_run_id", StringType(), True),
        StructField("producer_row_number", LongType(), True),
        StructField("producer_timestamp", StringType(), True),
        StructField("payload", MapType(StringType(), StringType()), True),
    ])


def required_fields_present(domains):
    any_domain_valid = lit(False)
    for domain, domain_config in domains.items():
        domain_valid = lit(True)
        for field in domain_config.get("required_fields", []):
            value = col("event.payload").getItem(field)
            domain_valid = domain_valid & value.isNotNull() & (trim(value) != "")
        any_domain_valid = any_domain_valid | ((col("event.domain") == domain) & domain_valid)
    return coalesce(any_domain_valid, lit(False))


def validate_envelopes(batch_df, domains):
    parsed = batch_df.withColumn("event", from_json(col("raw_value"), envelope_schema()))
    known_domain = coalesce(col("event.domain").isin(list(domains)), lit(False))
    valid_payload = required_fields_present(domains)
    return parsed.withColumn(
        "_validation_error",
        when(
            col("event").isNull()
            | col("event.domain").isNull()
            | col("event.payload").isNull()
            | (~known_domain),
            lit("invalid_kafka_envelope"),
        ).when(
            ~valid_payload,
            lit("missing_required_payload_fields"),
        ).otherwise(
            lit(""),
        ),
    )


def write_bronze_batch(batch_df, batch_id, config):
    kafka_config = config["kafka"]
    bronze_root = kafka_config["bronze_root"]
    domains = kafka_config["domains"]
    validated = validate_envelopes(batch_df, domains)
    invalid = validated.filter(col("_validation_error") != "")
    invalid_count = invalid.count()
    if invalid_count:
        invalid_payload = invalid.select(
            col("kafka_key").cast("string").alias("key"),
            to_json(
                struct(
                    col("_validation_error").alias("error_reason"),
                    col("raw_value").alias("raw_value"),
                )
            ).alias("value"),
        )
        invalid_payload.write.format("kafka") \
            .option("kafka.bootstrap.servers", kafka_config["bootstrap_servers"]) \
            .option("topic", kafka_config["dlq_topic"]) \
            .save()
        invalid.withColumn("ingestion_timestamp", current_timestamp()) \
            .write.format("delta").mode("append").option("mergeSchema", "true") \
            .save(os.path.join(bronze_root, "_invalid"))

    valid = validated.filter(col("_validation_error") == "")
    for domain in domains:
        domain_df = valid.filter(col("event.domain") == domain).select(
            col("event.domain").alias("domain"),
            col("event.producer_run_id").alias("producer_run_id"),
            col("event.producer_row_number").alias("producer_row_number"),
            col("event.producer_timestamp").alias("producer_timestamp"),
            col("event.payload").alias("payload"),
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
            "kafka_key",
            "raw_value",
        ).withColumn(
            "bronze_batch_id",
            lit(int(batch_id)),
        ).withColumn(
            "ingestion_timestamp",
            current_timestamp(),
        )
        if not domain_df.isEmpty():
            domain_df.coalesce(1).write.format("delta").mode("append").option("mergeSchema", "true") \
                .save(os.path.join(bronze_root, domain))
            print(f"[KAFKA BRONZE] batch={batch_id} domain={domain}")


def main():
    parser = argparse.ArgumentParser(description="Land nine Clinical 360 Kafka domains into Bronze Delta.")
    parser.add_argument("--config", default="config.yaml")
    args = parser.parse_args()

    config = load_config(args.config)
    kafka_config = config["kafka"]
    os.environ["STREAMING_SOURCE"] = "kafka"
    spark = get_optimized_spark_session(config["spark"])
    topics = ",".join(domain_config["topic"] for domain_config in kafka_config["domains"].values())
    stream = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", kafka_config["bootstrap_servers"]) \
        .option("subscribe", topics) \
        .option("startingOffsets", kafka_config.get("starting_offsets", "earliest")) \
        .load() \
        .select(
            col("topic").alias("kafka_topic"),
            col("partition").alias("kafka_partition"),
            col("offset").alias("kafka_offset"),
            col("timestamp").alias("kafka_timestamp"),
            col("key").cast("string").alias("kafka_key"),
            col("value").cast("string").alias("raw_value"),
        )

    query = stream.writeStream.foreachBatch(lambda df, batch_id: write_bronze_batch(df, batch_id, config)) \
        .option("checkpointLocation", kafka_config["bronze_checkpoint"]) \
        .trigger(availableNow=True) \
        .start()
    query.awaitTermination()


if __name__ == "__main__":
    main()
