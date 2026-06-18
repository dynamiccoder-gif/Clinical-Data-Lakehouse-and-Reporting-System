import argparse
import os

import yaml
from pyspark.sql.functions import array, coalesce, col, lit, row_number, sha2, to_json
from pyspark.sql.window import Window

from src.utils.spark_manager import get_optimized_spark_session


def load_config(path):
    with open(path, "r", encoding="utf-8") as file:
        return yaml.safe_load(file)


def configured_natural_key_fields(domain_config):
    fields = domain_config.get("natural_key_fields", [])
    if not fields:
        raise ValueError("Kafka domain is missing natural_key_fields")
    return fields


def latest_by_natural_key(source, key_fields):
    natural_key_values = [coalesce(col("payload").getItem(field), lit("")) for field in key_fields]
    return source.withColumn(
        "record_hash",
        sha2(to_json(col("payload")), 256),
    ).withColumn(
        "record_key",
        sha2(to_json(array(*natural_key_values)), 256),
    ).withColumn(
        "_latest",
        row_number().over(
            Window.partitionBy("record_key").orderBy(
                col("kafka_timestamp").desc(),
                col("kafka_partition").desc(),
                col("kafka_offset").desc(),
            )
        ),
    ).filter(
        col("_latest") == 1
    ).drop(
        "_latest",
        "raw_value",
    )


def refresh_domain(spark, bronze_root, silver_root, domain, domain_config):
    bronze_path = os.path.join(bronze_root, domain)
    if not os.path.exists(bronze_path):
        print(f"[KAFKA SILVER] skip {domain:13} no Bronze events")
        return

    source = spark.read.format("delta").load(bronze_path)
    latest = latest_by_natural_key(source, configured_natural_key_fields(domain_config))
    target = os.path.join(silver_root, domain)
    latest.coalesce(1).write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(target)
    print(f"[KAFKA SILVER] ready {domain:13} records={latest.count():,}")


def main():
    parser = argparse.ArgumentParser(description="Normalize and deduplicate Kafka Bronze clinical domains.")
    parser.add_argument("--config", default="config.yaml")
    args = parser.parse_args()

    config = load_config(args.config)
    spark = get_optimized_spark_session(config["spark"])
    kafka_config = config["kafka"]
    silver_root = kafka_config.get("silver_root", "lakehouse/silver/kafka")
    for domain, domain_config in kafka_config["domains"].items():
        refresh_domain(spark, kafka_config["bronze_root"], silver_root, domain, domain_config)
    spark.stop()


if __name__ == "__main__":
    main()
