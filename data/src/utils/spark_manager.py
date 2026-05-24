import os

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

def get_optimized_spark_session(spark_config):
    """Turns on Apache Spark with tuned memory configurations"""
    local_dir = spark_config.get("local_dir", "lakehouse/spark-tmp")
    os.makedirs(local_dir, exist_ok=True)
    extra_packages = []
    if os.getenv("STREAMING_SOURCE", "").lower() == "kafka" and spark_config.get("kafka_package"):
        extra_packages.append(spark_config["kafka_package"])

    builder = SparkSession.builder \
        .appName(spark_config['app_name']) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true") \
        .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
        .config("spark.sql.shuffle.partitions", str(spark_config['shuffle_partitions'])) \
        .config("spark.driver.memory", spark_config['driver_memory']) \
        .config("spark.local.dir", local_dir)

    if not spark_config.get("databricks_runtime", False):
        builder = builder \
            .config("spark.hadoop.fs.defaultFS", "file:///") \
            .master(spark_config['master'])

    return configure_spark_with_delta_pip(builder, extra_packages=extra_packages).getOrCreate()
