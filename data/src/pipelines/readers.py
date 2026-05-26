import os

from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import StringType, StructField, StructType


class ClinicalDataReader:
    def __init__(self, spark_session, config):
        self.spark = spark_session
        self.config = config

    def read_csv_uppercase(self, path, mode="PERMISSIVE"):
        if not os.path.exists(path):
            return None

        df = self.spark.read.option("header", "true").option("mode", mode).csv(path)
        for column in df.columns:
            df = df.withColumnRenamed(column, column.upper())
        return df

    def read_feature_table(self, name):
        feature_paths = self.config.get("paths", {}).get("features", {})
        feature_path = feature_paths.get(name)
        if not feature_path or not os.path.exists(feature_path):
            return None
        return self.spark.read.format("delta").load(feature_path)

    def read_patients_with_quarantine(self, path, quarantine_path):
        if not os.path.exists(path):
            return None

        patient_schema = StructType([
            StructField("ID", StringType(), True),
            StructField("BIRTHDATE", StringType(), True),
            StructField("DEATHDATE", StringType(), True),
            StructField("SSN", StringType(), True),
            StructField("DRIVERS", StringType(), True),
            StructField("PASSPORT", StringType(), True),
            StructField("PREFIX", StringType(), True),
            StructField("FIRST", StringType(), True),
            StructField("LAST", StringType(), True),
            StructField("SUFFIX", StringType(), True),
            StructField("MAIDEN", StringType(), True),
            StructField("MARITAL", StringType(), True),
            StructField("RACE", StringType(), True),
            StructField("ETHNICITY", StringType(), True),
            StructField("GENDER", StringType(), True),
            StructField("BIRTHPLACE", StringType(), True),
            StructField("ADDRESS", StringType(), True),
            StructField("_corrupt_record", StringType(), True),
        ])

        raw_patients = self.spark.read.option("header", "true") \
            .option("mode", "PERMISSIVE") \
            .option("columnNameOfCorruptRecord", "_corrupt_record") \
            .schema(patient_schema) \
            .csv(path)

        # Spark disallows querying only _corrupt_record directly from raw CSV.
        # Materializing the parsed dataframe first makes corrupt-row quarantine safe.
        raw_patients.count()

        corrupt_patients = raw_patients.filter(col("_corrupt_record").isNotNull())
        if corrupt_patients.count() > 0:
            corrupt_patients.withColumn("quarantine_timestamp", current_timestamp()) \
                .write.format("delta").mode("append").option("mergeSchema", "true").save(quarantine_path)

        valid_patients = raw_patients.filter(col("_corrupt_record").isNull()).drop("_corrupt_record")
        result = valid_patients.localCheckpoint(eager=True)
        return result
