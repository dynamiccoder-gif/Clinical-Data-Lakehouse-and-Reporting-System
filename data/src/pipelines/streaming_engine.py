import os

from pyspark.sql.functions import coalesce, col, from_json, get_json_object, lit
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

from src.pipelines.microbatch_processor import MicroBatchProcessor


class MedallionStreamingCoordinator:
    def __init__(self, spark_session, config):
        self.spark = spark_session
        self.config = config
        self.microbatch_processor = MicroBatchProcessor(spark_session, config)

    def _encounter_schema(self):
        return StructType([
            StructField("ID", StringType(), True),
            StructField("DATE", TimestampType(), True),
            StructField("PATIENT", StringType(), True),
            StructField("CODE", StringType(), True),
            StructField("DESCRIPTION", StringType(), True),
            StructField("REASONCODE", StringType(), True),
            StructField("REASONDESCRIPTION", StringType(), True),
        ])

    def _file_stream(self, encounter_schema):
        paths = self.config["paths"]
        streaming_source = self.spark.readStream \
            .format("csv") \
            .option("header", "true") \
            .option("pathGlobFilter", "*encounters*.csv") \
            .schema(encounter_schema) \
            .load(paths["raw_encounters"])

        return streaming_source \
            .withColumnRenamed("ID", "encounter_id") \
            .withColumnRenamed("PATIENT", "patient_id") \
            .withColumnRenamed("DATE", "admission_date") \
            .withColumnRenamed("CODE", "diagnosis_code") \
            .withColumn("source_system", lit("file")) \
            .withColumn("source_file", col("_metadata.file_path")), paths["bronze_checkpoint"]

    def _kafka_stream(self, encounter_schema):
        paths = self.config["paths"]
        kafka_config = self.config["kafka"]
        kafka_stream = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_config["bootstrap_servers"]) \
            .option("subscribe", kafka_config["encounters_topic"]) \
            .option("startingOffsets", kafka_config.get("starting_offsets", "earliest")) \
            .load()

        raw_value = col("value").cast("string")
        event_json = coalesce(get_json_object(raw_value, "$.payload"), raw_value)
        parsed_stream = kafka_stream.select(
            from_json(event_json, encounter_schema).alias("event"),
            get_json_object(raw_value, "$.producer_run_id").alias("producer_run_id"),
            get_json_object(raw_value, "$.producer_row_number").alias("producer_row_number"),
            col("topic").alias("kafka_topic"),
            col("partition").alias("kafka_partition"),
            col("offset").alias("kafka_offset"),
            col("timestamp").alias("kafka_timestamp"),
        )

        bronze_streaming_df = parsed_stream.select(
            col("event.ID").alias("encounter_id"),
            col("event.PATIENT").alias("patient_id"),
            col("event.DATE").alias("admission_date"),
            col("event.CODE").alias("diagnosis_code"),
            col("event.DESCRIPTION").alias("DESCRIPTION"),
            col("event.REASONCODE").alias("REASONCODE"),
            col("event.REASONDESCRIPTION").alias("REASONDESCRIPTION"),
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
            "producer_run_id",
            "producer_row_number",
        ).withColumn("source_system", lit("kafka"))

        return bronze_streaming_df, kafka_config.get("checkpoint", paths["bronze_checkpoint"])

    def start_engine(self):
        streaming_source_name = os.getenv(
            "STREAMING_SOURCE",
            self.config.get("streaming", {}).get("source", "file"),
        ).lower()
        encounter_schema = self._encounter_schema()

        if streaming_source_name == "kafka":
            bronze_streaming_df, checkpoint_location = self._kafka_stream(encounter_schema)
        else:
            bronze_streaming_df, checkpoint_location = self._file_stream(encounter_schema)

        print(f"\nHealthcare streaming engine running [{streaming_source_name.upper()} SOURCE]...")
        stream_writer = bronze_streaming_df.writeStream \
            .foreachBatch(self.microbatch_processor.process) \
            .option("checkpointLocation", checkpoint_location)

        trigger_mode = os.getenv(
            "STREAMING_TRIGGER",
            self.config.get("spark", {}).get("trigger_mode", "continuous"),
        ).lower()
        if trigger_mode == "availablenow":
            stream_writer = stream_writer.trigger(availableNow=True)
        elif trigger_mode == "once":
            stream_writer = stream_writer.trigger(once=True)

        query = stream_writer.start()
        try:
            query.awaitTermination()
        except Exception as error:
            known_spark_kafka_metrics_bug = (
                streaming_source_name == "kafka"
                and trigger_mode in {"availablenow", "once"}
                and "IterableOps.map" in str(error)
                and "Option.get()" in str(error)
            )
            if not known_spark_kafka_metrics_bug:
                raise
            print(
                "[KAFKA SOURCE] Ignoring Spark 4.1.1 post-commit progress-metrics bug "
                "after bounded processing completed (SPARK-55271)."
            )
