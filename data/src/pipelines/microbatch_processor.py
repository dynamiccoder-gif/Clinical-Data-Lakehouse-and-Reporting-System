import os
from datetime import datetime

from delta.tables import DeltaTable
from pyspark.sql.functions import (
    abs as spark_abs,
    avg,
    array_join,
    array_distinct,
    col,
    collect_list,
    collect_set,
    concat_ws,
    count,
    countDistinct,
    coalesce,
    current_timestamp,
    datediff,
    expr,
    explode,
    floor,
    flatten,
    hash as spark_hash,
    lag,
    least,
    lit,
    lower,
    max as spark_max,
    months_between,
    regexp_replace,
    round as spark_round,
    row_number,
    sha2,
    size,
    split,
    struct,
    sum as spark_sum,
    to_json,
    trim,
    when,
    year,
)
from pyspark.sql.window import Window

from src.pipelines.readers import ClinicalDataReader
from src.transformations.clinical_rules import (
    ANTIMICROBIAL_PATTERN,
    CHRONIC_CAREPLAN_PATTERN,
    HIGH_RISK_MEDICATION_PATTERN,
    MAJOR_PROCEDURE_PATTERN,
    OBSERVATION_CODES,
    PREVENTIVE_IMMUNIZATION_PATTERN,
    SEVERE_ALLERGY_PATTERN,
)
from src.transformations.quality_gates import apply_clinical_quality_rules, apply_hipaa_masking


class MicroBatchProcessor:
    def __init__(self, spark_session, config):
        self.spark = spark_session
        self.config = config
        self.reader = ClinicalDataReader(spark_session, config)

    def write_bronze_encounters(self, batch_df, batch_id):
        paths = self.config["paths"]
        bronze_path = paths.get("bronze")
        if not bronze_path:
            return

        metadata_columns = [
            column
            for column in [
                "source_system",
                "source_file",
                "kafka_topic",
                "kafka_partition",
                "kafka_offset",
                "kafka_timestamp",
                "producer_run_id",
                "producer_row_number",
            ]
            if column in batch_df.columns
        ]
        bronze_df = batch_df.select(
            col("encounter_id"),
            col("patient_id"),
            col("admission_date"),
            col("diagnosis_code"),
            col("DESCRIPTION").alias("description"),
            col("REASONCODE").alias("reason_code"),
            col("REASONDESCRIPTION").alias("reason_description"),
            *[col(column) for column in metadata_columns],
        ).withColumn("batch_id", lit(int(batch_id))) \
            .withColumn("ingestion_timestamp", current_timestamp()) \
            .withColumn("raw_payload", to_json(struct("*")))

        bronze_df.write.format("delta").mode("append").option("mergeSchema", "true").save(bronze_path)

    def validate_raw_encounters(self, batch_df, patients_df=None):
        validated_df = (
            batch_df
            .withColumn(
                "_raw_failure_reasons",
                concat_ws(
                    ", ",
                    when(col("encounter_id").isNull() | (trim(col("encounter_id")) == ""), lit("ERR_MISSING_ENCOUNTER_ID")),
                    when(col("patient_id").isNull() | (trim(col("patient_id")) == ""), lit("ERR_MISSING_PATIENT_ID")),
                    when(col("admission_date").isNull(), lit("ERR_MISSING_OR_INVALID_DATE")),
                    when(col("admission_date") > current_timestamp(), lit("ERR_FUTURE_DATE")),
                    when(col("diagnosis_code").isNull() | (trim(col("diagnosis_code")) == ""), lit("ERR_MISSING_CODE")),
                )
            )
            .withColumn("_raw_failure_reasons", trim(col("_raw_failure_reasons")))
        )

        if patients_df is not None:
            patient_keys = patients_df.select("patient_id").distinct().withColumn("_patient_found", lit(True))
            validated_df = (
                validated_df
                .join(patient_keys, "patient_id", "left")
                .withColumn(
                    "_raw_failure_reasons",
                    when(
                        col("patient_id").isNotNull()
                        & (trim(col("patient_id")) != "")
                        & col("_patient_found").isNull()
                        & (col("_raw_failure_reasons") == ""),
                        lit("ERR_UNRESOLVED_PATIENT_REFERENCE"),
                    )
                    .when(
                        col("patient_id").isNotNull()
                        & (trim(col("patient_id")) != "")
                        & col("_patient_found").isNull(),
                        concat_ws(", ", col("_raw_failure_reasons"), lit("ERR_UNRESOLVED_PATIENT_REFERENCE")),
                    )
                    .otherwise(col("_raw_failure_reasons"))
                )
                .drop("_patient_found")
            )

        clean_df = validated_df.filter(col("_raw_failure_reasons") == "").drop("_raw_failure_reasons")
        quarantine_df = (
            validated_df
            .filter(col("_raw_failure_reasons") != "")
            .withColumnRenamed("_raw_failure_reasons", "quarantine_reason")
            .withColumn("quality_stage", lit("RAW_VALIDATION"))
            .withColumn("quarantined_at", current_timestamp())
        )
        return clean_df, quarantine_df

    def processed_files_path(self):
        paths = self.config["paths"]
        return paths.get("processed_files", paths["audit_logs"] + "_processed_files")

    def processed_files_table(self):
        return self.config.get("tables", {}).get("processed_files", "workspace.default.processed_files")

    def patient_state_path(self):
        return self.config["paths"].get(
            "patient_last_encounter_state",
            self.config["paths"]["audit_logs"] + "_patient_last_encounter_state",
        )

    def patient_state_table(self):
        return self.config.get("tables", {}).get(
            "patient_last_encounter_state",
            "workspace.default.patient_last_encounter_state",
        )

    def existing_processed_files(self):
        processed_path = self.processed_files_path()
        if not os.path.exists(processed_path):
            return None
        return self.spark.read.format("delta").load(processed_path).select("file_name").distinct()

    def existing_patient_state(self):
        state_path = self.patient_state_path()
        if not os.path.exists(state_path):
            return None
        return self.spark.read.format("delta").load(state_path).select(
            "patient_id",
            "last_encounter_id",
            "last_admission_date",
            "last_discharge_date",
            "last_los_days",
        )

    def attach_return_state(self, batch_df):
        state_df = self.existing_patient_state()
        if state_df is None:
            return batch_df.withColumn("previous_discharge_date", lit(None).cast("date")) \
                .withColumn("days_since_previous_discharge", lit(None).cast("int")) \
                .withColumn("is_readmitted_30d", lit(0)) \
                .withColumn("reason_recent_readmission", lit(0))

        enriched = batch_df.join(state_df, "patient_id", "left")
        return enriched.withColumn(
            "previous_discharge_date",
            col("last_discharge_date"),
        ).withColumn(
            "days_since_previous_discharge",
            datediff(col("admission_date"), col("last_discharge_date")),
        ).withColumn(
            "is_readmitted_30d",
            when(
                col("last_discharge_date").isNotNull()
                & (col("days_since_previous_discharge") > 0)
                & (col("days_since_previous_discharge") <= 30)
                & (col("last_los_days") > 0),
                lit(1),
            ).otherwise(lit(0)),
        ).withColumn(
            "reason_recent_readmission",
            when(col("is_readmitted_30d") == 1, lit(1)).otherwise(lit(0)),
        ).drop(
            "last_encounter_id",
            "last_admission_date",
            "last_discharge_date",
            "last_los_days",
        )

    def update_patient_state(self, clean_silver_df):
        state_path = self.patient_state_path()
        latest_window = Window.partitionBy("patient_id").orderBy(
            col("admission_date").desc(),
            col("encounter_id").desc(),
        )
        state_updates = (
            clean_silver_df
            .select(
                "patient_id",
                "encounter_id",
                col("admission_date").cast("date").alias("admission_date"),
                col("discharge_date").cast("date").alias("discharge_date"),
                col("los_days").cast("int").alias("los_days"),
            )
            .withColumn("_latest", row_number().over(latest_window))
            .filter(col("_latest") == 1)
            .drop("_latest")
            .select(
                "patient_id",
                col("encounter_id").alias("last_encounter_id"),
                col("admission_date").alias("last_admission_date"),
                col("discharge_date").alias("last_discharge_date"),
                col("los_days").alias("last_los_days"),
            )
            .withColumn("updated_at", current_timestamp())
        )

        if state_updates.isEmpty():
            return

        if os.path.exists(state_path):
            delta_state = DeltaTable.forPath(self.spark, state_path)
            delta_state.alias("target").merge(
                state_updates.alias("source"),
                "target.patient_id = source.patient_id",
            ).whenMatchedUpdate(
                condition="source.last_admission_date >= target.last_admission_date",
                set={
                    "last_encounter_id": "source.last_encounter_id",
                    "last_admission_date": "source.last_admission_date",
                    "last_discharge_date": "source.last_discharge_date",
                    "last_los_days": "source.last_los_days",
                    "updated_at": "source.updated_at",
                },
            ).whenNotMatchedInsertAll().execute()
        else:
            state_updates.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(state_path)

        table_name = self.patient_state_table()
        if table_name:
            try:
                self.spark.sql(
                    f"CREATE TABLE IF NOT EXISTS {table_name} "
                    f"USING DELTA LOCATION '{state_path}'"
                )
            except Exception as error:
                print(f"[PATIENT STATE] Could not register {table_name}: {error}")

    def record_processed_files(self, batch_df, batch_id, status):
        if "source_file" not in batch_df.columns:
            return

        file_records = (
            batch_df
            .groupBy(col("source_file").alias("file_name"))
            .agg(count("*").alias("record_count"))
            .withColumn("file_hash", sha2(col("file_name"), 256))
            .withColumn("batch_id", lit(int(batch_id)))
            .withColumn("arrival_timestamp", current_timestamp())
            .withColumn("processing_timestamp", current_timestamp())
            .withColumn("status", lit(status))
        )

        processed_path = self.processed_files_path()
        file_records.write.format("delta").mode("append").option("mergeSchema", "true").save(processed_path)

        table_name = self.processed_files_table()
        if table_name:
            try:
                self.spark.sql(
                    f"CREATE TABLE IF NOT EXISTS {table_name} "
                    f"USING DELTA LOCATION '{processed_path}'"
                )
            except Exception as error:
                print(f"[PROCESSED FILES] Could not register {table_name}: {error}")

    def log_lineage_metadata(
        self,
        batch_id,
        total,
        silver,
        quarantine,
        started_at,
        input_file_count=0,
        duplicate_records_skipped=0,
        records_inserted=0,
        records_updated=0,
        warning_records=0,
        status=None,
        batch_type=None,
    ):
        finished_at = datetime.now()
        duration_seconds = max((finished_at - started_at).total_seconds(), 0.0)
        pass_rate = round(((total - quarantine) / total) * 100, 2) if total > 0 else 0.0
        warning_rate = round((warning_records / silver) * 100, 2) if silver > 0 else 0.0
        reconciliation_status = (
            "PASS"
            if int(total) == int(silver) + int(quarantine) + int(duplicate_records_skipped)
            else "FAIL"
        )
        audit_payload = [{
            "pipeline_name": self.config["project"]["name"],
            "batch_id": int(batch_id),
            "batch_type": batch_type or self.config.get("batch_type") or os.getenv("BATCH_TYPE", "INCREMENTAL"),
            "execution_timestamp": finished_at,
            "source_records_read": int(total),
            "silver_records_written": int(silver),
            "quarantine_records_written": int(quarantine),
            "hard_failure_records": int(quarantine),
            "warning_records": int(warning_records),
            "pass_rate_pct": float(pass_rate),
            "validation_pass_rate_pct": float(pass_rate),
            "warning_rate_pct": float(warning_rate),
            "status": status or ("SUCCESS" if quarantine == 0 else "ANOMALIES_ISOLATED"),
            "duration_seconds": float(round(duration_seconds, 2)),
            "throughput_rows_per_second": float(round(total / duration_seconds, 2)) if duration_seconds > 0 else 0.0,
            "input_file_count": int(input_file_count),
            "duplicate_records_skipped": int(duplicate_records_skipped),
            "records_inserted": int(records_inserted),
            "records_updated": int(records_updated),
            "reconciliation_status": reconciliation_status,
        }]
        self.spark.createDataFrame(audit_payload).write.format("delta").mode("append").option("mergeSchema", "true").save(self.config["paths"]["audit_logs"])

    def rebuild_gold(self):
        paths = self.config["paths"]
        if not os.path.exists(paths["silver"]):
            raise FileNotFoundError("Missing Silver Clinical 360 table. Run the streaming processor before refreshing Gold.")

        pat_df = self.reader.read_feature_table("patients")
        patient_condition_history = self.reader.read_feature_table("conditions_by_patient")
        silver_master = self.spark.read.format("delta").load(paths["silver"])
        final_analytics = silver_master.withColumn("discharge_date", expr("date_add(to_date(admission_date), cast(los_days as int))"))

        patient_window = Window.partitionBy("patient_id").orderBy("admission_date")
        final_analytics = final_analytics \
            .withColumn("prev_admission", lag("admission_date").over(patient_window)) \
            .withColumn("prev_discharge", lag("discharge_date").over(patient_window)) \
            .withColumn("prev_los", lag("los_days").over(patient_window)) \
            .withColumn("return_gap_days", datediff(col("admission_date"), col("prev_admission"))) \
            .withColumn("days_since_discharge", datediff(col("admission_date"), col("prev_discharge"))) \
            .withColumn("high_risk_med_count", least(col("high_risk_med_count"), col("unique_medications"))) \
            .withColumn(
                "is_readmitted_30d",
                when(col("prev_discharge").isNotNull() & (col("days_since_discharge") > 0) & (col("days_since_discharge") <= 30) & (col("prev_los") > 0), 1).otherwise(0)
            ) \
            .withColumn(
                "readmission_risk_score",
                least(lit(100), (
                    when(col("is_readmitted_30d") == 1, lit(35)).otherwise(lit(0))
                    + when(col("high_risk_med_count") > 0, lit(20)).otherwise(lit(0))
                    + when(col("unique_medications") >= 4, lit(15)).otherwise(lit(0))
                    + when(col("abnormal_vital_count") >= 2, lit(15)).otherwise(lit(0))
                    + when(col("condition_count") >= 3, lit(10)).otherwise(lit(0))
                    + when(col("chronic_condition_count") >= 2, lit(10)).otherwise(lit(0))
                    + when(col("major_procedure_count") > 0, lit(10)).otherwise(lit(0))
                    + when(col("chronic_careplan_count") > 0, lit(5)).otherwise(lit(0))
                    + when(col("severe_allergy_count") > 0, lit(5)).otherwise(lit(0))
                    + when(col("age") >= 65, lit(10)).otherwise(lit(0))
                    + when(col("los_days") >= 7, lit(5)).otherwise(lit(0))
                )).cast("int")
            ) \
            .withColumn("reason_recent_readmission", when(col("is_readmitted_30d") == 1, lit(1)).otherwise(lit(0))) \
            .withColumn("reason_age_65_plus", when(col("age") >= 65, lit(1)).otherwise(lit(0))) \
            .withColumn("reason_high_risk_medication", when(col("high_risk_med_count") > 0, lit(1)).otherwise(lit(0))) \
            .withColumn("reason_polypharmacy", when(col("unique_medications") >= 5, lit(1)).otherwise(lit(0))) \
            .withColumn("reason_chronic_condition_burden", when(col("chronic_condition_count") >= 2, lit(1)).otherwise(lit(0))) \
            .withColumn("reason_abnormal_vitals", when(col("abnormal_vital_count") >= 2, lit(1)).otherwise(lit(0))) \
            .withColumn("reason_major_procedure", when(col("major_procedure_count") > 0, lit(1)).otherwise(lit(0))) \
            .withColumn("reason_severe_allergy", when(col("severe_allergy_count") > 0, lit(1)).otherwise(lit(0))) \
            .withColumn(
                "readmission_risk_band",
                when(col("readmission_risk_score") >= 60, "HIGH")
                .when(col("readmission_risk_score") >= 30, "MEDIUM")
                .otherwise("LOW")
            ) \
            .withColumn(
                "clinical_burden_score",
                expr(
                    "least(coalesce(condition_count, 0), 5) + "
                    "least(coalesce(unique_medications, 0), 5) + "
                    "least(coalesce(procedure_count, 0), 5) + "
                    "least(coalesce(abnormal_vital_count, 0), 3) + "
                    "least(coalesce(careplan_count, 0), 2)"
                ).cast("int")
            ) \
            .withColumn("reason_high_utilization", when(col("clinical_burden_score") >= 12, lit(1)).otherwise(lit(0))) \
            .withColumn(
                "risk_reasons",
                concat_ws(
                    ", ",
                    when(col("reason_recent_readmission") == 1, lit("recent_readmission")),
                    when(col("reason_age_65_plus") == 1, lit("age_65_plus")),
                    when(col("reason_high_risk_medication") == 1, lit("high_risk_medication")),
                    when(col("reason_polypharmacy") == 1, lit("polypharmacy")),
                    when(col("reason_chronic_condition_burden") == 1, lit("chronic_condition_burden")),
                    when(col("reason_abnormal_vitals") == 1, lit("abnormal_vitals")),
                    when(col("reason_major_procedure") == 1, lit("major_procedure")),
                    when(col("reason_severe_allergy") == 1, lit("severe_allergy")),
                    when(col("reason_high_utilization") == 1, lit("high_utilization")),
                )
            ) \
            .withColumn(
                "care_gap_severity",
                when((col("chronic_condition_count") > 0) & (col("chronic_careplan_count") <= 0) & (col("abnormal_vital_count") >= 2), "CRITICAL")
                .when((col("chronic_condition_count") > 0) & (col("chronic_careplan_count") <= 0), "WARNING")
                .when((col("observation_count") <= 0) | (col("preventive_immunization_count") <= 0), "INFO")
                .otherwise("NONE")
            ) \
            .withColumn(
                "recommended_action",
                when(col("care_gap_severity") == "CRITICAL", "Schedule urgent chronic care follow-up")
                .when(col("care_gap_severity") == "WARNING", "Review care plan coverage")
                .when(col("care_gap_severity") == "INFO", "Refresh preventive care or vitals data")
                .otherwise("Monitor")
            )

        gold_clinical = final_analytics.groupBy("condition_name").agg(
            avg("los_days").alias("avg_los_days"),
            countDistinct("encounter_id").alias("total_encounters"),
            avg("is_readmitted_30d").alias("readmission_rate"),
            avg("readmission_risk_score").alias("avg_readmission_risk_score"),
            avg("unique_medications").alias("avg_unique_medications"),
            avg("abnormal_vital_count").alias("avg_abnormal_vitals"),
            avg("condition_count").alias("avg_condition_count"),
            avg("procedure_count").alias("avg_procedure_count"),
            avg("careplan_count").alias("avg_careplan_count"),
            avg("allergy_count").alias("avg_allergy_count"),
            avg("immunization_count").alias("avg_immunization_count"),
            spark_sum(when(col("readmission_risk_band") == "HIGH", lit(1)).otherwise(lit(0))).alias("high_risk_patients"),
        )
        gold_clinical.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(paths["gold"])

        gold_demographics = final_analytics.dropDuplicates(["patient_id"]).groupBy("race", "gender", "age_group").agg(
            count("patient_id").alias("unique_patients")
        )
        gold_demographics.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(paths["gold"] + "_demographics")

        gold_patient_360 = final_analytics.groupBy("patient_id").agg(
            spark_max("readmission_risk_score").alias("risk_score"),
            spark_max("clinical_burden_score").alias("clinical_burden_score"),
            spark_max("reason_recent_readmission").alias("reason_recent_readmission"),
            spark_max("reason_age_65_plus").alias("reason_age_65_plus"),
            spark_max("reason_high_risk_medication").alias("reason_high_risk_medication"),
            spark_max("reason_polypharmacy").alias("reason_polypharmacy"),
            spark_max("reason_chronic_condition_burden").alias("reason_chronic_condition_burden"),
            spark_max("reason_abnormal_vitals").alias("reason_abnormal_vitals"),
            spark_max("reason_major_procedure").alias("reason_major_procedure"),
            spark_max("reason_severe_allergy").alias("reason_severe_allergy"),
            spark_max("reason_high_utilization").alias("reason_high_utilization"),
            spark_max("condition_count").alias("condition_count"),
            spark_max("chronic_condition_count").alias("chronic_condition_count"),
            spark_max("unique_medications").alias("unique_medications"),
            spark_max("high_risk_med_count").alias("high_risk_med_count"),
            spark_max("abnormal_vital_count").alias("abnormal_vital_count"),
            spark_max("procedure_count").alias("procedure_count"),
            spark_max("major_procedure_count").alias("major_procedure_count"),
            spark_max("careplan_count").alias("careplan_count"),
            spark_max("preventive_immunization_count").alias("preventive_immunization_count"),
            spark_max("severe_allergy_count").alias("severe_allergy_count"),
            spark_max("gender").alias("gender"),
            spark_max("race").alias("race"),
            spark_max("age_group").alias("age_group"),
            array_join(array_distinct(flatten(collect_list(
                split(
                    when(
                        (col("condition_names").isNotNull()) & (col("condition_names") != "General Outpatient Checkup"),
                        col("condition_names"),
                    ).when(col("condition_name") != "General Clinical Encounter", col("condition_name"))
                    .otherwise(lit("")),
                    "\\s*\\|\\s*",
                )
            ))), " | ").alias("condition_names"),
        ).withColumn(
            "condition_names",
            when(trim(col("condition_names")) == "", "No condition linked").otherwise(col("condition_names"))
        ).withColumn(
            "risk_band",
            when(col("risk_score") >= 60, "HIGH")
            .when(col("risk_score") >= 30, "MEDIUM")
            .otherwise("LOW")
        ).withColumn(
            "risk_reasons",
            concat_ws(
                ", ",
                when(col("reason_recent_readmission") == 1, lit("recent_readmission")),
                when(col("reason_age_65_plus") == 1, lit("age_65_plus")),
                when(col("reason_high_risk_medication") == 1, lit("high_risk_medication")),
                when(col("reason_polypharmacy") == 1, lit("polypharmacy")),
                when(col("reason_chronic_condition_burden") == 1, lit("chronic_condition_burden")),
                when(col("reason_abnormal_vitals") == 1, lit("abnormal_vitals")),
                when(col("reason_major_procedure") == 1, lit("major_procedure")),
                when(col("reason_severe_allergy") == 1, lit("severe_allergy")),
                when(col("reason_high_utilization") == 1, lit("high_utilization")),
            )
        ).withColumn(
            "suggested_focus",
            when(col("reason_recent_readmission") == 1, "Review recent return and follow-up")
            .when(col("reason_abnormal_vitals") == 1, "Review abnormal vitals")
            .when(col("reason_high_risk_medication") == 1, "Medication safety review")
            .when(col("reason_chronic_condition_burden") == 1, "Chronic care plan review")
            .otherwise("Routine monitoring")
        ).orderBy(col("risk_score").desc(), col("clinical_burden_score").desc())
        gold_patient_360.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(paths.get("gold_patient_360", paths["gold"] + "_patient_360"))

        care_gap_priority_window = Window.partitionBy("patient_id").orderBy(
            when(col("care_gap_severity") == "CRITICAL", lit(1))
            .when(col("care_gap_severity") == "WARNING", lit(2))
            .otherwise(lit(3)),
            col("readmission_risk_score").desc(),
            col("admission_date").desc(),
        )
        gold_care_gaps = final_analytics.filter(col("care_gap_severity") != "NONE").withColumn(
            "_care_gap_rank",
            row_number().over(care_gap_priority_window),
        ).filter(col("_care_gap_rank") == 1).select(
            "patient_id",
            "encounter_id",
            "condition_name",
            "care_gap_severity",
            "recommended_action",
            "chronic_condition_count",
            "chronic_careplan_count",
            "observation_count",
            "abnormal_vital_count",
            "preventive_immunization_count",
            "readmission_risk_score",
        ).orderBy(col("readmission_risk_score").desc(), col("care_gap_severity"))
        gold_care_gaps.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(paths.get("gold_care_gaps", paths["gold"] + "_care_gaps"))

        patient_conditions = final_analytics.select(
            "patient_id",
            explode(split(coalesce(col("condition_names"), col("condition_name")), "\\s*\\|\\s*")).alias("condition"),
        ).filter(
            col("condition").isNotNull()
            & (trim(col("condition")) != "")
            & (~lower(col("condition")).rlike("general clinical encounter|general outpatient checkup"))
        ).select("patient_id", trim(col("condition")).alias("condition")).dropDuplicates()
        left_conditions = patient_conditions.alias("left")
        right_conditions = patient_conditions.alias("right")
        gold_comorbidity_pairs = left_conditions.join(
            right_conditions,
            (col("left.patient_id") == col("right.patient_id"))
            & (col("left.condition") < col("right.condition")),
        ).groupBy(
            col("left.condition").alias("condition_a"),
            col("right.condition").alias("condition_b"),
        ).agg(
            countDistinct(col("left.patient_id")).alias("patient_count")
        ).orderBy(col("patient_count").desc()).limit(50)
        gold_comorbidity_pairs.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(paths["gold_comorbidity_pairs"])

        gold_return_gaps = final_analytics.filter(col("return_gap_days").isNotNull() & (col("return_gap_days") >= 0)).withColumn(
            "return_gap_bucket",
            when(col("return_gap_days") <= 7, "0-7 days")
            .when(col("return_gap_days") <= 30, "8-30 days")
            .when(col("return_gap_days") <= 90, "31-90 days")
            .otherwise("90+ days")
        ).groupBy("return_gap_bucket").agg(
            countDistinct("encounter_id").alias("encounter_count"),
            countDistinct("patient_id").alias("patient_count"),
        )
        gold_return_gaps.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(paths["gold_return_gaps"])

        if patient_condition_history is not None and pat_df is not None:
            monitored_patients = final_analytics.select("patient_id").dropDuplicates(["patient_id"])
            monitored_conditions = patient_condition_history.join(
                monitored_patients,
                "patient_id",
                "inner",
            )
            disease_burden = monitored_conditions.groupBy("condition_name").agg(
                countDistinct("patient_id").alias("affected_patients")
            )
            deceased_patients = pat_df.filter(
                col("deathdate").isNotNull()
            ).select(
                "patient_id",
                "deathdate",
            ).dropDuplicates(["patient_id"])
            deceased_associations = monitored_conditions.join(
                deceased_patients,
                "patient_id",
                "inner",
            ).filter(
                col("condition_start").isNull() | (col("condition_start") <= col("deathdate"))
            ).groupBy("condition_name").agg(
                countDistinct("patient_id").alias("deceased_patients")
            )
            gold_disease_rankings = disease_burden.join(
                deceased_associations,
                "condition_name",
                "left",
            ).na.fill(
                {"deceased_patients": 0}
            ).withColumn(
                "deceased_association_pct",
                spark_round((col("deceased_patients") / col("affected_patients")) * 100, 2),
            )
            gold_disease_rankings.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(paths["gold_disease_rankings"])

        print("[GOLD] Rebuilt scheduled Clinical 360 analytics from Silver Delta.")

    def process(self, batch_df, batch_id):
        if batch_df.isEmpty():
            return

        started_at = datetime.now()
        paths = self.config["paths"]
        source_batch_count = batch_df.count()
        input_file_count = batch_df.select("source_file").distinct().count() if "source_file" in batch_df.columns else 0
        already_processed_count = 0

        if "source_file" in batch_df.columns:
            processed_files = self.existing_processed_files()
            if processed_files is not None:
                batch_df = batch_df.join(processed_files, batch_df.source_file == processed_files.file_name, "left_anti")
                already_processed_count = source_batch_count - batch_df.count()

        if batch_df.isEmpty():
            self.log_lineage_metadata(
                batch_id=batch_id,
                total=source_batch_count,
                silver=0,
                quarantine=0,
                started_at=started_at,
                input_file_count=input_file_count,
                duplicate_records_skipped=already_processed_count,
                status="SKIPPED_ALREADY_PROCESSED",
            )
            print(
                f"[BATCH #{batch_id} METRICS] Skipped {already_processed_count:,} records "
                "because all source files were already processed."
            )
            return

        pat_df = self.reader.read_feature_table("patients")
        if pat_df is None:
            raw_pat = self.reader.read_patients_with_quarantine(paths["raw_patients"], paths.get("patient_quarantine", "lakehouse/quarantine/patients"))
            if raw_pat is not None:
                pat_df = raw_pat.select(
                    col("ID").alias("patient_id"),
                    col("GENDER").alias("gender"),
                    col("RACE").alias("race"),
                    expr("try_cast(BIRTHDATE as date)").alias("birthdate"),
                    expr("try_cast(DEATHDATE as date)").alias("deathdate"),
                    col("SSN").alias("ssn"),
                ).withColumn(
                    "birthdate",
                    when(col("birthdate").isNotNull(), col("birthdate")).otherwise(expr("to_date('1980-01-01')"))
                ).dropDuplicates(["patient_id"])

        self.write_bronze_encounters(batch_df, batch_id)
        processable_count = batch_df.count()
        processable_files_df = batch_df
        raw_clean_df, raw_quarantine_df = self.validate_raw_encounters(batch_df, pat_df)
        raw_quarantine_count = raw_quarantine_df.count()
        if raw_quarantine_count > 0:
            raw_quarantine_df.write.format("delta").mode("append").option("mergeSchema", "true").save(paths["quarantine"])

        raw_clean_count = raw_clean_df.count()
        batch_df = raw_clean_df.dropDuplicates(["encounter_id"])
        batch_duplicate_count = raw_clean_count - batch_df.count()

        obs_df = self.reader.read_feature_table("observations_by_encounter")
        if os.path.exists(paths["raw_observations"]):
            if obs_df is None:
                raw_obs = self.reader.read_csv_uppercase(paths["raw_observations"])
                obs_df = raw_obs.groupBy(col("ENCOUNTER").alias("encounter_id")).agg(
                    count("*").alias("observation_count"),
                    *[
                        spark_max(when(col("CODE") == code, col("VALUE").cast("double"))).alias(name)
                        for name, code in OBSERVATION_CODES.items()
                    ],
                ).withColumn(
                    "abnormal_vital_count",
                    when(col("bmi") >= 30, lit(1)).otherwise(lit(0))
                    + when(col("systolic_bp") >= 140, lit(1)).otherwise(lit(0))
                    + when(col("diastolic_bp") >= 90, lit(1)).otherwise(lit(0))
                )

        cond_base = self.reader.read_feature_table("conditions_by_encounter")
        if cond_base is None:
            raw_cond = self.reader.read_csv_uppercase(paths["raw_conditions"])
            if raw_cond is not None:
                cond_detail = raw_cond.select(
                    col("ENCOUNTER").alias("encounter_id"),
                    col("DESCRIPTION").alias("condition_name"),
                    expr("try_cast(START as date)").alias("condition_start"),
                    expr("try_cast(STOP as date)").alias("condition_stop"),
                    datediff(expr("try_cast(STOP as date)"), expr("try_cast(START as date)")).alias("condition_duration_days"),
                ).filter(col("encounter_id").isNotNull())

                cond_base = cond_detail.groupBy("encounter_id").agg(
                    array_join(collect_set("condition_name"), " | ").alias("condition_names"),
                    spark_max("condition_name").alias("primary_condition_name"),
                    count("*").alias("condition_count"),
                    spark_sum(when(col("condition_stop").isNull(), lit(1)).otherwise(lit(0))).alias("chronic_condition_count"),
                    spark_max("condition_duration_days").alias("max_condition_duration_days")
                )

        med_df = self.reader.read_feature_table("medications_by_encounter")
        if med_df is None:
            raw_med = self.reader.read_csv_uppercase(paths["raw_medications"])
            if raw_med is not None:
                medication_windows = raw_med.select(
                    col("PATIENT").alias("med_patient_id"),
                    col("CODE").alias("medication_code"),
                    col("DESCRIPTION").alias("medication_name"),
                    expr("try_cast(START as date)").alias("medication_start"),
                    expr("try_cast(STOP as date)").alias("medication_stop"),
                )
                batch_encounters = batch_df.select(
                    "encounter_id",
                    "patient_id",
                    col("admission_date").cast("date").alias("encounter_date"),
                )
                active_medications = batch_encounters.join(
                    medication_windows,
                    (col("patient_id") == col("med_patient_id"))
                    & (col("medication_start") <= col("encounter_date"))
                    & (col("medication_stop").isNull() | (col("medication_stop") >= col("encounter_date"))),
                    "left",
                )
                med_df = active_medications.groupBy("encounter_id").agg(
                    count("medication_code").alias("medication_events"),
                    countDistinct("medication_code").alias("unique_medications"),
                    countDistinct(when(lower(col("medication_name")).rlike(HIGH_RISK_MEDICATION_PATTERN), col("medication_code"))).alias("high_risk_med_count"),
                    countDistinct(when(lower(col("medication_name")).rlike(ANTIMICROBIAL_PATTERN), col("medication_code"))).alias("antimicrobial_count"),
                    spark_max(datediff(col("medication_stop"), col("medication_start"))).alias("max_medication_days"),
                )

        proc_df = self.reader.read_feature_table("procedures_by_encounter")
        if proc_df is None:
            raw_proc = self.reader.read_csv_uppercase(paths.get("raw_procedures", ""))
            if raw_proc is not None:
                proc_df = raw_proc.groupBy(col("ENCOUNTER").alias("encounter_id")).agg(
                    count("*").alias("procedure_count"),
                    countDistinct("CODE").alias("unique_procedures"),
                    spark_sum(when(lower(col("DESCRIPTION")).rlike(MAJOR_PROCEDURE_PATTERN), lit(1)).otherwise(lit(0))).alias("major_procedure_count"),
                )

        careplan_df = self.reader.read_feature_table("careplans_by_encounter")
        if careplan_df is None:
            raw_careplan = self.reader.read_csv_uppercase(paths.get("raw_careplans", ""))
            if raw_careplan is not None:
                careplan_df = raw_careplan.groupBy(col("ENCOUNTER").alias("encounter_id")).agg(
                    count("*").alias("careplan_count"),
                    countDistinct("CODE").alias("unique_careplans"),
                    spark_sum(when(lower(col("DESCRIPTION")).rlike(CHRONIC_CAREPLAN_PATTERN), lit(1)).otherwise(lit(0))).alias("chronic_careplan_count"),
                )

        allergy_df = self.reader.read_feature_table("allergies_by_patient")
        if allergy_df is None:
            raw_allergy = self.reader.read_csv_uppercase(paths.get("raw_allergies", ""))
            if raw_allergy is not None:
                allergy_df = raw_allergy.groupBy(col("PATIENT").alias("patient_id")).agg(
                    countDistinct("CODE").alias("allergy_count"),
                    spark_sum(when(lower(col("DESCRIPTION")).rlike(SEVERE_ALLERGY_PATTERN), lit(1)).otherwise(lit(0))).alias("severe_allergy_count"),
                )

        immunization_df = self.reader.read_feature_table("immunizations_by_patient")
        if immunization_df is None:
            raw_immunization = self.reader.read_csv_uppercase(paths.get("raw_immunizations", ""))
            if raw_immunization is not None:
                immunization_df = raw_immunization.groupBy(col("PATIENT").alias("patient_id")).agg(
                    countDistinct("CODE").alias("immunization_count"),
                    spark_sum(when(lower(col("DESCRIPTION")).rlike(PREVENTIVE_IMMUNIZATION_PATTERN), lit(1)).otherwise(lit(0))).alias("preventive_immunization_count"),
                )

        enriched_batch = batch_df.drop("ssn")

        if cond_base is not None:
            enriched_batch = enriched_batch.join(cond_base, "encounter_id", "left")
        if obs_df is not None:
            enriched_batch = enriched_batch.join(obs_df, "encounter_id", "left")
        if med_df is not None:
            enriched_batch = enriched_batch.join(med_df, "encounter_id", "left")
        if proc_df is not None:
            enriched_batch = enriched_batch.join(proc_df, "encounter_id", "left")
        if careplan_df is not None:
            enriched_batch = enriched_batch.join(careplan_df, "encounter_id", "left")
        if pat_df is not None:
            enriched_batch = enriched_batch.join(pat_df, "patient_id", "left")
        if allergy_df is not None:
            enriched_batch = enriched_batch.join(allergy_df, "patient_id", "left")
        if immunization_df is not None:
            enriched_batch = enriched_batch.join(immunization_df, "patient_id", "left")

        enriched_batch = enriched_batch.withColumn(
            "condition_name",
            trim(regexp_replace(coalesce(col("REASONDESCRIPTION"), col("primary_condition_name"), lit("General Clinical Encounter")), "^\\d+\\s*", ""))
        )

        enriched_batch = enriched_batch \
            .withColumn(
                "los_days",
                when(lower(col("DESCRIPTION")).rlike("inpatient|hospital admission|emergency hospital"), lit(3))
                .when(lower(col("DESCRIPTION")).rlike("emergency"), lit(1))
                .when(lower(col("DESCRIPTION")).rlike("procedure|surgical"), lit(1))
                .otherwise(lit(1))
            ) \
            .withColumn("discharge_date", expr("cast(date_add(to_date(admission_date), cast(los_days as int)) as timestamp)")) \
            .withColumn("age", floor(months_between(current_timestamp(), col("birthdate")) / 12)) \
            .withColumn("age_group", when(col("age") < 18, "0-17").when(col("age") < 40, "18-39").when(col("age") < 60, "40-59").otherwise("60+")) \
            .withColumn("hospital_id", (spark_abs(spark_hash(col("patient_id"))) % 5 + 1).cast("int")) \
            .na.fill({
                "gender": "UNKNOWN",
                "race": "UNKNOWN",
                "ssn": "000-00-0000",
                "condition_name": "General Outpatient Checkup",
                "condition_names": "General Outpatient Checkup",
                "condition_count": 0,
                "chronic_condition_count": 0,
                "observation_count": 0,
                "abnormal_vital_count": 0,
                "medication_events": 0,
                "unique_medications": 0,
                "high_risk_med_count": 0,
                "antimicrobial_count": 0,
                "max_medication_days": 0,
                "procedure_count": 0,
                "unique_procedures": 0,
                "major_procedure_count": 0,
                "careplan_count": 0,
                "unique_careplans": 0,
                "chronic_careplan_count": 0,
                "allergy_count": 0,
                "severe_allergy_count": 0,
                "immunization_count": 0,
                "preventive_immunization_count": 0,
            })
        enriched_batch = self.attach_return_state(enriched_batch)

        governed_df = apply_hipaa_masking(enriched_batch)
        tagged_df = apply_clinical_quality_rules(governed_df, self.config["quality_gates"])

        clean_silver = tagged_df.filter(col("_failure_reasons") == "") \
            .withColumn("quality_status", when(col("_warning_reasons") == "", lit("VALID")).otherwise(lit("VALID_WITH_WARNINGS"))) \
            .withColumn("quality_warning_codes", col("_warning_reasons")) \
            .withColumn("quality_warning_count", when(col("_warning_reasons") == "", lit(0)).otherwise(size(split(col("_warning_reasons"), ", ")))) \
            .withColumn("encounter_year", year(col("admission_date"))) \
            .drop("_failure_reasons", "_warning_reasons", "birthdate")
        enriched_quarantine_df = (
            tagged_df
            .filter(col("_failure_reasons") != "")
            .withColumnRenamed("_failure_reasons", "quarantine_reason")
            .withColumn("quality_stage", lit("ENRICHED_VALIDATION"))
            .withColumn("quarantined_at", current_timestamp())
            .drop("_warning_reasons", "birthdate")
        )

        clean_silver_dedup = clean_silver.dropDuplicates(["encounter_id"])
        silver_count = clean_silver_dedup.count()
        warning_records = clean_silver_dedup.filter(col("quality_warning_count") > 0).count()
        records_inserted = silver_count
        records_updated = 0

        if os.path.exists(paths["silver"]):
            clean_silver_dedup.limit(0).write.format("delta").mode("append").option("mergeSchema", "true").save(paths["silver"])
            existing_silver_ids = self.spark.read.format("delta").load(paths["silver"]).select("encounter_id").distinct()
            records_updated = clean_silver_dedup.join(existing_silver_ids, "encounter_id", "inner").count()
            records_inserted = silver_count - records_updated
            delta_silver = DeltaTable.forPath(self.spark, paths["silver"])
            delta_silver.alias("target") \
                .merge(clean_silver_dedup.alias("source"), "target.encounter_id = source.encounter_id") \
                .whenMatchedUpdateAll() \
                .whenNotMatchedInsertAll() \
                .execute()
        else:
            clean_silver_dedup.write.format("delta").option("mergeSchema", "true").mode("append").partitionBy("encounter_year", "hospital_id").save(paths["silver"])

        self.update_patient_state(clean_silver_dedup)

        enriched_quarantine_count = enriched_quarantine_df.count()
        if enriched_quarantine_count > 0:
            enriched_quarantine_df.write.format("delta").mode("append").option("mergeSchema", "true").save(paths["quarantine"])

        quar_count = raw_quarantine_count + enriched_quarantine_count
        duplicate_records_skipped = already_processed_count + batch_duplicate_count
        self.record_processed_files(processable_files_df, batch_id, "SUCCESS" if quar_count == 0 else "ANOMALIES_ISOLATED")
        self.log_lineage_metadata(
            batch_id=batch_id,
            total=source_batch_count,
            silver=silver_count,
            quarantine=quar_count,
            started_at=started_at,
            input_file_count=input_file_count,
            duplicate_records_skipped=duplicate_records_skipped,
            records_inserted=records_inserted,
            records_updated=records_updated,
            warning_records=warning_records,
        )
        print(
            f"[BATCH #{batch_id} METRICS] Read: {source_batch_count:,} | "
            f"Clean: {silver_count:,} | Anomalies: {quar_count:,} | "
            f"Skipped duplicates/already processed: {duplicate_records_skipped:,} | "
            f"Inserted: {records_inserted:,} | Updated: {records_updated:,}"
        )
