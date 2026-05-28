# src/transformations/quality_gates.py
from pyspark.sql.functions import col, when, sha2, concat_ws, array, lit, trim

def apply_hipaa_masking(df, target_col="ssn"):
    """Anonymizes patient Social Security Numbers using SHA-256 one-way hashing"""
    return df.withColumn(target_col, sha2(col(target_col), 256))

def apply_clinical_quality_rules(df, gates_config):
    """
    Validates encounter chronology, verified patient vital observations, and schemas.
    Applies trim() to concat_ws to prevent whitespace bugs from silently dropping clean rows.
    """
    validated_df = df \
        .withColumn("err_missing_encounter", when(col("encounter_id").isNull() | (trim(col("encounter_id")) == ""), lit("ERR_MISSING_ENCOUNTER_ID")).otherwise(lit(None))) \
        .withColumn("err_missing_patient", when(col("patient_id").isNull() | (trim(col("patient_id")) == ""), lit("ERR_MISSING_PATIENT_ID")).otherwise(lit(None))) \
        .withColumn("err_missing_admission", when(col("admission_date").isNull(), lit("ERR_MISSING_ADMISSION_DATE")).otherwise(lit(None))) \
        .withColumn("err_schema", when(col("diagnosis_code").isNull() | (trim(col("diagnosis_code")) == ""), lit("ERR_MISSING_ICD10")).otherwise(lit(None))) \
        .withColumn("err_chrono", when(col("discharge_date") <= col("admission_date"), lit("ERR_CHRONO_FLOW")).otherwise(lit(None))) \
        .withColumn("warn_missing_bmi", when(col("bmi").isNull(), lit("WARN_MISSING_BMI")).otherwise(lit(None))) \
        .withColumn("warn_missing_bp", when(col("systolic_bp").isNull() | col("diastolic_bp").isNull(), lit("WARN_MISSING_BP")).otherwise(lit(None))) \
        .withColumn("warn_missing_demographics", when(col("gender").isNull() | (col("gender") == "UNKNOWN") | col("race").isNull() | (col("race") == "UNKNOWN"), lit("WARN_MISSING_DEMOGRAPHICS")).otherwise(lit(None))) \
        .withColumn("warn_missing_medications", when(col("unique_medications").isNull() | (col("unique_medications") <= 0), lit("WARN_MISSING_MEDICATION_CONTEXT")).otherwise(lit(None))) \
        .withColumn("warn_missing_conditions", when(col("condition_count").isNull() | (col("condition_count") <= 0), lit("WARN_MISSING_CONDITION_CONTEXT")).otherwise(lit(None))) \
        .withColumn("warn_missing_observations", when(col("observation_count").isNull() | (col("observation_count") <= 0), lit("WARN_MISSING_OBSERVATION_CONTEXT")).otherwise(lit(None)))

    error_array = array(
        "err_missing_encounter",
        "err_missing_patient",
        "err_missing_admission",
        "err_schema",
        "err_chrono",
    )
    warning_array = array(
        "warn_missing_bmi",
        "warn_missing_bp",
        "warn_missing_demographics",
        "warn_missing_medications",
        "warn_missing_conditions",
        "warn_missing_observations",
    )
    
    # concat_ws can leave whitespace when all error columns are null; trim keeps
    # the clean-row marker consistently empty.
    return validated_df.withColumn(
        "_failure_reasons",
        when(
            trim(concat_ws(", ", error_array)) == "", 
            lit("")
        ).otherwise(trim(concat_ws(", ", error_array)))
    ).withColumn(
        "_warning_reasons",
        when(
            trim(concat_ws(", ", warning_array)) == "",
            lit("")
        ).otherwise(trim(concat_ws(", ", warning_array)))
    ).drop(
        "err_missing_encounter",
        "err_missing_patient",
        "err_missing_admission",
        "err_schema",
        "err_chrono",
        "warn_missing_bmi",
        "warn_missing_bp",
        "warn_missing_demographics",
        "warn_missing_medications",
        "warn_missing_conditions",
        "warn_missing_observations",
    )
