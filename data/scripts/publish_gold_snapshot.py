import argparse
import inspect
import os
import sys
from pathlib import Path

import yaml
from pyspark.sql import Window
from pyspark.sql.functions import avg, col, count, countDistinct, lit, row_number, sum as spark_sum, when

CURRENT_FILE = Path(globals().get("__file__") or inspect.getfile(inspect.currentframe())).resolve()
sys.path.insert(0, str(CURRENT_FILE.parents[1]))

from src.serving.snapshot_writer import dataframe_to_records, write_snapshot
from src.utils.spark_manager import get_optimized_spark_session


def _load_delta_if_exists(spark, path):
    if not os.path.exists(path):
        return None
    return spark.read.format("delta").load(path)


def _balanced_patient_360_records(patient_360_df, per_band_limit=250):
    if patient_360_df is None:
        return []

    risk_window = Window.partitionBy("risk_band").orderBy(col("risk_score").desc(), col("patient_id"))
    balanced_df = (
        patient_360_df
        .withColumn("dashboard_rank", row_number().over(risk_window))
        .filter(col("dashboard_rank") <= per_band_limit)
        .drop("dashboard_rank")
        .orderBy(
            when(col("risk_band") == "HIGH", lit(1))
            .when(col("risk_band") == "MEDIUM", lit(2))
            .when(col("risk_band") == "LOW", lit(3))
            .otherwise(lit(4)),
            col("risk_score").desc(),
        )
    )
    return dataframe_to_records(balanced_df, limit=per_band_limit * 4)


def main():
    parser = argparse.ArgumentParser(description="Publish Gold Delta metrics to a serving JSON snapshot.")
    parser.add_argument("--config", default="config.yaml")
    args = parser.parse_args()

    with open(args.config, "r", encoding="utf-8") as file:
        config = yaml.safe_load(file)

    spark = get_optimized_spark_session(config["spark"])
    paths = config["paths"]
    serving_config = config.get("serving", {})

    clinical_df = _load_delta_if_exists(spark, paths["gold"])
    demographic_df = _load_delta_if_exists(spark, paths["gold"] + "_demographics")
    patient_360_df = _load_delta_if_exists(spark, paths.get("gold_patient_360", paths["gold"] + "_patient_360"))
    care_gap_df = _load_delta_if_exists(spark, paths.get("gold_care_gaps", paths["gold"] + "_care_gaps"))
    comorbidity_df = _load_delta_if_exists(spark, paths.get("gold_comorbidity_pairs", paths["gold"] + "_comorbidity_pairs"))
    return_gap_df = _load_delta_if_exists(spark, paths.get("gold_return_gaps", paths["gold"] + "_return_gaps"))
    completeness_df = _load_delta_if_exists(spark, paths.get("gold_completeness_scorecard", paths["gold"] + "_completeness_scorecard"))
    disease_rankings_df = _load_delta_if_exists(spark, paths.get("gold_disease_rankings", paths["gold"] + "_disease_rankings"))
    audit_df = _load_delta_if_exists(spark, paths["audit_logs"])

    if clinical_df is None:
        raise FileNotFoundError(f"Gold clinical table not found: {paths['gold']}")

    clinical_records = dataframe_to_records(clinical_df.orderBy("condition_name"), limit=500)
    demographic_records = dataframe_to_records(demographic_df, limit=500) if demographic_df is not None else []
    patient_360_records = _balanced_patient_360_records(patient_360_df)
    care_gap_records = dataframe_to_records(care_gap_df.orderBy(care_gap_df.readmission_risk_score.desc()), limit=250) if care_gap_df is not None else []
    comorbidity_records = dataframe_to_records(comorbidity_df.orderBy(comorbidity_df.patient_count.desc()), limit=50) if comorbidity_df is not None else []
    return_gap_records = dataframe_to_records(return_gap_df, limit=20) if return_gap_df is not None else []
    disease_ranking_records = dataframe_to_records(disease_rankings_df.orderBy(disease_rankings_df.affected_patients.desc()), limit=500) if disease_rankings_df is not None else []
    audit_records = dataframe_to_records(
        audit_df.orderBy(audit_df.execution_timestamp.desc(), audit_df.batch_id.desc()),
        limit=50,
    ) if audit_df is not None else []
    patient_kpis = patient_360_df.agg(
        countDistinct("patient_id").alias("patients_monitored"),
        spark_sum(when(col("risk_band") == "HIGH", lit(1)).otherwise(lit(0))).alias("high_risk_patients"),
        spark_sum(when(col("reason_recent_readmission") == 1, lit(1)).otherwise(lit(0))).alias("recent_return_signal_patients"),
        spark_sum(when(col("reason_polypharmacy") == 1, lit(1)).otherwise(lit(0))).alias("polypharmacy_patients"),
    ).collect()[0].asDict() if patient_360_df is not None else {}
    care_gap_kpis = care_gap_df.agg(
        countDistinct("patient_id").alias("care_gap_patients"),
        countDistinct(when(col("care_gap_severity") == "CRITICAL", col("patient_id"))).alias("critical_care_gaps"),
    ).collect()[0].asDict() if care_gap_df is not None else {}
    kpis = {**patient_kpis, **care_gap_kpis}
    if completeness_df is not None:
        kpis["pipeline_trust_score_pct"] = completeness_df.agg(avg("trust_score_pct")).collect()[0][0]

    snapshot_path = serving_config.get("snapshot_path", "../reports/gold_snapshot.json")
    write_snapshot(
        snapshot_path=snapshot_path,
        clinical_records=clinical_records,
        demographic_records=demographic_records,
        audit_records=audit_records,
        patient_360_records=patient_360_records,
        care_gap_records=care_gap_records,
        comorbidity_records=comorbidity_records,
        return_gap_records=return_gap_records,
        disease_ranking_records=disease_ranking_records,
        kpis=kpis,
    )
    print(f"[SERVING] Wrote serving snapshot: {snapshot_path}")


if __name__ == "__main__":
    main()
