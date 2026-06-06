import argparse
import inspect
import os
import sys
from pathlib import Path

import yaml

CURRENT_FILE = Path(globals().get("__file__") or inspect.getfile(inspect.currentframe())).resolve()
sys.path.insert(0, str(CURRENT_FILE.parents[1]))

from src.utils.spark_manager import get_optimized_spark_session


def _table_exists(path):
    return os.path.exists(path)


def _register_delta_view(spark, view_name, path):
    if not _table_exists(path):
        print(f"[REGISTER] Skipping missing table path: {path}")
        return
    spark.sql(f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM delta.`{path}`")
    print(f"[REGISTER] {view_name} -> {path}")


def _grant_dashboard_access(spark, catalog, schema, volume, principals):
    for principal in principals:
        principal = principal.strip()
        if not principal:
            continue
        spark.sql(f"GRANT USE CATALOG ON CATALOG {catalog} TO `{principal}`")
        spark.sql(f"GRANT USE SCHEMA ON SCHEMA {catalog}.{schema} TO `{principal}`")
        spark.sql(f"GRANT SELECT ON SCHEMA {catalog}.{schema} TO `{principal}`")
        spark.sql(f"GRANT READ VOLUME ON VOLUME {catalog}.{schema}.{volume} TO `{principal}`")
        print(f"[GRANT] Dashboard access refreshed for {principal}")


def main():
    parser = argparse.ArgumentParser(description="Register Clinical 360 Gold Delta outputs as Unity Catalog tables.")
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--catalog", default="workspace")
    parser.add_argument("--schema", default="default")
    parser.add_argument("--volume", default="healthcare_lakehouse")
    parser.add_argument(
        "--dashboard-principals",
        default="rohitkumarrkrd@gmail.com",
        help="Comma-separated users/groups/service principals that need dashboard read access.",
    )
    args = parser.parse_args()

    with open(args.config, "r", encoding="utf-8") as file:
        config = yaml.safe_load(file)

    spark = get_optimized_spark_session(config["spark"])
    paths = config["paths"]

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {args.catalog}.{args.schema}")

    table_specs = {
        "clinical_metrics": paths["gold"],
        "demographics": paths["gold"] + "_demographics",
        "patient_360": paths.get("gold_patient_360", paths["gold"] + "_patient_360"),
        "care_gaps": paths.get("gold_care_gaps", paths["gold"] + "_care_gaps"),
        "comorbidity_pairs": paths.get("gold_comorbidity_pairs", paths["gold"] + "_comorbidity_pairs"),
        "return_gaps": paths.get("gold_return_gaps", paths["gold"] + "_return_gaps"),
        "completeness_scorecard": paths.get("gold_completeness_scorecard", paths["gold"] + "_completeness_scorecard"),
        "disease_rankings": paths.get("gold_disease_rankings", paths["gold"] + "_disease_rankings"),
        "batch_audit": paths["audit_logs"],
        "processed_files": paths.get("processed_files", paths["audit_logs"] + "_processed_files"),
        "patient_last_encounter_state": paths.get(
            "patient_last_encounter_state",
            paths["audit_logs"] + "_patient_last_encounter_state",
        ),
    }

    for table, path in table_specs.items():
        _register_delta_view(spark, f"{args.catalog}.{args.schema}.{table}", path)

    dashboard_principals = [principal.strip() for principal in args.dashboard_principals.split(",")]
    _grant_dashboard_access(spark, args.catalog, args.schema, args.volume, dashboard_principals)


if __name__ == "__main__":
    main()
