import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator


DATA_DIR = os.getenv("HEALTHCARE_LAKEHOUSE_DATA_DIR") or Variable.get(
    "healthcare_lakehouse_data_dir",
    default_var="/media/rohit/New Volume/projectX/healthcare_lakehouse/data",
)

default_args = {
    "owner": "clinical-data-platform",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email": [os.getenv("QUALITY_ALERT_EMAIL", "data-ops@example.com")],
}


with DAG(
    dag_id="healthcare_lakehouse_streaming_workflow",
    description="Run Synthea ingestion, Spark medallion processing, scheduled Gold refresh, and quality alerts.",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=["healthcare", "lakehouse", "spark"],
) as dag:
    initialize_kafka_topics = BashOperator(
        task_id="initialize_kafka_topics",
        bash_command=(
            f"cd {DATA_DIR!r} && "
            "if [ \"${STREAMING_SOURCE:-file}\" = \"kafka\" ]; then "
            "PYTHONPATH=. ${PYTHON_BIN:-python3} scripts/init_kafka_topics.py; "
            "else true; fi"
        ),
    )

    ingest_encounters = BashOperator(
        task_id="ingest_encounters",
        bash_command=(
            f"cd {DATA_DIR!r} && "
            "if [ \"${STREAMING_SOURCE:-file}\" = \"kafka\" ]; then "
            "PYTHONPATH=. ${PYTHON_BIN:-python3} scripts/publish_clinical_topics_to_kafka.py "
            "--use-cursor "
            "--cursor-path ${KAFKA_CURSOR_PATH:-lakehouse/checkpoints/kafka_publish_cursor.json} "
            "--max-rows ${KAFKA_MAX_ROWS:-5000} "
            "--producer-run-id ${KAFKA_PRODUCER_RUN_ID:-airflow-${AIRFLOW_CTX_DAG_RUN_ID:-manual}}; "
            "else "
            "${PYTHON_BIN:-python3} simulator.py "
            "--max-chunks ${SIMULATOR_MAX_CHUNKS:-1} "
            "--interval-seconds ${SIMULATOR_INTERVAL_SECONDS:-0}; "
            "fi"
        ),
    )

    land_kafka_bronze_domains = BashOperator(
        task_id="land_kafka_bronze_domains",
        bash_command=(
            f"cd {DATA_DIR!r} && "
            "if [ \"${STREAMING_SOURCE:-file}\" = \"kafka\" ]; then "
            "PYTHONPATH=. ${PYTHON_BIN:-python3} scripts/ingest_kafka_bronze_domains.py; "
            "else true; fi"
        ),
    )

    refresh_kafka_silver_domains = BashOperator(
        task_id="refresh_kafka_silver_domains",
        bash_command=(
            f"cd {DATA_DIR!r} && "
            "if [ \"${STREAMING_SOURCE:-file}\" = \"kafka\" ]; then "
            "PYTHONPATH=. ${PYTHON_BIN:-python3} scripts/refresh_kafka_silver_domains.py; "
            "else true; fi"
        ),
    )

    refresh_features_from_kafka_silver = BashOperator(
        task_id="refresh_features_from_kafka_silver",
        bash_command=(
            f"cd {DATA_DIR!r} && "
            "if [ \"${STREAMING_SOURCE:-file}\" = \"kafka\" ]; then "
            "PYTHONPATH=. ${PYTHON_BIN:-python3} scripts/refresh_features_from_kafka_silver.py; "
            "else true; fi"
        ),
    )

    run_pyspark_streaming_engine = BashOperator(
        task_id="run_pyspark_streaming_engine",
        bash_command=(
            f"cd {DATA_DIR!r} && "
            "PYTHONPATH=. STREAMING_SOURCE=${STREAMING_SOURCE:-file} STREAMING_TRIGGER=${STREAMING_TRIGGER:-availableNow} "
            "${PYTHON_BIN:-python3} pipeline.py"
        ),
        execution_timeout=timedelta(minutes=45),
    )

    build_gold_analytics = BashOperator(
        task_id="build_gold_analytics",
        bash_command=(
            f"cd {DATA_DIR!r} && "
            "PYTHONPATH=. ${PYTHON_BIN:-python3} scripts/build_gold_analytics.py"
        ),
        execution_timeout=timedelta(minutes=45),
    )

    publish_gold_snapshot = BashOperator(
        task_id="publish_gold_snapshot",
        bash_command=(
            f"cd {DATA_DIR!r} && "
            "PYTHONPATH=. ${PYTHON_BIN:-python3} scripts/publish_gold_snapshot.py"
        ),
    )

    alert_on_quality_regression = BashOperator(
        task_id="alert_on_quality_regression",
        bash_command=(
            f"cd {DATA_DIR!r} && "
            "${PYTHON_BIN:-python3} scripts/check_quality_gate.py "
            "--audit-path lakehouse/audit/metadata "
            "--threshold ${QUALITY_GATE_THRESHOLD:-95}"
        ),
    )

    initialize_kafka_topics >> ingest_encounters >> land_kafka_bronze_domains >> refresh_kafka_silver_domains
    refresh_kafka_silver_domains >> refresh_features_from_kafka_silver >> run_pyspark_streaming_engine
    run_pyspark_streaming_engine >> build_gold_analytics >> publish_gold_snapshot >> alert_on_quality_regression
