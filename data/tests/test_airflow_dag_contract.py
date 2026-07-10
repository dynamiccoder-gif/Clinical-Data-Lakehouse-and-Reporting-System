from pathlib import Path


def test_airflow_dag_matches_current_pipeline_contract():
    dag_path = Path(__file__).resolve().parents[1] / "airflow" / "dags" / "healthcare_lakehouse_dag.py"
    dag_source = dag_path.read_text(encoding="utf-8")

    expected_tasks = [
        "initialize_kafka_topics",
        "ingest_encounters",
        "land_kafka_bronze_domains",
        "refresh_kafka_silver_domains",
        "refresh_features_from_kafka_silver",
        "run_pyspark_streaming_engine",
        "build_gold_analytics",
        "publish_gold_snapshot",
        "alert_on_quality_regression",
    ]

    for task_name in expected_tasks:
        assert task_name in dag_source

    assert "publish_gold_snapshot.py" in dag_source
    assert "publish_clinical_topics_to_kafka.py" in dag_source
    assert "ingest_kafka_bronze_domains.py" in dag_source
    assert "refresh_features_from_kafka_silver.py" in dag_source
    assert "build_gold_analytics.py" in dag_source
    assert "generate_report.py" not in dag_source
    assert "check_quality_gate.py" in dag_source
    removed_serving_tool = "h" + "base"
    assert removed_serving_tool not in dag_source.lower()
