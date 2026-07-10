from pathlib import Path

import yaml

from scripts.init_kafka_topics import topic_specs
from scripts.publish_clinical_topics_to_kafka import envelope, load_cursor, validate_row, write_cursor
from scripts.refresh_kafka_silver_domains import configured_natural_key_fields


DATA_DIR = Path(__file__).resolve().parents[1]


def load_config():
    return yaml.safe_load((DATA_DIR / "config.yaml").read_text(encoding="utf-8"))


def test_nine_clinical_domains_and_dlq_are_configured():
    config = load_config()
    domains = config["kafka"]["domains"]
    specs = topic_specs(config)

    assert set(domains) == {
        "encounters",
        "patients",
        "conditions",
        "medications",
        "observations",
        "procedures",
        "careplans",
        "allergies",
        "immunizations",
    }
    assert len(specs) == 10
    assert ("synthea.encounters", 4) in specs
    assert ("synthea.dlq", 1) in specs


def test_producer_rejects_embedded_headers_and_missing_required_fields():
    assert validate_row({"ID": "ID", "PATIENT": "PATIENT"}, ["ID", "PATIENT"]) == "embedded_csv_header"
    assert validate_row({"ID": "enc-1", "PATIENT": ""}, ["ID", "PATIENT"]) == "missing_required_fields:PATIENT"
    assert validate_row({"ID": "enc-1", "PATIENT": "pat-1"}, ["ID", "PATIENT"]) == ""


def test_envelope_retains_domain_and_producer_lineage():
    event = envelope("encounters", {"ID": "enc-1"}, 42, "demo-run")

    assert event["domain"] == "encounters"
    assert event["producer_run_id"] == "demo-run"
    assert event["producer_row_number"] == 42
    assert event["payload"]["ID"] == "enc-1"


def test_project_shell_exposes_offset_safe_kafka_commands():
    shell = (DATA_DIR.parent / "scripts" / "project.sh").read_text(encoding="utf-8")

    for command in [
        "kafka-init",
        "kafka-publish-all",
        "kafka-ingest",
        "kafka-silver",
        "kafka-features",
        "kafka-dlq-test",
        "kafka-demo",
        "kafka-reset",
        "kafka-cursor-reset",
    ]:
        assert command in shell
    kafka_run_block = shell.split("  kafka-run)", 1)[1].split("    ;;", 1)[0]
    assert "reset_kafka_offsets" not in kafka_run_block


def test_legacy_encounter_producer_delegates_to_validated_publisher():
    legacy = (DATA_DIR / "scripts" / "publish_encounters_to_kafka.py").read_text(encoding="utf-8")

    assert "publish_clinical_topics_to_kafka import main" in legacy
    assert '["--domain", "encounters"]' in legacy


def test_kafka_runner_documents_narrow_spark_411_metrics_workaround():
    engine = (DATA_DIR / "src" / "pipelines" / "streaming_engine.py").read_text(encoding="utf-8")

    assert "SPARK-55271" in engine
    assert '"IterableOps.map" in str(error)' in engine
    assert '"Option.get()" in str(error)' in engine


def test_cursor_round_trip_is_domain_specific(tmp_path):
    cursor_path = tmp_path / "kafka_cursor.json"

    write_cursor(cursor_path, {"encounters": 5000, "patients": 1000})

    assert load_cursor(cursor_path) == {"encounters": 5000, "patients": 1000}


def test_each_domain_configures_a_natural_key():
    domains = load_config()["kafka"]["domains"]

    for domain_config in domains.values():
        assert configured_natural_key_fields(domain_config)


def test_airflow_scheduled_kafka_publisher_uses_persisted_cursor():
    dag = (DATA_DIR / "airflow" / "dags" / "healthcare_lakehouse_dag.py").read_text(encoding="utf-8")

    assert "--use-cursor" in dag
    assert "KAFKA_CURSOR_PATH" in dag


def test_kafka_ui_image_is_pinned_to_a_digest():
    compose = (DATA_DIR.parent / "docker-compose.kafka.yml").read_text(encoding="utf-8")

    assert "provectuslabs/kafka-ui@sha256:" in compose
    assert "provectuslabs/kafka-ui:latest" not in compose
