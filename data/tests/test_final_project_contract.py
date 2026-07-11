from pathlib import Path

import pandas as pd

from scripts.check_quality_gate import _latest_audit_record


DATA_DIR = Path(__file__).resolve().parents[1]
ROOT_DIR = DATA_DIR.parent


def test_quality_checker_selects_latest_execution_timestamp_before_batch_id():
    audits = pd.DataFrame([
        {"batch_id": 99, "execution_timestamp": "2026-01-01T00:00:00", "pass_rate_pct": 10.0},
        {"batch_id": 1, "execution_timestamp": "2026-01-02T00:00:00", "pass_rate_pct": 100.0},
    ])

    assert _latest_audit_record(audits)["batch_id"] == 1


def test_dashboard_keeps_only_selected_visible_sections():
    dashboard = (ROOT_DIR / "dashboard" / "clinical_360_streamlit.py").read_text(encoding="utf-8")

    for heading in [
        "Priority Patient Worklist",
        "Population Health Profile",
        "Comorbidity Pairs",
        "Care Gap Worklist",
        "Highest-Risk Diagnoses",
        "Return Gap Distribution",
        "Recent Batch Ledger",
    ]:
        assert heading in dashboard
    for removed_heading in [
        "Medication Safety Review",
        "Patient Deterioration Tracker",
        "Data Completeness Scorecard",
    ]:
        assert removed_heading not in dashboard


def test_airflow_shell_exposes_clean_shutdown():
    shell = (ROOT_DIR / "scripts" / "project.sh").read_text(encoding="utf-8")

    assert "airflow-down" in shell
    assert "stop_airflow_runtime" in shell


def test_care_gap_gold_keeps_one_priority_row_per_patient():
    processor = (DATA_DIR / "src" / "pipelines" / "microbatch_processor.py").read_text(encoding="utf-8")

    assert 'Window.partitionBy("patient_id")' in processor
    assert '"_care_gap_rank"' in processor
    assert 'filter(col("_care_gap_rank") == 1)' in processor
