import json
from datetime import datetime

from src.serving.snapshot_writer import write_snapshot


def test_write_snapshot_creates_serving_payload(tmp_path):
    snapshot_path = tmp_path / "nested" / "gold_snapshot.json"

    write_snapshot(
        str(snapshot_path),
        clinical_records=[{"condition_name": "Diabetes", "total_encounters": 10}],
        demographic_records=[{"age_group": "40-59", "unique_patients": 4}],
        audit_records=[{"batch_id": 1, "execution_timestamp": datetime(2026, 1, 1)}],
        patient_360_records=[{"patient_id": "patient-1", "risk_score": 45}],
        care_gap_records=[{"patient_id": "patient-1", "care_gap_severity": "WARNING"}],
    )

    payload = json.loads(snapshot_path.read_text(encoding="utf-8"))

    assert payload["clinicalMetrics"][0]["condition_name"] == "Diabetes"
    assert payload["patient360"][0]["risk_score"] == 45
    assert payload["careGaps"][0]["care_gap_severity"] == "WARNING"
    assert "pharmacySafety" not in payload
    assert "careGapMatrix" not in payload
    assert "deterioration" not in payload
    assert "dataCompleteness" not in payload
    assert payload["audit"][0]["batch_id"] == 1
    assert payload["generatedAt"].endswith("Z")
