from __future__ import annotations

import random
from collections import Counter
from datetime import datetime, timedelta


ERROR_BY_VIOLATION = {
    "missing_encounter_id": "ERR_MISSING_ENCOUNTER_ID",
    "missing_patient_id": "ERR_MISSING_PATIENT_ID",
    "missing_admission_date": "ERR_MISSING_ADMISSION_DATE",
    "missing_diagnosis_code": "ERR_MISSING_ICD10",
    "chronology_violation": "ERR_CHRONO_FLOW",
}


def clean_encounter_rows(row_count: int = 12) -> list[dict]:
    base_admission = datetime(2026, 1, 1)
    rows = []
    for index in range(row_count):
        admission_date = base_admission + timedelta(days=index)
        rows.append(
            {
                "encounter_id": f"enc-{index:03d}",
                "patient_id": f"pat-{index:03d}",
                "admission_date": admission_date,
                "discharge_date": admission_date + timedelta(days=1),
                "diagnosis_code": f"A{index:03d}",
                "bmi": 24.0 + (index % 5),
                "systolic_bp": 118.0,
                "diastolic_bp": 78.0,
                "gender": "F" if index % 2 else "M",
                "race": "white",
                "unique_medications": 1,
                "condition_count": 1,
                "observation_count": 1,
                "expected_violation": "clean",
            }
        )
    return rows


def make_corrupted_batch(clean_rows: list[dict], seed: int = 42, rows_per_rule: int = 2):
    if len(clean_rows) < len(ERROR_BY_VIOLATION) * rows_per_rule:
        raise ValueError("Not enough clean rows for the requested corruption plan.")

    rng = random.Random(seed)
    rows = [dict(row) for row in clean_rows]
    selected_indexes = rng.sample(range(len(rows)), len(ERROR_BY_VIOLATION) * rows_per_rule)

    expected_counts = Counter()
    cursor = 0
    for violation, error_code in ERROR_BY_VIOLATION.items():
        for _ in range(rows_per_rule):
            row = rows[selected_indexes[cursor]]
            cursor += 1
            row["expected_violation"] = violation
            expected_counts[error_code] += 1

            if violation == "missing_encounter_id":
                row["encounter_id"] = ""
            elif violation == "missing_patient_id":
                row["patient_id"] = ""
            elif violation == "missing_admission_date":
                row["admission_date"] = None
            elif violation == "missing_diagnosis_code":
                row["diagnosis_code"] = ""
            elif violation == "chronology_violation":
                row["discharge_date"] = row["admission_date"] - timedelta(days=1)

    return rows, dict(expected_counts)
