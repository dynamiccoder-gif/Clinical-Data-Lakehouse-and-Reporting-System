import json
import math
import os
from datetime import date, datetime
from decimal import Decimal


def _clean_cell_value(value):
    if value is None:
        return ""
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return ""
    return str(value)


def dataframe_to_records(df, limit=None):
    rows = df.limit(limit).collect() if limit else df.collect()
    return [row.asDict(recursive=True) for row in rows]


def write_snapshot(
    snapshot_path,
    clinical_records,
    demographic_records,
    audit_records,
    patient_360_records=None,
    care_gap_records=None,
    comorbidity_records=None,
    return_gap_records=None,
    disease_ranking_records=None,
    kpis=None,
):
    payload = {
        "clinicalMetrics": clinical_records,
        "demographics": demographic_records,
        "patient360": patient_360_records or [],
        "careGaps": care_gap_records or [],
        "comorbidityPairs": comorbidity_records or [],
        "returnGaps": return_gap_records or [],
        "diseaseRankings": disease_ranking_records or [],
        "kpis": kpis or {},
        "audit": audit_records,
        "generatedAt": datetime.utcnow().isoformat() + "Z",
    }
    os.makedirs(os.path.dirname(os.path.abspath(snapshot_path)), exist_ok=True)
    with open(snapshot_path, "w", encoding="utf-8") as snapshot_file:
        json.dump(payload, snapshot_file, indent=2, default=_clean_cell_value)
