KPI_QUERY = """
SELECT
    COUNT(DISTINCT patient_id) AS patients_monitored,
    SUM(CASE WHEN UPPER(risk_band) = 'HIGH' THEN 1 ELSE 0 END) AS high_risk_patients,
    ROUND(AVG(risk_score), 2) AS avg_risk_score
FROM workspace.default.patient_360
"""

RISK_DISTRIBUTION_QUERY = """
SELECT
    COALESCE(risk_band, 'UNKNOWN') AS risk_band,
    COUNT(*) AS patient_count
FROM workspace.default.patient_360
GROUP BY COALESCE(risk_band, 'UNKNOWN')
ORDER BY patient_count DESC
"""

TOP_PATIENTS_QUERY = """
SELECT
    patient_id,
    risk_score,
    risk_band,
    risk_reasons,
    condition_names,
    suggested_focus
FROM workspace.default.patient_360
ORDER BY risk_score DESC
LIMIT 100
"""

CLINICAL_METRICS_QUERY = """
SELECT
    condition_name,
    total_encounters,
    readmission_rate,
    avg_readmission_risk_score,
    high_risk_patients
FROM workspace.default.clinical_metrics
ORDER BY total_encounters DESC
LIMIT 100
"""

CARE_GAPS_QUERY = """
SELECT *
FROM workspace.default.care_gaps
LIMIT 500
"""

COMPLETENESS_QUERY = """
SELECT *
FROM workspace.default.completeness_scorecard
"""

AUDIT_QUERY = """
SELECT *
FROM workspace.default.batch_audit
ORDER BY execution_timestamp DESC
LIMIT 100
"""
