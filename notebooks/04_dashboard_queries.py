# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Dashboard Queries
# MAGIC
# MAGIC Use these SQL cells as tiles in a Databricks Dashboard.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(DISTINCT patient_id) AS patients_monitored,
# MAGIC   SUM(CASE WHEN risk_band = 'HIGH' THEN 1 ELSE 0 END) AS high_risk_patients,
# MAGIC   ROUND(AVG(risk_score), 2) AS avg_risk_score
# MAGIC FROM workspace.default.patient_360;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT patient_id, risk_band, risk_score, risk_reasons, suggested_focus
# MAGIC FROM workspace.default.patient_360
# MAGIC ORDER BY risk_score DESC
# MAGIC LIMIT 25;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT care_gap_severity, COUNT(*) AS gap_count
# MAGIC FROM workspace.default.care_gaps
# MAGIC GROUP BY care_gap_severity
# MAGIC ORDER BY gap_count DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT condition_name, affected_patients
# MAGIC FROM workspace.default.disease_rankings
# MAGIC ORDER BY affected_patients DESC
# MAGIC LIMIT 15;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT batch_id, execution_timestamp, source_records_read, silver_records_written, quarantine_records_written, pass_rate_pct
# MAGIC FROM workspace.default.batch_audit
# MAGIC ORDER BY execution_timestamp DESC
# MAGIC LIMIT 10;

