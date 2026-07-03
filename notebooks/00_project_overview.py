# Databricks notebook source
# MAGIC %md
# MAGIC # Healthcare Data Lakehouse and Reporting System
# MAGIC
# MAGIC This notebook set deploys the project as a simple Databricks demo:
# MAGIC
# MAGIC 1. Land a small Synthea encounter batch.
# MAGIC 2. Build reusable healthcare feature tables.
# MAGIC 3. Run the Bronze/Silver streaming pipeline with quality gates.
# MAGIC 4. Build Gold Patient 360 analytics.
# MAGIC 5. Publish a dashboard snapshot and register SQL dashboard views.
# MAGIC
# MAGIC The heavy project logic lives in `data/`; these notebooks are thin orchestration and explanation layers.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Main Data Flow
# MAGIC
# MAGIC ```text
# MAGIC Synthea CSV sample
# MAGIC   -> Bronze Delta encounter stream
# MAGIC   -> Silver healthcare enrichment + quality gates
# MAGIC   -> Gold Patient 360, care gaps, disease rankings
# MAGIC   -> Databricks SQL views for dashboarding
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Order
# MAGIC
# MAGIC Run these notebooks in order:
# MAGIC
# MAGIC ```text
# MAGIC 01_setup_and_validate
# MAGIC 02_run_lakehouse_pipeline
# MAGIC 03_register_dashboard_views
# MAGIC 04_dashboard_queries
# MAGIC ```
