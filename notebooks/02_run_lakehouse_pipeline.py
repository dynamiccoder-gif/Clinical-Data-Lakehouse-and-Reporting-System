# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Run Lakehouse Pipeline
# MAGIC
# MAGIC This notebook runs the core lakehouse stages using the existing project scripts.

# COMMAND ----------

from pathlib import Path
import runpy
import sys

PROJECT_ROOT = Path.cwd()
DATA_DIR = PROJECT_ROOT / "data"
CONFIG_PATH = DATA_DIR / "config.databricks.yaml"

sys.path.insert(0, str(DATA_DIR))

print(f"Using config: {CONFIG_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Land One Encounter Chunk

# COMMAND ----------

sys.argv = [
    "simulator.py",
    "--source",
    "/Volumes/workspace/default/healthcare_lakehouse/source_dump/encounters_combined.csv",
    "--target",
    "/Volumes/workspace/default/healthcare_lakehouse/raw_ingestion",
    "--max-chunks",
    "1",
    "--chunk-size",
    "40000",
    "--interval-seconds",
    "0",
]
runpy.run_path(str(DATA_DIR / "simulator.py"), run_name="__main__")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Build Healthcare Feature Tables

# COMMAND ----------

sys.argv = ["prepare_feature_tables.py", "--config", str(CONFIG_PATH)]
runpy.run_path(str(DATA_DIR / "scripts" / "prepare_feature_tables.py"), run_name="__main__")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Run Bronze/Silver Streaming Pipeline

# COMMAND ----------

sys.argv = ["pipeline.py", "--config", str(CONFIG_PATH)]
runpy.run_path(str(DATA_DIR / "pipeline.py"), run_name="__main__")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Build Gold Analytics

# COMMAND ----------

sys.argv = ["build_gold_analytics.py", "--config", str(CONFIG_PATH)]
runpy.run_path(str(DATA_DIR / "scripts" / "build_gold_analytics.py"), run_name="__main__")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Publish JSON Snapshot

# COMMAND ----------

sys.argv = ["publish_gold_snapshot.py", "--config", str(CONFIG_PATH)]
runpy.run_path(str(DATA_DIR / "scripts" / "publish_gold_snapshot.py"), run_name="__main__")
