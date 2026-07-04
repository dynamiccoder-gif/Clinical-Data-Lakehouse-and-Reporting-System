# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Setup And Validate
# MAGIC
# MAGIC This notebook checks that the sample data exists in the Unity Catalog Volume and that the project code can be imported.

# COMMAND ----------

from pathlib import Path
import sys

PROJECT_ROOT = Path.cwd()
DATA_DIR = PROJECT_ROOT / "data"
CONFIG_PATH = DATA_DIR / "config.databricks.yaml"

sys.path.insert(0, str(DATA_DIR))

print(f"Project root: {PROJECT_ROOT}")
print(f"Config path:  {CONFIG_PATH}")

# COMMAND ----------

import yaml

with open(CONFIG_PATH, "r", encoding="utf-8") as config_file:
    config = yaml.safe_load(config_file)

paths = config["paths"]

required_files = [
    paths["source_encounters"],
    paths["raw_patients"],
    paths["raw_conditions"],
    paths["raw_medications"],
    paths["raw_observations"],
    paths["raw_procedures"],
    paths["raw_careplans"],
    paths["raw_allergies"],
    paths["raw_immunizations"],
]

missing = [path for path in required_files if not Path(path).exists()]
if missing:
    raise FileNotFoundError("Missing uploaded Volume files:\n" + "\n".join(missing))

print("All required sample CSV files found.")

# COMMAND ----------

from src.utils.spark_manager import get_optimized_spark_session

spark_session = get_optimized_spark_session(config["spark"])
print("Spark session is ready.")

# COMMAND ----------

display(spark.read.option("header", "true").csv(paths["source_encounters"]).limit(10))

