# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Register Dashboard Views
# MAGIC
# MAGIC This notebook creates Databricks SQL views over the Gold Delta outputs.

# COMMAND ----------

from pathlib import Path
import runpy
import sys

PROJECT_ROOT = Path.cwd()
DATA_DIR = PROJECT_ROOT / "data"
CONFIG_PATH = DATA_DIR / "config.databricks.yaml"

sys.path.insert(0, str(DATA_DIR))

# COMMAND ----------

sys.argv = [
    "register_gold_tables.py",
    "--config",
    str(CONFIG_PATH),
    "--catalog",
    "workspace",
    "--schema",
    "default",
]
runpy.run_path(str(DATA_DIR / "scripts" / "register_gold_tables.py"), run_name="__main__")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW VIEWS IN workspace.default;

