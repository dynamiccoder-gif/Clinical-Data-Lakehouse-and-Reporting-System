import argparse
import inspect
import sys
from pathlib import Path

import yaml

CURRENT_FILE = Path(globals().get("__file__") or inspect.getfile(inspect.currentframe())).resolve()
sys.path.insert(0, str(CURRENT_FILE.parents[1]))

from src.pipelines.microbatch_processor import MicroBatchProcessor
from src.utils.spark_manager import get_optimized_spark_session


def main():
    parser = argparse.ArgumentParser(description="Rebuild Clinical 360 Gold analytics from Silver Delta.")
    parser.add_argument("--config", default="config.yaml")
    args = parser.parse_args()

    with open(args.config, encoding="utf-8") as config_file:
        config = yaml.safe_load(config_file)

    spark = get_optimized_spark_session(config["spark"])
    try:
        MicroBatchProcessor(spark, config).rebuild_gold()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
