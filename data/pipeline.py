import argparse
import inspect
import os
import sys
from pathlib import Path

import yaml

CURRENT_FILE = Path(globals().get("__file__") or inspect.getfile(inspect.currentframe())).resolve()
sys.path.insert(0, str(CURRENT_FILE.parent))

from src.utils.spark_manager import get_optimized_spark_session
from src.pipelines.streaming_engine import MedallionStreamingCoordinator

def main():
    parser = argparse.ArgumentParser(description="Run the Clinical 360 medallion streaming pipeline.")
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--batch-type", default=None)
    args = parser.parse_args()

    if args.batch_type:
        os.environ["BATCH_TYPE"] = args.batch_type

    with open(args.config, 'r') as file:
        config = yaml.safe_load(file)
    if args.batch_type:
        config["batch_type"] = args.batch_type

    spark = get_optimized_spark_session(config['spark'])
    pipeline = MedallionStreamingCoordinator(spark, config)
    pipeline.start_engine()

if __name__ == "__main__":
    main()
