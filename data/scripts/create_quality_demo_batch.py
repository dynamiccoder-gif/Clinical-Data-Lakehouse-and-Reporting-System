import argparse
import os
from datetime import datetime

import pandas as pd


def create_quality_demo_batch(source, target, rows=5000):
    os.makedirs(target, exist_ok=True)
    df = pd.read_csv(source, nrows=rows)

    if len(df) < rows:
        raise ValueError(f"Expected at least {rows} rows, found {len(df)}")

    required_columns = {"ID", "DATE", "PATIENT", "CODE"}
    missing_columns = required_columns - set(df.columns)
    if missing_columns:
        raise ValueError(f"Missing required columns: {sorted(missing_columns)}")

    demo = df.copy()
    for column in ["ID", "DATE", "PATIENT", "CODE"]:
        demo[column] = demo[column].astype("object")

    demo.loc[0:19, "DATE"] = ""
    demo.loc[20:39, "CODE"] = ""
    demo.loc[40:49, "DATE"] = "not-a-date"
    demo.loc[50:59, "PATIENT"] = ""
    demo.loc[60:69, "PATIENT"] = "missing-demo-patient-reference"

    duplicate_source_id = demo.loc[80, "ID"]
    demo.loc[81:90, "ID"] = duplicate_source_id

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_name = f"quality_demo_encounters_{timestamp}.csv"
    output_path = os.path.join(target, output_name)
    demo.to_csv(output_path, index=False)

    print(f"[QUALITY DEMO] Wrote {len(demo):,} rows to {output_path}")
    print("[QUALITY DEMO] Expected hard failures: 70")
    print("[QUALITY DEMO] Expected duplicate encounter IDs skipped: 10")


def main():
    parser = argparse.ArgumentParser(description="Create a controlled bad encounter batch for quality-demo runs.")
    parser.add_argument("--source", required=True)
    parser.add_argument("--target", required=True)
    parser.add_argument("--rows", type=int, default=5000)
    args = parser.parse_args()
    create_quality_demo_batch(args.source, args.target, args.rows)


if __name__ == "__main__":
    main()
