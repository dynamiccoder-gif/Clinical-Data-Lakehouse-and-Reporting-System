import argparse
import glob
import json
import os
import urllib.request

import pandas as pd


def _read_delta_parquet(table_path):
    files = glob.glob(os.path.join(table_path, "**", "*.parquet"), recursive=True)
    if not files:
        raise FileNotFoundError(f"No parquet files found under {table_path}")
    return pd.concat([pd.read_parquet(path) for path in files], ignore_index=True)


def _send_webhook(webhook_url, payload):
    request = urllib.request.Request(
        webhook_url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=10) as response:
        return response.status


def _latest_audit_record(audit_df):
    ordering = ["execution_timestamp", "batch_id"] if "execution_timestamp" in audit_df.columns else ["batch_id"]
    return audit_df.sort_values(ordering, ascending=[False] * len(ordering)).iloc[0].to_dict()


def check_quality_gate(audit_path, threshold):
    latest = _latest_audit_record(_read_delta_parquet(audit_path))
    pass_rate = float(latest["pass_rate_pct"])
    message = (
        f"Latest healthcare lakehouse quality pass rate is {pass_rate:.2f}% "
        f"for batch {int(latest['batch_id'])}; threshold is {threshold:.2f}%."
    )

    if pass_rate < threshold:
        webhook_url = os.getenv("QUALITY_ALERT_WEBHOOK_URL")
        payload = {
            "text": f"Healthcare Lakehouse Quality Alert: {message}",
            "pass_rate_pct": pass_rate,
            "threshold_pct": threshold,
            "batch_id": int(latest["batch_id"]),
        }
        if webhook_url:
            _send_webhook(webhook_url, payload)
        raise RuntimeError(message)

    print(f"[QUALITY] OK: {message}")


def main():
    parser = argparse.ArgumentParser(description="Fail when the latest audit pass rate is below the target threshold.")
    parser.add_argument("--audit-path", default="lakehouse/audit/metadata")
    parser.add_argument("--threshold", type=float, default=95.0)
    args = parser.parse_args()
    check_quality_gate(args.audit_path, args.threshold)


if __name__ == "__main__":
    main()
