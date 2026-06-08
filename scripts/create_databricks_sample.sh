#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SAMPLE_DIR="$ROOT_DIR/data/databricks_sample"
ROWS="${1:-50000}"

mkdir -p "$SAMPLE_DIR/raw_ingestion" "$SAMPLE_DIR/source_dump"

copy_sample() {
  local source="$1"
  local target="$2"
  if [ ! -f "$source" ]; then
    echo "Missing source file: $source" >&2
    exit 1
  fi
  head -n "$((ROWS + 1))" "$source" > "$target"
  echo "Wrote $target"
}

copy_sample "$ROOT_DIR/data/source_dump/encounters_combined.csv" "$SAMPLE_DIR/source_dump/encounters_combined.csv"
copy_sample "$ROOT_DIR/data/raw_ingestion/patients_combined.csv" "$SAMPLE_DIR/raw_ingestion/patients_combined.csv"
copy_sample "$ROOT_DIR/data/raw_ingestion/conditions_combined.csv" "$SAMPLE_DIR/raw_ingestion/conditions_combined.csv"
copy_sample "$ROOT_DIR/data/raw_ingestion/medications_combined.csv" "$SAMPLE_DIR/raw_ingestion/medications_combined.csv"
copy_sample "$ROOT_DIR/data/raw_ingestion/observations_combined.csv" "$SAMPLE_DIR/raw_ingestion/observations_combined.csv"
copy_sample "$ROOT_DIR/data/raw_ingestion/procedures_combined.csv" "$SAMPLE_DIR/raw_ingestion/procedures_combined.csv"
copy_sample "$ROOT_DIR/data/raw_ingestion/careplans_combined.csv" "$SAMPLE_DIR/raw_ingestion/careplans_combined.csv"
copy_sample "$ROOT_DIR/data/raw_ingestion/allergies_combined.csv" "$SAMPLE_DIR/raw_ingestion/allergies_combined.csv"
copy_sample "$ROOT_DIR/data/raw_ingestion/immunizations_combined.csv" "$SAMPLE_DIR/raw_ingestion/immunizations_combined.csv"

du -sh "$SAMPLE_DIR"
