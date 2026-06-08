#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DATA_DIR="$ROOT_DIR/data"
AIRFLOW_VERSION="${AIRFLOW_VERSION:-3.1.8}"
AIRFLOW_RUNTIME_DIR="$DATA_DIR/airflow/runtime"
AIRFLOW_HOME_DIR="$AIRFLOW_RUNTIME_DIR/home"
AIRFLOW_VENV="$AIRFLOW_RUNTIME_DIR/.venv"

usage() {
  cat <<'EOF'
Healthcare Lakehouse local commands

Usage:
  ./scripts/project.sh setup
  ./scripts/project.sh run [count]
  ./scripts/project.sh kafka-up
  ./scripts/project.sh kafka-init
  ./scripts/project.sh kafka-publish-all [rows_per_topic] [start_row]
  ./scripts/project.sh kafka-ingest
  ./scripts/project.sh kafka-silver
  ./scripts/project.sh kafka-features
  ./scripts/project.sh kafka-dlq-test
  ./scripts/project.sh kafka-demo [rows_per_topic]
  ./scripts/project.sh kafka-run [count] [rows_per_batch]
  ./scripts/project.sh kafka-reset
  ./scripts/project.sh kafka-cursor-reset
  ./scripts/project.sh gold-refresh
  ./scripts/project.sh dashboard
  ./scripts/project.sh airflow-setup
  ./scripts/project.sh airflow-up
  ./scripts/project.sh airflow-down
  ./scripts/project.sh airflow-check
  ./scripts/project.sh reset
  ./scripts/project.sh clean
  ./scripts/project.sh test

Main path:
  setup        Build feature tables from the 8 supporting Synthea files.
  run          Reset demo state and run file-stream batch(es). Default count: 1.
  test         Run pytest checks.
  dashboard    Start the auto-refreshing Streamlit dashboard.

Kafka path:
  kafka-up     Start Kafka and Kafka UI with Docker Compose.
  kafka-init   Create 9 clinical topics and the ingestion DLQ.
  kafka-publish-all
               Publish validated envelopes for all 9 clinical domains.
  kafka-ingest Land available domain events into independent Bronze Delta tables.
  kafka-silver Normalize and deduplicate Kafka Bronze domain events.
  kafka-features
               Upsert Kafka Silver state into reusable Clinical 360 features.
  kafka-dlq-test
               Publish and verify one intentionally invalid ingestion event.
  kafka-demo   Run the complete 9-topic Kafka landing flow and refresh Gold.
  kafka-run    Preserve offsets and run Kafka encounter batch(es). Default count: 3, rows: 5000.
  kafka-reset  Deliberately reset Kafka consumer checkpoints for a replay demo.
  kafka-cursor-reset
               Reset the scheduled producer cursor so Airflow can replay source rows intentionally.

Maintenance:
  reset        Reset streaming checkpoints and generated live encounter chunks.
  clean        Remove generated lakehouse data and the serving snapshot.
  gold-refresh Rebuild Gold analytics from Silver and publish the Streamlit snapshot.
  airflow-setup
               Install a project-volume-backed local Airflow runtime.
  airflow-up   Start local Airflow standalone in the background at http://localhost:8080.
  airflow-down Stop project-backed Airflow scheduler, workers, and UI processes.
  airflow-check
               List parsed Airflow DAGs from the local runtime.
EOF
}

reset_file_demo() {
  rm -rf "$DATA_DIR/lakehouse/checkpoints/bronze"
  find "$DATA_DIR/raw_ingestion" -maxdepth 1 -type f -name 'live_encounters*.csv' -delete
  echo "Reset file-stream checkpoint and generated live encounter chunks. Delta tables are preserved."
}

reset_kafka_offsets() {
  rm -rf "$DATA_DIR/lakehouse/checkpoints/kafka_encounters" "$DATA_DIR/lakehouse/checkpoints/kafka_bronze_domains"
  echo "Reset Kafka consumer checkpoints intentionally. The next Kafka run will replay retained topic events."
}

reset_kafka_publish_cursor() {
  rm -f "$DATA_DIR/lakehouse/checkpoints/kafka_publish_cursor.json"
  echo "Reset scheduled Kafka producer cursor intentionally. The next cursor-based publish starts at source row zero."
}

reset_all() {
  reset_file_demo
  reset_kafka_offsets
  rm -rf "$DATA_DIR/lakehouse"
  rm -f "$ROOT_DIR/reports/gold_snapshot.json"
  echo "Reset all generated Delta tables, feature tables, checkpoints, live chunks, and serving snapshot."
}

setup_features() {
  cd "$DATA_DIR"
  echo "[SETUP] Building Clinical 360 feature tables..."
  PYTHONPATH=. python3 scripts/prepare_feature_tables.py
  echo "[SETUP] Feature tables ready under data/lakehouse/features."
}

run_pipeline_once() {
  cd "$DATA_DIR"
  local source="${STREAMING_SOURCE:-file}"

  if [ "$source" = "kafka" ]; then
    echo "[1/4] Publishing encounter events to Kafka..."
    PYTHONPATH=. python3 scripts/publish_clinical_topics_to_kafka.py \
      --domain encounters \
      --start-row "${KAFKA_START_ROW:-0}" \
      --max-rows "${KAFKA_MAX_ROWS:-5000}" \
      --producer-run-id "${KAFKA_PRODUCER_RUN_ID:-manual-$(date +%Y%m%d%H%M%S)}"
  else
    echo "[1/4] Landing one simulator chunk..."
    python3 simulator.py --max-chunks "${SIMULATOR_MAX_CHUNKS:-1}" --interval-seconds "${SIMULATOR_INTERVAL_SECONDS:-0}"
  fi

  echo "[2/4] Running Spark Bronze/Silver pipeline in bounded mode..."
  PYTHONPATH=. STREAMING_SOURCE="$source" STREAMING_TRIGGER="${STREAMING_TRIGGER:-availableNow}" python3 pipeline.py

  echo "[3/4] Rebuilding scheduled Gold analytics..."
  PYTHONPATH=. python3 scripts/build_gold_analytics.py

  echo "[4/4] Publishing Streamlit serving snapshot..."
  PYTHONPATH=. python3 scripts/publish_gold_snapshot.py

  echo "Done. Gold serving data refreshed at reports/gold_snapshot.json"
}

initialize_kafka_topics() {
  cd "$DATA_DIR"
  PYTHONPATH=. python3 scripts/init_kafka_topics.py
}

publish_all_kafka_domains() {
  cd "$DATA_DIR"
  PYTHONPATH=. python3 scripts/publish_clinical_topics_to_kafka.py \
    --start-row "${2:-${KAFKA_START_ROW:-0}}" \
    --max-rows "${1:-${KAFKA_MAX_ROWS:-5000}}" \
    --producer-run-id "${KAFKA_PRODUCER_RUN_ID:-manual-all-$(date +%Y%m%d%H%M%S)}"
}

land_kafka_bronze_domains() {
  cd "$DATA_DIR"
  PYTHONPATH=. python3 scripts/ingest_kafka_bronze_domains.py
}

refresh_kafka_silver_domains() {
  cd "$DATA_DIR"
  PYTHONPATH=. python3 scripts/refresh_kafka_silver_domains.py
}

refresh_features_from_kafka_silver() {
  cd "$DATA_DIR"
  PYTHONPATH=. python3 scripts/refresh_features_from_kafka_silver.py
}

refresh_gold_from_available_encounters() {
  cd "$DATA_DIR"
  echo "[SILVER] Running encounter-triggered Clinical 360 processor..."
  PYTHONPATH=. STREAMING_SOURCE=kafka STREAMING_TRIGGER=availableNow python3 pipeline.py
  refresh_gold_snapshot
}

refresh_gold_snapshot() {
  cd "$DATA_DIR"
  echo "[GOLD] Rebuilding scheduled Clinical 360 analytics..."
  PYTHONPATH=. python3 scripts/build_gold_analytics.py
  echo "[SERVING] Publishing Streamlit snapshot..."
  PYTHONPATH=. python3 scripts/publish_gold_snapshot.py
}

run_batches() {
  local count="${1:-${BATCH_COUNT:-5}}"
  local start_chunk="${START_CHUNK:-0}"
  local source="${STREAMING_SOURCE:-file}"

  cd "$ROOT_DIR"
  echo "Running $count automatic $source batches..."

  for ((batch=0; batch<count; batch++)); do
    echo
    echo "========== AUTO BATCH $((batch + 1))/$count =========="

    if [ "$source" = "kafka" ]; then
      local rows_per_batch="${KAFKA_MAX_ROWS:-5000}"
      export KAFKA_START_ROW=$((start_chunk * rows_per_batch + batch * rows_per_batch))
      export KAFKA_MAX_ROWS="$rows_per_batch"
    else
      export SIMULATOR_START_CHUNK=$((start_chunk + batch))
      export SIMULATOR_MAX_CHUNKS=1
      export SIMULATOR_INTERVAL_SECONDS="${SIMULATOR_INTERVAL_SECONDS:-0}"
    fi

    STREAMING_SOURCE="$source" run_pipeline_once
  done

  echo
  echo "Done. Processed $count automatic batches."
}

setup_airflow_runtime() {
  mkdir -p "$AIRFLOW_RUNTIME_DIR" "$AIRFLOW_HOME_DIR" "$AIRFLOW_RUNTIME_DIR/tmp" "$AIRFLOW_RUNTIME_DIR/pip-cache"
  if [ ! -x "$AIRFLOW_VENV/bin/airflow" ]; then
    echo "[AIRFLOW] Installing Apache Airflow $AIRFLOW_VERSION under data/airflow/runtime..."
    python3 -m venv "$AIRFLOW_VENV"
    local python_version
    python_version="$("$AIRFLOW_VENV/bin/python" -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
    PIP_CACHE_DIR="$AIRFLOW_RUNTIME_DIR/pip-cache" TMPDIR="$AIRFLOW_RUNTIME_DIR/tmp" \
      "$AIRFLOW_VENV/bin/pip" install \
      "apache-airflow==$AIRFLOW_VERSION" \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-$AIRFLOW_VERSION/constraints-$python_version.txt"
  fi

  PATH="$AIRFLOW_VENV/bin:$PATH" \
    AIRFLOW_HOME="$AIRFLOW_HOME_DIR" \
    AIRFLOW__CORE__DAGS_FOLDER="$DATA_DIR/airflow/dags" \
    "$AIRFLOW_VENV/bin/airflow" db migrate
}

start_airflow_runtime() {
  setup_airflow_runtime
  local pipeline_python="${PYTHON_BIN:-$(command -v python3)}"
  if [ -f "$AIRFLOW_RUNTIME_DIR/standalone.pid" ] && kill -0 "$(cat "$AIRFLOW_RUNTIME_DIR/standalone.pid")" 2>/dev/null; then
    echo "Airflow is already running: http://localhost:8080"
    return
  fi

  echo "[AIRFLOW] Starting standalone scheduler and UI..."
  PATH="$AIRFLOW_VENV/bin:$PATH" \
    AIRFLOW_HOME="$AIRFLOW_HOME_DIR" \
    AIRFLOW__CORE__DAGS_FOLDER="$DATA_DIR/airflow/dags" \
    AIRFLOW__CORE__LOAD_EXAMPLES=false \
    AIRFLOW__CORE__PARALLELISM="${AIRFLOW_PARALLELISM:-2}" \
    AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG="${AIRFLOW_MAX_ACTIVE_TASKS_PER_DAG:-2}" \
    HEALTHCARE_LAKEHOUSE_DATA_DIR="$DATA_DIR" \
    PYTHON_BIN="$pipeline_python" \
    STREAMING_SOURCE="${STREAMING_SOURCE:-kafka}" \
    nohup setsid "$AIRFLOW_VENV/bin/airflow" standalone </dev/null > "$AIRFLOW_RUNTIME_DIR/standalone.log" 2>&1 &
  echo $! > "$AIRFLOW_RUNTIME_DIR/standalone.pid"
  echo "Airflow UI: http://localhost:8080"
  echo "Log:        data/airflow/runtime/standalone.log"
}

stop_airflow_runtime() {
  local groups=()
  mapfile -t groups < <(
    ps -eo pgid=,comm=,args= |
      awk -v executable="$AIRFLOW_VENV/bin/airflow" '$2 !~ /^(awk|bash|sh)$/ && index($0, executable) {print $1}' |
      sort -u
  )
  if [ "${#groups[@]}" -eq 0 ]; then
    rm -f "$AIRFLOW_RUNTIME_DIR/standalone.pid"
    echo "Airflow is not running."
    return
  fi

  echo "[AIRFLOW] Stopping project-backed Airflow process groups..."
  for group in "${groups[@]}"; do
    kill -- "-$group" 2>/dev/null || true
  done
  sleep 2
  for group in "${groups[@]}"; do
    kill -9 -- "-$group" 2>/dev/null || true
  done
  rm -f "$AIRFLOW_RUNTIME_DIR/standalone.pid"
  echo "Airflow stopped."
}

check_airflow_runtime() {
  setup_airflow_runtime
  PATH="$AIRFLOW_VENV/bin:$PATH" \
    AIRFLOW_HOME="$AIRFLOW_HOME_DIR" \
    AIRFLOW__CORE__DAGS_FOLDER="$DATA_DIR/airflow/dags" \
    HEALTHCARE_LAKEHOUSE_DATA_DIR="$DATA_DIR" \
    "$AIRFLOW_VENV/bin/airflow" dags list
}

start_dashboard() {
  cd "$ROOT_DIR"
  streamlit run dashboard/clinical_360_streamlit.py --server.port "${STREAMLIT_PORT:-8501}"
}

case "${1:-help}" in
  setup)
    setup_features
    ;;
  run)
    reset_file_demo
    STREAMING_SOURCE=file run_batches "${2:-1}"
    ;;
  kafka-up)
    cd "$ROOT_DIR"
    docker compose -f docker-compose.kafka.yml up -d
    echo "Kafka broker: localhost:9092"
    echo "Kafka UI:     http://localhost:8085"
    ;;
  kafka-init)
    initialize_kafka_topics
    ;;
  kafka-publish-all)
    publish_all_kafka_domains "${2:-5000}" "${3:-0}"
    ;;
  kafka-ingest)
    land_kafka_bronze_domains
    ;;
  kafka-silver)
    refresh_kafka_silver_domains
    ;;
  kafka-features)
    refresh_features_from_kafka_silver
    ;;
  kafka-dlq-test)
    cd "$DATA_DIR"
    PYTHONPATH=. python3 scripts/rehearse_kafka_dlq.py
    ;;
  kafka-demo)
    initialize_kafka_topics
    publish_all_kafka_domains "${2:-5000}" "${KAFKA_START_ROW:-0}"
    land_kafka_bronze_domains
    refresh_kafka_silver_domains
    refresh_features_from_kafka_silver
    refresh_gold_from_available_encounters
    ;;
  kafka-run)
    export STREAMING_SOURCE=kafka
    export KAFKA_MAX_ROWS="${3:-${KAFKA_MAX_ROWS:-5000}}"
    run_batches "${2:-${BATCH_COUNT:-3}}"
    ;;
  kafka-reset)
    reset_kafka_offsets
    ;;
  kafka-cursor-reset)
    reset_kafka_publish_cursor
    ;;
  dashboard)
    start_dashboard
    ;;
  gold-refresh)
    refresh_gold_snapshot
    ;;
  airflow-setup)
    setup_airflow_runtime
    ;;
  airflow-up)
    start_airflow_runtime
    ;;
  airflow-down)
    stop_airflow_runtime
    ;;
  airflow-check)
    check_airflow_runtime
    ;;
  reset)
    reset_file_demo
    ;;
  clean)
    reset_all
    ;;
  test)
    cd "$DATA_DIR"
    PYTHONPATH=. pytest -q
    ;;
  help|--help|-h)
    usage
    ;;
  *)
    echo "Unknown command: $1" >&2
    echo >&2
    usage >&2
    exit 1
    ;;
esac
