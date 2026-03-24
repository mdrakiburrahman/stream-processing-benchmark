#!/bin/bash
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

DURATION=${1:-120}
LOGS_DIR=".logs"
TEMP_DIR=".temp"
RESOURCE_CSV="$TEMP_DIR/resource_stats.csv"
SCRIPT_DIR="src/.scripts"

[[ -f .env ]] || { echo "Error: Copy .env.template to .env and fill in values"; exit 1; }
source .env

wait_healthy() {
  local service=$1
  echo -n "Waiting for $service to be healthy..."
  while true; do
    local cid
    cid=$(docker compose ps -q "$service" 2>/dev/null)
    if [[ -z "$cid" ]]; then
      echo " FAILED (container not found)"
      echo "ERROR: $service exited before becoming healthy. Check $LOGS_DIR/${service}.log"
      return 1
    fi
    local state
    state=$(docker inspect --format='{{.State.Status}}' "$cid" 2>/dev/null || echo "missing")
    if [[ "$state" == "exited" || "$state" == "dead" || "$state" == "missing" ]]; then
      echo " FAILED (container $state)"
      echo "ERROR: $service exited before becoming healthy. Check $LOGS_DIR/${service}.log"
      return 1
    fi
    local health
    health=$(docker inspect --format='{{.State.Health.Status}}' "$cid" 2>/dev/null || echo "none")
    if [[ "$health" == "healthy" ]]; then
      echo " ready!"
      return 0
    fi
    echo -n "."
    sleep 2
  done
}

clean_adls_dir() {
  local dir_name=$1
  az storage fs directory delete \
    --account-name "$ADLSG2_ACCOUNT_NAME" \
    --account-key "$ADLSG2_ACCOUNT_KEY" \
    --file-system "$ADLSG2_CONTAINER" \
    --name "$dir_name" -y 2>/dev/null || true
}

run_consumer() {
  local consumer=$1
  local run_label=$2

  echo "=== ${run_label}: ${consumer} for ${DURATION}s ==="
  docker compose up -d "$consumer"
  docker compose up -d producer-csharp

  docker compose logs -f --no-color "$consumer"      >> "$LOGS_DIR/${consumer}.log" 2>&1 &
  local log_pid_consumer=$!
  docker compose logs -f --no-color producer-csharp   >> "$LOGS_DIR/producer-csharp-${consumer}.log" 2>&1 &
  local log_pid_producer=$!

  wait_healthy "$consumer"
  wait_healthy producer-csharp

  "$SCRIPT_DIR/monitor-resources.sh" "$RESOURCE_CSV" "$consumer" producer-csharp &
  monitor_pid=$!

  sleep "$DURATION"

  docker compose stop "$consumer" producer-csharp
  docker compose rm -f "$consumer" producer-csharp

  if [[ -n "${monitor_pid:-}" ]]; then
    kill "$monitor_pid" 2>/dev/null || true
    wait "$monitor_pid" 2>/dev/null || true
  fi

  sleep 2
  kill "$log_pid_consumer" "$log_pid_producer" 2>/dev/null || true
  wait "$log_pid_consumer" "$log_pid_producer" 2>/dev/null || true

  echo "${run_label} complete."
}

rm -rf "$LOGS_DIR"
mkdir -p "$LOGS_DIR"
rm -rf "$TEMP_DIR"
mkdir -p "$TEMP_DIR"

find results/ -type f ! -name '.gitkeep' -delete 2>/dev/null || true

echo "=== Tearing down any running containers ==="
docker compose down --remove-orphans 2>/dev/null || true

echo "=== Cleaning ADLS Gen2 data ==="
clean_adls_dir flink1.16
clean_adls_dir spark35
clean_adls_dir spark42
clean_adls_dir resource_stats
echo "ADLS cleaned."

echo "=== Building images ==="
docker compose build

run_consumer flink-consumer-116 "Run 1"
run_consumer spark-consumer-35 "Run 2"
run_consumer spark-consumer-42 "Run 3"

docker compose down

echo "=== Uploading resource stats to ADLS ==="
if [[ -f "$RESOURCE_CSV" ]]; then
  az storage blob upload \
    --account-name "$ADLSG2_ACCOUNT_NAME" \
    --account-key "$ADLSG2_ACCOUNT_KEY" \
    --container-name "$ADLSG2_CONTAINER" \
    --name "resource_stats/resource_stats.csv" \
    --file "$RESOURCE_CSV" \
    --overwrite 2>/dev/null
  echo "Resource stats uploaded."
else
  echo "WARNING: No resource stats CSV found at $RESOURCE_CSV"
fi

echo "=== Running analytics ==="
docker compose up analytics-pyspark 2>&1 | tee "$LOGS_DIR/analytics-pyspark.log"
docker compose rm -f analytics-pyspark

echo "=== Done ==="
echo "Results saved to results/"
