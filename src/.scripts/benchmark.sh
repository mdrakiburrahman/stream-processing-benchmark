#!/bin/bash
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

DURATION=${1:-120}
LOGS_DIR=".logs"

[[ -f .env ]] || { echo "Error: Copy .env.template to .env and fill in values"; exit 1; }
source .env

wait_healthy() {
  local service=$1
  echo -n "Waiting for $service to be healthy..."
  until [ "$(docker inspect --format='{{.State.Health.Status}}' "$(docker compose ps -q "$service")")" = "healthy" ]; do
    echo -n "."
    sleep 2
  done
  echo " ready!"
}

clean_adls_dir() {
  local dir_name=$1
  az storage fs directory delete \
    --account-name "$ADLSG2_ACCOUNT_NAME" \
    --account-key "$ADLSG2_ACCOUNT_KEY" \
    --file-system "$ADLSG2_CONTAINER" \
    --name "$dir_name" -y 2>/dev/null || true
}

run_spark() {
  local consumer=$1
  local run_label=$2

  echo "=== ${run_label}: ${consumer} for ${DURATION}s ==="
  docker compose up -d "$consumer"
  docker compose up -d producer-csharp
  wait_healthy "$consumer"
  wait_healthy producer-csharp
  sleep "$DURATION"
  docker compose logs --no-color "$consumer" > "$LOGS_DIR/${consumer}.log" 2>&1
  docker compose logs --no-color producer-csharp > "$LOGS_DIR/producer-csharp-${consumer}.log" 2>&1
  docker compose stop "$consumer" producer-csharp
  docker compose rm -f "$consumer" producer-csharp
  echo "${run_label} complete."
}

rm -rf "$LOGS_DIR"
mkdir -p "$LOGS_DIR"

echo "=== Tearing down any running containers ==="
docker compose down --remove-orphans 2>/dev/null || true

echo "=== Cleaning ADLS Gen2 data ==="
clean_adls_dir spark35
clean_adls_dir spark42
echo "ADLS cleaned."

echo "=== Building images ==="
docker compose build

run_spark spark-consumer-35 "Run 1"
run_spark spark-consumer-42 "Run 2"

docker compose down

echo "=== Running analytics ==="
docker compose up analytics-pyspark 2>&1 | tee "$LOGS_DIR/analytics-pyspark.log"
docker compose rm -f analytics-pyspark

echo "=== Done ==="
echo "Results saved to results/"
