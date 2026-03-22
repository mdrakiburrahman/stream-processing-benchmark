#!/bin/bash
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

DURATION=${1:-120}

[[ -f .env ]] || { echo "Error: Copy .env.template to .env and fill in values"; exit 1; }
source .env

echo "=== Tearing down any running containers ==="
docker compose down --remove-orphans 2>/dev/null || true

echo "=== Cleaning ADLS Gen2 data ==="
az storage fs directory delete \
  --account-name "$ADLSG2_ACCOUNT_NAME" \
  --account-key "$ADLSG2_ACCOUNT_KEY" \
  --file-system "$ADLSG2_CONTAINER" \
  --name spark35 -y 2>/dev/null || true
az storage fs directory delete \
  --account-name "$ADLSG2_ACCOUNT_NAME" \
  --account-key "$ADLSG2_ACCOUNT_KEY" \
  --file-system "$ADLSG2_CONTAINER" \
  --name spark42 -y 2>/dev/null || true
echo "ADLS cleaned."

echo "=== Building images ==="
docker compose build

echo "=== Starting producer ==="
docker compose up -d producer-csharp
echo -n "Waiting for producer to be healthy..."
until [ "$(docker inspect --format='{{.State.Health.Status}}' "$(docker compose ps -q producer-csharp)")" = "healthy" ]; do
  echo -n "."
  sleep 2
done
echo " ready!"

echo "=== Run 1: Spark 3.5 for ${DURATION}s ==="
docker compose up -d spark-consumer-35
sleep "$DURATION"
docker compose stop spark-consumer-35
docker compose rm -f spark-consumer-35
echo "Spark 3.5 run complete."

echo "=== Run 2: Spark 4.2 for ${DURATION}s ==="
docker compose up -d spark-consumer-42
sleep "$DURATION"
docker compose stop spark-consumer-42
docker compose rm -f spark-consumer-42
echo "Spark 4.2 run complete."

echo "=== Stopping producer ==="
docker compose down

echo "=== Running analytics ==="
docker compose up analytics-pyspark
docker compose rm -f analytics-pyspark

echo "=== Done ==="
echo "Results saved to results/"
