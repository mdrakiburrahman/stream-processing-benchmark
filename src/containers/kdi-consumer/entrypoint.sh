#!/bin/bash
set -euo pipefail

# ---------------------------------------------------------------------------
# Map benchmark environment variables to Azure SDK expected names
# ---------------------------------------------------------------------------
export AZURE_STORAGE_ACCOUNT_NAME="${ADLSG2_ACCOUNT_NAME}"
export AZURE_STORAGE_ACCOUNT_KEY="${ADLSG2_ACCOUNT_KEY}"

# ---------------------------------------------------------------------------
# Parse EventHub connection string → Kafka bootstrap servers + SASL password
# ---------------------------------------------------------------------------
NAMESPACE=$(echo "$EVENTHUB_CONNECTION_STRING" | grep -oP 'Endpoint=sb://\K[^.]+')
BOOTSTRAP_SERVERS="${NAMESPACE}.servicebus.windows.net:9093"
SASL_PASSWORD="$EVENTHUB_CONNECTION_STRING"

TOPIC="${EVENTHUB_NAME:-benchmark}"
TABLE_URI="abfss://${ADLSG2_CONTAINER}@${ADLSG2_ACCOUNT_NAME}.dfs.core.windows.net/${DELTA_TABLE_PATH:-kdi/benchmark}"
CONSUMER_GROUP="${CONSUMER_GROUP:-kdi}"

# ---------------------------------------------------------------------------
# Pre-create Delta table if it does not exist
# ---------------------------------------------------------------------------
echo "Ensuring Delta table exists at ${TABLE_URI} ..."
python3 /opt/create_table.py "$TABLE_URI"

# ---------------------------------------------------------------------------
# Lifecycle management
# ---------------------------------------------------------------------------
cleanup() {
  rm -f /tmp/kdi-healthy
  if [[ -n "${KDI_PID:-}" ]]; then
    kill "$KDI_PID" 2>/dev/null || true
    wait "$KDI_PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT SIGTERM SIGINT

# ---------------------------------------------------------------------------
# Launch kafka-delta-ingest with low-latency tuning
# ---------------------------------------------------------------------------
/build/kafka-delta-ingest ingest \
  "$TOPIC" \
  "$TABLE_URI" \
  --kafka "$BOOTSTRAP_SERVERS" \
  --consumer_group "$CONSUMER_GROUP" \
  --app_id "kdi-benchmark" \
  --auto_offset_reset latest \
  --allowed_latency "${ALLOWED_LATENCY:-2}" \
  --max_messages_per_batch "${MAX_MESSAGES_PER_BATCH:-50000}" \
  --min_bytes_per_file "${MIN_BYTES_PER_FILE:-262144}" \
  -K "security.protocol=SASL_SSL" \
  -K "sasl.mechanism=PLAIN" \
  -K 'sasl.username=$ConnectionString' \
  -K "sasl.password=${SASL_PASSWORD}" \
  -K "fetch.min.bytes=${KAFKA_FETCH_MIN_BYTES:-1}" \
  -K "fetch.wait.max.ms=${KAFKA_FETCH_MAX_WAIT_MS:-100}" \
  -K "socket.receive.buffer.bytes=${KAFKA_RECEIVE_BUFFER_BYTES:-10485760}" \
  -K "session.timeout.ms=${KAFKA_SESSION_TIMEOUT_MS:-60000}" \
  &

KDI_PID=$!

# Wait for process to stabilise, then signal healthy
sleep 5
if kill -0 "$KDI_PID" 2>/dev/null; then
  touch /tmp/kdi-healthy
  echo "kafka-delta-ingest is running (PID ${KDI_PID})"
fi

wait "$KDI_PID"
