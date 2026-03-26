#!/bin/bash
# monitor-resources.sh — Poll docker stats and write CSV
# Usage: monitor-resources.sh <output_csv> <container1> [container2 ...]
# Runs until killed (SIGTERM/SIGINT).
set -euo pipefail

OUTPUT_CSV="$1"; shift
CONTAINERS=("$@")

if [[ ${#CONTAINERS[@]} -eq 0 ]]; then
  echo "Usage: monitor-resources.sh <output_csv> <container1> [container2 ...]"
  exit 1
fi

mkdir -p "$(dirname "$OUTPUT_CSV")"

# Always write header if file is new or missing it (handles partial re-runs)
if [[ ! -f "$OUTPUT_CSV" ]] || ! head -1 "$OUTPUT_CSV" 2>/dev/null | grep -q "^timestamp,"; then
  echo "timestamp,container,cpu_pct,mem_mb,net_in_mb,net_out_mb" > "$OUTPUT_CSV"
fi

parse_size_to_mb() {
  local val="$1"
  val=$(echo "$val" | tr -d ' ')
  local num unit
  num=$(echo "$val" | sed 's/[^0-9.]//g')
  unit=$(echo "$val" | sed 's/[0-9.]//g')
  case "$unit" in
    B)   echo "$num" | awk '{printf "%.3f", $1 / 1048576}' ;;
    kB)  echo "$num" | awk '{printf "%.3f", $1 / 1024}' ;;
    KB)  echo "$num" | awk '{printf "%.3f", $1 / 1024}' ;;
    MiB) echo "$num" ;;
    MB)  echo "$num" ;;
    GiB) echo "$num" | awk '{printf "%.3f", $1 * 1024}' ;;
    GB)  echo "$num" | awk '{printf "%.3f", $1 * 1024}' ;;
    TiB) echo "$num" | awk '{printf "%.3f", $1 * 1048576}' ;;
    TB)  echo "$num" | awk '{printf "%.3f", $1 * 1048576}' ;;
    *)   echo "$num" ;;
  esac
}

cleanup() {
  exit 0
}
trap cleanup SIGTERM SIGINT

while true; do
  ts=$(date +%s.%3N)

  for cname in "${CONTAINERS[@]}"; do
    # Get the compose project container ID
    cid=$(docker compose ps -q "$cname" 2>/dev/null || echo "")
    [[ -z "$cid" ]] && continue

    # docker stats --no-stream for one container
    line=$(docker stats --no-stream --format '{{.CPUPerc}}|{{.MemUsage}}|{{.NetIO}}' "$cid" 2>/dev/null || echo "")
    [[ -z "$line" ]] && continue

    cpu_raw=$(echo "$line" | cut -d'|' -f1)
    mem_raw=$(echo "$line" | cut -d'|' -f2)
    net_raw=$(echo "$line" | cut -d'|' -f3)

    # CPU: strip % sign
    cpu_pct=$(echo "$cpu_raw" | tr -d '%' | tr -d ' ')

    # Memory: take usage part (before /)
    mem_usage=$(echo "$mem_raw" | cut -d'/' -f1 | tr -d ' ')
    mem_mb=$(parse_size_to_mb "$mem_usage")

    # Network: "X / Y" → net_in / net_out
    net_in_raw=$(echo "$net_raw" | cut -d'/' -f1 | tr -d ' ')
    net_out_raw=$(echo "$net_raw" | cut -d'/' -f2 | tr -d ' ')
    net_in_mb=$(parse_size_to_mb "$net_in_raw")
    net_out_mb=$(parse_size_to_mb "$net_out_raw")

    echo "${ts},${cname},${cpu_pct},${mem_mb},${net_in_mb},${net_out_mb}" >> "$OUTPUT_CSV"
  done

  sleep 1
done
