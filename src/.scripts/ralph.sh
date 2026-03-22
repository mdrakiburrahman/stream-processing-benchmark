#!/usr/bin/env bash
# Ralph Wiggum — long-running Copilot agent loop
# Usage: ralph.sh <prompt.md> [--mcp <server>]... [-n 10] [--skip-to "Step 4"]
set -euo pipefail

COPILOT="/home/mdrrahman/.local/bin/copilot"
if [[ ! -x "$COPILOT" ]]; then
  echo "Error: Copilot CLI not found at $COPILOT" >&2
  exit 1
fi

# ── Argument parsing ──────────────────────────────────────────────
PROMPT_FILE=""
MCP_SERVERS=()
MAX_ITER=30
SKIP_TO=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mcp)
      MCP_SERVERS+=("$2"); shift 2 ;;
    -n|--iterations)
      MAX_ITER="$2"; shift 2 ;;
    --skip-to)
      SKIP_TO="$2"; shift 2 ;;
    -*)
      echo "Error: Unknown option '$1'" >&2; exit 1 ;;
    *)
      if [[ -z "$PROMPT_FILE" ]]; then
        PROMPT_FILE="$1"; shift
      else
        echo "Error: Unexpected argument '$1'" >&2; exit 1
      fi
      ;;
  esac
done

if [[ -z "$PROMPT_FILE" ]]; then
  echo "Usage: ralph.sh <prompt.md> [--mcp <server>]... [-n <count>] [--skip-to <instruction>]" >&2
  exit 1
fi

if [[ ! -r "$PROMPT_FILE" ]]; then
  echo "Error: Cannot read '$PROMPT_FILE'" >&2
  exit 1
fi

PROMPT=$(<"$PROMPT_FILE")

if [[ -n "$SKIP_TO" ]]; then
  PROMPT="**INSTRUCTION: ${SKIP_TO}** — Skip earlier steps and begin from this point.

${PROMPT}"
fi

# ── Helpers ───────────────────────────────────────────────────────
SEPARATOR="==============================================================="

parse_completion_signal() {
  local output="$1"
  # Check last 20 lines for { "status": "Succeeded"|"Failed" }
  echo "$output" | tail -n 20 | grep -oP '\{\s*"?status"?\s*:\s*"(Succeeded|Failed)"\s*\}' | head -n 1 | grep -oP '(Succeeded|Failed)' || true
}

# ── Main loop ─────────────────────────────────────────────────────
echo "Starting Ralph — Prompt: ${PROMPT_FILE} — Max iterations: ${MAX_ITER}"
[[ -n "$SKIP_TO" ]] && echo "Skip-to: ${SKIP_TO}"
(( ${#MCP_SERVERS[@]} > 0 )) && echo "MCP servers: $(IFS=', '; echo "${MCP_SERVERS[*]}")"

for (( i = 1; i <= MAX_ITER; i++ )); do
  printf '\n%s\n  Ralph Iteration %d of %d\n%s\n' "$SEPARATOR" "$i" "$MAX_ITER" "$SEPARATOR"

  COPILOT_ARGS=()
  for srv in "${MCP_SERVERS[@]}"; do
    COPILOT_ARGS+=(--mcp "$srv")
  done
  COPILOT_ARGS+=(-p "$PROMPT" --yolo)

  OUTPUT=""
  # Run copilot, tee stdout+stderr to terminal while capturing combined output
  OUTPUT=$("$COPILOT" "${COPILOT_ARGS[@]}" 2>&1 | tee /dev/stderr) || true

  SIGNAL=$(parse_completion_signal "$OUTPUT")

  if [[ "$SIGNAL" == "Succeeded" ]]; then
    printf '\n%s\n  Ralph completed successfully!\n  Completed at iteration %d of %d\n%s\n' \
      "$SEPARATOR" "$i" "$MAX_ITER" "$SEPARATOR"
    exit 0
  fi

  if [[ "$SIGNAL" == "Failed" ]]; then
    printf '\n%s\n  Ralph reported failure.\n  Failed at iteration %d of %d\n%s\n' \
      "$SEPARATOR" "$i" "$MAX_ITER" "$SEPARATOR"
    exit 1
  fi

  echo "Iteration ${i} complete — no completion signal found. Continuing..."
  sleep 2
done

echo ""
echo "Ralph reached max iterations (${MAX_ITER}) without completing."
exit 1
