#!/bin/bash
# destroy.sh — Destroy Azure infrastructure for the stream processing benchmark
# Usage: destroy.sh --subscription <sub-id> --resource-group <rg-name>
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

# ── Argument parsing ──────────────────────────────────────────────
SUBSCRIPTION=""
RESOURCE_GROUP=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --subscription|-s)  SUBSCRIPTION="$2"; shift 2 ;;
    --resource-group|-g) RESOURCE_GROUP="$2"; shift 2 ;;
    *) echo "Error: Unknown option '$1'"; echo "Usage: destroy.sh --subscription <sub-id> --resource-group <rg-name>"; exit 1 ;;
  esac
done

if [[ -z "$SUBSCRIPTION" || -z "$RESOURCE_GROUP" ]]; then
  echo "Usage: destroy.sh --subscription <sub-id> --resource-group <rg-name>"
  exit 1
fi

echo "=== Stream Processing Benchmark — Destroy ==="
echo "Subscription:   $SUBSCRIPTION"
echo "Resource Group: $RESOURCE_GROUP"
echo ""

# ── Verify az CLI login ──────────────────────────────────────────
az account show --subscription "$SUBSCRIPTION" > /dev/null 2>&1 || {
  echo "Error: Not logged in to Azure or subscription not found. Run 'az login' first."
  exit 1
}

# ── Check resource group exists ───────────────────────────────────
if ! az group show --name "$RESOURCE_GROUP" --subscription "$SUBSCRIPTION" > /dev/null 2>&1; then
  echo "Resource group '$RESOURCE_GROUP' does not exist. Nothing to destroy."
  exit 0
fi

# ── Delete resource group ─────────────────────────────────────────
echo "Deleting resource group '$RESOURCE_GROUP' and all resources within..."
az group delete \
  --name "$RESOURCE_GROUP" \
  --subscription "$SUBSCRIPTION" \
  --yes
echo "Resource group '$RESOURCE_GROUP' deleted."

# ── Clean up .env ─────────────────────────────────────────────────
if [[ -f .env ]]; then
  rm .env
  echo ".env removed."
fi

echo ""
echo "=== Destroy complete ==="
