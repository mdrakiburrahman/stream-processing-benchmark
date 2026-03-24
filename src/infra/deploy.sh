#!/bin/bash
# deploy.sh — Deploy Azure infrastructure for the stream processing benchmark
# Usage: deploy.sh --subscription <sub-id> --resource-group <rg-name>
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

# ── Argument parsing ──────────────────────────────────────────────
SUBSCRIPTION=""
RESOURCE_GROUP=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --subscription|-s)  SUBSCRIPTION="$2"; shift 2 ;;
    --resource-group|-g) RESOURCE_GROUP="$2"; shift 2 ;;
    *) echo "Error: Unknown option '$1'"; echo "Usage: deploy.sh --subscription <sub-id> --resource-group <rg-name>"; exit 1 ;;
  esac
done

if [[ -z "$SUBSCRIPTION" || -z "$RESOURCE_GROUP" ]]; then
  echo "Usage: deploy.sh --subscription <sub-id> --resource-group <rg-name>"
  exit 1
fi

LOCATION="westcentralus"
SUFFIX=$(date +%Y%m%d%H%M%S)
NUM_CORES=$(nproc)
BICEP_DIR="src/infra/bicep"

echo "=== Stream Processing Benchmark — Deploy ==="
echo "Subscription:   $SUBSCRIPTION"
echo "Resource Group: $RESOURCE_GROUP"
echo "Location:       $LOCATION"
echo "Suffix:         $SUFFIX"
echo "Cores (nproc):  $NUM_CORES"
echo ""

# ── Verify az CLI login ──────────────────────────────────────────
az account show --subscription "$SUBSCRIPTION" > /dev/null 2>&1 || {
  echo "Error: Not logged in to Azure or subscription not found. Run 'az login' first."
  exit 1
}
az account set --subscription "$SUBSCRIPTION"

# ── Create Resource Group if not exists ───────────────────────────
echo "=== Creating resource group (if not exists) ==="
az group create \
  --name "$RESOURCE_GROUP" \
  --location "$LOCATION" \
  --subscription "$SUBSCRIPTION" \
  --output none
echo "Resource group '$RESOURCE_GROUP' ready."

# ── Deploy Bicep ──────────────────────────────────────────────────
echo ""
echo "=== Deploying Bicep template ==="
az deployment group create \
  --resource-group "$RESOURCE_GROUP" \
  --subscription "$SUBSCRIPTION" \
  --template-file "$BICEP_DIR/main.bicep" \
  --parameters suffix="$SUFFIX" partitionCount=$NUM_CORES \
  --output none

echo "Bicep deployment complete."

# ── Retrieve outputs ──────────────────────────────────────────────
echo ""
echo "=== Retrieving deployment outputs ==="

EH_NAMESPACE=$(az deployment group show \
  --resource-group "$RESOURCE_GROUP" \
  --subscription "$SUBSCRIPTION" \
  --name main \
  --query 'properties.outputs.ehNamespaceName.value' -o tsv)

EH_NAME=$(az deployment group show \
  --resource-group "$RESOURCE_GROUP" \
  --subscription "$SUBSCRIPTION" \
  --name main \
  --query 'properties.outputs.eventHubName.value' -o tsv)

SA_NAME=$(az deployment group show \
  --resource-group "$RESOURCE_GROUP" \
  --subscription "$SUBSCRIPTION" \
  --name main \
  --query 'properties.outputs.storageAccountName.value' -o tsv)

SA_CONTAINER=$(az deployment group show \
  --resource-group "$RESOURCE_GROUP" \
  --subscription "$SUBSCRIPTION" \
  --name main \
  --query 'properties.outputs.containerName.value' -o tsv)

EH_CONN_STR=$(az eventhubs namespace authorization-rule keys list \
  --resource-group "$RESOURCE_GROUP" \
  --subscription "$SUBSCRIPTION" \
  --namespace-name "$EH_NAMESPACE" \
  --name RootManageSharedAccessKey \
  --query 'primaryConnectionString' -o tsv)

SA_KEY=$(az storage account keys list \
  --resource-group "$RESOURCE_GROUP" \
  --subscription "$SUBSCRIPTION" \
  --account-name "$SA_NAME" \
  --query '[0].value' -o tsv)

echo "EH Namespace:  $EH_NAMESPACE"
echo "EH Name:       $EH_NAME"
echo "SA Name:       $SA_NAME"
echo "SA Container:  $SA_CONTAINER"

# ── Hydrate .env from .env.template ───────────────────────────────
echo ""
echo "=== Hydrating .env ==="

ENV_TEMPLATE=".env.template"
ENV_FILE=".env"

if [[ ! -f "$ENV_TEMPLATE" ]]; then
  echo "Error: $ENV_TEMPLATE not found"
  exit 1
fi

cp "$ENV_TEMPLATE" "$ENV_FILE"

sed -i "s|{{EVENTHUB_CONNECTION_STRING}}|${EH_CONN_STR}|g" "$ENV_FILE"
sed -i "s|{{ADLSG2_ACCOUNT_NAME}}|${SA_NAME}|g" "$ENV_FILE"
sed -i "s|{{ADLSG2_ACCOUNT_KEY}}|${SA_KEY}|g" "$ENV_FILE"
sed -i "s|{{NUM_CORES}}|${NUM_CORES}|g" "$ENV_FILE"

echo ".env written successfully."

echo ""
echo "=== Deploy complete ==="
echo "Next: ./src/.scripts/benchmark.sh <duration_seconds>"
