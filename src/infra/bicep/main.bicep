// Main orchestrator — deploys Event Hub and Storage modules
targetScope = 'resourceGroup'

@description('Azure region for all resources')
param location string = resourceGroup().location

@description('Unique suffix for globally-unique resource names (e.g. timestamp)')
param suffix string

@description('Number of Event Hub partitions (default: host core count or 100)')
param partitionCount int = 100

// ── Event Hub ────────────────────────────────────────────────────
module eventHub 'modules/eventhub.bicep' = {
  name: 'eventhub-${suffix}'
  params: {
    location: location
    suffix: suffix
    partitionCount: partitionCount
  }
}

// ── Storage Account ──────────────────────────────────────────────
module storage 'modules/storage.bicep' = {
  name: 'storage-${suffix}'
  params: {
    location: location
    suffix: suffix
  }
}

// ── Outputs ──────────────────────────────────────────────────────
output ehNamespaceName string = eventHub.outputs.namespaceName
output eventHubName string = eventHub.outputs.eventHubName
output storageAccountName string = storage.outputs.storageAccountName
output containerName string = storage.outputs.containerName
