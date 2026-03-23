// Event Hub Namespace, Event Hub (topic), and Consumer Groups
// Matches the manually deployed stream-processing-benchmark-1 configuration

@description('Azure region for all resources')
param location string

@description('Unique suffix for globally-unique resource names')
param suffix string

@description('Event Hub Namespace name')
param namespaceName string = 'spb-eh-${suffix}'

@description('Event Hub (topic) name')
param eventHubName string = 'benchmark'

@description('Number of Processing Units for Premium SKU')
param capacity int = 16

@description('Number of partitions on the Event Hub')
param partitionCount int = 100

@description('Message retention in hours')
param retentionTimeInHours int = 1

// ── Event Hub Namespace ──────────────────────────────────────────
resource ehNamespace 'Microsoft.EventHub/namespaces@2024-01-01' = {
  name: namespaceName
  location: location
  sku: {
    name: 'Premium'
    tier: 'Premium'
    capacity: capacity
  }
  properties: {
    isAutoInflateEnabled: false
    maximumThroughputUnits: 0
    kafkaEnabled: true
    zoneRedundant: false
    disableLocalAuth: false
    minimumTlsVersion: '1.2'
    publicNetworkAccess: 'Enabled'
  }
  tags: {
    'hybrid-vm-migration-eventhubnamespaces-shoulddisablelocalauth-skip-flag': 'true'
    'hybrid-vm-migration-eventhubnamespaces-shoulddisablelocalauth-skip-justification': 'mdrrahman is performing a benchmark, this will be deleted ASAP'
  }
}

// ── Event Hub (topic) ────────────────────────────────────────────
resource eventHub 'Microsoft.EventHub/namespaces/eventhubs@2024-01-01' = {
  parent: ehNamespace
  name: eventHubName
  properties: {
    partitionCount: partitionCount
    messageRetentionInDays: 1
    retentionDescription: {
      cleanupPolicy: 'Delete'
      retentionTimeInHours: retentionTimeInHours
    }
  }
}

// ── Consumer Groups ──────────────────────────────────────────────
resource cgSpark35 'Microsoft.EventHub/namespaces/eventhubs/consumergroups@2024-01-01' = {
  parent: eventHub
  name: 'spark35'
}

resource cgSpark42 'Microsoft.EventHub/namespaces/eventhubs/consumergroups@2024-01-01' = {
  parent: eventHub
  name: 'spark42'
}

// ── Outputs ──────────────────────────────────────────────────────
output namespaceName string = ehNamespace.name
output eventHubName string = eventHub.name
