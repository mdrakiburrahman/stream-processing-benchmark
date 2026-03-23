// ADLS Gen2 Storage Account and Blob Container
// Matches the manually deployed streamprocbench1 configuration

@description('Azure region for all resources')
param location string

@description('Unique suffix for globally-unique resource names')
param suffix string

@description('Storage account name (max 24 chars, lowercase alphanumeric)')
param storageAccountName string = 'spbsa${suffix}'

@description('Blob container name')
param containerName string = 'benchmark'

// ── Storage Account (ADLS Gen2) ──────────────────────────────────
resource storageAccount 'Microsoft.Storage/storageAccounts@2023-05-01' = {
  name: storageAccountName
  location: location
  kind: 'StorageV2'
  sku: {
    name: 'Standard_LRS'
  }
  properties: {
    isHnsEnabled: true
    accessTier: 'Hot'
    allowBlobPublicAccess: false
    allowSharedKeyAccess: true
    allowCrossTenantReplication: false
    minimumTlsVersion: 'TLS1_2'
    supportsHttpsTrafficOnly: true
    publicNetworkAccess: 'Enabled'
    networkAcls: {
      defaultAction: 'Allow'
      bypass: 'AzureServices'
    }
  }
  tags: {
    'hybrid-vm-migration-storageaccounts-shoulddisablelocalauth-skip-flag': 'true'
    'hybrid-vm-migration-storageaccounts-shoulddisablelocalauth-skip-justification': 'mdrrahman is performing a benchmark, this will be deleted ASAP'
  }
}

// ── Blob Service (implicit) ───────────────────────────────────────
resource blobService 'Microsoft.Storage/storageAccounts/blobServices@2023-05-01' = {
  parent: storageAccount
  name: 'default'
}

// ── Blob Container ───────────────────────────────────────────────
resource blobContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-05-01' = {
  parent: blobService
  name: containerName
  properties: {
    publicAccess: 'None'
  }
}

// ── Outputs ──────────────────────────────────────────────────────
output storageAccountName string = storageAccount.name
output containerName string = containerName
