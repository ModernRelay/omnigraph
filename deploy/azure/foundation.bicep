@description('Short lowercase name used as the prefix for deployment resources.')
@minLength(3)
@maxLength(20)
param namePrefix string

@description('Azure region for every regional resource.')
param location string = resourceGroup().location

@description('Private Blob container that owns the OmniGraph cluster root.')
@minLength(3)
@maxLength(63)
param containerName string = 'omnigraph'

var compactPrefix = toLower(replace(namePrefix, '-', ''))
var stableSuffix = take(uniqueString(resourceGroup().id, namePrefix), 12)
// Reserve the complete 12-character stable suffix. Truncating the assembled
// name would make long human prefixes erase most of the globally unique part.
var storageName = 'st${take(compactPrefix, 10)}${stableSuffix}'
var registryName = take('cr${compactPrefix}${stableSuffix}', 50)

resource storage 'Microsoft.Storage/storageAccounts@2023-05-01' = {
  name: storageName
  location: location
  sku: {
    name: 'Standard_LRS'
  }
  kind: 'StorageV2'
  properties: {
    allowBlobPublicAccess: false
    allowSharedKeyAccess: false
    minimumTlsVersion: 'TLS1_2'
    publicNetworkAccess: 'Enabled'
    supportsHttpsTrafficOnly: true
  }
}

resource blobService 'Microsoft.Storage/storageAccounts/blobServices@2023-05-01' = {
  parent: storage
  name: 'default'
  properties: {
    deleteRetentionPolicy: {
      enabled: false
    }
  }
}

resource clusterContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-05-01' = {
  parent: blobService
  name: containerName
  properties: {
    publicAccess: 'None'
  }
}

resource registry 'Microsoft.ContainerRegistry/registries@2023-07-01' = {
  name: registryName
  location: location
  sku: {
    name: 'Basic'
  }
  properties: {
    adminUserEnabled: false
    publicNetworkAccess: 'Enabled'
  }
}

resource identity 'Microsoft.ManagedIdentity/userAssignedIdentities@2023-01-31' = {
  name: '${namePrefix}-identity'
  location: location
}

var blobDataContributorRole = subscriptionResourceId(
  'Microsoft.Authorization/roleDefinitions',
  'ba92f5b4-2d11-453d-a403-e96b0029c9fe'
)
resource blobDataContributor 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(clusterContainer.id, identity.id, blobDataContributorRole)
  scope: clusterContainer
  properties: {
    principalId: identity.properties.principalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: blobDataContributorRole
  }
}

var acrPullRole = subscriptionResourceId(
  'Microsoft.Authorization/roleDefinitions',
  '7f951dda-4ed3-4680-a7ca-43fe172d538d'
)
resource acrPull 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(registry.id, identity.id, acrPullRole)
  scope: registry
  properties: {
    principalId: identity.properties.principalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: acrPullRole
  }
}

resource logs 'Microsoft.OperationalInsights/workspaces@2023-09-01' = {
  name: '${namePrefix}-logs'
  location: location
  properties: {
    retentionInDays: 30
  }
}

resource environment 'Microsoft.App/managedEnvironments@2024-03-01' = {
  name: '${namePrefix}-environment'
  location: location
  properties: {
    appLogsConfiguration: {
      destination: 'log-analytics'
      logAnalyticsConfiguration: {
        customerId: logs.properties.customerId
        sharedKey: logs.listKeys().primarySharedKey
      }
    }
  }
}

output storageAccountName string = storage.name
output containerName string = clusterContainer.name
output registryName string = registry.name
output registryLoginServer string = registry.properties.loginServer
output identityName string = identity.name
output identityId string = identity.id
output identityClientId string = identity.properties.clientId
output environmentName string = environment.name
output environmentId string = environment.id
