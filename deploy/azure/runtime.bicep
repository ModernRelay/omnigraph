@description('Prefix used for the Container App and bootstrap Job names.')
param namePrefix string

@description('Azure region containing the existing Container Apps environment.')
param location string = resourceGroup().location

@description('Existing StorageV2 account created by foundation.bicep.')
param storageAccountName string

@description('Existing private Blob container created by foundation.bicep.')
param containerName string

@description('Non-empty object prefix dedicated to this cluster.')
param clusterPrefix string

@description('Existing Azure Container Registry name.')
param registryName string

@description('Existing user-assigned managed identity name.')
param identityName string

@description('Existing Container Apps environment name.')
param environmentName string

@description('Server image pinned by manifest digest.')
param serverImage string

@description('Bootstrap bundle image pinned by manifest digest.')
param bootstrapImage string

@secure()
@description('Bearer token exposed to the server as the single-token source.')
param serverBearerToken string

@description('Actor recorded by cluster import/apply.')
param bootstrapActor string

@allowed([
  'readiness'
  'apply'
])
@description('Read-only identity readiness probe or the one-shot lease-owning bootstrap.')
param bootstrapMode string = 'apply'

@minValue(60)
@description('Maximum runtime for one bootstrap/readiness replica.')
param bootstrapReplicaTimeoutSeconds int = 1800

@description('Create or update the serving app. Keep false until the bootstrap Job succeeds.')
param activateServer bool = false

@minValue(30)
@description('Inner drain budget owned by the admission wrapper.')
param admissionDrainSeconds int = 90

// Derive rather than independently configure the outer deadline: every direct
// Bicep caller therefore preserves a fixed lease-release window.
var terminationGraceSeconds = admissionDrainSeconds + 60

// The server acquires admission before it opens the remote cluster and binds
// /healthz. A Startup probe keeps Container Apps from applying liveness during
// that lease-owning phase. Ten checks 35 seconds apart leave the deployer's
// five-minute health deadline ahead of the platform's first possible restart.
var serverStartupInitialDelaySeconds = 3
var serverStartupPeriodSeconds = 35
var serverStartupFailureThreshold = 10

var clusterRoot = 'az://${containerName}/${clusterPrefix}'

resource storage 'Microsoft.Storage/storageAccounts@2023-05-01' existing = {
  name: storageAccountName
}

resource registry 'Microsoft.ContainerRegistry/registries@2023-07-01' existing = {
  name: registryName
}

resource identity 'Microsoft.ManagedIdentity/userAssignedIdentities@2023-01-31' existing = {
  name: identityName
}

resource environment 'Microsoft.App/managedEnvironments@2024-03-01' existing = {
  name: environmentName
}

resource bootstrapJob 'Microsoft.App/jobs@2024-03-01' = {
  name: '${namePrefix}-bootstrap'
  location: location
  identity: {
    type: 'UserAssigned'
    userAssignedIdentities: {
      '${identity.id}': {}
    }
  }
  properties: {
    environmentId: environment.id
    configuration: {
      triggerType: 'Manual'
      replicaTimeout: bootstrapReplicaTimeoutSeconds
      replicaRetryLimit: 0
      manualTriggerConfig: {
        parallelism: 1
        replicaCompletionCount: 1
      }
      registries: [
        {
          server: registry.properties.loginServer
          identity: identity.id
        }
      ]
    }
    template: {
      containers: [
        {
          name: 'bootstrap'
          image: bootstrapImage
          env: [
            {
              name: 'OMNIGRAPH_CLUSTER'
              value: clusterRoot
            }
            {
              name: 'OMNIGRAPH_BOOTSTRAP_ACTOR'
              value: bootstrapActor
            }
            {
              name: 'OMNIGRAPH_BOOTSTRAP_MODE'
              value: bootstrapMode
            }
            {
              name: 'OMNIGRAPH_AZURE_ADMISSION_GRACE_SECONDS'
              value: string(admissionDrainSeconds)
            }
            {
              name: 'AZURE_STORAGE_ACCOUNT_NAME'
              value: storage.name
            }
            {
              name: 'AZURE_STORAGE_CLIENT_ID'
              value: identity.properties.clientId
            }
          ]
          resources: {
            cpu: json('1.0')
            memory: '2Gi'
          }
        }
      ]
    }
  }
}

resource server 'Microsoft.App/containerApps@2024-03-01' = if (activateServer) {
  name: '${namePrefix}-server'
  location: location
  identity: {
    type: 'UserAssigned'
    userAssignedIdentities: {
      '${identity.id}': {}
    }
  }
  properties: {
    environmentId: environment.id
    configuration: {
      activeRevisionsMode: 'Single'
      ingress: {
        external: true
        targetPort: 8080
        transport: 'auto'
        allowInsecure: false
      }
      registries: [
        {
          server: registry.properties.loginServer
          identity: identity.id
        }
      ]
      secrets: [
        {
          name: 'server-bearer-token'
          value: serverBearerToken
        }
      ]
    }
    template: {
      terminationGracePeriodSeconds: terminationGraceSeconds
      containers: [
        {
          name: 'server'
          image: serverImage
          args: [
            '--cluster'
            clusterRoot
            '--bind'
            '0.0.0.0:8080'
          ]
          env: [
            {
              name: 'OMNIGRAPH_AZURE_ADMISSION_GRACE_SECONDS'
              value: string(admissionDrainSeconds)
            }
            {
              name: 'OMNIGRAPH_SERVER_BEARER_TOKEN'
              secretRef: 'server-bearer-token'
            }
            {
              name: 'AZURE_STORAGE_ACCOUNT_NAME'
              value: storage.name
            }
            {
              name: 'AZURE_STORAGE_CLIENT_ID'
              value: identity.properties.clientId
            }
          ]
          probes: [
            {
              type: 'Startup'
              httpGet: {
                path: '/healthz'
                port: 8080
                scheme: 'HTTP'
              }
              initialDelaySeconds: serverStartupInitialDelaySeconds
              periodSeconds: serverStartupPeriodSeconds
              timeoutSeconds: 3
              failureThreshold: serverStartupFailureThreshold
            }
            {
              type: 'Readiness'
              httpGet: {
                path: '/healthz'
                port: 8080
                scheme: 'HTTP'
              }
              initialDelaySeconds: 3
              periodSeconds: 5
              timeoutSeconds: 3
              failureThreshold: 12
            }
            {
              type: 'Liveness'
              httpGet: {
                path: '/healthz'
                port: 8080
                scheme: 'HTTP'
              }
              initialDelaySeconds: 15
              periodSeconds: 15
              timeoutSeconds: 3
              failureThreshold: 4
            }
          ]
          resources: {
            cpu: json('1.0')
            memory: '2Gi'
          }
        }
      ]
      scale: {
        minReplicas: 1
        maxReplicas: 1
      }
    }
  }
}

output bootstrapJobName string = bootstrapJob.name
output serverAppName string = '${namePrefix}-server'
output serverFqdn string = activateServer ? server!.properties.configuration.ingress.fqdn : ''
output clusterRoot string = clusterRoot
