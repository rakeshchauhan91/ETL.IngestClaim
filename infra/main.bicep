// ============================================================
// Health Insurance ETL - Azure Infrastructure
// Deploys: Storage (ADLS Gen2), PostgreSQL Flexible Server,
//          Container Apps Environment + Job, Log Analytics + App Insights,
//          Key Vault for secrets, Managed Identity for passwordless auth.
// Deploy: az deployment group create -g <rg> -f infra/main.bicep -p @infra/main.parameters.json
// ============================================================

@description('Short project prefix, e.g. hins')
param namePrefix string = 'hins'

@description('Azure region')
param location string = resourceGroup().location

@description('Environment: dev / test / prod')
param environmentName string = 'dev'

@secure()
@description('PostgreSQL admin password')
param pgAdminPassword string

@description('Cron schedule for the ETL job (default: daily 2am UTC)')
param cronSchedule string = '0 2 * * *'

var suffix = uniqueString(resourceGroup().id)
var storageAccountName = toLower('${namePrefix}st${suffix}')
var pgServerName = '${namePrefix}-pg-${environmentName}-${suffix}'
var kvName = '${namePrefix}-kv-${suffix}'
var lawName = '${namePrefix}-law-${environmentName}'
var aiName = '${namePrefix}-appi-${environmentName}'
var acaEnvName = '${namePrefix}-cae-${environmentName}'
var acaJobName = '${namePrefix}-etl-job-${environmentName}'
var acrName = toLower('${namePrefix}acr${suffix}')
var identityName = '${namePrefix}-etl-identity-${environmentName}'

// ---------------- Managed Identity (passwordless auth to Storage/KV) ----------------
resource identity 'Microsoft.ManagedIdentity/userAssignedIdentities@2023-01-31' = {
  name: identityName
  location: location
}

// ---------------- Storage (landing/bronze zones) ----------------
resource storage 'Microsoft.Storage/storageAccounts@2023-01-01' = {
  name: storageAccountName
  location: location
  sku: { name: 'Standard_LRS' }
  kind: 'StorageV2'
  properties: {
    isHnsEnabled: true // ADLS Gen2 hierarchical namespace
    minimumTlsVersion: 'TLS1_2'
    allowBlobPublicAccess: false
  }
}

resource blobService 'Microsoft.Storage/storageAccounts/blobServices@2023-01-01' = {
  parent: storage
  name: 'default'
}

resource landingContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-01-01' = {
  parent: blobService
  name: 'landing'
}

resource bronzeContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-01-01' = {
  parent: blobService
  name: 'bronze'
}

// Grant the identity "Storage Blob Data Contributor" on the storage account
resource storageRoleAssignment 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  scope: storage
  name: guid(storage.id, identity.id, 'blob-data-contributor')
  properties: {
    principalId: identity.properties.principalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: subscriptionResourceId(
      'Microsoft.Authorization/roleDefinitions',
      'ba92f5b4-2d11-453d-a403-e96b0029c9fe' // Storage Blob Data Contributor
    )
  }
}

// ---------------- PostgreSQL Flexible Server (Silver/Gold warehouse) ----------------
resource pgServer 'Microsoft.DBforPostgreSQL/flexibleServers@2023-06-01-preview' = {
  name: pgServerName
  location: location
  sku: {
    name: 'Standard_B2s'
    tier: 'Burstable'
  }
  properties: {
    version: '16'
    administratorLogin: 'etl_admin'
    administratorLoginPassword: pgAdminPassword
    storage: { storageSizeGB: 32 }
    backup: { backupRetentionDays: 7, geoRedundantBackup: 'Disabled' }
    highAvailability: { mode: 'Disabled' }
  }
}

resource pgDb 'Microsoft.DBforPostgreSQL/flexibleServers/databases@2023-06-01-preview' = {
  parent: pgServer
  name: 'health_dw'
}

resource pgFirewallAzureServices 'Microsoft.DBforPostgreSQL/flexibleServers/firewallRules@2023-06-01-preview' = {
  parent: pgServer
  name: 'AllowAzureServices'
  properties: {
    startIpAddress: '0.0.0.0'
    endIpAddress: '0.0.0.0'
  }
}

// ---------------- Key Vault (secrets: PG password, App Insights conn string) ----------------
resource keyVault 'Microsoft.KeyVault/vaults@2023-07-01' = {
  name: kvName
  location: location
  properties: {
    tenantId: subscription().tenantId
    sku: { family: 'A', name: 'standard' }
    accessPolicies: [
      {
        tenantId: subscription().tenantId
        objectId: identity.properties.principalId
        permissions: { secrets: ['get', 'list'] }
      }
    ]
  }
}

resource pgPasswordSecret 'Microsoft.KeyVault/vaults/secrets@2023-07-01' = {
  parent: keyVault
  name: 'pg-admin-password'
  properties: { value: pgAdminPassword }
}

// ---------------- Observability ----------------
resource logAnalytics 'Microsoft.OperationalInsights/workspaces@2022-10-01' = {
  name: lawName
  location: location
  properties: { sku: { name: 'PerGB2018' }, retentionInDays: 30 }
}

resource appInsights 'Microsoft.Insights/components@2020-02-02' = {
  name: aiName
  location: location
  kind: 'web'
  properties: {
    Application_Type: 'other'
    WorkspaceResourceId: logAnalytics.id
  }
}

// ---------------- Container Registry (for the pipeline image) ----------------
resource acr 'Microsoft.ContainerRegistry/registries@2023-07-01' = {
  name: acrName
  location: location
  sku: { name: 'Basic' }
  properties: { adminUserEnabled: false }
}

resource acrPullRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  scope: acr
  name: guid(acr.id, identity.id, 'acrpull')
  properties: {
    principalId: identity.properties.principalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: subscriptionResourceId(
      'Microsoft.Authorization/roleDefinitions',
      '7f951dda-4ed3-4680-a7ca-43fe172d538d' // AcrPull
    )
  }
}

// ---------------- Container Apps Environment ----------------
resource acaEnv 'Microsoft.App/managedEnvironments@2023-05-01' = {
  name: acaEnvName
  location: location
  properties: {
    appLogsConfiguration: {
      destination: 'log-analytics'
      logAnalyticsConfiguration: {
        customerId: logAnalytics.properties.customerId
        sharedKey: logAnalytics.listKeys().primarySharedKey
      }
    }
  }
}

// ---------------- Container Apps Job (runs the ETL on a schedule) ----------------
resource etlJob 'Microsoft.App/jobs@2023-05-01' = {
  name: acaJobName
  location: location
  identity: {
    type: 'UserAssigned'
    userAssignedIdentities: { '${identity.id}': {} }
  }
  properties: {
    environmentId: acaEnv.id
    configuration: {
      triggerType: 'Schedule'
      scheduleTriggerConfig: {
        cronExpression: cronSchedule
        parallelism: 1
        replicaCompletionCount: 1
      }
      replicaTimeout: 3600
      replicaRetryLimit: 2
      registries: [
        {
          server: '${acr.name}.azurecr.io'
          identity: identity.id
        }
      ]
      secrets: [
        { name: 'pg-password', keyVaultUrl: '${keyVault.properties.vaultUri}secrets/pg-admin-password', identity: identity.id }
      ]
    }
    template: {
      containers: [
        {
          name: 'etl-pipeline'
          // Replace :latest with an immutable tag/digest per release in CI/CD
          image: '${acr.name}.azurecr.io/health-insurance-etl:latest'
          resources: { cpu: json('1.0'), memory: '2Gi' }
          env: [
            { name: 'AZURE_STORAGE_ACCOUNT_URL', value: storage.properties.primaryEndpoints.blob }
            { name: 'PG_HOST', value: pgServer.properties.fullyQualifiedDomainName }
            { name: 'PG_DB', value: 'health_dw' }
            { name: 'PG_USER', value: 'etl_admin' }
            { name: 'PG_PASSWORD', secretRef: 'pg-password' }
            { name: 'APPLICATIONINSIGHTS_CONNECTION_STRING', value: appInsights.properties.ConnectionString }
            { name: 'MAX_QUARANTINE_RATE', value: '0.10' }
          ]
        }
      ]
    }
  }
}

output storageAccountName string = storage.name
output pgServerFqdn string = pgServer.properties.fullyQualifiedDomainName
output containerRegistry string = acr.properties.loginServer
output appInsightsConnectionString string = appInsights.properties.ConnectionString
output etlJobName string = etlJob.name
