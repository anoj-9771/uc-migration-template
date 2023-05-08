/*
DESCRIPTION:
  This template was developed to deploy resources required for Databricks Unity Catalog metastore and managed tables storage.
  It requires the following permissions for the account performing this deplyment:
    - 'SWC-Subnet Join' for network write actions.
    - 'SWC-Private Dns Zones Join' to private DNS RG for A record creation.
    - Azure AD > Read directory data	AAD > Microsoft Graph > Directory.Read.All permission.

AUTHOR: Nathanael Wolstencroft
VERSION: 1.3
*/

/*
------------------------
Parameters
------------------------
*/
@description('Resources location. Default is resource group location.')
param location string = resourceGroup().location

@description('Optional name of Datbricks workspace access connector resource, if a new one is to be created.')
param dbwAccessConnectorName string = ''

@description('Name of storage account for managed tables storage.')
param storageAccountName string

@description('Set SKU type of storage account for resiliance.')
@allowed([
  'Standard_LRS'
  'Standard_ZRS'
  'Standard_GRS'
  'Standard_RAGRS'
])
param storageSkuName string = 'Standard_LRS'

@description('Optional array of container names to be created in storage account for Unity.')
param storageAccountContainerNames array = []

@description('Vnet where Private Endpoints will be deployed.')
param peVnetName string

@description('Subnet where Private Endpoints will be deployed.')
param peSubnetName string

@description('ResourceGroup where Vnet/Subnet deployed.')
param vnetResourceGroup string

@description('Subscription ID where DNS private DNS zones deployed. Default is MasterHub.')
param dnsZoneSubscriptionId string = 'd80fb2db-7677-49a6-8a0a-1b546427b71e'

@description('Name of Resource Group where DNS private DNS zones deployed. Default is MasterHub.')
param dnsZoneResourceGroupName string = 'rg-swcmasterhub-dns'

@description('Resource tags for new resources.')
param ResourceTags object

@description('Optional array of resource IDs for other DBW AccessConnectors that need to be added to unity catalogue storage account resourceAccessRules.')
param dbwAccessConnectorIds array = []

/*
------------------------
Global variables
------------------------
*/
// Create array which is combination of optional dbwAccessConnectorIds parameter and optional dbwAccessConnector created in this deployment
var allDbwAccessConnectorIds = concat(dbwAccessConnectorIds, !empty(dbwAccessConnectorName) ? array(dbwAccessConnectorResource.id) : [])

/*
------------------------
External resource references
------------------------
*/
// Reference existing subnet resource to get ID or properties
resource peSubnetResource 'Microsoft.Network/virtualNetworks/subnets@2022-01-01' existing = {
  name: '${peVnetName}/${peSubnetName}'
  scope: resourceGroup(vnetResourceGroup)
}

// Reference existing private DNS zone resource to get ID or properties
resource mhDFSPrivateDNSZoneResource 'Microsoft.Network/privateDnsZones@2020-06-01' existing = {
  name: 'privatelink.dfs.${environment().suffixes.storage}'
  scope: resourceGroup(dnsZoneSubscriptionId, dnsZoneResourceGroupName)
}

// Reference existing private DNS zone resource to get ID or properties
resource mhBlobPrivateDNSZoneResource 'Microsoft.Network/privateDnsZones@2020-06-01' existing = {
  name: 'privatelink.blob.${environment().suffixes.storage}'
  scope: resourceGroup(dnsZoneSubscriptionId, dnsZoneResourceGroupName)
}

/*
------------------------
Resources
------------------------
*/

// Optionally create DBW Access Connector managed identity for Databricks Unity Catalog storage access
resource dbwAccessConnectorResource 'Microsoft.Databricks/accessConnectors@2022-04-01-preview' = if (!empty(dbwAccessConnectorName))  {
  name: !empty(dbwAccessConnectorName) ? dbwAccessConnectorName : 'placeholder'
  location: location
  tags: ResourceTags
  identity: {
    type: 'SystemAssigned'
  }
  properties: {}
}

// Create Storage account for Databricks Unity Catalog storage
resource storageAccountResource 'Microsoft.Storage/storageAccounts@2022-09-01' = {
  name: storageAccountName
  location: location
  tags: ResourceTags
  sku: {
    name: storageSkuName
  }
  kind: 'StorageV2'
  properties: {
    isHnsEnabled: true
    allowBlobPublicAccess: false
    minimumTlsVersion: 'TLS1_2'
    supportsHttpsTrafficOnly: true
    publicNetworkAccess: 'Disabled'
    accessTier: 'Hot'
    allowSharedKeyAccess: false
    networkAcls: {
      defaultAction:  'Deny'
      bypass: 'None'
      resourceAccessRules: [for dbwAccessConnectorId in allDbwAccessConnectorIds: {
        resourceId: dbwAccessConnectorId
        tenantId: tenant().tenantId
      }]
    }
  }

  // Create Storage account Blob service
  resource blobServiceResource 'blobServices' = {
    name: 'default'

    // Create Storage account containers for Unity Catalogue if specified
    resource storageAccountContainerResource 'containers' = [for containerName in storageAccountContainerNames:  {
      name:  !empty(storageAccountContainerNames) ? '${toLower(containerName)}' : 'placeholder'
      properties: {
        publicAccess: 'None'
      }
    }]
  }
}

// Create Storage account DFS Private Endpoint
resource storageAccountDFSPrivateEndpointResource 'Microsoft.Network/privateEndpoints@2022-07-01' = {
  name: 'pe-dfs-${storageAccountResource.name}'
  location: location
  tags: ResourceTags
  properties: {
    privateLinkServiceConnections: [
      {
        name: 'pe-dfs-${storageAccountResource.name}'
        properties: {
          privateLinkServiceId: storageAccountResource.id
          groupIds: ['dfs']
        }
      }
    ]
    subnet: {
      id: peSubnetResource.id
    }
  }

  // Create Storage account DFS Private Endpoint DNS zone group config
  resource storageAccountDFSPrivateEndpointDnsZoneGroup 'privateDnsZoneGroups' = {
    name: 'default'
    properties: {
      privateDnsZoneConfigs: [
        {
          name: 'mhDnsConfig'
          properties: {
            privateDnsZoneId: mhDFSPrivateDNSZoneResource.id
          }
        }
      ]
    }          
  }
}

// Create Storage account Blob Private Endpoint
resource storageAccountBlobPrivateEndpointResource 'Microsoft.Network/privateEndpoints@2022-07-01' = {
  name: 'pe-blob-${storageAccountResource.name}'
  location: location
  tags: ResourceTags
  properties: {
    privateLinkServiceConnections: [
      {
        name: 'pe-blob-${storageAccountResource.name}'
        properties: {
          privateLinkServiceId: storageAccountResource.id
          groupIds: ['blob']
        }
      }
    ]
    subnet: {
      id: peSubnetResource.id
    }
  }

  // Create Storage account Blob Private Endpoint DNS zone group config
  resource storageAccountBlobPrivateEndpointDnsZoneGroup 'privateDnsZoneGroups' = {
    name: 'default'
    properties: {
      privateDnsZoneConfigs: [
        {
          name: 'mhDnsConfig'
          properties: {
            privateDnsZoneId: mhBlobPrivateDNSZoneResource.id
          }
        }
      ]
    }          
  }
}

/*
------------------------
Outputs
------------------------
*/
// Output resource IDs of deployed resources
output storageResourceId string = storageAccountResource.id
output dbwAccessConnectorResourceId string = !empty(dbwAccessConnectorName) ? dbwAccessConnectorResource.id : 'DBW Access Connector not deployed'
