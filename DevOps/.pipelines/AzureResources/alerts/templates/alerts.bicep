param environment string
param program string
param dataFactoryName string
param emailForAlerts string
param enableAlerts bool

var increment = 1
var incrementStr = padLeft(increment, 2, '0')
var subscriptionName = toLower(subscription().displayName)
var resourceNameSuffix = '${subscriptionName}-${program}-${environment}'
var shortResourceNameSuffix = environment == 'preprod' ? 'ppd' : environment
var emailForAlertsArray = split(emailForAlerts, ',')

var alertsActionGroupName = 'ag-${resourceNameSuffix}-${incrementStr}'
var alertsActionGroupShortName = 'ag-${shortResourceNameSuffix}-${incrementStr}'

resource alertsActionGroup 'microsoft.insights/actionGroups@2017-04-01' = {
  name: alertsActionGroupName
  location: 'global'
  properties: {
    groupShortName: alertsActionGroupShortName
    enabled: true
    emailReceivers: [for (item, i) in emailForAlertsArray: {
      name: '${alertsActionGroupName}-${padLeft(i, 2, '0')}'
      emailAddress: trim(item)
    }]
    smsReceivers: []
    webhookReceivers: []
  }
}

resource environment_adf_trigger_failure_alert 'microsoft.insights/metricalerts@2018-03-01' = {
  name: 'alert-${resourceNameSuffix}-adf-activities-failure'
  location: 'global'
  properties: {
    description: 'This alert goes off when there are failed activities in ADF'
    severity: 1
    enabled: enableAlerts
    scopes: [
      resourceId('Microsoft.DataFactory/factories', dataFactoryName)
    ]
    evaluationFrequency: 'PT5M'
    windowSize: 'PT5M'
    criteria: {
      allOf: [
        {
          threshold: 1
          name: 'Metric1'
          metricNamespace: 'microsoft.datafactory/factories'
          metricName: 'ActivityFailedRuns'
          operator: 'GreaterThanOrEqual'
          timeAggregation: 'Count'
          criterionType: 'StaticThresholdCriterion'
        }
      ]
      'odata.type': 'Microsoft.Azure.Monitor.SingleResourceMultipleMetricCriteria'
    }
    autoMitigate: true
    targetResourceType: 'Microsoft.DataFactory/factories'
    targetResourceRegion: resourceGroup().location
    actions: [
      {
        actionGroupId: alertsActionGroup.id
        webHookProperties: {
        }
      }
    ]
  }
}
