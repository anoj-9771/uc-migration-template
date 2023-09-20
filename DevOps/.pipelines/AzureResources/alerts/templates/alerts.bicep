param environment string
param program string
param dataFactoryName string
param emailForAlerts string
param enableAlerts bool
param SQLAlertsActionGroup string

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

resource environment_alert_pr_suppress 'Microsoft.AlertsManagement/actionRules@2021-08-08' = {
  name: 'alert-pr-${resourceNameSuffix}-suppress'
  location: 'Global'
  properties: {
    scopes: [
      resourceId('Microsoft.DataFactory/factories', dataFactoryName)
    ]
    schedule: {
      effectiveFrom: '2023-08-08T00:00:00'
      timeZone: 'AUS Eastern Standard Time'
      recurrences: [
        {
          daysOfWeek: [
            'Monday'
            'Tuesday'
            'Wednesday'
            'Thursday'
            'Friday'
          ]
          recurrenceType: 'Weekly'
          startTime: '17:00:00'
          endTime: '08:00:00'
        }
        {
          daysOfWeek: [
            'Saturday'
            'Sunday'
          ]
          recurrenceType: 'Weekly'
        }
      ]
    }
    conditions: [
      {
        field: 'AlertRuleName'
        operator: 'Equals'
        values: [
          'alert-${resourceNameSuffix}-adf-activities-failure'
        ]
      }
    ]
    enabled: enableAlerts
    actions: [
      {
        actionType: 'RemoveAllActionGroups'
      }
    ]
  }
}

resource Alert_for_SQL_DTU_consumption_in_resource_group_name 'microsoft.insights/metricAlerts@2018-03-01' = {
  name: 'Alert for SQL DTU consumption in resource group ${resourceGroup().name}'
  location: 'global'
  properties: {
    description: 'Alert for SQL Database DTU consumption'
    severity: 2
    enabled: enableAlerts
    scopes: [
      resourceGroup().id
    ]
    evaluationFrequency: 'PT5M'
    windowSize: 'PT5M'
    criteria: {
      allOf: [
        {
          threshold: 80
          name: 'sql-dtu-consumption-percentage'
          metricNamespace: 'Microsoft.Sql/servers/databases'
          metricName: 'dtu_consumption_percent'
          operator: 'GreaterThan'
          timeAggregation: 'Average'
          criterionType: 'StaticThresholdCriterion'
        }
      ]
      'odata.type': 'Microsoft.Azure.Monitor.MultipleResourceMultipleMetricCriteria'
    }
    autoMitigate: true
    targetResourceType: 'Microsoft.Sql/servers/databases'
    targetResourceRegion: resourceGroup().location
    actions: [
      {
        actionGroupId: SQLAlertsActionGroup
        webHookProperties: {
        }
      }
    ]
  }
}

resource Alert_for_SQL_CPU_utilisation_in_resource_group_name 'microsoft.insights/metricAlerts@2018-03-01' = {
  name: 'Alert for SQL CPU utilisation in resource group ${resourceGroup().name}'
  location: 'global'
  properties: {
    description: 'Alert for SQL Database CPU utilisation'
    severity: 2
    enabled: enableAlerts
    scopes: [
      resourceGroup().id
    ]
    evaluationFrequency: 'PT5M'
    windowSize: 'PT5M'
    criteria: {
      allOf: [
        {
          threshold: 80
          name: 'sql-cpu-percentage'
          metricNamespace: 'Microsoft.Sql/servers/databases'
          metricName: 'cpu_percent'
          operator: 'GreaterThan'
          timeAggregation: 'Average'
          criterionType: 'StaticThresholdCriterion'
        }
      ]
      'odata.type': 'Microsoft.Azure.Monitor.MultipleResourceMultipleMetricCriteria'
    }
    autoMitigate: true
    targetResourceType: 'Microsoft.Sql/servers/databases'
    targetResourceRegion: resourceGroup().location
    actions: [
      {
        actionGroupId: SQLAlertsActionGroup
        webHookProperties: {
        }
      }
    ]
  }
}

resource Alert_for_SQL_storage_utilisation_in_resource_group_name 'microsoft.insights/metricAlerts@2018-03-01' = {
  name: 'Alert for SQL storage utilisation in resource group ${resourceGroup().name}'
  location: 'global'
  properties: {
    description: 'Alert for SQL Database storage utilisation'
    severity: 2
    enabled: enableAlerts
    scopes: [
      resourceGroup().id
    ]
    evaluationFrequency: 'PT5M'
    windowSize: 'PT5M'
    criteria: {
      allOf: [
        {
          threshold: 90
          name: 'storage-percentage'
          metricNamespace: 'Microsoft.Sql/servers/databases'
          metricName: 'storage_percent'
          operator: 'GreaterThan'
          timeAggregation: 'Maximum'
          criterionType: 'StaticThresholdCriterion'
        }
      ]
      'odata.type': 'Microsoft.Azure.Monitor.MultipleResourceMultipleMetricCriteria'
    }
    autoMitigate: true
    targetResourceType: 'Microsoft.Sql/servers/databases'
    targetResourceRegion: resourceGroup().location
    actions: [
      {
        actionGroupId: SQLAlertsActionGroup
        webHookProperties: {
        }
      }
    ]
  }
}

resource Alert_for_SQL_log_IO_utilisation_in_resource_group_name 'microsoft.insights/metricAlerts@2018-03-01' = {
  name: 'Alert for SQL log IO utilisation in resource group ${resourceGroup().name}'
  location: 'global'
  properties: {
    description: 'Alert for SQL Database log IO utilisation'
    severity: 2
    enabled: enableAlerts
    scopes: [
      resourceGroup().id
    ]
    evaluationFrequency: 'PT5M'
    windowSize: 'PT5M'
    criteria: {
      allOf: [
        {
          threshold: 80
          name: 'log-write-percentage'
          metricNamespace: 'Microsoft.Sql/servers/databases'
          metricName: 'log_write_percent'
          operator: 'GreaterThan'
          timeAggregation: 'Average'
          criterionType: 'StaticThresholdCriterion'
        }
      ]
      'odata.type': 'Microsoft.Azure.Monitor.MultipleResourceMultipleMetricCriteria'
    }
    autoMitigate: true
    targetResourceType: 'Microsoft.Sql/servers/databases'
    targetResourceRegion: resourceGroup().location
    actions: [
      {
        actionGroupId: SQLAlertsActionGroup
        webHookProperties: {
        }
      }
    ]
  }
}
