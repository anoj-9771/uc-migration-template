# Databricks notebook source
# MAGIC %run ../../Common/common-transform

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {DEFAULT_TARGET}.viewFactpreventiveMaintenance AS
(
    select 
    preventiveMaintenanceSK
,	preventiveMaintenanceID
,	TO_DATE(preventiveMaintenanceChangedTimestamp) AS preventiveMaintenanceChangedDate
,	workOrderJobPlanFK
,	assetContractFK
,	assetFK
,	preventiveMaintenanceChangedByUserName
,	preventiveMaintenanceDescription
,	preventiveMaintenanceParentId
,	preventiveMaintenanceMasterId
,	preventiveMaintenanceChildIndicator
,	preventiveMaintenanceFrequencyIndicator
,	preventiveMaintenanceFrequencyUnitName
,	preventiveMaintenanceUpdateByMasterOverrideIndicator
,	preventiveMaintenanceFrequencyBasedWorkOrderGenerationIndicator
,	preventiveMaintenanceNonEstimateMeterReadingWorkOrderGenerationIndicator
,	preventiveMaintenanceActivitySundayIndicator
,	preventiveMaintenanceActivityMondayIndicator
,	preventiveMaintenanceActivityTuesdayIndicator
,	preventiveMaintenanceActivityWednesdayIndicator
,	preventiveMaintenanceActivityThursdayIndicator
,	preventiveMaintenanceActivityFridayIndicator
,	preventiveMaintenanceActivitySaturdayIndicator
,	preventiveMaintenanceStatusDescription
,	preventiveMaintenanceAssetLocationRouteCode
,	prevnetiveMaintenanceServiceDepartmentCode
,	preventiveMaintenanceWorkCategoryCode
,	preventiveMaintenanceWorkTypeCode
,	preventiveMaintenanceLAlertLeadDaysQuantity
,	preventiveMaintenanceServiceTypeCode
,	preventiveMaintenanceCategoryCode
,	preventiveMaintenanceStatutoryIndicator
,	preventiveMaintenancePriorityIndicator
,	preventiveMaintenanceDowntimeFlag
,	TO_DATE(preventiveMaintenanceFirstStartTimestamp) as preventiveMaintenanceFirstStartDate
,	TO_DATE(preventiveMaintenanceLastStartTimestamp) as preventiveMaintenanceLastStartDate
,	TO_DATE(preventiveMaintenanceLastCompletionTimestamp) as preventiveMaintenanceLastCompletionDate
,	preventiveMaintenanceAdjustNextDueDateFlag
,	preventiveMaintenanceUniqueIdentifier
,	snapshotDate
,	_recordStart
,	_BusinessKey
,	_DLCuratedZoneTimeStamp
,	_recordEnd
,	_recordCurrent
,	_recordDeleted
 from {DEFAULT_TARGET}.factPreventiveMaintenance where _recordCurrent=1)
""")


# COMMAND ----------


