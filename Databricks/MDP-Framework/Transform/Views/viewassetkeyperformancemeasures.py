# Databricks notebook source
# MAGIC %run ../../Common/common-helpers

# COMMAND ----------

# MAGIC %run ../../Common/common-transform

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {get_env()}curated.asset_performance.assetkeyperformancemeasures AS
(
   SELECT
fwo.workorderWorkTypeCode,
fwo.workOrderClassDescription,
fwo.workOrderStatusDescription,
fwo.workOrderCompliantIndicator,
dal.assetLocationFacilityCode,
dal.assetLocationFacilityDescription,
loc_hier.assetLocationAncestorLevel83Description as processName,
dac.assetContractNumber,
dac.assetContractDescription,
rst.serviceTypeGroup,
dd.calendarYear,
dd.monthOfYear,
COUNT (DISTINCT workOrderCreationId) breakdownMaintenanceTotalWorkOrderCount,
CASE
  WHEN fwo.workOrderCompliantIndicator = 'YES'
  THEN COUNT (DISTINCT workOrderCreationId)
END compliantBreakdownMaintenanceWorkOrderCount,
(
try_divide(
CASE
  WHEN fwo.workOrderCompliantIndicator = 'YES'
  THEN COUNT (DISTINCT workOrderCreationId)
END, COUNT (DISTINCT workOrderCreationId)
)
) breakdownMaintenanceWorkorderReponseRate,
CASE
  WHEN fwo.workOrderCompliantIndicator = 'YES'
  THEN SUM ( fwo.breakdownMaintenanceWorkOrderTargetHour)
END breakdownMaintenanceWorkOrderTotalTargetHour,
SUM(fwo.breakdownMaintenanceWorkOrderRepairHour) breakdownMaintenanceWorkOrderTotalRepairHourQuantity,
try_divide(SUM ( fwo.breakdownMaintenanceWorkOrderRepairHour),COUNT (DISTINCT workOrderCreationId)) mttrBMHours,
CASE  
  WHEN (fwo.breakdownMaintenanceWorkOrderTargetHour is NULL or fwo.breakdownMaintenanceWorkOrderTargetHour = 0) or fwo.workOrderFinishedDate is NULL then NULL
  ELSE try_divide(sum((fwo.breakdownMaintenanceWorkOrderRepairHour - fwo.breakdownMaintenanceWorkOrderTargetHour)),sum(fwo.breakdownMaintenanceWorkOrderTargetHour))
END breakdownMaintenanceWorkOrderChangeRepairHourPercent,
CASE 
WHEN fwo.workOrderStatusDescription NOT in ('CAN', 'CANDUP', 'DRAFT')
THEN SUM(nvl(fwo.actualWorkOrderLaborCostAmount,0)+
nvl(fwo.actualWorkOrderMaterialCostAmount,0)+
nvl(fwo.actualWorkOrderServiceCostAmount,0)+
nvl(fwo.actualWorkOrderLaborCostFromActivityAmount,0)+
nvl(fwo.actualWorkOrderMaterialCostFromActivityAmount,0)+
nvl(fwo.actualWorkOrderServiceCostFromActivityAmount,0))
END breakdownMaintenanceWorkOrderTotalCostAmount,
CASE 
  WHEN fwo.workOrderStatusDescription NOT in ('CAN', 'CANDUP', 'DRAFT')
  THEN count( DISTINCT fwo.workOrderCreationId) 
END breakdownMaintenanceTotalWorkOrderRaisedCount,
CASE
  WHEN fwo.workOrderStatusDescription not in ('CAN', 'CANDUP', 'DRAFT')
  THEN SUM(da.assetNetworkLengthPerKilometerValue) 
END breakdownMaintenanceWorkOrderFailedLengthValue

from {get_table_namespace('curated', 'viewfactworkordercurrent')} fwo

left join {get_table_namespace('curated', 'dimAsset')} da
on fwo.assetFK = da.assetSK
and da.sourceRecordCurrent = 1

left join {get_table_namespace('curated', 'dimAssetLocation')} dal
on da.assetLocationName = dal.assetLocationName
and dal.sourceRecordCurrent = 1

left join {get_table_namespace('curated', 'dimassetcontract')} dac
on dac.assetContractSK = fwo.assetContractFK
and dac.sourceRecordCurrent = 1

inner join {get_table_namespace('curated', 'dimdate')} dd
on to_date(fwo.workOrderFinishedDate) = dd.calendardate

left join {get_table_namespace('curated', 'viewdimassetlocationancestorhierarchypivot')} loc_hier
on dal.assetLocationSK = loc_hier.assetLocationFK

left join {get_table_namespace('curated', 'viewRefAssetPerformanceServiceType')} rst
on rst.serviceTypeCode = fwo.workOrderServiceTypeCode

where 1=1
and da.assetStatusDescription = 'EXISTING'
and dal.assetLocationStatusDescription = 'OPERATING'
and loc_hier.assetLocationAncestorHierarchySystemName = 'PRIMARY'
and fwo.finishDate >= to_date('2018-07-01')
and fwo._recordCurrent = 1
and fwo.workorderWorkTypeCode = 'BM' 
AND fwo.workOrderClassDescription = 'WORKORDER'

group by
fwo.workorderWorkTypeCode,
fwo.workOrderStatusDescription,
fwo.workOrderClassDescription,
dal.assetLocationFacilityCode,
dal.assetLocationFacilityDescription,
loc_hier.assetLocationAncestorLevel83Description,
dac.assetContractNumber,
dac.assetContractDescription,
rst.serviceTypeGroup,
dd.calendarDate,
fwo.workOrderCompliantIndicator,
fwo.breakdownMaintenanceWorkOrderTargetHour,
fwo.workOrderFinishedDate,
dd.calendarYear,
dd.monthOfYear
)
""")

