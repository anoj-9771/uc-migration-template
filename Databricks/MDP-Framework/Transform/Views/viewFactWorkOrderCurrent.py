# Databricks notebook source
# MAGIC %run ../../Common/common-transform

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {DEFAULT_TARGET}.viewFactWorkOrderCurrent AS
(
   select 
      	workOrderSK
,	workOrderCreationId
,	TO_DATE(workOrderChangeTimestamp) AS workOrderChangeDate
,	assetFK
,	assetLocationFK
,	assetContractFK
,	workOrderJobPlanFK
,	workOrderProblemTypeFK
,	workOrderId
,	workOrderDescription
,	workOrderChildIndicator
,	parentWorkOrderCreationId
,	workOrderStatusDescription
,	workOrderDispatchSystemName
,	workOrderWorkTypeCode
,	workOrderClassDescription
,	workOrderInitialPriorityCode
,	workOrderServiceDepartmentCode
,	workOrderServiceDepartmentDescription
,	workOrderServiceTypeCode
,	workOrderTaskCode
,	workOrderFinancialControlIdentfier
,	workOrderAssessedPriorityCode
,	billedWorkOrderUnitCount
,	actualWorkOrderLabourHoursQuantity
,	actualWorkOrderLaborCostAmount
,	actualWorkOrderMaterialCostAmount
,	actualWorkOrderServiceCostAmount
,	actualWorkOrderLaborCostFromActivityAmount
,	actualWorkOrderMaterialCostFromActivityAmount
,	actualWorkOrderServiceCostFromActivityAmount
,	estimatedWorkOrderLaborHoursQuantity
,	estimatedWorkOrderRemainingHoursQuantity
,	TO_DATE(actualWorkOrderFinishTimestamp) AS actualWorkOrderFinishDate
,	actualWorkOrderInternalLaborCostAmount	
,	actualWorkOrderInternalLaborHoursQuantity	
,	actualWorkOrderExternalLaborCostAmount	
,	actualWorkOrderExternalLaborHoursQuantity	
,	TO_DATE(actualWorkOrderStartTimestamp) AS actualWorkOrderStartDate
,	actualWorkOrderToolCostAmount
,	estimatedWorkOrderLaborCostAmount	
,	estimatedWorkOrderMaterialCostAmount	
,	estimatedWorkOrderServiceCostAmount	
,	estimatedWorkOrderToolCostAmount	
,	estimatedWorkOrderInternalLaborCostAmount	
,	estimatedWorkOrderInternalLaborHoursQuantity	
,	estimatedWorkOrderExternalLaborHoursAmount	
,	estimatedWorkOrderExternalLaborHoursQuantity	
,	workOrderTargetDescription
,	TO_DATE(workOrderCreationTimestamp) AS workOrderCreationDate
,	TO_DATE(workOrderServiceProviderNotifiedTimestamp
) AS workOrderServiceProviderNotifiedDate
,	TO_DATE(workOrderTargetFinishTimestamp) AS workOrderTargetFinishDate
,	TO_DATE(workOrderTargetStartTimestamp) AS workOrderTargetStartDate
,	TO_DATE(workOrderscheduledStartTimestamp) AS workOrderscheduledStartDate
,	TO_DATE(workOrderscheduledFinishTimestamp) AS workOrderscheduledFinishDate
,	TO_DATE(workOrderTargetRespondByTimestamp) AS workOrderTargetRespondByDate
,	TO_DATE(workOrderReportedTimestamp) AS workOrderReportedDate
,	workOrderExternalStatusCode
,	TO_DATE(workOrderExternalStatusTimestamp) AS externalStatusDate
,	TO_DATE(workOrderScheduledTimestamp) AS workOrderScheduledDate
,	TO_DATE(workOrderInProgressTimestamp) AS workOrderInProgressDate
,	TO_DATE(workOrderCancelledTimestamp) AS workOrderCancelledDate
,	TO_DATE(workOrderApprovedTimestamp) AS workOrderApprovedDate
,	TO_DATE(workOrderCompletedTimestamp) AS workOrderCompletedDate
,	TO_DATE(workOrderClosedTimestamp) AS workOrderClosedDate
,	TO_DATE(workOrderFinishedTimestamp) AS workOrderFinishedDate
,	snapshotDate
,	TO_DATE(workOrderTrendTimestamp) AS workOrderTrendDate
,	TO_DATE(calculatedWorkOrderTargetDateTimestamp) AS calculatedTargetDate
,	preventiveMaintenanceWorkOrderFrequencyIndicator
,	preventiveMaintenanceWorkOrderFrequencyUnitName
,	TO_DATE(workOrderTolerancedDueTimestamp) AS workOrderTolerancedDueDate
,	workOrderServiceContract
,	TO_DATE(workOrderStatusCloseTimestamp) AS workOrderStatusCloseDate
,	TO_DATE(workOrderFinishTimestamp) AS finishDate
,	TO_DATE(breakdownMaintenancePriorityToleranceDate) AS breakdownMaintenancePriorityToleranceDate
,	TO_DATE(actualStartDateTimestamp) AS actualStartDate
,	workOrderCompliantIndicator
,	relatedCorrectiveMaintenanceWorkorderCount
,	TO_DATE(workorderAcceptedLogStatusMinDate) AS workorderAcceptedLogStatusMinDate
,	calculatedWorkOrderTargetYear
,	calculatedWorkOrderTargetMonth
,	workOrderFinishYear
,	workOrderFinishMonth
,	workOrderTargetPeriod
,	workOrderFinishPeriod
,	workOrderFinishedBeforeTargetMonthIndicator
,	breakdownMaintenanceWorkOrderTargetHour
,	breakdownMaintenanceWorkOrderRepairHour
,   workOrderTotalCostAmount
,	_recordStart
,	_BusinessKey
,	_DLCuratedZoneTimeStamp
,	_recordEnd
,	_recordCurrent
,	_recordDeleted  
    from (
            select *, row_number() over(partition by workOrderCreationId order by snapshotDate desc) as rownumb from {DEFAULT_TARGET}.factWorkOrder where _recordCurrent=1
            )dt where rownumb = 1
)
""")


# COMMAND ----------


