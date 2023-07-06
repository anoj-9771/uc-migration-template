# Databricks notebook source
# MAGIC %run ../../Common/common-transform 

# COMMAND ----------

TARGET = DEFAULT_TARGET

# COMMAND ----------

# CleanSelf()

# COMMAND ----------

def Transform():
    global df
    # ------------- TABLES ----------------- #

    #   Cleansed WorkOrder table has multiple entries for the same date since the history is managed. Curated requirement is to pick the latest record for that specific date.
    workOrder_windowSpec  = Window.partitionBy("workOrder","changedDate_date_part")
    df = get_recent_records(f"{SOURCE}","maximo_workOrder","changedDate","workOrderChangeTimestamp").alias("wo") \
    .withColumn("changedDate_date_part",to_date(col("changedDate"))) \
    .withColumn("rank",rank().over(workOrder_windowSpec.orderBy(col("changedDate").desc()))) \
    .filter("rank == 1").drop("rank")
    child_df = GetTable(get_table_name(f"{SOURCE}","maximo","workOrder")).filter("_RecordDeleted == 0").select(col("workOrder").alias("childWorkOrder"),col("parentWo").alias("childParentWo"),col("workOrderClass").alias("childWorkOrderClass")).filter("childWorkOrderClass == 'WORKORDER'").filter("childParentWo is not null").drop_duplicates().cache()
    df = df.join(child_df,df.workOrder == child_df.childParentWo,"left")
    df = df.withColumn("hasChildren", expr("case when childWorkOrder is not null then 'YES' else 'NO' end")).drop("childWorkOrderClass","childParentWo","childWorkOrder").drop_duplicates()

    asset_df = GetTable(f"{get_table_namespace(f'{TARGET}', 'dimAsset')}").select("assetNumber","assetSK","sourceValidFromTimestamp","sourceValidToTimestamp").drop_duplicates().cache()
    assetLocation_df = GetTable(f"{get_table_namespace(f'{TARGET}', 'dimAssetLocation')}").select("assetLocationName","assetLocationSK","assetLocationTypeCode","sourceValidFromTimestamp","sourceValidToTimestamp").drop_duplicates().cache()
    asset_contract_df = GetTable(f"{get_table_namespace(f'{TARGET}', 'dimAssetContract')}").select("assetContractSK","assetContractNumber","assetContractRevisionNumber","sourceValidFromTimestamp","sourceValidToTimestamp")\
    .withColumn("rank",rank().over(Window.partitionBy("assetContractNumber").orderBy(col("assetContractRevisionNumber").desc()))) \
    .filter("rank == 1").drop("rank").cache()
    jobPlan_df = GetTable(f"{get_table_namespace(f'{TARGET}', 'dimWorkOrderJobPlan')}").select("workOrderJobPlanNumber","workOrderJobPlanSK","workOrderJobPlanRevisionNumber","sourceValidFromTimestamp","sourceValidToTimestamp").withColumn("rank",rank().over(Window.partitionBy("workOrderJobPlanNumber").orderBy(col("workOrderJobPlanRevisionNumber").desc()))) \
    .filter("rank == 1").drop("rank").cache()
    problemType_df = GetTable(f"{get_table_namespace(f'{TARGET}', 'dimWorkOrderProblemType')}").select(col("workOrderProblemTypeName").alias("dim_problemType"),"workOrderProblemTypeSK","sourceValidFromTimestamp","sourceValidToTimestamp").drop_duplicates().cache()
    
    swchierarchy_df = GetTable(get_table_name(f"{SOURCE}","maximo","swchierarchy")).filter("_RecordDeleted == 0").select("code",col("description").alias("serviceDepartmentDesc")).drop_duplicates().cache()
    swcwoext_df = GetTable(get_table_name(f"{SOURCE}","maximo","swcwoext")).filter("_RecordDeleted == 0").select("workOrderId","externalStatus","externalStatusDate").drop_duplicates().cache()    
    
    pm_df = spark.sql(f"""select * from
        (select pm.pm, pm.frequency, pm.frequencyUnits, row_number() over(partition by pm.pm order by pm.changeddate desc) as rownumb from {get_env()}{SOURCE}.maximo.pm pm where _RecordDeleted = 0) dt where rownumb = 1""").cache()
    
    parent_df = GetTable(get_table_name(f"{SOURCE}","maximo","workOrder")).filter("_RecordDeleted == 0").select(col("workOrder").alias("parentWorkOrder"),col("serviceContract").alias("parentServiceContract"),col("pm").alias("parentpm")) \
    .join(pm_df.select(col("pm").alias("parentpm"), col("frequency").alias("parent_frequency"),col("frequencyUnits").alias("parent_frequencyUnits")),"parentpm","left").cache()


    date_df = GetTable(f"{get_table_namespace(f'{TARGET}', 'dimdate')}").select("calendarDate","nextBusinessDay").alias("dte").cache()
    
    # WorkOrder Status table has multiple entries for the same date. Curated requirement is to pick the first record when the status changed.
    woStatus_windowSpec  = Window.partitionBy("woWorkOrder","woStatus")
    wo_status_df = GetTable(get_table_name(f"{SOURCE}","maximo","woStatus")).filter("_RecordDeleted == 0").filter("Status in ('SCHED','INPRG','CAN','APPR','COMP','CLOSE','FINISHED')").select(col("workOrder").alias("woWorkOrder"),col("Status").alias("woStatus"),col("statusDate").alias("woStatusDate")) \
    .withColumn("rank",rank().over(woStatus_windowSpec.orderBy(col("woStatusDate")))) \
    .filter("rank == 1").drop("rank")
    pivot_df = wo_status_df.groupBy("woWorkOrder").pivot("woStatus").agg(min(col("woStatusDate")))

    pivot_df = pivot_df.withColumn("workOrderScheduledDate",pivot_df.SCHED)\
        .withColumn("workOrderInProgressDate",pivot_df.INPRG)\
        .withColumn("workOrderCancelledDate",pivot_df.CAN)\
        .withColumn("workOrderApprovedDate",pivot_df.APPR)\
        .withColumn("workOrderCompletedDate",pivot_df.COMP)\
        .withColumn("workOrderClosedDate",pivot_df.CLOSE)\
        .withColumn("workOrderFinishedDate",pivot_df.FINISHED)
   
    log_status_minDate_df = spark.sql(f"""SELECT DISTINCT WL.record as workOrder, MIN(to_date(WL.workLogDate)) as workOrderAcceptedLogStatusMinDate, WL.status as log_status FROM {get_table_name(f"{SOURCE}","maximo","worklog")} WL WHERE WL.status = 'ACCEPTED' and _RecordDeleted = 0 GROUP BY wl.record,wl.status""").cache()
   
    related_record_df = spark.sql(f"""SELECT RR.recordKey, COUNT(DISTINCT(WOR.workOrder)) AS relatedCorrectiveMaintenanceWorkOrderCount from
    (select * from (
            select recordKey, row_number() over(partition by recordKey order by rowStamp desc) as rownumb from {get_table_name(f"{SOURCE}","maximo","relatedrecord")} where _RecordDeleted = 0
            )dt where rownumb = 1) RR 
    inner join {get_table_name(f"{SOURCE}","maximo","workorder")} WOR on RR.recordKey = WOR.originatingRecord
    AND WOR.workType ='CM'
    AND WOR.status NOT IN ('DRAFT','CANDUP','CAN')
    GROUP BY RR.recordKey""").cache()
    
    
    # ------------- JOINS ------------------ #
    
    df = df.join(assetLocation_df,(df.location == assetLocation_df.assetLocationName) & (df.changedDate.between (assetLocation_df.sourceValidFromTimestamp,assetLocation_df.sourceValidToTimestamp)), "left") \
    .join(asset_contract_df, (df.serviceContract == asset_contract_df.assetContractNumber) & (df.changedDate.between (asset_contract_df.sourceValidFromTimestamp,asset_contract_df.sourceValidToTimestamp)),"left") \
    .join(jobPlan_df, (df.jobPlan == jobPlan_df.workOrderJobPlanNumber ) & (df.changedDate.between (jobPlan_df.sourceValidFromTimestamp,jobPlan_df.sourceValidToTimestamp)),"left") \
    .join(asset_df,(df.asset == asset_df.assetNumber)& (df.changedDate.between (asset_df.sourceValidFromTimestamp,asset_df.sourceValidToTimestamp)),"left") \
    .join(problemType_df, (df.problemType == problemType_df.dim_problemType)& (df.changedDate.between (problemType_df.sourceValidFromTimestamp,problemType_df.sourceValidToTimestamp)),"left") \
    .join(swchierarchy_df, df.serviceDepartment == swchierarchy_df.code ,"left") \
    .join(swcwoext_df,"workOrderId","left")\
    .join(parent_df,df.parentWo == parent_df.parentWorkOrder,"left") \
    .join(date_df,expr("CAST(wo.reportedDateTime as date) = dte.calendarDate"),"left") \
    .join(pivot_df,df.workOrder == wo_status_df.woWorkOrder,"left") \
    .withColumnRenamed("woStatusDate","workOrderFinishedDate").drop("woWorkOrder","woStatus") \
    .join(log_status_minDate_df,"workOrder","left") \
    .join(pm_df,"pm","left") \
    .join(related_record_df,(df.workOrder == related_record_df.recordKey) & (df.workType == 'PM'),"left").cache()

     
    # derived Fields 
    df = df.withColumn("etl_key",concat_ws('|',df.workOrder, df.changedDate)).alias("wo") \
    .withColumn("workOrderTrendDate",expr("CASE WHEN wo.WORKTYPE ='PM' \
    THEN COALESCE(wo.targetFinish, wo.targetStart,wo.scheduledStart, \
    wo.scheduledFinish, wo.targetRespondBy, wo.reportedDateTime) \
    ELSE \
    wo.reportedDateTime \
    END")).alias("wo")
    df = df.withColumn("calculatedTargetDateTimestamp", \
    expr("CASE WHEN wo.workType ='PM' THEN workOrderTrendDate \
         WHEN wo.workType IN ('BM','CM','GN') AND wo.initialPriority in (0,1) THEN to_timestamp(concat(date_add(wo.workOrderTrendDate, 42),' ',date_format(wo.workOrderTrendDate, 'HH:mm:ss')))  \
         WHEN wo.workType IN ('BM','CM','GN') AND wo.initialPriority = 2  THEN to_timestamp(concat(date_add(wo.workOrderTrendDate, 21),' ',date_format(wo.workOrderTrendDate, 'HH:mm:ss'))) \
         WHEN wo.workType IN ('BM','CM','GN') AND wo.initialPriority = 3  THEN to_timestamp(concat(date_add(wo.workOrderTrendDate, 5),' ',date_format(wo.workOrderTrendDate, 'HH:mm:ss'))) \
         WHEN wo.workType IN ('BM','CM','GN') AND wo.initialPriority = 4  THEN to_timestamp(concat(date_add(wo.workOrderTrendDate,2),' ',date_format(wo.workOrderTrendDate, 'HH:mm:ss'))) \
         WHEN wo.workType IN ('BM','CM','GN') AND wo.initialPriority = 5  THEN wo.workOrderTrendDate + INTERVAL 6 hours \
         WHEN wo.workType IN ('BM','CM','GN') AND wo.initialPriority = 6  THEN wo.workOrderTrendDate + INTERVAL 3 hours \
         END"))
    df = df.withColumn("workOrderStatusCloseDate", least(col("workOrderFinishedDate"),col("workOrderCompletedDate"),col("workOrderClosedDate"))).alias("wo")
    df = df.withColumn("finishDateTimestamp", expr("case when wo.actualFinish <= wo.workOrderStatusCloseDate then wo.actualFinish else wo.workOrderStatusCloseDate end")) \
    .withColumn("breakdownMaintenancePriorityToleranceTimestamp",expr("CASE WHEN wo.workOrderClass = 'WORKORDER' and wo.workType = 'BM' and wo.initialPriority = 6 THEN wo.reportedDateTime + INTERVAL 1 hour \
    WHEN wo.workOrderClass = 'WORKORDER' and wo.workType = 'BM' and wo.initialPriority = 5 THEN wo.reportedDateTime + INTERVAL 3 hours \
    WHEN wo.workOrderClass = 'WORKORDER' and wo.workType = 'BM' and wo.initialPriority = 4 THEN to_timestamp(concat(date_add(wo.reportedDateTime,1),' ',date_format(wo.reportedDateTime, 'HH:mm:ss'))) \
    WHEN wo.workOrderClass = 'WORKORDER' and wo.workType = 'BM' and wo.initialPriority = 3 THEN wo.nextBusinessDay \
    WHEN wo.workOrderClass = 'WORKORDER' and wo.workType = 'BM' and wo.initialPriority = 2 THEN to_timestamp(concat(date_add(wo.reportedDateTime,14),' ',date_format(wo.reportedDateTime, 'HH:mm:ss'))) \
    WHEN wo.workOrderClass = 'WORKORDER' and wo.workType = 'BM' and wo.initialPriority = 1 THEN to_timestamp(concat(add_months(cast(wo.reportedDateTime as date), 1),' ',date_format(wo.reportedDateTime, 'HH:mm:ss'))) \
    WHEN wo.workOrderClass = 'WORKORDER' and wo.workType = 'BM' and wo.initialPriority = 0 THEN to_timestamp(concat(add_months(cast(wo.reportedDateTime as date), 1),' ',date_format(wo.reportedDateTime, 'HH:mm:ss'))) \
    END")) 
    df = df.withColumn("actualStartDateTimestamp",expr("COALESCE(actualStart,finishDateTimestamp)")).alias("wo")
    df = df.withColumn("workOrderCompliantIndicator", \
    expr("case when wo.workOrderClass = 'WORKORDER' and wo.workType = 'BM' and wo.actualStartDateTimestamp <= (wo.breakdownMaintenancePriorityToleranceTimestamp) then 'YES' \
    when wo.workOrderClass = 'WORKORDER' and wo.workType = 'BM' and wo.actualStartDateTimestamp > (wo.breakdownMaintenancePriorityToleranceTimestamp) then 'NO'\
    END")) \
    .withColumn("calculatedTargetYear",expr("YEAR(wo.calculatedTargetDateTimestamp)"))\
    .withColumn("calculatedTargetMonth",expr("MONTH(wo.calculatedTargetDateTimestamp)"))\
    .withColumn("workOrderFinishYear",expr("YEAR(wo.finishDateTimestamp)"))\
    .withColumn("workOrderFinishMonth",expr("MONTH(wo.finishDateTimestamp)")).alias("wo")
    df = df.withColumn("workOrderTargetPeriod",expr("(calculatedTargetYear * 100)  + calculatedTargetMonth"))\
    .withColumn("workOrderFinishPeriod",expr("(workOrderFinishYear * 100)  + workOrderFinishMonth"))
    df  = df.withColumn("workOrderFinishedBeforeTargetMonthIndicator",expr("CASE when (workOrderFinishPeriod < workOrderTargetPeriod) AND (finishDateTimestamp is not NULL) THEN 1 ELSE 0 END")) \
    .withColumn("breakdownMaintenanceWorkOrderTargetHour",expr("case when wo.WorkType = 'BM' and wo.workOrderClass = 'WORKORDER' \
                     then DATEDIFF(HOUR, wo.reportedDateTime, wo.calculatedTargetDateTimestamp) \
                    end")) \
    .withColumn("breakdownMaintenanceWorkOrderRepairHour",expr("case when wo.WorkType = 'BM' and wo.workOrderClass = 'WORKORDER' \
                     then DATEDIFF(HOUR, wo.reportedDateTime, wo.finishDateTimestamp) \
                    end")).alias("wo")\
    .withColumn("actualWorkOrderLaborCostAmount",expr("case when wo.workOrderClass = 'WORKORDER' then wo.actualLaborCost end")) \
    .withColumn("actualWorkOrderMaterialCostAmount",expr("case when wo.workOrderClass = 'WORKORDER' then wo.actualMaterialCost end")) \
    .withColumn("actualWorkOrderServiceCostAmount",expr("case when wo.workOrderClass = 'WORKORDER' then wo.actualServiceCost end")) \
    .withColumn("actualWorkOrderLaborCostFromActivityAmount",expr("case when wo.workOrderClass = 'ACTIVITY' then wo.actualLaborCost end")) \
    .withColumn("actualWorkOrderMaterialCostFromActivityAmount",expr("case when wo.workOrderClass = 'ACTIVITY' then wo.actualMaterialCost end")) \
    .withColumn("actualWorkOrderServiceCostFromActivityAmount",expr("case when wo.workOrderClass = 'ACTIVITY' then wo.actualServiceCost end")) \
    .withColumn("preventiveMaintenanceWorkOrderFrequencyIndicator",expr("COALESCE(frequency, parent_frequency)")) \
    .withColumn("preventiveMaintenanceWorkOrderFrequencyUnitName",expr("COALESCE(frequencyUnits, parent_frequencyUnits)")).alias("wo")
    df = df.withColumn("workOrderTolerancedDueTimestamp", \
    expr("case when wo.workType ='PM' and   wo.preventiveMaintenanceWorkOrderFrequencyUnitName = 'YEARS' and wo.preventiveMaintenanceWorkOrderFrequencyIndicator >= 5 then to_timestamp(concat(add_months(wo.workOrderTrendDate, 3),' ',date_format(wo.workOrderTrendDate, 'HH:mm:ss'))) \
    when wo.workType ='PM' and   wo.preventiveMaintenanceWorkOrderFrequencyUnitName = 'YEARS' and wo.preventiveMaintenanceWorkOrderFrequencyIndicator >= 0 and wo.preventiveMaintenanceWorkOrderFrequencyIndicator < 5 then to_timestamp(concat(add_months(wo.workOrderTrendDate,1),' ',date_format(wo.workOrderTrendDate, 'HH:mm:ss'))) \
    when wo.workType ='PM' and   wo.preventiveMaintenanceWorkOrderFrequencyUnitName = 'MONTHS' and wo.preventiveMaintenanceWorkOrderFrequencyIndicator >= 60 then to_timestamp(concat(add_months(wo.workOrderTrendDate,3),' ',date_format(wo.workOrderTrendDate, 'HH:mm:ss'))) \
    when wo.workType ='PM' and   wo.preventiveMaintenanceWorkOrderFrequencyUnitName = 'MONTHS' and wo.preventiveMaintenanceWorkOrderFrequencyIndicator >= 6 and wo.preventiveMaintenanceWorkOrderFrequencyIndicator < 12 then to_timestamp(concat(date_add(wo.workOrderTrendDate, 14),' ',date_format(wo.workOrderTrendDate, 'HH:mm:ss'))) \
    when wo.workType ='PM' and   wo.preventiveMaintenanceWorkOrderFrequencyUnitName = 'MONTHS' and wo.preventiveMaintenanceWorkOrderFrequencyIndicator >= 12 and wo.preventiveMaintenanceWorkOrderFrequencyIndicator < 60 then to_timestamp(concat(add_months(wo.workOrderTrendDate ,1),' ',date_format(wo.workOrderTrendDate, 'HH:mm:ss'))) \
    when wo.workType ='PM' and   wo.preventiveMaintenanceWorkOrderFrequencyUnitName = 'MONTHS' and wo.preventiveMaintenanceWorkOrderFrequencyIndicator >= 1 and wo.preventiveMaintenanceWorkOrderFrequencyIndicator < 6 then to_timestamp(concat(date_add(wo.workOrderTrendDate, 7),' ',date_format(wo.workOrderTrendDate, 'HH:mm:ss'))) \
    when wo.workType ='PM' and   wo.preventiveMaintenanceWorkOrderFrequencyUnitName = 'MONTHS' and wo.preventiveMaintenanceWorkOrderFrequencyIndicator = 0 then workOrderTrendDate \
    when wo.workType ='PM' and   wo.preventiveMaintenanceWorkOrderFrequencyUnitName = 'WEEKS' and wo.preventiveMaintenanceWorkOrderFrequencyIndicator >= 260 then to_timestamp(concat(add_months(wo.workOrderTrendDate,3),' ',date_format(wo.workOrderTrendDate, 'HH:mm:ss'))) \
    when wo.workType ='PM' and   wo.preventiveMaintenanceWorkOrderFrequencyUnitName = 'WEEKS' and wo.preventiveMaintenanceWorkOrderFrequencyIndicator >= 52 and wo.preventiveMaintenanceWorkOrderFrequencyIndicator < 260 then to_timestamp(concat(add_months(wo.workOrderTrendDate,1),' ',date_format(wo.workOrderTrendDate, 'HH:mm:ss'))) \
    when wo.workType ='PM' and   wo.preventiveMaintenanceWorkOrderFrequencyUnitName = 'WEEKS' and wo.preventiveMaintenanceWorkOrderFrequencyIndicator >= 26 and wo.preventiveMaintenanceWorkOrderFrequencyIndicator < 52 then to_timestamp(concat(date_add(wo.workOrderTrendDate, 14),' ',date_format(wo.workOrderTrendDate, 'HH:mm:ss'))) \
    when wo.workType ='PM' and   wo.preventiveMaintenanceWorkOrderFrequencyUnitName = 'WEEKS' and wo.preventiveMaintenanceWorkOrderFrequencyIndicator >= 5 and wo.preventiveMaintenanceWorkOrderFrequencyIndicator < 26 then to_timestamp(concat(date_add(wo.workOrderTrendDate, 7),' ',date_format(wo.workOrderTrendDate, 'HH:mm:ss'))) \
    when wo.workType ='PM' and   wo.preventiveMaintenanceWorkOrderFrequencyUnitName = 'WEEKS' and wo.preventiveMaintenanceWorkOrderFrequencyIndicator >= 0 and wo.preventiveMaintenanceWorkOrderFrequencyIndicator < 5 then wo.workOrderTrendDate \
    when wo.workType ='PM' and   wo.preventiveMaintenanceWorkOrderFrequencyUnitName = 'DAYS' and wo.preventiveMaintenanceWorkOrderFrequencyIndicator >= 1825 then to_timestamp(concat(add_months(wo.workOrderTrendDate,3),' ',date_format(wo.workOrderTrendDate, 'HH:mm:ss'))) \
    when wo.workType ='PM' and   wo.preventiveMaintenanceWorkOrderFrequencyUnitName = 'DAYS' and wo.preventiveMaintenanceWorkOrderFrequencyIndicator >= 365 and wo.preventiveMaintenanceWorkOrderFrequencyIndicator < 1825 then to_timestamp(concat(add_months(wo.workOrderTrendDate,1),' ',date_format(wo.workOrderTrendDate, 'HH:mm:ss'))) \
    when wo.workType ='PM' and   wo.preventiveMaintenanceWorkOrderFrequencyUnitName = 'DAYS' and wo.preventiveMaintenanceWorkOrderFrequencyIndicator >= 182 and wo.preventiveMaintenanceWorkOrderFrequencyIndicator < 365 then to_timestamp(concat(date_add(wo.workOrderTrendDate, 14),' ',date_format(wo.workOrderTrendDate, 'HH:mm:ss'))) \
    when wo.workType ='PM' and   wo.preventiveMaintenanceWorkOrderFrequencyUnitName = 'DAYS' and wo.preventiveMaintenanceWorkOrderFrequencyIndicator >= 30 and wo.preventiveMaintenanceWorkOrderFrequencyIndicator < 182 then to_timestamp(concat(date_add(wo.workOrderTrendDate, 7),' ',date_format(wo.workOrderTrendDate, 'HH:mm:ss'))) \
    when wo.workType ='PM' and   wo.preventiveMaintenanceWorkOrderFrequencyUnitName = 'DAYS' and wo.preventiveMaintenanceWorkOrderFrequencyIndicator >= 0 and wo.preventiveMaintenanceWorkOrderFrequencyIndicator < 30 then wo.workOrderTrendDate \
    when wo.workType ='PM' and   wo.preventiveMaintenanceWorkOrderFrequencyUnitName is null then workOrderTrendDate \
    when wo.workType in ('BM','CM','GN') and   wo.initialPriority in (0,1)  then to_timestamp(concat(date_add(wo.workOrderTrendDate, 42),' ',date_format(wo.workOrderTrendDate, 'HH:mm:ss')))\
    when wo.workType in ('BM','CM','GN') and   wo.initialPriority = 2  then to_timestamp(concat(date_add(wo.workOrderTrendDate, 21),' ',date_format(wo.workOrderTrendDate, 'HH:mm:ss')))\
    when wo.workType in ('BM','CM','GN') and   wo.initialPriority = 3  then to_timestamp(concat(date_add(wo.workOrderTrendDate, 5),' ',date_format(wo.workOrderTrendDate, 'HH:mm:ss')))\
    when wo.workType in ('BM','CM','GN') and   wo.initialPriority = 4  then to_timestamp(concat(date_add(wo.workOrderTrendDate, 2),' ',date_format(wo.workOrderTrendDate, 'HH:mm:ss')))\
    when wo.workType in ('BM','CM','GN') and   wo.initialPriority = 5  then wo.workOrderTrendDate + INTERVAL 6 hours \
    when wo.workType in ('BM','CM','GN') and  wo.initialPriority = 6  then wo.workOrderTrendDate + INTERVAL 3 hours end"))\
    .withColumn("workOrderTotalCostAmount",expr("coalesce(actualWorkOrderLaborCostAmount,0)+ coalesce(actualWorkOrderMaterialCostAmount,0)+ coalesce(actualWorkOrderServiceCostAmount,0)+ coalesce(actualWorkOrderLaborCostFromActivityAmount,0)+ coalesce(actualWorkOrderMaterialCostFromActivityAmount,0)+ coalesce(actualWorkOrderServiceCostFromActivityAmount,0)").cast("decimal(38,18)"))

   

    # ------------- TRANSFORMS ------------- #
    _.Transforms = [
        f"etl_key {BK}"
        ,"workOrder workOrderCreationId"
        ,"changedDate workOrderChangeTimestamp"
        ,"assetSK assetFK"
        ,"assetLocationSK assetLocationFK"
        ,"assetContractSK assetContractFK"
        ,"workOrderJobPlanSK workOrderJobPlanFK"
        ,"workOrderProblemTypeSK workOrderProblemTypeFK"
        ,"workOrderId"
        ,"description workOrderDescription"
        ,"hasChildren workOrderChildIndicator"
        ,"parentWo parentWorkOrderCreationId"
        ,"status workOrderStatusDescription"
        ,"dispatchSystem workOrderDispatchSystemName"
        ,"workType workOrderWorkTypeCode"
        ,"workOrderClass workOrderClassDescription"
        ,"initialPriority workOrderInitialPriorityCode"
        ,"serviceDepartment workOrderServiceDepartmentCode"
        ,"serviceDepartmentDesc workOrderServiceDepartmentDescription"
        ,"serviceType workOrderServiceTypeCode"
        ,"taskCode workOrderTaskCode"
        ,"fCId workOrderFinancialControlIdentifier"
        ,"assessedPriority workOrderAssessedPriorityCode"
        ,"numberOfUnits billedWorkOrderUnitCount"
        ,"actualLaborHours actualWorkOrderLabourHoursQuantity"
        ,"actualWorkOrderLaborCostAmount"
        ,"actualWorkOrderMaterialCostAmount"
        ,"actualWorkOrderServiceCostAmount"
        ,"actualWorkOrderLaborCostFromActivityAmount"
        ,"actualWorkOrderMaterialCostFromActivityAmount"
        ,"actualWorkOrderServiceCostFromActivityAmount"
        ,"estimatedLaborHours estimatedWorkOrderLaborHoursQuantity"
        ,"estimatedDuration estimatedWorkOrderRemainingHoursQuantity"
        ,"actualFinish actualWorkOrderFinishTimestamp"
        ,"actualCostOfInternalLabor actualWorkOrderInternalLaborCostAmount"
        ,"actualHoursOfInternalLabor actualWorkOrderInternalLaborHoursQuantity"
        ,"actualCostOfExternalLabor actualWorkOrderExternalLaborCostAmount"
        ,"actualHoursOfExternalLabor actualWorkOrderExternalLaborHoursQuantity"
        ,"actualStart actualWorkOrderStartTimestamp"
        ,"actualToolCost actualWorkOrderToolCostAmount"
        ,"estimatedLaborCost estimatedWorkOrderLaborCostAmount"
        ,"estimatedMaterialCost estimatedWorkOrderMaterialCostAmount"
        ,"estimatedServiceCost estimatedWorkOrderServiceCostAmount"
        ,"estimatedToolCost estimatedWorkOrderToolCostAmount"
        ,"estimatedCostOfInternalLabor estimatedWorkOrderInternalLaborCostAmount"
        ,"estimatedHoursOfInternalLabor estimatedWorkOrderInternalLaborHoursQuantity"
        ,"estimatedCostOfExternalLabor estimatedWorkOrderExternalLaborHoursAmount"
        ,"estimatedHoursOfExternalLabor estimatedWorkOrderExternalLaborHoursQuantity"
        ,"targetDescription workOrderTargetDescription"
        ,"woCreationDate workOrderCreationTimestamp"
        ,"serviceProviderNotified workOrderServiceProviderNotifiedTimestamp"
        ,"targetFinish workOrderTargetFinishTimestamp"
        ,"targetStart workOrderTargetStartTimestamp"
        ,"scheduledStart workOrderScheduledStartTimestamp"
        ,"scheduledFinish workOrderScheduledFinishTimestamp"
        ,"targetRespondBy workOrderTargetRespondByTimestamp"
        ,"reportedDateTime workOrderReportedTimestamp"
        ,"externalStatus workOrderExternalStatusCode"
        ,"externalStatusDate workOrderExternalStatusTimestamp"
        ,"workOrderScheduledDate workOrderScheduledTimestamp"
        ,"workOrderInProgressDate workOrderInProgressTimestamp"
        ,"workOrderCancelledDate workOrderCancelledTimestamp"
        ,"workOrderApprovedDate workOrderApprovedTimestamp"
        ,"workOrderCompletedDate workOrderCompletedTimestamp"
        ,"workOrderClosedDate workOrderClosedTimestamp"
        ,"workOrderFinishedDate workOrderFinishedTimestamp"
        ,"changedDate_date_part snapshotDate"
        ,"workOrderTrendDate workOrderTrendTimestamp"
        ,"calculatedTargetDateTimestamp calculatedWorkOrderTargetDateTimestamp"
        ,"preventiveMaintenanceWorkOrderFrequencyIndicator"
        ,"preventiveMaintenanceWorkOrderFrequencyUnitName"
        ,"workOrderTolerancedDueTimestamp"
        ,"COALESCE(serviceContract, parentServiceContract) workOrderServiceContract"
        ,"workOrderStatusCloseDate workOrderStatusCloseTimestamp"
        ,"finishDateTimestamp workOrderFinishTimestamp"
        ,"breakdownMaintenancePriorityToleranceTimestamp"
        ,"actualStartDateTimestamp"
        ,"workOrderCompliantIndicator"
        ,"relatedCorrectiveMaintenanceWorkOrderCount"
        ,"workOrderAcceptedLogStatusMinDate"
        ,"calculatedTargetYear calculatedWorkOrderTargetYear"
        ,"calculatedTargetMonth calculatedWorkOrderTargetMonth"
        ,"workOrderFinishYear"
        ,"workOrderFinishMonth"
        ,"workOrderTargetPeriod"
        ,"workOrderFinishPeriod"
        ,"workOrderFinishedBeforeTargetMonthIndicator"
        ,"breakdownMaintenanceWorkOrderTargetHour"
        ,"breakdownMaintenanceWorkOrderRepairHour"
        ,"workOrderTotalCostAmount"
    ]
    df = df.selectExpr(
        _.Transforms
    ).drop_duplicates()
    # ------------- CLAUSES ---------------- #

    # ------------- SAVE ------------------- #
#     display(df)
    # print(df.count())
    Save(df)
    #DisplaySelf()
    
pass
Transform()
