# Databricks notebook source
# MAGIC %run ../../Common/common-transform 

# COMMAND ---------- 

# MAGIC %run ../../Common/common-helpers 
# COMMAND ---------- 


# COMMAND ----------

TARGET = DEFAULT_TARGET

# COMMAND ----------

# CleanSelf()

# COMMAND ----------

def Transform():
    global df
    # ------------- TABLES ----------------- #

    #   Cleansed WorkOrder table has multiple entries for the same date since the history is managed. Curated requirement is to pick the latest record for that specific date.
    workOrder_windowSpec  = Window.partitionBy("workOrder","changeDate_date_part")
    df = GetTable(get_table_name(f"{SOURCE}","maximo","workOrder")).alias("wo") \
    .withColumn("changeDate_date_part",to_date(col("changeDate"))) \
    .withColumn("rank",rank().over(workOrder_windowSpec.orderBy(col("changeDate").desc()))) \
    .filter("rank == 1").drop("rank")

    asset_df = GetTable(f"{get_table_namespace(f'{TARGET}', 'dimAsset')}").select("assetNumber","assetSK","sourceValidFromTimestamp","sourceValidToTimestamp").drop_duplicates().cache()
    assetLocation_df = GetTable(f"{get_table_namespace(f'{TARGET}', 'dimAssetLocation')}").select("assetLocationName","assetLocationSK","assetLocationTypeCode","sourceValidFromTimestamp","sourceValidToTimestamp").drop_duplicates().cache()
    asset_contract_df = GetTable(f"{get_table_namespace(f'{TARGET}', 'dimAssetContract')}").select("assetContractSK","assetContractNumber","assetContractRevisionNumber","sourceValidFromTimestamp","sourceValidToTimestamp")\
    .withColumn("rank",rank().over(Window.partitionBy("assetContractNumber").orderBy(col("assetContractRevisionNumber").desc()))) \
    .filter("rank == 1").drop("rank").cache()
    jobPlan_df = GetTable(f"{get_table_namespace(f'{TARGET}', 'dimWorkOrderJobPlan')}").select("workOrderJobPlanNumber","workOrderJobPlanSK","workOrderJobPlanRevisionNumber","sourceValidFromTimestamp","sourceValidToTimestamp").withColumn("rank",rank().over(Window.partitionBy("workOrderJobPlanNumber").orderBy(col("workOrderJobPlanRevisionNumber").desc()))) \
    .filter("rank == 1").drop("rank").cache()
    problemType_df = GetTable(f"{get_table_namespace(f'{TARGET}', 'dimWorkOrderProblemType')}").select(col("workOrderProblemTypeId").alias("dim_problemType"),"workOrderProblemTypeSK","_recordStart","_recordEnd").drop_duplicates().cache()
    
    swchierarchy_df = GetTable(get_table_name(f"{SOURCE}","maximo","swchierarchy")).select("code",col("description").alias("serviceDepartmentDesc")).drop_duplicates().cache()
    swcwoext_df = GetTable(get_table_name(f"{SOURCE}","maximo","swcwoext")).select("workOrderId","externalStatus","externalStatusDate").drop_duplicates().cache()    
    child_df = GetTable(get_table_name(f"{SOURCE}","maximo","workOrder")).select(col("workOrder").alias("childWorkOrder"),col("parentWo").alias("childParentWo")).drop_duplicates().cache()
    pm_df = spark.sql(f"""select * from
        (select pm.pm, pm.frequency, pm.frequencyUnits, row_number() over(partition by pm.pm order by pm.changeddate desc) as rownumb from {0} pm) dt where rownumb = 1""".format(get_table_name(f"{SOURCE}","maximo","pm"))).cache()
    
    parent_df = GetTable(get_table_name(f"{SOURCE}","maximo","workOrder")).select(col("workOrder").alias("parentWorkOrder"),col("serviceContract").alias("parentServiceContract"),col("pm").alias("parentpm")) \
    .join(pm_df.select(col("pm").alias("parentpm"), col("frequency").alias("parent_frequency"),col("frequencyUnits").alias("parent_frequencyUnits")),"parentpm","left").cache()


    date_df = GetTable(f"{get_table_namespace('curated', 'dimdate')}").select("calendarDate","nextBusinessDay").alias("dte").cache()
    
    # WorkOrder Status table has multiple entries for the same date. Curated requirement is to pick the first record when the status changed.
    woStatus_windowSpec  = Window.partitionBy("woWorkOrder","woStatus")
    wo_status_df = GetTable(get_table_name(f"{SOURCE}","maximo","woStatus")).filter("Status in ('SCHED','INPRG','CAN','APPR','COMP','CLOSE','FINISHED')").select(col("workOrder").alias("woWorkOrder"),col("Status").alias("woStatus"),col("statusDate").alias("woStatusDate")) \
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
   
    log_status_minDate_df = spark.sql(f"""SELECT DISTINCT WL.record as workOrder, MIN(to_date(WL.Date)) as workorderAcceptedLogStatusMinDate, WL.status as log_status FROM {0} WL WHERE WL.status = 'ACCEPTED' GROUP BY wl.record,wl.status""".format(get_table_name(f"{SOURCE}","maximo","worklog"))).cache()
   
    related_record_df = spark.sql(f"""SELECT                 
    RR.recordKey, COUNT(DISTINCT(WOR.workOrder)) AS relatedCorrectiveMaintenanceWorkorderCount from
    (select * from (
            select recordKey, row_number() over(partition by recordKey order by rowStamp desc) as rownumb from {0}
            )dt where rownumb = 1) RR 
    inner join {1} WOR on RR.recordKey = WOR.originatingRecord
    AND WOR.workType ='CM'
    AND WOR.status NOT IN ('DRAFT','CANDUP','CAN')
    GROUP BY RR.recordKey""".format(get_table_name(f"{SOURCE}","maximo","relatedrecord"),get_table_name(f"{SOURCE}","maximo","workorder"))).cache()
    
    
    # ------------- JOINS ------------------ #
    
    df = df.join(assetLocation_df,(df.location == assetLocation_df.assetLocationName) & (df.changeDate.between (assetLocation_df.sourceValidFromTimestamp,assetLocation_df.sourceValidToTimestamp)), "left") \
    .join(asset_contract_df, (df.serviceContract == asset_contract_df.assetContractNumber) & (df.changeDate.between (asset_contract_df.sourceValidFromTimestamp,asset_contract_df.sourceValidToTimestamp)),"left") \
    .join(jobPlan_df, (df.jobPlan == jobPlan_df.workOrderJobPlanNumber ) & (df.changeDate.between (jobPlan_df.sourceValidFromTimestamp,jobPlan_df.sourceValidToTimestamp)),"left") \
    .join(asset_df,(df.asset == asset_df.assetNumber)& (df.changeDate.between (asset_df.sourceValidFromTimestamp,asset_df.sourceValidToTimestamp)),"left") \
    .join(problemType_df, (df.problemType == problemType_df.dim_problemType)& (df.changeDate.between (problemType_df._recordStart,problemType_df._recordEnd)),"left") \
    .join(swchierarchy_df, df.serviceDepartment == swchierarchy_df.code ,"left") \
    .join(swcwoext_df,"workOrderId","left")\
    .join(child_df,df.workOrder == child_df.childParentWo,"left") \
    .join(parent_df,df.parentWo == parent_df.parentWorkOrder,"left") \
    .join(date_df,expr("CAST(wo.reportedDateTime as date) = dte.calendarDate"),"left") \
    .join(pivot_df,df.workOrder == wo_status_df.woWorkOrder,"left") \
    .withColumnRenamed("woStatusDate","workOrderFinishedDate").drop("woWorkOrder","woStatus") \
    .withColumn("hasChildren", when(child_df.childWorkOrder != None,"YES").otherwise("NO")) \
    .join(log_status_minDate_df,"workOrder",\"left") \
    .join(pm_df,"pm","left") \
    .join(related_record_df,(df.workOrder == related_record_df.recordKey) & (df.workType == 'PM'),"left").cache()

     
    # derived Fields 
    df = df.withColumn("etl_key",concat_ws('|',df.workOrder, df.changeDate)).alias("wo") \
    .withColumn("workOrderTrendDate",expr("CASE WHEN wo.WORKTYPE ='PM' \
    THEN COALESCE(wo.targetFinish, wo.targetStart,wo.scheduledStart, \
    wo.scheduledFinish, wo.targetRespondBy, wo.reportedDateTime) \
    ELSE \
    wo.reportedDateTime \
    END")).alias("wo")
    df = df.withColumn("calculatedTargetDateTimestamp", \
    expr("CASE WHEN wo.workType ='PM' THEN workOrderTrendDate \
         WHEN wo.workType IN ('BM','CM','GN') AND wo.initialPriority in (0,1) THEN date_add(wo.workOrderTrendDate, 42)  \
         WHEN wo.workType IN ('BM','CM','GN') AND wo.initialPriority = 2  THEN date_add(wo.workOrderTrendDate, 21) \
         WHEN wo.workType IN ('BM','CM','GN') AND wo.initialPriority = 3  THEN date_add(wo.workOrderTrendDate, 5) \
         WHEN wo.workType IN ('BM','CM','GN') AND wo.initialPriority = 4  THEN date_add(wo.workOrderTrendDate,2) \
         WHEN wo.workType IN ('BM','CM','GN') AND wo.initialPriority = 5  THEN wo.workOrderTrendDate + INTERVAL 6 hours \
         WHEN wo.workType IN ('BM','CM','GN') AND wo.initialPriority = 6  THEN wo.workOrderTrendDate + INTERVAL 3 hours \
         END"))
    df = df.withColumn("workOrderStatusCloseDate", least(col("workOrderFinishedDate"),col("workOrderCompletedDate"),col("workOrderClosedDate"))).alias("wo")
    df = df.withColumn("finishDateTimestamp", expr("case when wo.actualFinish <= wo.workOrderStatusCloseDate then wo.actualFinish else wo.workOrderStatusCloseDate end")) \
    .withColumn("breakdownMaintenancePriorityToleranceDate",expr("CASE WHEN wo.class = 'WORKORDER' and wo.workType = 'BM' and wo.initialPriority = 6 THEN wo.reportedDateTime + INTERVAL 1 hour \
    WHEN wo.class = 'WORKORDER' and wo.workType = 'BM' and wo.initialPriority = 5 THEN wo.reportedDateTime + INTERVAL 3 hours \
    WHEN wo.class = 'WORKORDER' and wo.workType = 'BM' and wo.initialPriority = 4 THEN date_add(wo.reportedDateTime,1) \
    WHEN wo.class = 'WORKORDER' and wo.workType = 'BM' and wo.initialPriority = 3 THEN wo.nextBusinessDay \
    WHEN wo.class = 'WORKORDER' and wo.workType = 'BM' and wo.initialPriority = 2 THEN date_add(wo.reportedDateTime,14) \
    WHEN wo.class = 'WORKORDER' and wo.workType = 'BM' and wo.initialPriority = 1 THEN add_months(cast(wo.reportedDateTime as date), 1) \
    WHEN wo.class = 'WORKORDER' and wo.workType = 'BM' and wo.initialPriority = 0 THEN add_months(cast(wo.reportedDateTime as date), 1) \
    END")) 
    df = df.withColumn("actualStartDateTimestamp",expr("COALESCE(actualStart,finishDateTimestamp)")).alias("wo")
    df = df.withColumn("workOrderCompliantIndicator", \
    expr("case when wo.class = 'WORKORDER' and wo.workType = 'BM' and wo.actualStartDateTimestamp <= (wo.breakdownMaintenancePriorityToleranceDate) then 'YES' \
    when wo.Class = 'WORKORDER' and wo.workType = 'BM' and wo.actualStartDateTimestamp > (wo.breakdownMaintenancePriorityToleranceDate) then 'NO'\
    END")) \
    .withColumn("calculatedTargetYear",expr("YEAR(wo.calculatedTargetDateTimestamp)"))\
    .withColumn("calculatedTargetMonth",expr("MONTH(wo.calculatedTargetDateTimestamp)"))\
    .withColumn("workOrderFinishYear",expr("YEAR(wo.finishDateTimestamp)"))\
    .withColumn("workOrderFinishMonth",expr("MONTH(wo.finishDateTimestamp)")).alias("wo")
    df = df.withColumn("workOrderTargetPeriod",expr("(calculatedTargetYear * 100)  + calculatedTargetMonth"))\
    .withColumn("workOrderFinishPeriod",expr("(workOrderFinishYear * 100)  + workOrderFinishMonth"))
    df  = df.withColumn("workOrderFinishedBeforeTargetMonthIndicator",expr("CASE when (workOrderFinishPeriod < workOrderTargetPeriod) AND (finishDateTimestamp is not NULL) THEN 1 ELSE 0 END")) \
    .withColumn("breakdownMaintenanceWorkOrderTargetHour",expr("case when wo.WorkType = 'BM' and wo.class = 'WORKORDER' \
                     then DATEDIFF(HOUR, wo.reportedDateTime, wo.calculatedTargetDateTimestamp) \
                    end")) \
    .withColumn("breakdownMaintenanceWorkOrderRepairHour",expr("case when wo.WorkType = 'BM' and wo.class = 'WORKORDER' \
                     then DATEDIFF(HOUR, wo.reportedDateTime, wo.finishDateTimestamp) \
                    end")).alias("wo")\
    .withColumn("actualWorkOrderLaborCostFromActivityAmount",expr("case when wo.class = 'ACTIVITY' then wo.actualLaborCost end")) \
    .withColumn("actualWorkOrderMaterialCostFromActivityAmount",expr("case when wo.class = 'ACTIVITY' then wo.actualMaterialCost end")) \
    .withColumn("actualWorkOrderServiceCostFromActivityAmount",expr("case when wo.class = 'ACTIVITY' then wo.actualServiceCost end")) \
    .withColumn("preventiveMaintenanceWorkOrderFrequencyIndicator",expr("COALESCE(frequency, parent_frequency)")) \
    .withColumn("preventiveMaintenanceWorkOrderFrequencyUnitName",expr("COALESCE(frequencyUnits, parent_frequencyUnits)")).alias("wo")
    df = df.withColumn("workOrderTolerancedDueDate", \
    expr("case when wo.workType ='PM' and   wo.frequencyUnits = 'YEARS' and wo.frequency >= 5 then add_months(cast(wo.workOrderTrendDate as date), 3) \
    when wo.workType ='PM' and   wo.frequencyUnits = 'YEARS' and wo.frequency >= 0 and wo.frequency < 5 then add_months(cast(wo.workOrderTrendDate as date),1) \
    when wo.workType ='PM' and   wo.frequencyUnits = 'MONTHS' and wo.frequency >= 60 then add_months(cast(wo.workOrderTrendDate as date),3) \
    when wo.workType ='PM' and   wo.frequencyUnits = 'MONTHS' and wo.frequency >= 6 and wo.frequency < 12 then date_add(cast(wo.workOrderTrendDate as date), 14) \
    when wo.workType ='PM' and   wo.frequencyUnits = 'MONTHS' and wo.frequency >= 12 and wo.frequency < 60 then add_months(cast(wo.workOrderTrendDate as date) ,1) \
    when wo.workType ='PM' and   wo.frequencyUnits = 'MONTHS' and wo.frequency >= 1 and wo.frequency < 6 then date_add(cast(wo.workOrderTrendDate as date), 7) \
    when wo.workType ='PM' and   wo.frequencyUnits = 'MONTHS' and wo.frequency = 0 then workOrderTrendDate \
    when wo.workType ='PM' and   wo.frequencyUnits = 'WEEKS' and wo.frequency >= 260 then add_months(cast(wo.workOrderTrendDate as date),3) \
    when wo.workType ='PM' and   wo.frequencyUnits = 'WEEKS' and wo.frequency >= 52 and wo.frequency < 260 then add_months(cast(wo.workOrderTrendDate as date),1) \
    when wo.workType ='PM' and   wo.frequencyUnits = 'WEEKS' and wo.frequency >= 26 and wo.frequency < 52 then date_add(cast(wo.workOrderTrendDate as date), 14) \
    when wo.workType ='PM' and   wo.frequencyUnits = 'WEEKS' and wo.frequency >= 5 and wo.frequency < 26 then date_add(cast(wo.workOrderTrendDate as date), 7) \
    when wo.workType ='PM' and   wo.frequencyUnits = 'WEEKS' and wo.frequency >= 0 and wo.frequency < 5 then wo.workOrderTrendDate \
    when wo.workType ='PM' and   wo.frequencyUnits = 'DAYS' and wo.frequency >= 1825 then add_months(cast(wo.workOrderTrendDate as date),3) \
    when wo.workType ='PM' and   wo.frequencyUnits = 'DAYS' and wo.frequency >= 365 and wo.frequency < 1825 then add_months(cast(wo.workOrderTrendDate as date),1) \
    when wo.workType ='PM' and   wo.frequencyUnits = 'DAYS' and wo.frequency >= 182 and wo.frequency < 365 then date_add(cast(wo.workOrderTrendDate as date), 14) \
    when wo.workType ='PM' and   wo.frequencyUnits = 'DAYS' and wo.frequency >= 30 and wo.frequency < 182 then date_add(cast(wo.workOrderTrendDate as date), 7) \
    when wo.workType ='PM' and   wo.frequencyUnits = 'DAYS' and wo.frequency >= 0 and wo.frequency < 30 then wo.workOrderTrendDate \
    when wo.workType ='PM' and   wo.frequencyUnits is null then workOrderTrendDate \
    when wo.workType in ('BM','CM','GN') and   wo.initialPriority in (0,1)  then date_add(cast(wo.workOrderTrendDate as date), 42)\
    when wo.workType in ('BM','CM','GN') and   wo.initialPriority = 2  then date_add(cast(wo.workOrderTrendDate as date), 21)\
    when wo.workType in ('BM','CM','GN') and   wo.initialPriority = 3  then date_add(cast(wo.workOrderTrendDate as date), 5)\
    when wo.workType in ('BM','CM','GN') and   wo.initialPriority = 4  then date_add(cast(wo.workOrderTrendDate as date), 2)\
    when wo.workType in ('BM','CM','GN') and   wo.initialPriority = 5  then wo.workOrderTrendDate + INTERVAL 6 hours \
    when wo.workType in ('BM','CM','GN') and  wo.initialPriority = 6  then wo.workOrderTrendDate + INTERVAL 3 hours end"))\
    .withColumn("workOrderTotalCostAmount",expr("actualLaborCost+ actualMaterialCost+ actualServiceCost+ actualWorkOrderLaborCostFromActivityAmount+ actualWorkOrderMaterialCostFromActivityAmount+ actualWorkOrderServiceCostFromActivityAmount").cast("decimal(38,18)"))

   

    # ------------- TRANSFORMS ------------- #
    _.Transforms = [
        f"etl_key {BK}"
        ,"workOrder workOrderCreationId"
        ,"changeDate workOrderChangeTimestamp"
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
        ,"class workOrderClassDescription"
        ,"initialPriority workOrderInitialPriorityCode"
        ,"serviceDepartment workOrderServiceDepartmentCode"
        ,"serviceDepartmentDesc workOrderServiceDepartmentDescription"
        ,"serviceType workOrderServiceTypeCode"
        ,"taskCode workOrderTaskCode"
        ,"fCId workOrderFinancialControlIdentfier"
        ,"assessedPriority workOrderAssessedPriorityCode"
        ,"numberOfUnits billedWorkOrderUnitCount"
        ,"actualLaborHours actualWorkOrderLabourHoursQuantity"
        ,"actualLaborCost actualWorkOrderLaborCostAmount"
        ,"actualMaterialCost actualWorkOrderMaterialCostAmount"
        ,"actualServiceCost actualWorkOrderServiceCostAmount"
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
        ,"scheduledStart workOrderscheduledStartTimestamp"
        ,"scheduledFinish workOrderscheduledFinishTimestamp"
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
        ,"changeDate_date_part snapshotDate"
        ,"workOrderTrendDate workOrderTrendTimestamp"
        ,"calculatedTargetDateTimestamp calculatedWorkOrderTargetDateTimestamp"
        ,"preventiveMaintenanceWorkOrderFrequencyIndicator"
        ,"preventiveMaintenanceWorkOrderFrequencyUnitName"
        ,"workOrderTolerancedDueDate workOrderTolerancedDueTimestamp"
        ,"COALESCE(serviceContract, parentServiceContract) workOrderServiceContract"
        ,"workOrderStatusCloseDate workOrderStatusCloseTimestamp"
        ,"finishDateTimestamp workOrderFinishTimestamp"
        ,"breakdownMaintenancePriorityToleranceDate"
        ,"actualStartDateTimestamp"
        ,"workOrderCompliantIndicator"
        ,"relatedCorrectiveMaintenanceWorkorderCount"
        ,"workorderAcceptedLogStatusMinDate"
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

# COMMAND ----------

spark.sql(
        f"""Select workOrderSK, count(1) from {get_table_namespace('curated', 'factWorkOrder')} group by workOrderSK having count(1) >1
        """)

# COMMAND ----------

spark.sql(
        f"""create or replace view {get_table_namespace('curated', 'factworkorder')} AS (select * from {get_table_namespace('curated', 'factworkorder')})
        """)

# COMMAND ----------


