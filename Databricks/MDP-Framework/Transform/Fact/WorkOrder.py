# Databricks notebook source
# MAGIC %run ../../Common/common-transform

# COMMAND ----------

TARGET = DEFAULT_TARGET

# COMMAND ----------

# from pyspark.sql.functions import pandas_udf, PandasUDFType
# from pyspark.sql.window import Window
# import pandas as pd

# @pandas_udf("timestamp")
# def calculate_target_date_time(workType: pd.Series, woPriority: pd.Series, workOrderTrendDate: pd.Series)-> pd.Series:
#     if workType == 'PM':
#         return workOrderTrendDate
#     elif workType.eq('BM') | workType.eq('CM')  | workType.eq('GN'):
#         if woPriority.eq(0) | woPriority.eq(1): 
#             return (workOrderTrendDate + 42)
#         elif woPriority.eq(2):
#             return (workOrderTrendDate + 21)
#         elif woPriority.eq(3):
#             return (workOrderTrendDate + 5)
#         elif woPriority.eq(4):
#             return (workOrderTrendDate + 2)
#         elif woPriority.eq(5):
#             return (workOrderTrendDate + 6)/24
#         elif woPriority.eq(6):
#             return (workOrderTrendDate + 3)/24
#         else:
#             return None
#     else:
#         return None
    

# COMMAND ----------

# workOrder_windowSpec  = Window.partitionBy("workOrder","changeDate_date_part")
# df = GetTable(f"{SOURCE}.maximo_workOrder").alias("wo") \
# .withColumn("changeDate_date_part",to_date(col("changeDate"))) \
# .withColumn("rank",rank().over(workOrder_windowSpec.orderBy(col("changeDate").desc()))) \
# .filter("rank == 1").drop("rank") \
# .withColumn("workOrderTrendDate",expr("CASE WHEN wo.WORKTYPE ='PM' \
# THEN COALESCE(wo.targetFinish, wo.targetStart,wo.scheduledStart, \
# wo.scheduledFinish, wo.targetRespondBy, wo.reportedDateTime) \
# ELSE \
# wo.reportedDateTime \
# END")) \
# .withColumn("calculatedTargetDateTime", calculate_target_date_time(col("workType"), col("initialPriority"), col("workOrderTrendDate")))  

# COMMAND ----------

# display(df)

# COMMAND ----------

#%sql
# select case 
#            when wo.worktype ='PM' and   pm.frequencyUnits = 'YEARS' and pm.frequency >= 5 then add_months(cast(wo.workOrderTrendDate as date), 3)
#            when wo.worktype ='PM' and   pm.frequencyUnits = 'YEARS' and pm.frequency >= 0 and pm.frequency < 5 then add_months(cast(wo.workOrderTrendDate as date),1)
#            when wo.worktype ='PM' and   pm.frequencyUnits = 'MONTHS' and pm.frequency >= 60 then add_months(cast(wo.workOrderTrendDate as date),3)
#            when wo.worktype ='PM' and   pm.frequencyUnits = 'MONTHS' and pm.frequency >= 6 and pm.frequency < 12 then date_add(cast(wo.workOrderTrendDate as date), 14)
#            when wo.worktype ='PM' and   pm.frequencyUnits = 'MONTHS' and pm.frequency >= 12 and pm.frequency < 60 then add_months(cast(wo.workOrderTrendDate as date) ,1)
#            when wo.worktype ='PM' and   pm.frequencyUnits = 'MONTHS' and pm.frequency >= 1 and pm.frequency < 6 then date_add(cast(wo.workOrderTrendDate as date), 7)
#            when wo.worktype ='PM' and   pm.frequencyUnits = 'MONTHS' and pm.frequency = 0 then wo.workOrderTrendDate
#            when wo.worktype ='PM' and   pm.frequencyUnits = 'WEEKS' and pm.frequency >= 260 then add_months(cast(wo.workOrderTrendDate as date),3)
#            when wo.worktype ='PM' and   pm.frequencyUnits = 'WEEKS' and pm.frequency >= 52 and pm.frequency < 260 then add_months(cast(wo.workOrderTrendDate as date),1)
#            when wo.worktype ='PM' and   pm.frequencyUnits = 'WEEKS' and pm.frequency >= 26 and pm.frequency < 52 then date_add(cast(wo.workOrderTrendDate as date), 14)
#            when wo.worktype ='PM' and   pm.frequencyUnits = 'WEEKS' and pm.frequency >= 5 and pm.frequency < 26 then date_add(cast(wo.workOrderTrendDate as date), 7)
#            when wo.worktype ='PM' and   pm.frequencyUnits = 'WEEKS' and pm.frequency >= 0 and pm.frequency < 5 then wo.workOrderTrendDate
#            when wo.worktype ='PM' and   pm.frequencyUnits = 'DAYS' and pm.frequency >= 1825 then add_months(cast(wo.workOrderTrendDate as date),3)
#            when wo.worktype ='PM' and   pm.frequencyUnits = 'DAYS' and pm.frequency >= 365 and pm.frequency < 1825 then add_months(cast(wo.workOrderTrendDate as date),1)
#            when wo.worktype ='PM' and   pm.frequencyUnits = 'DAYS' and pm.frequency >= 182 and pm.frequency < 365 then date_add(cast(wo.workOrderTrendDate as date), 14)
#            when wo.worktype ='PM' and   pm.frequencyUnits = 'DAYS' and pm.frequency >= 30 and pm.frequency < 182 then date_add(cast(wo.workOrderTrendDate as date), 7)
#            when wo.worktype ='PM' and   pm.frequencyUnits = 'DAYS' and pm.frequency >= 0 and pm.frequency < 30 then wo.workOrderTrendDate
#            when wo.worktype ='PM' and   pm.frequencyUnits is null then wo.workOrderTrendDate
#            when wo.worktype in ('BM','CM','GN') and   mwo.initialPriority in (0,1)  then date_add(cast(wo.workOrderTrendDate as date), 42)
#            when wo.worktype in ('BM','CM','GN') and   mwo.initialPriority = 2  then date_add(cast(wo.workOrderTrendDate as date), 21)
#            when wo.worktype in ('BM','CM','GN') and   mwo.initialPriority = 3  then date_add(cast(wo.workOrderTrendDate as date), 5)
#            when wo.worktype in ('BM','CM','GN') and   mwo.initialPriority = 4  then date_add(cast(wo.workOrderTrendDate as date), 2)
#            --when wo.worktype in ('BM','CM','GN') and   mwo.initialPriority = 5  then date_add(cast(wo.workOrderTrendDate as date), 6/24)
#            ---when wo.worktype in ('BM','CM','GN') and   mwo.initialPriority = 6  then date_add(cast(wo.workOrderTrendDate as date) , 3/24)
# end as toleranced_due_date
# from cleansed.maximo_workOrder mwo
# inner join curated_v2.factWorkOrder wo on mwo.workOrder = wo.workOrderNumber
# LEFT JOIN curated_v2.dimPreventiveMaintenance pm on wo.preventiveMaintenanceFK = pm.preventiveMaintenanceSK

# COMMAND ----------

def Transform():
    global df
    # ------------- TABLES ----------------- #
#   Cleansed WorkOrder table has multiple entries for the same date since the history is managed. Curated requirement is to pick the latest record for that specific date.
    workOrder_windowSpec  = Window.partitionBy("workOrder","changeDate_date_part")
    df = GetTable(f"{SOURCE}.maximo_workOrder").alias("wo") \
    .withColumn("changeDate_date_part",to_date(col("changeDate"))) \
    .withColumn("rank",rank().over(workOrder_windowSpec.orderBy(col("changeDate").desc()))) \
    .filter("rank == 1").drop("rank")
    
    assetLocation_df = GetTable(f"{TARGET}.dimAssetLocation").select("assetLocation","assetLocationSK","locationType").drop_duplicates()
    jobPlan_df = GetTable(f"{TARGET}.dimJobPlan").select("jobPlan","jobPlanSK").drop_duplicates()
    asset_df = GetTable(f"{TARGET}.dimAsset").select("assetNumber","assetSK").drop_duplicates()
    problemType_df = GetTable(f"{TARGET}.dimProblemType").select("problemType","problemTypeSK").drop_duplicates()
    preventive_maintenance_df = GetTable(f"{TARGET}.dimPreventiveMaintenance").select("preventiveMaintenance","preventiveMaintenanceSK", "frequency","frequencyUnits").drop_duplicates()
    childPM_df = GetTable(f"{TARGET}.dimPreventiveMaintenance").select(col("preventiveMaintenance").alias("child_pM"), col("frequency").alias("child_frequency"),col("frequencyUnits").alias("child_frequencyUnits")).drop_duplicates()
    asset_contract_df = GetTable(f"{TARGET}.dimAssetContract").select("assetContractSK","contractNumber").drop_duplicates()
    swchierarchy_df = GetTable(f"{SOURCE}.maximo_swchierarchy").select("code",col("description").alias("serviceDepartmentDesc")).drop_duplicates()    
    child_df = GetTable(f"{SOURCE}.maximo_workOrder").select(col("workOrder").alias("childWorkOrder"),col("parentWo").alias("childParentWo"),col("serviceContract").alias("childServiceContract"),col("pM").alias("childpM")).drop_duplicates()
    
    # WorkOrder Status table has multiple entries for the same date. Curated requirement is to pick the first record when the status changed.
    woStatus_windowSpec  = Window.partitionBy("woWorkOrder")
    wo_status_df = GetTable(f"{SOURCE}.maximo_woStatus").select(col("workOrder").alias("woWorkOrder"),col("Status").alias("woStatus"),col("statusDate").alias("woStatusDate")) \
    .withColumn("rank",rank().over(woStatus_windowSpec.orderBy(col("woStatusDate")))) \
    .filter("rank == 1").drop("rank")
    
    
    # ------------- JOINS ------------------ #
    df = df.join(assetLocation_df,df.location == assetLocation_df.assetLocation, "left") \
    .join(jobPlan_df, df.jobPlan == jobPlan_df.jobPlan,"left") \
    .join(asset_df,df.asset == asset_df.assetNumber,"left") \
    .join(preventive_maintenance_df, df.pM == preventive_maintenance_df.preventiveMaintenance,"left") \
    .join(asset_contract_df, df.serviceContract == asset_contract_df.contractNumber,"left") \
    .join(problemType_df, "problemType","left") \
    .join(swchierarchy_df, df.serviceDepartment == swchierarchy_df.code ,"left") \
    .join(child_df,df.workOrder == child_df.childParentWo,"left") \
    .join(childPM_df, child_df.childpM == childPM_df.child_pM,"left") \
    .join(wo_status_df,((df.workOrder == wo_status_df.woWorkOrder) & (wo_status_df.woStatus == 'SCHED')) ,"left") \
    .withColumnRenamed("woStatusDate","scheduledDate").drop("woWorkOrder","woStatus") \
    .join(wo_status_df,((df.workOrder == wo_status_df.woWorkOrder) & (wo_status_df.woStatus == 'INPRG')) ,"left") \
    .withColumnRenamed("woStatusDate","inProgressDate").drop("woWorkOrder","woStatus") \
    .join(wo_status_df,((df.workOrder == wo_status_df.woWorkOrder) & (wo_status_df.woStatus == 'CAN')) ,"left") \
    .withColumnRenamed("woStatusDate","cancelledDate").drop("woWorkOrder","woStatus") \
    .join(wo_status_df,((df.workOrder == wo_status_df.woWorkOrder) & (wo_status_df.woStatus == 'APPR')) ,"left") \
    .withColumnRenamed("woStatusDate","approvedDate").drop("woWorkOrder","woStatus") \
    .join(wo_status_df,((df.workOrder == wo_status_df.woWorkOrder) & (wo_status_df.woStatus == 'COMP')),"left") \
    .withColumnRenamed("woStatusDate","completedDate").drop("woWorkOrder","woStatus") \
    .join(wo_status_df,((df.workOrder == wo_status_df.woWorkOrder) & (wo_status_df.woStatus == 'CLOSE')) ,"left") \
    .withColumnRenamed("woStatusDate","closedDate").drop("woWorkOrder","woStatus") \
    .join(wo_status_df,((df.workOrder == wo_status_df.woWorkOrder) & (wo_status_df.woStatus == 'FINSHED')) ,"left") \
    .withColumnRenamed("woStatusDate","finishedDate").drop("woWorkOrder","woStatus") \
    .withColumn("hasChildren", when(child_df.childWorkOrder != None,"YES").otherwise("NO")) \
    .withColumn("workOrderTrendDate",expr("CASE WHEN wo.WORKTYPE ='PM' \
    THEN COALESCE(wo.targetFinish, wo.targetStart,wo.scheduledStart, \
    wo.scheduledFinish, wo.targetRespondBy, wo.reportedDateTime) \
    ELSE \
    wo.reportedDateTime \
    END")) \
    .withColumn("finishDateTime", expr(" CASE WHEN wo.actualFinish <= wo.statusDate and wo.status in ('FINISHED', 'COMP', 'CLOSE') THEN wo.actualFinish \
             WHEN wo.actualFinish >= wo.statusDate and wo.status in ('FINISHED', 'COMP', 'CLOSE') \
             THEN wo.statusDate \
             WHEN wo.actualFinish is NULL and wo.status in ('FINISHED', 'COMP', 'CLOSE') \
             THEN wo.statusDate \
        		 ELSE NULL \
             END "))                             
#     df = df.withColumn("calculatedTargetDateTime", calculate_target_date_time(col("workType"), col("initialPriority"), col("workOrderTrendDate")))  
                                                                                                 


   
    # ------------- TRANSFORMS ------------- #
    _.Transforms = [
        f"workOrder||'|'||changeDate_date_part {BK}"
        ,"assetLocationSK assetLocationFK"
        ,"jobPlanSK jobPlanFK"
        ,"assetSK assetFK"
        ,"problemTypeSK problemTypeFK"
        ,"preventiveMaintenanceSK preventiveMaintenanceFK"
        ,"assetContractSK assetContractFK"
        ,"workOrder workOrderNumber"
        ,"parentWo parentWorkOrderNumber"
        ,"status statusCode"
        ,"status status"
        ,"dispatchSystem dispatchSystem"
        ,"workType workType"
        ,"class workOrderClass"
        ,"initialPriority initialPriority"
        ,"serviceDepartment serviceDepartmentCode"
        ,"serviceDepartmentDesc serviceDepartment"
        ,"serviceType serviceType"
        ,"taskCode taskCode"
        ,"fCId fCId"
        ,"assessedPriority assessedPriority"
        ,"numberOfUnits numberOfUnits"
        ,"actualLaborHours actualLaborHours"
        ,"actualLaborCost actualLaborCost"
        ,"actualMaterialCost actualMaterialCost"
        ,"actualServiceCost actualServiceCost"
        ,"estimatedLaborHours estimatedLaborHours"
        ,"estimatedDuration estimatedDuration"
        ,"actualFinish actualFinish"
        ,"actualCostOfInternalLabor actualCostOfInternalLabor"
        ,"actualHoursOfInternalLabor actualHoursOfInternalLabor"
        ,"actualCostOfExternalLabor actualCostOfExternalLabor"
        ,"actualHoursOfExternalLabor actualHoursOfExternalLabor"
        ,"actualStart actualStart"
        ,"actualToolCost actualToolCost"
        ,"estimatedLaborCost estimatedLaborCost"
        ,"estimatedMaterialCost estimatedMaterialCost"
        ,"estimatedServiceCost estimatedServiceCost"
        ,"estimatedToolCost estimatedToolCost"
        ,"estimatedCostOfInternalLabor estimatedCostOfInternalLabor"
        ,"estimatedHoursOfInternalLabor estimatedHoursOfInternalLabor"
        ,"estimatedCostOfExternalLabor estimatedCostOfExternalLabor"
        ,"estimatedHoursOfExternalLabor estimatedHoursOfExternalLabor"
        ,"targetDescription targetDescription"
        ,"targetFinish targetFinish"
        ,"targetStart targetStart"
        ,"scheduledStart scheduledStart"
        ,"scheduledFinish scheduledFinish"
        ,"targetRespondBy targetRespondBy"
        ,"reportedDateTime reportedDateTime"
        ,"changeDate changeDate"
        ,"scheduledDate scheduledDate"
        ,"inProgressDate inProgressDate"
        ,"cancelledDate cancelledDate"
        ,"approvedDate approvedDate"
        ,"completedDate completedDate"
        ,"closedDate closedDate"
        ,"finishedDate finishedDate"
        ,"hasChildren hasChildren"
        ,"workOrderTrendDate workOrderTrendDate"
        ,"finishDateTime finishDateTime"
#         ,"calculatedTargetDateTime calculatedTargetDateTime"
        ,"COALESCE(frequencyUnits, child_frequencyUnits) freqUnit"
        ,"COALESCE(frequency, child_frequency) frequency"
#         ,"tolerancedDueDate tolerancedDueDate"
        ,"COALESCE(pM, childpM) preventiveMaintenanceNumber"
        ,"COALESCE(serviceContract, childServiceContract) contract"
        ,"least(completedDate,closedDate,finishedDate) statusCloseDate"
        ,"locationType locationType"
#         ,"'' periodStartDate"
#         ,"'' periodEndDate"
#         ,"'' comparisonPeriodStartDate"
#         ,"'' comparisonPeriodEndDate"
#         ,"'' repairHour"
        
       
    ]
    df = df.selectExpr(
        _.Transforms
    ).drop_duplicates()
    # ------------- CLAUSES ---------------- #

    # ------------- SAVE ------------------- #
#     display(df)
    CleanSelf()
    Save(df)
    #DisplaySelf()
pass
Transform()

# COMMAND ----------

# MAGIC %sql
# MAGIC Select workorderSK, count(1) from curated_v2.factWorkOrder group by workorderSK having count(1) >1

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from curated_v2.factWorkOrder where workorderSK = '7aaf7dc99631131311357d04410d035d'

# COMMAND ----------


