# Databricks notebook source
# MAGIC %run ../../Common/common-transform 

# COMMAND ----------

#  CleanSelf()

# COMMAND ----------

TARGET = DEFAULT_TARGET

# COMMAND ----------

def Transform():
    
    # ------------- TABLES ----------------- #
    df = get_recent_records(f"{SOURCE}","maximo_pM","changedDate","preventiveMaintenanceChangedTimestamp").alias("maximo_pM")\
        .withColumn("changedDate_date_part",to_date(col("changedDate"))) \
        .withColumn("rank",rank().over(Window.partitionBy("pM","changedDate_date_part").orderBy(col("rowStamp").desc()))).filter("rank == 1").drop("rank")

    jobPlan_df = GetTable(f"{get_table_namespace(f'{TARGET}', 'dimWorkOrderJobPlan')}")\
        .filter("_recordCurrent == 1").filter("_recordDeleted == 0")\
        .withColumn("rank",rank().over(Window.partitionBy("workOrderJobPlanNumber").orderBy(col("workOrderJobPlanRevisionNumber").desc()))) \
    .filter("rank == 1")\
        .select("workOrderJobPlanNumber","workOrderJobPlanSK","sourceValidFromTimestamp","sourceValidToTimestamp").cache()

    assetContract_df = GetTable(f"{get_table_namespace(f'{TARGET}', 'dimAssetContract')}")\
        .filter("_recordCurrent == 1").filter("_recordDeleted == 0")\
        .withColumn("rank",rank().over(Window.partitionBy("assetContractNumber").orderBy(col("assetContractRevisionNumber").desc()))).filter("rank == 1") \
        .select("assetContractNumber","assetContractSK","sourceValidFromTimestamp","sourceValidToTimestamp").cache()
    
    asset_df = GetTable(f"{get_table_namespace(f'{TARGET}', 'dimAsset')}")\
        .filter("_recordCurrent == 1").filter("_recordDeleted == 0")\
        .select("assetNumber","assetSK","sourceValidFromTimestamp","sourceValidToTimestamp")
    
    
    # ------------- JOINS ------------------ #
    df = df.join(jobPlan_df,(df.jobPlan == jobPlan_df.workOrderJobPlanNumber) & (df.changedDate.between (jobPlan_df.sourceValidFromTimestamp,jobPlan_df.sourceValidToTimestamp)),"left") \
    .join(assetContract_df, (df.serviceContract == assetContract_df.assetContractNumber) & (df.changedDate.between (assetContract_df.sourceValidFromTimestamp,assetContract_df.sourceValidToTimestamp)),"left") \
    .join(asset_df, (df.asset == asset_df.assetNumber)& (df.changedDate.between (asset_df.sourceValidFromTimestamp,asset_df.sourceValidToTimestamp)),"left") 

    # ------------- TRANSFORMS ------------- #
    _.Transforms = [
        f"pM ||'|'||changedDate_date_part {BK}"
        ,"pM preventiveMaintenanceId"
        ,"changedDate preventiveMaintenanceChangedTimestamp"
        ,"workOrderJobPlanSK workOrderJobPlanFK"
        ,"assetContractSK assetContractFK"
        ,"assetSK assetFK"
        ,"changedBy preventiveMaintenanceChangedByUserName"
        ,"description preventiveMaintenanceDescription"
        ,"parent preventiveMaintenanceParentId"
        ,"masterPM preventiveMaintenanceMasterId"
        ,"hasChildren preventiveMaintenanceChildIndicator"
        ,"frequency preventiveMaintenanceFrequencyIndicator"
        ,"frequencyUnits preventiveMaintenanceFrequencyUnitName"
        ,"overrideUpdatesFromMasterPm preventiveMaintenanceUpdateByMasterOverrideIndicator"
        ,"generateWorkOrderWhenMeterFrequencyIsReached preventiveMaintenanceFrequencyBasedWorkOrderGenerationIndicator"
        ,"generateWorkOrderBasedOnMeterReadingsDoNotEstimate preventiveMaintenanceNonEstimateMeterReadingWorkOrderGenerationIndicator"
        ,"sunday preventiveMaintenanceActivitySundayIndicator"
        ,"monday preventiveMaintenanceActivityMondayIndicator"
        ,"tuesday preventiveMaintenanceActivityTuesdayIndicator"
        ,"wednesday preventiveMaintenanceActivityWednesdayIndicator"
        ,"thursday preventiveMaintenanceActivityThursdayIndicator"
        ,"friday preventiveMaintenanceActivityFridayIndicator"
        ,"saturday preventiveMaintenanceActivitySaturdayIndicator"
        ,"status preventiveMaintenanceStatusDescription" 
        ,"route preventiveMaintenanceAssetLocationRouteCode"
        ,"serviceDepartment prevnetiveMaintenanceServiceDepartmentCode"
        ,"workCategory preventiveMaintenanceWorkCategoryCode"
        ,"workType preventiveMaintenanceWorkTypeCode"
        ,"alertLeadDays preventiveMaintenanceAlertLeadDaysQuantity"
        ,"serviceType preventiveMaintenanceServiceTypeCode"
        ,"maintenanceCategory preventiveMaintenanceCategoryCode"
        ,"statutory preventiveMaintenanceStatutoryIndicator"
        ,"priority preventiveMaintenancePriorityIndicator" 
        ,"downtime preventiveMaintenanceDowntimeFlag"
        ,"firstStartDate preventiveMaintenanceFirstStartTimestamp"
        ,"lastStartDate preventiveMaintenanceLastStartTimestamp"
        ,"lastCompletionDate preventiveMaintenanceLastCompletionTimestamp"
        ,"adjustNextDueDate preventiveMaintenanceAdjustNextDueDateFlag"
        ,"pmUID preventiveMaintenanceUniqueIdentifier"
        ,"changedDate_date_part snapshotDate"
        
    ]
    df = df.selectExpr(
        _.Transforms
    ).drop_duplicates()
    # ------------- CLAUSES ---------------- #

    # ------------- SAVE ------------------- #
#     display(df)
   
    Save(df)
    #DisplaySelf()
pass
Transform()
