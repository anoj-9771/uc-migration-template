# Databricks notebook source
# MAGIC %run ../../Common/common-transform 

# COMMAND ----------

# MAGIC %run ../../Common/common-helpers 

# COMMAND ----------



# COMMAND ----------

TARGET = DEFAULT_TARGET

# COMMAND ----------

def Transform():
    
    # ------------- TABLES ----------------- #
    df = get_recent_records(f"{SOURCE}","maximo_pM","changedDate","preventiveMaintenanceChangedTimestamp").alias("maximo_pM")
    jobPlan_df = GetTable(f"{get_table_namespace(f'{TARGET}', 'dimWorkOrderJobPlan')}").select("workOrderJobPlanNumber","workOrderJobPlanRevisionNumber","workOrderJobPlanSK").withColumn("rank",rank().over(Window.partitionBy("workOrderJobPlanNumber").orderBy(col("workOrderJobPlanRevisionNumber").desc()))) \
    .filter("rank == 1").drop("rank").cache()
    assetContract_df = GetTable(f"{get_table_namespace(f'{TARGET}', 'dimAssetContract')}").select("assetContractNumber","assetContractRevisionNumber","assetContractSK").withColumn("rank",rank().over(Window.partitionBy("assetContractNumber").orderBy(col("assetContractRevisionNumber").desc()))) \
    .filter("rank == 1").drop("rank").cache()
    asset_df = GetTable(f"{get_table_namespace(f'{TARGET}', 'dimAsset')}").select("assetNumber","assetSK")
    
    
    # ------------- JOINS ------------------ #
    df = df.join(jobPlan_df,df.jobPlan == jobPlan_df.workOrderJobPlanNumber,"left") \
    .join(assetContract_df, df.serviceContract == assetContract_df.assetContractNumber,"left") \
    .join(asset_df, df.asset == asset_df.assetNumber,"left") 

    # ------------- TRANSFORMS ------------- #
    _.Transforms = [
        f"pM ||'|'||changedDate {BK}"
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
        ,"TO_DATE(changedDate) snapshotDate"
        
    ]
    df = df.selectExpr(
        _.Transforms
    ).drop_duplicates()
    # ------------- CLAUSES ---------------- #

    # ------------- SAVE ------------------- #
#     display(df)
    # CleanSelf()
    Save(df)
    #DisplaySelf()
pass
Transform()
