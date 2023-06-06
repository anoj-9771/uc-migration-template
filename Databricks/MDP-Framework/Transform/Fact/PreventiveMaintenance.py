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
    df = get_recent_cleansed_records(f"{SOURCE}","maximo","pM","changed_date","preventiveMaintenanceChangedTimestamp").alias("maximo_pM")
    jobPlan_df = GetTable(f"{get_table_namespace(f'{TARGET}', 'dimWorkOrderJobPlan')}").select("workOrderJobPlanNumber","workOrderJobPlanSK")
    assetContract_df = GetTable(f"{get_table_namespace(f'{TARGET}', 'dimAssetContract')}").select("assetContractNumber","assetContractSK")
    asset_df = GetTable(f"{get_table_namespace(f'{TARGET}', 'dimAsset')}").select("assetNumber","assetSK")
    
    
    # ------------- JOINS ------------------ #
    df = df.join(jobPlan_df,df.jobPlan == jobPlan_df.workOrderJobPlanNumber,"left") \
    .join(assetContract_df, df.serviceContract == assetContract_df.assetContractNumber,"left") \
    .join(asset_df, df.asset == asset_df.assetNumber,"left") 

    # ------------- TRANSFORMS ------------- #
    _.Transforms = [
        f"pM ||'|'||changedDate {BK}"
        ,"pM preventiveMaintenanceID"
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
        ,"alertLeadDays preventiveMaintenanceLAlertLeadDaysQuantity"
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

# COMMAND ----------

spark.sql(
    f"""create or replace view {get_table_namespace('curated', 'factPreventiveMaintenance')} AS (SELECT * from {get_table_namespace('curated', 'factpreventivemaintenance')})
          """)

# COMMAND ----------


