# Databricks notebook source
# MAGIC %run ../../Common/common-transform

# COMMAND ----------

TARGET = DEFAULT_TARGET

# COMMAND ----------

def Transform():
    global df
    # ------------- TABLES ----------------- #
    df = GetTable(f"{SOURCE}.maximo_pM").alias("maximo_pM")
    jobPlan_df = GetTable(f"{TARGET}.dimJobPlan").select("jobPlan","jobPlanSK")
    assetContract_df = GetTable(f"{TARGET}.dimAssetContract").select("contractNumber","assetContractSK")
    asset_df = GetTable(f"{TARGET}.dimAsset").select("assetNumber","assetSK")
    
    
   
    # ------------- JOINS ------------------ #
    df = df.join(jobPlan_df,"jobPlan","left") \
    .join(assetContract_df, df.serviceContract == assetContract_df.contractNumber,"left") \
    .join(asset_df, df.asset == asset_df.assetNumber,"left") \
    .withColumn("frequencyInDays",expr("CASE WHEN maximo_pM.frequencyUnits = 'YEARS' THEN maximo_pM.frequency*365 \
    WHEN maximo_pM.frequencyUnits = 'MONTHS' THEN maximo_pM.frequency*30 \
    WHEN maximo_pM.frequencyUnits = 'WEEKS' THEN maximo_pM.frequency*7 \
    WHEN maximo_pM.frequencyUnits = 'DAYS' THEN maximo_pM.frequency \
    END"))

    # ------------- TRANSFORMS ------------- #
    _.Transforms = [
        f"pM {BK}"
        ,"jobPlanSK jobPlanFK"
        ,"assetContractSK assetContractFK"
        ,"assetSK assetFK"
        ,"pM preventiveMaintenance"
        ,"description pMDescription"
        ,"frequency frequency"
        ,"frequencyUnits frequencyUnits"
        ,"frequencyInDays frequencyInDays"
        ,"downtime downtime"
        ,"firstStartDate firstStartDate"
        ,"lastStartDate lastStartDate"
        ,"lastCompletionDate lastCompletionDate"
        ,"masterPM masterPM"
        ,"parent parent"
        ,"pmUID pmUID"
        ,"priority priority" 
        ,"route route"
        ,"status status" 
        ,"serviceDepartment serviceDepartment"
        ,"workCategory workCategory"
        ,"workType workType"
        
    ]
    df = df.selectExpr(
        _.Transforms
    )
    # ------------- CLAUSES ---------------- #

    # ------------- SAVE ------------------- #
#     display(df)
    # CleanSelf()
    Save(df)
    #DisplaySelf()
pass
Transform()

# COMMAND ----------


