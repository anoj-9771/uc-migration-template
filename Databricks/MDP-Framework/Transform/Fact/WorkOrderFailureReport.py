# Databricks notebook source
# MAGIC %run ../../Common/common-transform

# COMMAND ----------

TARGET = DEFAULT_TARGET

# COMMAND ----------

def Transform():
    global df
    # ------------- TABLES ----------------- #
    df = GetTable(f"{SOURCE}.maximo_failurereport")
    asset_df = GetTable(f"{TARGET}.dimAsset").select("assetNumber","assetSK","sourcevalidFromTimestamp","sourcevalidToTimestamp")
    factworkorder_df = GetTable(f"{TARGET}.factWorkOrder").select("workOrderCreationId","workOrderSK","workOrderChangeTimestamp")
    failureCode_df = GetTable(get_table_name(f"{SOURCE}","maximo","failureCode")).select("failureCode",col("description").alias("failureDescription"))
    
    # ------------- JOINS ------------------ #
    df = df.join(failureCode_df,"failureCode", "left") \
    .join(factworkorder_df,df.workOrder == factworkorder_df.workOrderCreationId,"inner") \
    .join(asset_df,(df.asset == asset_df.assetSK) & (factworkorder_df.workOrderChangeTimestamp.between(asset_df.sourcevalidFromTimestamp,asset_df.sourcevalidToTimestamp )),"left")
   
    # ------------- TRANSFORMS ------------- #
    _.Transforms = [
        f"failureReportId||'|'||workOrderChangeTimestamp {BK}"
        ,"failureReportId workOrderFailureReportId"
        ,"workOrderChangeTimestamp workOrderFailureReportChangeTimestamp"
        ,"assetSK assetFK"
        ,"workOrderSK workOrderFK"
        ,"ticket workOrderFailureReportTicketIdentifier"
        ,"ticketClass workOrderFailureReportTicketClass"
        ,"failureCode workOrderFailureReportCode"
        ,"failureDescription workOrderFailureReportDescription"
        ,"line workOrderFailureReportListId"
        ,"type workOrderFailureReportType"
        ,"rowStamp workOrderFailureReportRowIdentifer"
        ,"TO_DATE(workOrderChangeTimestamp) snapshotDate"
       
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

# MAGIC %sql
# MAGIC select workorderfailureReportSK, count(1) from curated.factworkorderfailurereport group by workorderfailureReportSK having count(1) > 1

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace view  curated_v3.factworkOrderfailurereport as (select * from curated.factWorkOrderFailureReport)

# COMMAND ----------

