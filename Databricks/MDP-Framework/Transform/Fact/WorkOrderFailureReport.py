# Databricks notebook source
# MAGIC %run ../../Common/common-transform 

# COMMAND ---------- 

# MAGIC %run ../../Common/common-helpers 
# COMMAND ---------- 


# COMMAND ----------

TARGET = DEFAULT_TARGET

# COMMAND ----------

def Transform():
    global df
    # ------------- TABLES ----------------- #
    df = GetTable(f"{get_table_namespace(f'{SOURCE}', 'maximo_failurereport')}")
    factworkorder_df = GetTable(f"{get_table_namespace(f'{TARGET}', 'factWorkOrder')}").select("workOrderCreationId","workOrderSK","assetFK","workOrderChangeTimestamp")
    failureCode_df = GetTable(get_table_name(f"{SOURCE}","maximo","failureCode")).select("failureCode",col("description").alias("failureDescription"))
    
    # ------------- JOINS ------------------ #
    df = df.join(failureCode_df,"failureCode", "left") \
    .join(factworkorder_df,df.workOrder == factworkorder_df.workOrderCreationId,"inner") 
   
    # ------------- TRANSFORMS ------------- #
    _.Transforms = [
        f"failureReportId||'|'||workOrderChangeTimestamp {BK}"
        ,"failureReportId workOrderFailureReportId"
        ,"workOrderChangeTimestamp workOrderFailureReportChangeTimestamp"
        ,"assetFK"
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
# MAGIC select workorderfailureReportSK, count(1) from {get_table_namespace('curated', 'factworkorderfailurereport')} group by workorderfailureReportSK having count(1) > 1

# COMMAND ----------



# COMMAND ----------


