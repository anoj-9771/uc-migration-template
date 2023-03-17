# Databricks notebook source
# MAGIC %run ../../Common/common-transform

# COMMAND ----------

TARGET = DEFAULT_TARGET

# COMMAND ----------

def Transform():
    global df
    # ------------- TABLES ----------------- #
    df = GetTable(f"{SOURCE}.maximo_failurereport")
    asset_df = GetTable(f"{TARGET}.dimAsset").select("assetNumber","assetSK")
    workorder_df = GetTable(f"{TARGET}.factWorkOrder").select("workOrderNumber","workOrderSK")
    failureCode_df = GetTable(f"{SOURCE}.maximo_failureCode").select("failureCode",F.col("description").alias("failureDescription"))
    
    # ------------- JOINS ------------------ #
    df = df.join(failureCode_df,"failureCode", "inner")
   
    # ------------- TRANSFORMS ------------- #
    _.Transforms = [
        f"assetSK||'|'||failureReportId {BK}"
        ,"assetSK assetFK"
        ,"workOrderSK workOrderFK"
        ,"'' ticketFK" 
        ,"failureReportId failureReportId"
        ,"failureCode failureCode"
        ,"failureDescription failureDescription"
        ,"line lineNumber"
        ,"type failureType"
        ,"ticketClass ticketClass"
        ,"rowStamp rowStamp"
       
    ]
    df = df.selectExpr(
        _.Transforms
    )
    # ------------- CLAUSES ---------------- #

    # ------------- SAVE ------------------- #
#     display(df)
    CleanSelf()
    Save(df)
    #DisplaySelf()
pass
Transform()

# COMMAND ----------


