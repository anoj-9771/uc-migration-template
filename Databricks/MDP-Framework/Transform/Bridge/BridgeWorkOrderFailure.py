# Databricks notebook source
# MAGIC %run ../../Common/common-transform

# COMMAND ----------

TARGET = DEFAULT_TARGET

# COMMAND ----------

def Transform():
    
    # ------------- TABLES ----------------- #
    df = GetTable(f"{SOURCE}.maximo_failurereport")
    factWorkOrder_df = GetTable(f"{TARGET}.factWorkOrder").select("workOrderSK","workOrderNumber")
    factFailureReport_df = GetTable(f"{TARGET}.factFailureReport").select("failureCode","failureReportSK")
      
                                
    # ------------- JOINS ------------------ #
    df  = df.join(factWorkOrder_df,df.workOrder == factWorkOrder_df.workOrderNumber,"left") \
    .join(factFailureReport_df,"failureCode","left")
    # ------------- TRANSFORMS ------------- #
      
    _.Transforms = [
         f"workOrder||'|'||failureReportId {BK}"
        ,"workOrderSK workOrderFK"
        ,"failureReportSK failureReportFK"
    ]
    df = df.selectExpr(
        _.Transforms
    )
    # ------------- CLAUSES ---------------- #

    # ------------- SAVE ------------------- #
#     display(df)
    # CleanSelf()
    Save(df)
#     DisplaySelf()
pass
Transform()
