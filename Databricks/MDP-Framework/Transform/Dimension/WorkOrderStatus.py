# Databricks notebook source
# MAGIC %run ../../Common/common-transform

# COMMAND ----------

TARGET = DEFAULT_TARGET

# COMMAND ----------

def Transform():
    global df
    # ------------- TABLES ----------------- #
    df = GetTable(f"{SOURCE}.maximo_wostatus")
    wo_df = GetTable(f"{TARGET}.factWorkOrder").select("workOrderNumber","workOrderSK")
  
    # ------------- JOINS ------------------ #
    df = df.join(wo_df,df.workOrder == wo_df.workOrderNumber,"left") 

    # ------------- TRANSFORMS ------------- #
    _.Transforms = [
        f"workOrderSK||'|'||status {BK}"
        ,"workOrderSK workOrderFK"
        ,"status workOrderStatus"
        ,"statusDate statusDate"
        ,"changedBy changedBy"
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
