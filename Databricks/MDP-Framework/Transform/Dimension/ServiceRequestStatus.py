# Databricks notebook source
# MAGIC %run ../../Common/common-transform

# COMMAND ----------

def Transform():
    global df
    # ------------- TABLES ----------------- #
    df = GetTable(f"{SOURCE}.crm_tj30t")
    # ------------- JOINS ------------------ #

    # ------------- TRANSFORMS ------------- #
    _.Transforms = [
        f"statusProfile||' | '|| statusCode {BK}"
        ,"statusProfile statusProfile"
        ,"statusCode statusCode"
        ,"statusShortDescription statusShortDescription"
        ,"status status"
    ]
    df = df.selectExpr(
        _.Transforms
    )
    # ------------- CLAUSES ---------------- #

    # ------------- SAVE ------------------- #
    #display(df)
#     CleanSelf()
    Save(df)
    #DisplaySelf()
pass
Transform()

# COMMAND ----------


