# Databricks notebook source
# MAGIC %run ../../Common/common-transform

# COMMAND ----------

def Transform():
    global df
    # ------------- TABLES ----------------- #
    df = GetTable(f"{SOURCE}.crm_0crm_proc_type_text")
    # ------------- JOINS ------------------ #

    # ------------- TRANSFORMS ------------- #
    _.Transforms = [
        f"processTypeCode {BK}"
        ,"processTypeCode processTypeCode"
        ,"processTypeShortDescription processTypeShortDescription"
        ,"processTypeDescription processTypeDescription"
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
