# Databricks notebook source
# MAGIC %run ../../Common/common-transform

# COMMAND ----------

def Transform():
    # ------------- TABLES ----------------- #
    df = GetTable(f"{SOURCE}.crm_0crm_category_text")
    # ------------- JOINS ------------------ #

    # ------------- TRANSFORMS ------------- #
    _.Transforms = [
        f"categoryCode {BK}"
        ,"categoryDescription channel"
        ,"categoryCode channelCode"
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
