# Databricks notebook source
# MAGIC %run ../../Common/common-transform

# COMMAND ----------

def Transform():
    # ------------- TABLES ----------------- #
    df = GetTable(f"{SOURCE}.crm_0crm_category_text")
    # ------------- JOINS ------------------ #

    # ------------- TRANSFORMS ------------- #
    df = df.withColumn("sourceSystemCode",lit("CRM")) \
    .withColumn("channelCode",lit("")) \
    .withColumn("channelDescription",lit(""))
        
    _.Transforms = [
         f"categoryCode||'|'||categoryDescription {BK}"
        ,"categoryCode sourceChannelCode"
        ,"categoryDescription sourceChannelDescription"
        ,"channelCode channelCode"
        ,"channelDescription channelDescription"
        ,"sourceSystemCode sourceSystemCode"
    ]
    df = df.selectExpr(
        _.Transforms
    )
    # ------------- CLAUSES ---------------- #

    # ------------- SAVE ------------------- #
#     display(df)
#     CleanSelf()
    Save(df)
    #DisplaySelf()
pass
Transform()

# COMMAND ----------


