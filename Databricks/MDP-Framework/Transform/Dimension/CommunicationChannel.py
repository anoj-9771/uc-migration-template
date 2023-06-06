# Databricks notebook source
# MAGIC %run ../../Common/common-transform 

# COMMAND ---------- 

# MAGIC %run ../../Common/common-helpers 
# COMMAND ---------- 


# COMMAND ----------

def Transform():
    # ------------- TABLES ----------------- #
    df = GetTable(f"{get_table_namespace(f'{SOURCE}', 'crm_0crm_category_text')}")
    # ------------- JOINS ------------------ #

    # ------------- TRANSFORMS ------------- #
    df = df.withColumn("sourceSystemCode",lit("CRM")) \
       
    _.Transforms = [
         f"categoryCode||'|'||sourceSystemCode {BK}"
        ,"categoryCode customerServiceChannelCode"
        ,"categoryDescription customerServiceChannelDescription"
        ,"sourceSystemCode sourceSystemCode"
    ]
    df = df.selectExpr(
        _.Transforms
    )
    # ------------- CLAUSES ---------------- #

    # ------------- SAVE ------------------- #
#     display(df)
    #CleanSelf()
    Save(df)
    #DisplaySelf()
pass
Transform()
