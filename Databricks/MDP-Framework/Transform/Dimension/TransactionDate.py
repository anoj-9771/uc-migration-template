# Databricks notebook source
# MAGIC %run ../../Common/common-transform 

# COMMAND ---------- 

# MAGIC %run ../../Common/common-helpers 
# COMMAND ---------- 


# COMMAND ----------

def Transform():
    global df
    # ------------- TABLES ----------------- #
    df = GetTable(f"{get_table_namespace(f'{SOURCE}', 'crm_scapptseg')}")
#     desc_df = GetTable(f"{get_table_namespace(f'{SOURCE}', 'crm_scapttxt')}").select('apptType','apptTypeDescription')
  
    # ------------- JOINS ------------------ #
#     df = df.join(desc_df,"apptType","inner") 

    # ------------- TRANSFORMS ------------- #
    _.Transforms = [
        f"applicationGUID||'|'||apptType {BK}"
        ,"apptStartDatetime activityDate"
        ,"apptTypeDescription transactionDescription"
        ,"apptType transactionCode"
        ,"entryBy createdBy"
        ,"entryTimestamp createdDateTime"
        ,"changedBy changedBy"
        ,"changedDatetime changedDateTime"
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
