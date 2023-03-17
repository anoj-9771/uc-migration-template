# Databricks notebook source
# MAGIC %run ../../Common/common-transform

# COMMAND ----------

def Transform():
    global df
    # ------------- TABLES ----------------- #
    df = GetTable(f"{SOURCE}.maximo_jobplan")
   
    # ------------- JOINS ------------------ #
    
    

    # ------------- TRANSFORMS ------------- #
    _.Transforms = [
        f"jobPlan {BK}"
        ,"site jobSite"
        ,"organization organizationName"
        ,"jobPlan jobPlan"
        ,"masterJobPlan masterJobPlan"
        ,"jobPlanId jobPlanId"
        ,"classStructure classStructure"
        ,"duration jobDuration"
        ,"description jobDescription"
        ,"woPriority woPriority"
        ,"product product"
        ,"workType workType"
        ,"taskCode taskCode" 
        ,"serviceType serviceType" 
        ,"status status"
        ,"cloneType cloneType"
        ,"level level"
        ,"changeBy changeBy"
        ,"changeDate changeDate"
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

# MAGIC %sql
# MAGIC select * from curated_v2.dimJobPlan

# COMMAND ----------


