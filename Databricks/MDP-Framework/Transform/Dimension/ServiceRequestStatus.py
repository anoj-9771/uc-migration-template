# Databricks notebook source
# MAGIC %md 
# MAGIC Vno| Date      | Who         |Purpose
# MAGIC ---|:---------:|:-----------:|:--------:
# MAGIC 1  |28/02/2023 |Mag          |Initial

# COMMAND ----------

# MAGIC %run ../../Common/common-transform

# COMMAND ----------

from pyspark.sql.functions import broadcast
def Transform():
    global df 
    
    # ------------- TABLES ----------------- #   
    srStatus = GetTable(f"{SOURCE}.crm_0crm_srv_req_inci_h").select("statusProfile").distinct().filter("statusProfile IS NOT NULL") 
    
    recStatus = GetTable(f"{SOURCE}.crm_tj30t") \
    .select("statusProfile", "statusCode", "statusShortDescription","status").dropDuplicates() 
    
        
    # ------------- JOIN AND SELECT ----------------- #
    df = recStatus.alias("aa").join(broadcast(srStatus.alias("bb")), recStatus["statusProfile"] == srStatus["statusProfile"], "inner").select("aa.*")
   
    #------------- TRANSFORMS ------------- #
    _.Transforms = [
        f"statusProfile||'|'||statusCode {BK}" 
        ,"statusProfile"
        ,"statusCode"
        ,"statusShortDescription"
        ,"status"        
        
    ]
    
    df = df.selectExpr(
        _.Transforms
    )
    
    #df.display()
    #display(df)
    # CleanSelf()
    Save(df)
    #DisplaySelf()
    
Transform()
