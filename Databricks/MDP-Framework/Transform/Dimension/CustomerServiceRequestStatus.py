# Databricks notebook source
# MAGIC %md 
# MAGIC Vno| Date      | Who         |Purpose
# MAGIC ---|:---------:|:-----------:|:--------:
# MAGIC 1  |28/02/2023 |Mag          |Initial

# COMMAND ----------

# MAGIC %run ../../Common/common-transform 

# COMMAND ---------- 

# MAGIC %run ../../Common/common-helpers 
# COMMAND ---------- 


# COMMAND ----------

from pyspark.sql.functions import broadcast
def Transform():
    global df 
    
    # ------------- TABLES ----------------- #   
    srStatus = GetTable(f"{get_table_namespace(f'{SOURCE}', 'crm_0crm_srv_req_inci_h')}").select("statusProfile").distinct().filter("statusProfile IS NOT NULL") 
    
    recStatus = GetTable(f"{get_table_namespace(f'{SOURCE}', 'crm_tj30t')}") \
    .select("statusProfile", "statusCode", "statusShortDescription","status").dropDuplicates() 
    
        
    # ------------- JOIN AND SELECT ----------------- #
    df = recStatus.alias("aa").join(broadcast(srStatus.alias("bb")), recStatus["statusProfile"] == srStatus["statusProfile"], "inner").select("aa.*")
   
    #------------- TRANSFORMS ------------- #
    _.Transforms = [
        f"statusProfile||'|'||statusCode {BK}" 
        ,"statusProfile             customerServiceRequestStatusProfile"
        ,"statusCode                customerServiceRequestStatusCode"
        ,"statusShortDescription    customerServiceRequestStatusShortDescription"
        ,"status                    customerServiceRequestStatusDescription"        
        
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
