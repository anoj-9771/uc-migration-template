# Databricks notebook source
# MAGIC %run ../../Common/common-transform 

# COMMAND ----------

from pyspark.sql.functions import broadcast

srStatus = (GetTable(f"{getEnv()}cleansed.crm.0crm_srv_req_inci_h")
             .select("statusProfile").distinct().filter("statusProfile IS NOT NULL"))
    
recStatus = (GetTable(f"{getEnv()}cleansed.crm.tj30t")
             .select("statusProfile", "statusCode", "statusShortDescription","status").dropDuplicates())

finaldf = (recStatus.alias("aa").join(broadcast(srStatus.alias("bb")), recStatus["statusProfile"] == srStatus["statusProfile"], "inner")
               .select("aa.*"))

if not(TableExists(_.Destination)):
    finaldf = finaldf.unionByName(spark.createDataFrame([dummyRecord(finaldf.schema)], finaldf.schema))

# COMMAND ----------

def Transform():
    global df
    df = finaldf
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
    #CleanSelf()
    Save(df)
    #DisplaySelf()
    
Transform()
