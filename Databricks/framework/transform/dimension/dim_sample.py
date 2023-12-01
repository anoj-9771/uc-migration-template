# Databricks notebook source
# MAGIC %run ../../Common/common-transform 

# COMMAND ----------

driverTable1 = 'cleansed.source.tablename'  
derivedDF1 = GetTable(f"{getEnv()}{driverTable1}") 

df = (derivedDF1.select (concat_ws('|',col("categoryCode"), lit("CRM")).alias(f"{_.BK}")
                         ,col("categoryCode").alias("customerServiceChannelCode")
                         ,col("categoryDescription").alias("customerServiceChannelDescription")
                         ,lit("CRM").alias("sourceSystemCode")
                         )) 
                         

if not(TableExists(_.Destination)):
    df = df.unionByName(spark.createDataFrame([dummyRecord(df.schema)], df.schema))
#CleanSelf()
Save(df)
#SaveWithCDF(df, 'Full Load')
