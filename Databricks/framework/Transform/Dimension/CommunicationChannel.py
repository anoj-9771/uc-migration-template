# Databricks notebook source
# MAGIC %run ../../Common/common-transform 

# COMMAND ----------

###cleansed layer table (cleansed.crm.0crm_category_text) is full reload ###
driverTable1 = 'cleansed.crm.0crm_category_text'  
derivedDF1 = GetTable(f"{getEnv()}{driverTable1}") #.withColumn("_change_type", lit('Full Load'))
df = (derivedDF1.select (concat_ws('|',col("categoryCode"), lit("CRM")).alias(f"{_.BK}")
                         ,col("categoryCode").alias("customerServiceChannelCode")
                         ,col("categoryDescription").alias("customerServiceChannelDescription")
                         ,lit("CRM").alias("sourceSystemCode")
                         #,col("_RecordDeleted").alias("_recordDeleted")
                         #,col("_change_type")
                         )) 
if not(TableExists(_.Destination)):
    df = df.unionByName(spark.createDataFrame([dummyRecord(df.schema)], df.schema))
#CleanSelf()
Save(df)
#SaveWithCDF(df, 'Full Load')
