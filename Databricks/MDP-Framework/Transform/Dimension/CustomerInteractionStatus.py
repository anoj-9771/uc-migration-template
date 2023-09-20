# Databricks notebook source
# MAGIC %run ../../Common/common-transform 

# COMMAND ----------

###cleansed layer table (cleansed.crm.crm_tj30t) is full reload ###
df = spark.sql(f""" Select distinct statusProfile||'|'||statusCode as {BK}
                ,statusProfile as customerInteractionStatusProfile
                ,statusCode as customerInteractionStatusCode
                ,statusShortDescription as customerInteractionStatusShortDescription
                ,status as customerInteractionStatusDescription
                from {getEnv()}cleansed.crm.tj30t
                where statusProfile in (Select statusProfile From  {getEnv()}cleansed.crm.0crm_sales_act_1 where statusProfile is not null) """)

if not(TableExists(_.Destination)):
    df = df.unionByName(spark.createDataFrame([dummyRecord(df.schema)], df.schema))
#CleanSelf()
Save(df)
#SaveWithCDF(df, 'Full Load')
