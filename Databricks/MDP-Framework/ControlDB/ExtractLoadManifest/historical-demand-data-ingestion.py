# Databricks notebook source
# MAGIC %run ../../Common/common-helpers

# COMMAND ----------

# DBTITLE 1,Creating a table out of manual demand historical csv
(spark.read
 .option('header', True)
 .csv('/mnt/datalake-raw/cleansed_csv/historical_demand_reference.csv')
 .drop_duplicates()
 .write
 .mode('overwrite')
 .saveAsTable(f'{get_env()}cleansed.iicats.manualdemandhistorical')
 )
