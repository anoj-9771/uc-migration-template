# Databricks notebook source
# MAGIC %md
# MAGIC #Load data to Delta Tables in Raw Zone from files

# COMMAND ----------

from pyspark.sql.functions import mean, min, max, desc, abs, coalesce, when, expr
from pyspark.sql.functions import date_add, to_utc_timestamp, from_utc_timestamp, datediff
from pyspark.sql.functions import regexp_replace, concat, col, lit, substring, greatest
from pyspark.sql.functions import countDistinct, count

from pyspark.sql import functions as F
from pyspark.sql import SparkSession, SQLContext, Window

from pyspark.sql.types import *

from datetime import datetime

import math

from pyspark.context import SparkContext

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

#dbutils.widgets.removeAll()

# COMMAND ----------

# DBTITLE 1,Define Widgets (Parameters) at the start
#Initialize the Entity Object to be passed to the Notebook
dbutils.widgets.text("file_object", "", "01:File-Path")
dbutils.widgets.text("debug_mode", "false", "02:Debug-Mode")
dbutils.widgets.text("source_param", "", "03:Parameters")
dbutils.widgets.text("file_date_time_stamp", "", "04:File-Date-Time-Stamp")


# COMMAND ----------

# DBTITLE 1,Get Values from Widget
file_object = dbutils.widgets.get("file_object")
debug_mode = dbutils.widgets.get("debug_mode")
source_param = dbutils.widgets.get("source_param")
file_date_time_stamp = dbutils.widgets.get("file_date_time_stamp")

print(file_object)
print(debug_mode)
print(source_param)
print(file_date_time_stamp)


# COMMAND ----------

import json
Params = json.loads(source_param)
print(json.dumps(Params, indent=4, sort_keys=True))

# COMMAND ----------

file_object = file_object.replace("//", "/")
print(file_object)

# COMMAND ----------

# MAGIC %run ../includes/include-all-util

# COMMAND ----------

DeltaExtract = Params[PARAMS_DELTA_EXTRACT]
DataLoadMode = Params["DataLoadMode"]
Debug = GeneralGetBoolFromString(debug_mode)

print(DeltaExtract)
print(DataLoadMode)
print(Debug)


# COMMAND ----------

if DeltaExtract or DataLoadMode == "FULL-EXTRACT" :
  write_mode = ADS_WRITE_MODE_APPEND
else:
  write_mode = ADS_WRITE_MODE_OVERWRITE

print(write_mode)

# COMMAND ----------

file_type = file_object.strip('.gz').split('.')[-1]
print (file_type)

# COMMAND ----------

# DBTITLE 1,Connect to Azure Data Lake and mount the raw container
DATA_LAKE_MOUNT_POINT = DataLakeGetMountPoint(ADS_CONTAINER_RAW)

# COMMAND ----------

# DBTITLE 1,Load the file that was passed in Widget
#Start of Fix to use Target Name from Parameter String
#Variable 'source_table'has been replaced by 'raw_table'
source_group = Params["SourceGroup"]
target_table = Params["TargetName"]
source_system = target_table.split('_')[0]
raw_table = target_table
#Source
#The below code is commented as part of this fix
#source_system = file_object.split('/')[0]
#raw_table = file_object.split('/')[1]
#End of Fix to use Target Name from Parameter String
source_file_path = "dbfs:{mount}/{sourcefile}".format(mount=DATA_LAKE_MOUNT_POINT, sourcefile = file_object)
print ("source_group: " + source_group)
print ("target_table:" + target_table)
print ("source_system: " + source_system)
print ("raw_table: " + raw_table)
print ("source_file_path: " + source_file_path)


# COMMAND ----------

df = spark.read \
      .format(file_type) \
      .option("header", True) \
      .option("inferSchema", False) \
      .option("delimiter", "|") \
      .load(source_file_path) 

current_record_count = df.count()
print("Records read from file : " + str(current_record_count))

# COMMAND ----------

if Debug:
  display(df.limit(10))

# COMMAND ----------

# DBTITLE 1,If there are no records then Exit the Notebook
if current_record_count == 0:
  print("Exiting Notebook as no records to process")
  output["TargetTableRecordCount"] = 0
  print(output) 
  dbutils.notebook.exit("0")
else:
  print("Processing " + str(current_record_count) + " records")

  


# COMMAND ----------

# MAGIC %run ./utility/transform_data_rawzone

# COMMAND ----------

# DBTITLE 1,Apply any transformation at this stage
df_updated = transform_raw_dataframe(df, Params, file_date_time_stamp)

# COMMAND ----------

df_updated.printSchema()

# COMMAND ----------

if Debug:
  display(df_updated.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ###Save data to Delta Lake in parquet format and also as Delta Table

# COMMAND ----------

if DeltaExtract or DeltaTablePartitioned(f"{ADS_DATABASE_RAW}.{raw_table}"):
  partition_keys = ("year","month", "day")
else:
  partition_keys = ""

# COMMAND ----------

print("source_group: " + source_group)
print("raw_table: " + raw_table)
print("ADS_DATABASE_RAW: " + ADS_DATABASE_RAW)
print("ADS_CONTAINER_RAW: " + ADS_CONTAINER_RAW)
print("write_mode: " + write_mode)
print("partition_keys: " + partition_keys)

#DeltaSaveDataframeDirect(df_updated, source_system, raw_table, ADS_DATABASE_RAW, ADS_CONTAINER_RAW, write_mode, partition_keys)
DeltaSaveDataframeDirect(df_updated, source_group, raw_table, ADS_DATABASE_RAW, ADS_CONTAINER_RAW, write_mode, partition_keys)

# COMMAND ----------

output = {"DataFileRecordCount" : -1, "TargetTableRecordCount": -1} 
output["DataFileRecordCount"] = df_updated.count()
print(output)


# COMMAND ----------

#Start of Fix to use Target Name from Parameter String
#The below code is commented as part of this fix
#delta_raw_tbl_name = "{0}.{1}_{2}".format(ADS_DATABASE_RAW, source_system, raw_table)
delta_raw_tbl_name = "{0}.{1}".format(ADS_DATABASE_RAW, raw_table)
#End of Fix to use Target Name from Parameter String

if Params[PARAMS_SOURCE_TYPE] == "BLOB Storage (json)" and DeltaTableExists(delta_raw_tbl_name): 
  sql_query = "SELECT COUNT(*) FROM {0} WHERE _FileDateTimeStamp = {1}".format(delta_raw_tbl_name, file_date_time_stamp)
  df_dl = spark.sql(sql_query)
  output["TargetTableRecordCount"] = df_dl.collect()[0][0]
else:
  output["TargetTableRecordCount"] = -1

print(output)


# COMMAND ----------

dbutils.notebook.exit(json.dumps(output))
