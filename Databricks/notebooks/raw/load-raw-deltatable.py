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


# COMMAND ----------

# DBTITLE 1,Get Values from Widget
file_object = dbutils.widgets.get("file_object")
debug_mode = dbutils.widgets.get("debug_mode")
source_param = dbutils.widgets.get("source_param")

print(file_object)
print(debug_mode)
print(source_param)


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
Debug = GeneralGetBoolFromString(debug_mode)

print(DeltaExtract)
print(Debug)


# COMMAND ----------

if DeltaExtract:
  write_mode = ADS_WRITE_MODE_APPEND
else:
  write_mode = ADS_WRITE_MODE_OVERWRITE


# COMMAND ----------

file_type = file_object.strip('.gz').split('.')[-1]
print (file_type)

# COMMAND ----------

# DBTITLE 1,Connect to Azure Data Lake and mount the raw container
DATA_LAKE_MOUNT_POINT = DataLakeGetMountPoint(ADS_CONTAINER_RAW)

# COMMAND ----------

# DBTITLE 1,Load the file that was passed in Widget
#Source
source_system = file_object.split('/')[0]
source_table = file_object.split('/')[1]
source_file_path = "dbfs:{mount}/{sourcefile}".format(mount=DATA_LAKE_MOUNT_POINT, sourcefile = file_object)
#Source

print (source_system)
print (source_table)
print (source_file_path)

# COMMAND ----------

df = spark.read \
      .format(file_type) \
      .option("header", True) \
      .option("inferSchema", False) \
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
  dbutils.notebook.exit("0")
else:
  print("Processing " + str(current_record_count) + " records")


# COMMAND ----------

# MAGIC %run ./utility/transform_data_rawzone

# COMMAND ----------

# DBTITLE 1,Apply any transformation at this stage
#df_updated = transform_raw_dataframe(df, Params)

# COMMAND ----------

#df_updated.printSchema()

# COMMAND ----------

#if Debug:
#  display(df_updated.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ###Save data to Delta Lake in parquet format and also as Delta Table

# COMMAND ----------

if DeltaExtract or DeltaTablePartitioned(f"{ADS_DATABASE_RAW}.{source_table}"):
  partition_keys = ("year","month", "day")
else:
  partition_keys = ""


# COMMAND ----------

DeltaSaveDataframeDirect(df, source_system, source_table, ADS_DATABASE_RAW, ADS_CONTAINER_RAW, write_mode) #df_updated, , partition_keys

# COMMAND ----------

record_count = df.count()

# COMMAND ----------

dbutils.notebook.exit(record_count)
