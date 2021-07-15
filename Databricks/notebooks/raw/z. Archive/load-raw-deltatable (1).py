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
  dbutils.notebook.exit("0")
else:
  print("Processing " + str(current_record_count) + " records")


# COMMAND ----------

# MAGIC %run ./utility/transform_data_rawzone

# COMMAND ----------

display(df)

# COMMAND ----------

  dataframe = df
  source_object = Params[PARAMS_SOURCE_NAME].lower()
  source_system = Params[PARAMS_SOURCE_GROUP].lower()
  print ("Starting : transform_raw_dataframe for : " + source_object)
  
  is_cdc = Params[PARAMS_CDC_SOURCE]
  is_delta_extract = Params[PARAMS_DELTA_EXTRACT]
  delta_column = GeneralGetUpdatedDeltaColumn(Params[PARAMS_WATERMARK_COLUMN])
  
  if Params[PARAMS_SOURCE_TYPE] == "Flat File":
    dataframe = transform_raw_dataframe_trim(dataframe)
  
  if is_cdc:
    #If we are doing CDC load, then some additional transformations to be done for CDC column
    dataframe = transform_raw_dataframe_cdc(dataframe)
  else:
    dataframe = dataframe
    
  #If Schema file is available, update the column types based on schema file
  dataframe = transform_update_column_data_type_from_schemafile(source_system, source_object, dataframe, Params)
    
  #Remove spaces from column names
  dataframe = transform_update_column_name(dataframe)
  
  #Custom changes for MySQL as the datetime delta columns are stored as number
  if source_system == "lms" or source_system == "cms":
    dataframe = transform_custom_mysql_lms_update_delta_col(dataframe, Params)

  #Make sure the delta columns are stored as TimestampType
  ts_format = Params[PARAMS_SOURCE_TS_FORMAT]
  dataframe = transform_raw_update_datatype_delta_col(dataframe, delta_column, ts_format)

  #Add a Generic Transaction Date column to the dataframe
  #This is helpful when there are multiple extraction columns e.g. CREATED_DATE and UPDATED_DATE
  dataframe = transform_raw_add_col_transaction_date(dataframe)
  
  curr_time = str(datetime.now())
  dataframe = dataframe.withColumn(COL_DL_RAW_LOAD, from_utc_timestamp(F.lit(curr_time).cast(TimestampType()), ADS_TZ_LOCAL))
  
  #If it is a Delta/CDC add year, month, day partition columns so that the Delta Table can be partitioned on YMD
  #dataframe = transform_raw_add_partition_cols(dataframe, source_object, is_delta_extract, delta_column, is_cdc)
  print ("Starting : transform_raw_add_partition_cols. delta_column : " + delta_column)
  
  is_partitioned = True #if DeltaTablePartitioned(f"{ADS_DATABASE_RAW}.{source_object}") or is_delta_extract else False
  
  if is_cdc:
    print ("Adding Partition Columns for CDC Load")
    df_updated = dataframe \
      .withColumn('year', F.year(dataframe.rowtimestamp).cast(StringType())) \
      .withColumn('month', F.month(dataframe.rowtimestamp).cast(StringType())) \
      .withColumn('day', F.dayofmonth(dataframe.rowtimestamp).cast(StringType()))

  elif is_partitioned:
    partition_column = COL_DL_RAW_LOAD if delta_column == "" else delta_column
    print (f"Adding Partition Column for Delta Load using {partition_column}")
    df_updated = dataframe \
      .withColumn('year', F.year(dataframe[partition_column]).cast(StringType())) \
      .withColumn('month', F.month(dataframe[partition_column]).cast(StringType())) \
      .withColumn('day', F.dayofmonth(dataframe[partition_column]).cast(StringType()))

  if is_partitioned:
    #Add leading 0 to day and month column if it is a single digit
    print ("Adding leading 0 to day and month")
    df_updated = df_updated \
      .withColumn('day', F.when(F.length('day') == 1, F.concat(F.lit('0'), F.col('day'))).otherwise(F.col('day'))) \
      .withColumn('month', F.when(F.length('month') == 1, F.concat(F.lit('0'), F.col('month'))).otherwise(F.col('month')))
  else:
    print ("No partition columns added as not Delta or CDC load.")
    df_updated = dataframe

  # Call the specific routime based on the source type
  #if source_object == "sales_orders".upper():
  #  dataframe = transform_raw_sales_orders(dataframe)
  #else:
  #  dataframe = dataframe


# COMMAND ----------

display(df_updated)

# COMMAND ----------

# DBTITLE 1,Apply any transformation at this stage
df_updated = transform_raw_dataframe(df, Params)

# COMMAND ----------

df_updated.printSchema()

# COMMAND ----------

if Debug:
  display(df_updated.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ###Save data to Delta Lake in parquet format and also as Delta Table

# COMMAND ----------

if DeltaExtract or DeltaTablePartitioned(f"{ADS_DATABASE_RAW}.{source_table}"):
  partition_keys = ("year","month", "day")
else:
  partition_keys = ""


# COMMAND ----------

DeltaSaveDataframeDirect(df_updated, source_system, source_table, ADS_DATABASE_RAW, ADS_CONTAINER_RAW, write_mode, partition_keys)

# COMMAND ----------

DeltaSaveDataframeDirect(df_updated, source_system, "source_table1", ADS_DATABASE_RAW, ADS_CONTAINER_RAW, write_mode, partition_keys)

# COMMAND ----------

record_count = df_updated.count()

# COMMAND ----------

dbutils.notebook.exit(record_count)
