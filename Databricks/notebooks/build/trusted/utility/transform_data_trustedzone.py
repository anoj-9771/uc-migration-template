# Databricks notebook source
from pyspark.context import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import concat, col, lit, substring, to_utc_timestamp, from_utc_timestamp, datediff, countDistinct, count



# COMMAND ----------

def transform_trusted_dataframe(source_type, dataframe):

  curr_time = str(datetime.now())
  end_date = "9999-12-31 00:00:00"

  #Add default columns for all trusted tables
  df = dataframe \
    .withColumn(COL_DL_TRUSTED_LOAD, from_utc_timestamp(F.lit(curr_time).cast(TimestampType()), ADS_TZ_LOCAL)) \
    .withColumn(COL_RECORD_START, from_utc_timestamp(F.lit(curr_time).cast(TimestampType()), ADS_TZ_LOCAL)) \
    .withColumn(COL_RECORD_END, F.lit(end_date).cast(TimestampType())) \
    .withColumn(COL_RECORD_CURRENT, F.lit(1)) \
    .withColumn(COL_RECORD_DELETED, F.lit(0))
  
  # Call the specific routime based on the source type
  if source_type == "table1":
    df = transform_trusted_table1(df)
  elif source_type == "table2":
    df = transform_trusted_table1(df)
  else:
    df = df

  return df

# COMMAND ----------

def transform_trusted_table1(df):
  df_updated = df 

  return df_updated