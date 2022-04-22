# Databricks notebook source
# MAGIC %md
# MAGIC #Get number of recods in target delta table  

# COMMAND ----------

#from pyspark.sql.functions import mean, min, max, desc, abs, coalesce, when, expr
#from pyspark.sql.functions import date_add, to_utc_timestamp, from_utc_timestamp, datediff
#from pyspark.sql.functions import regexp_replace, concat, col, lit, substring, greatest
from pyspark.sql.functions import countDistinct, count

#from pyspark.sql import functions as F
from pyspark.sql import SparkSession, SQLContext, Window

#from pyspark.sql.types import *

#from datetime import datetime

#import math

#from pyspark.context import SparkContext

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

#dbutils.widgets.removeAll()

# COMMAND ----------

# DBTITLE 1,Define Widgets (Parameters) at the start
#Initialize the Entity Object to be passed to the Notebook
dbutils.widgets.text("target_table", "", "01:Target_Table")
dbutils.widgets.text("debug_mode", "false", "02:Debug-Mode")
dbutils.widgets.text("filter_clause", "", "03:Filter_Clause")
#dbutils.widgets.text("source_param", "", "03:Parameters")


# COMMAND ----------

# DBTITLE 1,Get Values from Widget
target_table = dbutils.widgets.get("target_table")
debug_mode = dbutils.widgets.get("debug_mode")
filter_clause = dbutils.widgets.get("filter_clause")
#source_param = dbutils.widgets.get("source_param")

print(target_table)
print(debug_mode)
print(filter_clause)
#print(source_param)


# COMMAND ----------

# MAGIC %run ../../includes/include-all-util

# COMMAND ----------

Debug = GeneralGetBoolFromString(debug_mode)

print(Debug)

# COMMAND ----------

# DBTITLE 1,Read the record count fields
#target_name = "sapisu_DBERCHZ2"
#Below Code commented as part of ADF Folder Structure Redesign
#delta_cleansed_tbl_name = "{0}.stg_{1}".format(ADS_DATABASE_CLEANSED, target_table)
delta_cleansed_tbl_name = "{0}.{1}".format(ADS_DATABASE_CLEANSED, target_table)
sql_query = "SELECT COUNT(*) FROM {0}".format(delta_cleansed_tbl_name)
if filter_clause != '':
  sql_query = "{0} {1}".format(sql_query,filter_clause)

print(delta_cleansed_tbl_name)
print(sql_query)

df_cleansed = spark.sql(sql_query)
if df_cleansed.count() > 0:
  TotalNoRecords = df_cleansed.collect()[0][0]
  print(TotalNoRecords)
else:
  TotalNoRecords = -1


# COMMAND ----------

dbutils.notebook.exit(TotalNoRecords)
