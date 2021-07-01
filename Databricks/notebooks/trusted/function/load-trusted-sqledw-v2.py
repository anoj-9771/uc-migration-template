# Databricks notebook source
# MAGIC %md
# MAGIC #Load data from Trusted Zone to SQLEDW

# COMMAND ----------

from pyspark.sql.functions import mean, min, max, desc, abs, coalesce, when, expr
from pyspark.sql.functions import date_add, to_utc_timestamp, from_utc_timestamp, datediff
from pyspark.sql.functions import regexp_replace, concat, col, lit, substring
from pyspark.sql.functions import countDistinct, count

from pyspark.sql import functions as F
from pyspark.sql import SparkSession, SQLContext, Window

from pyspark.sql.types import *

from datetime import datetime, timedelta

import math

from pyspark.context import SparkContext

# COMMAND ----------

# MAGIC %md
# MAGIC ###Define Widgets (Parameters) at the start

# COMMAND ----------

#dbutils.widgets.removeAll()

# COMMAND ----------

#Initialize the Entity source_object to be passed to the Notebook
dbutils.widgets.text("source_object", "", "Source Object")
dbutils.widgets.text("delta_column", "", "Delta Column")
dbutils.widgets.text("start_counter", "", "Start Counter")
dbutils.widgets.text("end_counter", "", "End Counter")
dbutils.widgets.text("source_param", "", "Params")


# COMMAND ----------

# MAGIC %md
# MAGIC ####Get Values from Widget

# COMMAND ----------

source_object = dbutils.widgets.get("source_object")
delta_column = dbutils.widgets.get("delta_column")
start_counter = dbutils.widgets.get("start_counter")
end_counter = dbutils.widgets.get("end_counter")
source_param = dbutils.widgets.get("source_param")

print(source_object)
print(delta_column)
print(start_counter)
print(end_counter)
print(source_param)


# COMMAND ----------

import json
Params = json.loads(source_param)
print(json.dumps(Params, indent=4, sort_keys=True))

# COMMAND ----------

# MAGIC %run ../../includes/include-all-util

# COMMAND ----------

delta_column = GeneralGetUpdatedDeltaColumn(delta_column)
source_object = GeneralAlignTableName(source_object)
#Delta and SQL tables are case insensitive. Seems Delta table are always lower case
delta_trusted_tbl_name = "{0}.{1}".format(ADS_DATABASE_TRUSTED, source_object)
data_load_mode = GeneralGetDataLoadMode(Params[PARAMS_TRUNCATE_TARGET], Params[PARAMS_UPSERT_TARGET], Params[PARAMS_APPEND_TARGET])
schema_file_url = f"/dbfs/mnt/datalake-raw/{Params[PARAMS_SOURCE_GROUP]}/schema/{source_object}.schema".lower()

print (delta_column)
print (source_object)
print (delta_trusted_tbl_name)
print (data_load_mode)
print (schema_file_url)

# COMMAND ----------

DeltaSyncToSQLEDW (delta_trusted_tbl_name, ADS_SQL_SCHEMA_TRUSTED, source_object, Params[PARAMS_BUSINESS_KEY_COLUMN], delta_column, start_counter, data_load_mode, Params[PARAMS_TRACK_CHANGES], Params[PARAMS_DELTA_EXTRACT], schema_file_url, Params[PARAMS_ADDITIONAL_PROPERTY])

# COMMAND ----------

#Exit the Notebook with Status as 1 if the SQL Merge Query execution was successful
dbutils.notebook.exit("1")
