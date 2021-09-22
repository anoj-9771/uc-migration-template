# Databricks notebook source
#Notebook structure/Method 
#1.Load libraries
#2.

# COMMAND ----------

# MAGIC %md
# MAGIC #Load data to Trusted Zone from Raw Zone

# COMMAND ----------

#1.Load libraries
from pyspark.sql.functions import mean, min, max, desc, abs, coalesce, when, expr
from pyspark.sql.functions import date_add, to_utc_timestamp, from_utc_timestamp, datediff
from pyspark.sql.functions import regexp_replace, concat, col, lit, substring
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

# DBTITLE 1,Define Widgets (Parameters) at the start
#Initialize the Entity source_object to be passed to the Notebook
dbutils.widgets.text("source_object", "", "Source Object")
dbutils.widgets.text("start_counter", "", "Start Counter")
dbutils.widgets.text("end_counter", "", "End Counter")
dbutils.widgets.text("delta_column", "", "Delta Column")
dbutils.widgets.text("source_param", "", "Param")


# COMMAND ----------

# DBTITLE 1,Get Values from Widget
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

# DBTITLE 1,Format the Parameters into JSON
import json
Params = json.loads(source_param)
print(json.dumps(Params, indent=4, sort_keys=True))

# COMMAND ----------

# MAGIC %run ../../includes/include-all-util

# COMMAND ----------

# MAGIC %run ./../utility/transform_data_cleansedzone

# COMMAND ----------

start_counter = start_counter.replace("T", " ")
print(start_counter)

source_object = GeneralAlignTableName(source_object)
print(source_object)

delta_column = GeneralGetUpdatedDeltaColumn(delta_column)
print(delta_column)

data_load_mode = GeneralGetDataLoadMode(Params[PARAMS_TRUNCATE_TARGET], Params[PARAMS_UPSERT_TARGET], Params[PARAMS_APPEND_TARGET])
print(data_load_mode)


# COMMAND ----------

#Delta and SQL tables are case INsensitive. Seems Delta table are always lower case
delta_cleansed_tbl_name = "{0}.{1}".format(ADS_DATABASE_CLEANSED, source_object)
delta_raw_tbl_name = "{0}.{1}".format(ADS_DATABASE_RAW, source_object)


#Destination
print(delta_cleansed_tbl_name)
print(delta_raw_tbl_name)


# COMMAND ----------

DeltaSaveToDeltaTable (
  source_table = delta_raw_tbl_name, 
  target_table = source_object, 
  target_data_lake_zone = ADS_DATALAKE_ZONE_CLEANSED, 
  target_database = ADS_DATABASE_CLEANSED,
  data_lake_folder = Params[PARAMS_SOURCE_GROUP],
  data_load_mode = data_load_mode,
  track_changes =  Params[PARAMS_TRACK_CHANGES], 
  is_delta_extract =  Params[PARAMS_DELTA_EXTRACT], 
  business_key =  Params[PARAMS_BUSINESS_KEY_COLUMN], 
  delta_column = delta_column, 
  start_counter = start_counter, 
  end_counter = end_counter
)

# COMMAND ----------

df = spark.sql("select partner as Business_Partner_NUM,ZZPAS_INDICATOR as Payment_Assist_Scheme_IND,ZZBA_INDICATOR as Bill_Assist_IND from raw.sap_0bpartner_attr" )
display(df)

# COMMAND ----------

changes_df = spark.read.format("delta").option("readChangeData", True).option("startingVersion", 2).table('raw.sap_0bpartner_attr')
display(changes_df)

# COMMAND ----------

DeltaSaveDataFrameToDeltaTable(
    dataframe = df, 
    target_table = "T_" + source_object, 
    target_data_lake_zone = ADS_DATALAKE_ZONE_CLEANSED, 
    target_database = ADS_DATABASE_CLEANSED,
    data_lake_folder = Params[PARAMS_SOURCE_GROUP],
    data_load_mode = data_load_mode,
    track_changes = False, 
    is_delta_extract = False, 
    business_key = "", 
    AddSKColumn = False,
    delta_column = "", 
    start_counter = "0", 
    end_counter = "0")

# COMMAND ----------

dbutils.notebook.exit("1")
