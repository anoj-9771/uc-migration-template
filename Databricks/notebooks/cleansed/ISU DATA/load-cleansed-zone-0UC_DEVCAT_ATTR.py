# Databricks notebook source
# DBTITLE 1,Notebook Structure/Method 
#Notebook structure/Method 
#1.Import libraries/functions -- Generic
#2.Create spark session -- Generic
#3.Define Widgets/Parameters -- Generic
#4.Get Values from parameters/widgets -- Generic
#5.Format the Source_param parameter value into JSON -- Generic
#6.Include all util user function for the notebook -- Generic
#7.Include User functions (CleansedZone) for the notebook -- Generic
#8.Initilize/update parameter values -- Generic
#9.Set raw and cleansed table name -- Generic
#10.Load to Cleanse Delta Table from Raw Delta Table -- Generic
#11.Update/Rename Columns and load into a dataframe -- Custom
#12.Save Data frame into Cleansed Delta table (Final) -- Generic
#13.Exit Notebook -- Generic

# COMMAND ----------

# MAGIC %md
# MAGIC #Load data to Trusted Zone from Raw Zone

# COMMAND ----------

# DBTITLE 1,1. Import libraries/functions
#1.Import libraries/functions
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

# DBTITLE 1,2. Create spark session
#2.Create spark session
spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# DBTITLE 1,3. Define Widgets (Parameters) at the start
#3.Define Widgets/Parameters
#Initialize the Entity source_object to be passed to the Notebook
dbutils.widgets.text("source_object", "", "Source Object")
dbutils.widgets.text("start_counter", "", "Start Counter")
dbutils.widgets.text("end_counter", "", "End Counter")
dbutils.widgets.text("delta_column", "", "Delta Column")
dbutils.widgets.text("source_param", "", "Param")


# COMMAND ----------

# DBTITLE 1,4. Get Values from Widget
#4.Get Values from parameters/widgets
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

# DBTITLE 1,5. Format the Parameters into JSON
#5.Format the Source_param parameter value into JSON
import json
Params = json.loads(source_param)
print(json.dumps(Params, indent=4, sort_keys=True))

# COMMAND ----------

# DBTITLE 1,6. Include all util user functions for this notebook
# MAGIC %run ../../includes/include-all-util

# COMMAND ----------

# DBTITLE 1,7. Include User functions (CleansedZone) for the notebook
# MAGIC %run ./../utility/transform_data_cleansedzone

# COMMAND ----------

# DBTITLE 1,8. Initilize/update parameter values
#Align Table Name (replace '[-@ ,;{}()]' charecter by '_')
source_object = GeneralAlignTableName(source_object)
print(source_object)

#Get delta columns form the delta_columnn parameter
delta_column = GeneralGetUpdatedDeltaColumn(delta_column)
print(delta_column)

#Get the Data Load Mode using the params
data_load_mode = GeneralGetDataLoadMode(Params[PARAMS_TRUNCATE_TARGET], Params[PARAMS_UPSERT_TARGET], Params[PARAMS_APPEND_TARGET])
print(data_load_mode)


# COMMAND ----------

# DBTITLE 1,9. Set raw and cleansed table name
#Set raw and cleansed table name
#Delta and SQL tables are case Insensitive. Seems Delta table are always lower case
delta_cleansed_tbl_name = "{0}.{1}".format(ADS_DATABASE_CLEANSED, "stg_"+source_object)
delta_raw_tbl_name = "{0}.{1}".format(ADS_DATABASE_RAW, source_object)
#delta_raw_tbl_name = "raw.sap_0uc_devcat_attr"

#Destination
print(delta_cleansed_tbl_name)
print(delta_raw_tbl_name)


# COMMAND ----------

# DBTITLE 1,10. Load to Cleanse Delta Table from Raw Delta Table
#This method uses the source table to load data into target Delta Table
DeltaSaveToDeltaTable (
  source_table = delta_raw_tbl_name, 
  target_table = "stg_"+source_object, 
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

# DBTITLE 1,11. Update/Rename Columns and Load into a Dataframe
#Update/rename Column
df_updated_column_temp = spark.sql("SELECT \
                                  DEVCAT.MATNR as materialNumber,\
                                  DEVCAT.KOMBINAT as deviceCategoryCombination,\
                                  DEVCAT.FUNKLAS as functionClassCode,\
                                  FKLASTX.functionClass as functionClass,\
                                  DEVCAT.BAUKLAS as constructionClassCode,\
                                  BKLASTX.constructionClass as constructionClass,\
                                  DEVCAT.BAUFORM as deviceCategoryDescription,\
                                  DEVCAT.BAUTXT as deviceCategoryName,\
                                  DEVCAT.PTBNUM as ptiNumber,\
                                  DEVCAT.DVGWNUM as ggwaNumber,\
                                  DEVCAT.BGLKZ as certificationRequirementType,\
                                  DEVCAT.ZWGRUPPE as registerGroupCode,\
                                  REGGRP.registerGroup as registerGroup,\
                                  cast(DEVCAT.UEBERVER as decimal (10,3)) as transformationRatio,\
                                  DEVCAT.AENAM as changedBy,\
                                  to_date(DEVCAT.AEDAT) as lastChangedDate,\
                                  DEVCAT.SPARTE as division,\
                                  cast(DEVCAT.NENNBEL as decimal(10,4)) as nominalLoad,\
                                  DEVCAT.STELLPLATZ as containerSpaceCount,\
                                  cast(DEVCAT.HOEHEBEH as decimal(7,2)) as containerCategoryHeight,\
                                  cast(DEVCAT.BREITEBEH as decimal(7,2)) as containerCategoryWidth,\
                                  cast(DEVCAT.TIEFEBEH as decimal(7,2)) as containerCategoryDepth,\
                                  DEVCAT._RecordStart, \
                                  DEVCAT._RecordEnd, \
                                  DEVCAT._RecordDeleted, \
                                  DEVCAT._RecordCurrent \
                              FROM CLEANSED.STG_isu_0UC_DEVCAT_ATTR DEVCAT \
                              LEFT OUTER JOIN CLEANSED.T_isu_0UC_FUNKLAS_TEXT FKLASTX ON DEVCAT.FUNKLAS = FKLASTX.functionClassCode \
                              LEFT OUTER JOIN CLEANSED.T_isu_0UC_BAUKLAS_TEXT BKLASTX ON DEVCAT.BAUKLAS = BKLASTX.constructionClassCode \
                              LEFT OUTER JOIN CLEANSED.T_isu_0UC_REGGRP_TEXT REGGRP ON DEVCAT.ZWGRUPPE = REGGRP.registerGroupCode")
                                   
display(df_updated_column_temp)

# COMMAND ----------

# Create schema for the cleanse table
cleanse_Schema = StructType(
  [
    StructField("materialNumber", StringType(), False),
    StructField("deviceCategoryCombination", StringType(), True),
    StructField("functionClassCode", StringType(), True),
    StructField("functionClass", StringType(), True),
    StructField("constructionClassCode", StringType(), True),
    StructField("constructionClass", StringType(), True),
    StructField("deviceCategoryDescription", StringType(), True),
    StructField("deviceCategoryName", StringType(), True),
    StructField("ptiNumber", StringType(), True),
    StructField("ggwaNumber", StringType(), True),
    StructField("certificationRequirementType", StringType(), True),
    StructField("registerGroupCode", StringType(), True),
    StructField("registerGroup", StringType(), True),
    StructField("transformationRatio", DecimalType(10,3), True),  
    StructField("changedBy", StringType(), True),
    StructField("lastChangedDate", DateType(), True),
    StructField("division", StringType(), True),
    StructField("nominalLoad", DecimalType(10,4), True),
    StructField("containerSpaceCount", StringType(), True),
    StructField("containerCategoryHeight", DecimalType(7,2), True),
    StructField("containerCategoryWidth", DecimalType(7,2), True),
    StructField("containerCategoryDepth", DecimalType(7,2), True),   
    StructField('_RecordStart',TimestampType(),False),
    StructField('_RecordEnd',TimestampType(),False),
    StructField('_RecordDeleted',IntegerType(),False),
    StructField('_RecordCurrent',IntegerType(),False)
  ]
)
# Apply the new schema to cleanse Data Frame
df_updated_column = spark.createDataFrame(df_updated_column_temp.rdd, schema=cleanse_Schema)
display(df_updated_column)

# COMMAND ----------

# DBTITLE 1,12. Save Data frame into Cleansed Delta table (Final)
#Save Data frame into Cleansed Delta table (final)
DeltaSaveDataframeDirect(df_updated_column, "t", source_object, ADS_DATABASE_CLEANSED, ADS_CONTAINER_CLEANSED, "overwrite", "")


# COMMAND ----------

# DBTITLE 1,13. Exit Notebook
dbutils.notebook.exit("1")
