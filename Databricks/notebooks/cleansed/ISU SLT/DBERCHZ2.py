# Databricks notebook source
# DBTITLE 1,Generate parameter and source object name for unit testing
import json
#For unit testing...
#Use this string in the Param widget: 
#$PARAM

#Use this string in the Source Object widget
#$GROUP_$SOURCE

# COMMAND ----------

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
#Initialise the Entity source_object to be passed to the Notebook
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

# DBTITLE 1,8. Initialise/update parameter values
#Get and Align Source Group (replace '[-@ ,;{}()]' character by '_')
source_group = Params[PARAMS_SOURCE_GROUP]
source_group = GeneralAlignTableName(source_group)
print("source_group: " + source_group)

#Get Data Lake Folder
data_lake_folder = source_group + "/stg"
print("data_lake_folder: " + data_lake_folder)

#Get and Align Source Table Name (replace '[-@ ,;{}()]' character by '_')
source_object = Params["SourceName"]
source_object = GeneralAlignTableName(source_object)
print("source_object: " + source_object)

#Get Target Object 
#Get and Align Target Table Name (replace '[-@ ,;{}()]' character by '_')
target_object = Params["TargetName"]
target_object = GeneralAlignTableName(target_object)

if target_object != "":
    target_table = target_object
else:
    target_table = source_object
print("target_table: "+target_table)
#Get delta columns form the delta_columnn parameter
delta_column = GeneralGetUpdatedDeltaColumn(delta_column)
print("delta_column: " + delta_column)

#Get the Data Load Mode using the params
data_load_mode = GeneralGetDataLoadMode(Params[PARAMS_TRUNCATE_TARGET], Params[PARAMS_UPSERT_TARGET], Params[PARAMS_APPEND_TARGET])
print("data_load_mode: " + data_load_mode)

# COMMAND ----------

# DBTITLE 1,9. Set raw and cleansed table name
#Set raw and cleansed table name
#Delta and SQL tables are case Insensitive. Seems Delta table are always lower case
delta_cleansed_tbl_name = "{0}.{1}".format(ADS_DATABASE_CLEANSED, target_table)
delta_raw_tbl_name = "{0}.{1}".format(ADS_DATABASE_RAW, source_object)

#Destination
print(delta_cleansed_tbl_name)
print(delta_raw_tbl_name)


# COMMAND ----------

# DBTITLE 1,10. Load to Cleanse Delta Table from Raw Delta Table
#This method uses the source table to load data into target Delta Table
DeltaSaveToDeltaTable (
    source_table = delta_raw_tbl_name,
    target_table = target_table,
    target_data_lake_zone = ADS_DATALAKE_ZONE_CLEANSED,
    target_database = ADS_DATABASE_STAGE,
    data_lake_folder = data_lake_folder,
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
#Pass 'MANDATORY' as second argument to function ToValidDate() on key columns to ensure correct value settings for those columns
df_cleansed = spark.sql(f"SELECT  \
                                case when BELNR = 'na' then '' else BELNR end as billingDocumentNumber, \
                                case when BELZEILE = 'na' then '' else BELZEILE end as billingDocumentLineItemId, \
                                EQUNR as equipmentNumber, \
                                GERAET as deviceNumber, \
                                MATNR as materialNumber, \
                                ZWNUMMER as registerNumber, \
                                INDEXNR as registerRelationshipConsecutiveNumber, \
                                ABLESGR as meterReadingReasonCode, \
                                ABLESGRV as previousMeterReadingReasonCode, \
                                ATIM as billingMeterReadingTime, \
                                ATIMVA as previousMeterReadingTime, \
                                ToValidDate(ADATMAX) as  maxMeterReadingDate, \
                                ATIMMAX as maxMeterReadingTime, \
                                ToValidDate(THGDATUM) as  serviceAllocationDate, \
                                ToValidDate(ZUORDDAT) as  meterReadingAllocationDate, \
                                ABLBELNR as suppressedMeterReadingDocumentId, \
                                LOGIKNR as logicalDeviceNumber, \
                                LOGIKZW as logicalRegisterNumber, \
                                ISTABLART as meterReadingTypeCode, \
                                ISTABLARTVA as previousMeterReadingTypeCode, \
                                EXTPKZ as meterReadingResultsSimulationIndicator, \
                                ToValidDate(BEGPROG) as  forecastPeriodStartDate, \
                                ToValidDate(ENDEPROG) as  forecastPeriodEndDate, \
                                ABLHINW as meterReaderNoteText, \
                                cast(V_ZWSTAND as dec(17)) as meterReadingBeforeDecimalPoint, \
                                cast(N_ZWSTAND as dec(14,14)) as meterReadingAfterDecimalPoint, \
                                cast(V_ZWSTNDAB as dec(17)) as billedMeterReadingBeforeDecimalPlaces, \
                                cast(N_ZWSTNDAB as dec(14,14)) as billedMeterReadingAfterDecimalPlaces, \
                                cast(V_ZWSTVOR as dec(17)) as prevousMeterReadingBeforeDecimalPlaces, \
                                cast(N_ZWSTVOR as dec(14,14)) as previousMeterReadingAfterDecimalPlaces, \
                                cast(V_ZWSTDIFF as dec(17)) as meterReadingDifferenceBeforeDecimalPlaces, \
                                cast(N_ZWSTDIFF as dec(14,14)) as meterReadingDifferenceAfterDecimalPlaces, \
                                _RecordStart, \
                                _RecordEnd, \
                                _RecordDeleted, \
                                _RecordCurrent \
                               FROM {ADS_DATABASE_STAGE}.{source_object}")

print(f'Number of rows: {df_cleansed.count()}')

# COMMAND ----------

newSchema = StructType([
                          StructField('billingDocumentNumber', StringType(), False),
                          StructField('billingDocumentLineItemId', StringType(), False),
                          StructField('equipmentNumber', StringType(), True),
                          StructField('deviceNumber', StringType(), True),
                          StructField('materialNumber', StringType(), True),
                          StructField('registerNumber', StringType(), True),
                          StructField('registerRelationshipConsecutiveNumber', StringType(), True),
                          StructField('meterReadingReasonCode', StringType(), True),
                          StructField('previousMeterReadingReasonCode', StringType(), True),
                          StructField('billingMeterReadingTime', StringType(), True),
                          StructField('previousMeterReadingTime', StringType(), True),
                          StructField('maxMeterReadingDate', DateType(), True),
                          StructField('maxMeterReadingTime', StringType(), True),
                          StructField('serviceAllocationDate', DateType(), True),
                          StructField('meterReadingAllocationDate', DateType(), True),
                          StructField('suppressedMeterReadingDocumentId', StringType(), True),
                          StructField('logicalDeviceNumber', StringType(), True),
                          StructField('logicalRegisterNumber', StringType(), True),
                          StructField('meterReadingTypeCode', StringType(), True),
                          StructField('previousMeterReadingTypeCode', StringType(), True),
                          StructField('meterReadingResultsSimulationIndicator', StringType(), True),
                          StructField('forecastPeriodStartDate', DateType(), True),
                          StructField('forecastPeriodEndDate', DateType(), True),
                          StructField('meterReaderNoteText', StringType(), True),
                          StructField('meterReadingBeforeDecimalPoint', DecimalType(17), True),
                          StructField('meterReadingAfterDecimalPoint', DecimalType(14,14), True),
                          StructField('billedMeterReadingBeforeDecimalPlaces', DecimalType(17), True),
                          StructField('billedMeterReadingAfterDecimalPlaces', DecimalType(14,14), True),
                          StructField('previousMeterReadingBeforeDecimalPlaces', DecimalType(17), True),
                          StructField('previousMeterReadingAfterDecimalPlaces', DecimalType(14,14), True),
                          StructField('meterReadingDifferenceBeforeDecimalPlaces', DecimalType(17), True),
                          StructField('meterReadingDifferenceAfterDecimalPlaces', DecimalType(14,14), True),
                          StructField('_RecordStart', TimestampType(), False),
                          StructField('_RecordEnd', TimestampType(), False),
                          StructField('_RecordDeleted', IntegerType(), False),
                          StructField('_RecordCurrent', IntegerType(), False)
                      ])


# COMMAND ----------

# DBTITLE 1,12. Save Data frame into Cleansed Delta table (Final)
#Save Data frame into Cleansed Delta table (final)
DeltaSaveDataframeDirect(df_cleansed, source_group, target_table, ADS_DATABASE_CLEANSED, ADS_CONTAINER_CLEANSED, "overwrite", newSchema, "")

# COMMAND ----------

# DBTITLE 1,13. Exit Notebook
dbutils.notebook.exit("1")
