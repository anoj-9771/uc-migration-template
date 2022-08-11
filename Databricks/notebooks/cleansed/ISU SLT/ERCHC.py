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
# MAGIC #Load data from Raw Zone to Trusted Zone

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
data_lake_folder = source_group
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

#Get the start time of the last successful cleansed load execution
LastSuccessfulExecutionTS = Params["LastSuccessfulExecutionTS"]
print("LastSuccessfulExecutionTS: " + LastSuccessfulExecutionTS)

#Get current time
#CurrentTimeStamp = spark.sql("select current_timestamp()").first()[0]
CurrentTimeStamp = GeneralLocalDateTime()
CurrentTimeStamp = CurrentTimeStamp.strftime("%Y-%m-%d %H:%M:%S")

#Get business key,track_changes and delta_extract flag
business_key =  Params[PARAMS_BUSINESS_KEY_COLUMN]
track_changes =  Params[PARAMS_TRACK_CHANGES]
is_delta_extract =  Params[PARAMS_DELTA_EXTRACT]


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

# DBTITLE 1,10. Load Raw to Dataframe & Do Transformations
df = spark.sql(f"WITH stage AS \
                      (Select *, ROW_NUMBER() OVER (PARTITION BY BELNR,LFDNR ORDER BY _DLRawZoneTimestamp DESC, DELTA_TS DESC) AS _RecordVersion FROM {delta_raw_tbl_name} WHERE _DLRawZoneTimestamp >= '{LastSuccessfulExecutionTS}' and IS_DELETED != 'X') \
                           select \
                                  case when BELNR = 'na' then '' else BELNR end as billingDocumentNumber, \
                                  case when LFDNR = 'na' then '' else LFDNR end as sequenceNumber, \
                                  OPBEL as contractDocumentNumber, \
                                  ToValidDate(CPUDT) as  documentEnteredDate, \
                                  ToValidDate(BUDAT) as  postingDate, \
                                  INTOPBEL as  printDocumentNumber, \
                                  ToValidDate(INTCPUDT) as  invoiceReversalEnteredDate, \
                                  ToValidDate(INTBUDAT) as  invoiceReversalPostingDate, \
                                  case when TOBRELEASD = 'X' then 'Y' else 'N' end as documentNotReleasedIndicator, \
                                  case when SIMULATED = 'X' then 'Y' else 'N' end as billingSimulationIndicator, \
                                  case when INVOICED = 'X' then 'Y' else 'N' end as invoicePostedIndicator, \
                                  SPCANC as adjustmentReversalIndicator, \
                                  case when STATUPD = 'X' then 'Y' else 'N' end as documentInSalesStatsIndicator, \
                                  case when STATUPD_CANC = 'X' then 'Y' else 'N' end as reversalDocumentInSalesStatsIndicator, \
                                  cast('1900-01-01' as TimeStamp) as _RecordStart, \
                                  cast('9999-12-31' as TimeStamp) as _RecordEnd, \
                                  '0' as _RecordDeleted, \
                                  '1' as _RecordCurrent, \
                                  cast('{CurrentTimeStamp}' as TimeStamp) as _DLCleansedZoneTimeStamp \
                        FROM stage stg \
                        where stg._RecordVersion = 1 ")

#print(f'Number of rows: {df.count()}')

# COMMAND ----------

newSchema = StructType([
                          StructField('billingDocumentNumber', StringType(), False),
                          StructField('sequenceNumber', StringType(), False),
                          StructField('contractDocumentNumber', StringType(), True),
                          StructField('documentEnteredDate', DateType(), True),
                          StructField('postingDate', DateType(), True),
                          StructField('printDocumentNumber', StringType(), True),
                          StructField('invoiceReversalEnteredDate', DateType(), True),
                          StructField('invoiceReversalPostingDate', DateType(), True),
                          StructField('documentNotReleasedIndicator', StringType(), True),
                          StructField('billingSimulationIndicator', StringType(), True),
                          StructField('invoicePostedIndicator', StringType(), True),
                          StructField('adjustmentReversalIndicator', StringType(), True),
                          StructField('documentInSalesStatsIndicator', StringType(), True),
                          StructField('reversalDocumentInSalesStatsIndicator', StringType(), True),
                          StructField('_RecordStart', TimestampType(), True),
                          StructField('_RecordEnd', TimestampType(), False),
                          StructField('_RecordDeleted', IntegerType(), False),
                          StructField('_RecordCurrent', IntegerType(), False),
                          StructField('_DLCleansedZoneTimeStamp',TimestampType(),False)
])


# COMMAND ----------

# DBTITLE 1,12. Save Dataframe to Staged & Cleansed
DeltaSaveDataFrameToDeltaTable(df, target_table, ADS_DATALAKE_ZONE_CLEANSED, ADS_DATABASE_CLEANSED, data_lake_folder, ADS_WRITE_MODE_MERGE, newSchema, track_changes, is_delta_extract, business_key, AddSKColumn = False, delta_column = "", start_counter = "0", end_counter = "0")

# COMMAND ----------

# DBTITLE 1,13.1 Identify Deleted records from Raw table
df = spark.sql(f"select distinct coalesce(BELNR,'') as BELNR, coalesce(LFDNR,'') as LFDNR from ( \
Select *, ROW_NUMBER() OVER (PARTITION BY BELNR,LFDNR ORDER BY _DLRawZoneTimeStamp DESC, DELTA_TS DESC) AS _RecordVersion FROM {delta_raw_tbl_name} WHERE _DLRawZoneTimestamp >= '{LastSuccessfulExecutionTS}' ) \
where  _RecordVersion = 1 and IS_DELETED ='X'")
df.createOrReplaceTempView("isu_erchc_deleted_records")

# COMMAND ----------

# DBTITLE 1,13.2 Update _RecordDeleted and _RecordCurrent Flags
#Get current time
CurrentTimeStamp = GeneralLocalDateTime()
CurrentTimeStamp = CurrentTimeStamp.strftime("%Y-%m-%d %H:%M:%S")

spark.sql(f" \
    MERGE INTO cleansed.isu_ERCHC \
    using isu_erchc_deleted_records \
    and isu_ERCHC.billingDocumentNumber = isu_erchc_deleted_records.BELNR \
    and isu_ERCHC.sequenceNumber = isu_erchc_deleted_records.LFDNR \
    WHEN MATCHED THEN UPDATE SET \
    _DLCleansedZoneTimeStamp = cast('{CurrentTimeStamp}' as TimeStamp) \
    ,_RecordDeleted=1 \
    ,_RecordCurrent=0 \
    ")

# COMMAND ----------

# DBTITLE 1,14. Exit Notebook
dbutils.notebook.exit("1")
