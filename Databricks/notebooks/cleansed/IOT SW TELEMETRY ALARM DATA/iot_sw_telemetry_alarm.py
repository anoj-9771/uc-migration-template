# Databricks notebook source
# DBTITLE 1,Generate parameter and source object name for unit testing
import json
#For unit testing...
#Use this string in the Param widget: 
#{"SourceType":"BLOB Storage (json)","SourceServer":"daf-sa-lake-sastoken","SourceGroup":"iotswtelemetryalarmdata","SourceName":"iot_iot_sw_telemetry_alarm","SourceLocation":"iotswtelemetryalarmdata/iot_sw_telemetry_alarm","AdditionalProperty":"","Processor":"databricks-token|1103-023442-me8nqcm9|Standard_DS3_v2|8.3.x-scala2.12|2:8|interactive","IsAuditTable":false,"SoftDeleteSource":"","ProjectName":"IOT SW TELEMETRY ALARM DATA","ProjectId":19,"TargetType":"BLOB Storage (json)","TargetName":"iot_iot_sw_telemetry_alarm","TargetLocation":"iotswtelemetryalarmdata/iot_sw_telemetry_alarm","TargetServer":"daf-sa-lake-sastoken","DataLoadMode":"APPEND","DeltaExtract":false,"CDCSource":false,"TruncateTarget":false,"UpsertTarget":false,"AppendTarget":true,"TrackChanges":false,"LoadToSqlEDW":true,"TaskName":"iot_iot_sw_telemetry_alarm","ControlStageId":2,"TaskId":874,"StageSequence":200,"StageName":"Raw to Cleansed","SourceId":874,"TargetId":874,"ObjectGrain":"Day","CommandTypeId":8,"Watermarks":null,"WatermarksDT":null,"WatermarkColumn":"","BusinessKeyColumn":"_lsn","UpdateMetaData":null,"SourceTimeStampFormat":"","Command":"/build/cleansed/IOT SW TELEMETRY ALARM DATA/iot_sw_telemetry_alarm","LastLoadedFile":null}

#Use this string in the Source Object widget
#iot_iot_sw_telemetry_alarm

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
# MAGIC #Load data to Cleansed Zone from Raw Zone

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

#Get max date from staging table assign it to start_counter
row_count = spark.sql(f"SELECT count(*) as cnt FROM stage.iot_iot_sw_telemetry_alarm ")
row_count = row_count.select('cnt').distinct().count()
if row_count > 0:
    start_counter = spark.sql(f"SELECT cast(max(_DLRawZoneTimeStamp) as varchar(50)) as max_date FROM stage.iot_iot_sw_telemetry_alarm ")
    start_counter = start_counter.first()["max_date"]
else:
    start_counter = "2000-01-01"
print(start_counter)

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
# df_cleansed = spark.sql(f"SELECT * FROM {ADS_DATABASE_STAGE}.{source_object}")

df_cleansed = spark.sql(f"SELECT \
                            _etag, \
                            _lsn, \
                            _rid, \
                            _self, \
                            _ts, \
                            alarmPriority, \
                            alarmSeverity, \
                            deviceRawId, \
                            groupDVId, \
                            groupLevel2, \
                            id, \
                            isAlarmActive, \
                            isPointActive, \
                            lat, \
                            long, \
                            pointDVId, \
                            pointName, \
                            pointRawId, \
                            pointVal, \
                            recordTime, \
                            scamp, \
                            sitecode, \
                            _DLRawZoneTimeStamp, \
                            _FileDateTimeStamp, \
                            year, \
                            month, \
                            day, \
                            _DLCleansedZoneTimeStamp, \
                            _RecordStart, \
                            _RecordEnd, \
                            _RecordDeleted, \
                            _RecordCurrent \
                        FROM {ADS_DATABASE_STAGE}.{source_object}"
                       )
# display(df_cleansed)
# print(f'Number of rows: {df_cleansed.count()}')

# COMMAND ----------

df_cleansed.printSchema()

# COMMAND ----------

# DBTITLE 1,12. Save Data frame into Cleansed Delta table (Final)
#Save Data frame into Cleansed Delta table (final)
DeltaSaveDataframeDirect(df_cleansed, source_group, target_table, ADS_DATABASE_CLEANSED, ADS_CONTAINER_CLEANSED, "overwrite", "")

# COMMAND ----------

# DBTITLE 1,13. Exit Notebook
dbutils.notebook.exit("1")
