# Databricks notebook source
# DBTITLE 1,Generate parameter and source object name for unit testing
import json
#For unit testing...
#Use this string in the Param widget: 
#$PARAM
# {
# 	"SourceType": "BLOB Storage (json)", 
# 	"SourceServer": "daf-sa-lake-sastoken", 
# 	"SourceGroup": "isudata", 
# 	"SourceName": "isu_0UCINSTALLA_ATTR_2", 
# 	"SourceLocation": "isudata/0UCINSTALLA_ATTR_2", 
# 	"AdditionalProperty": "", 
# 	"Processor": "databricks-token|0527-214324-ytwxx0tv|Standard_DS12_v2|10.4.x-scala2.12|2:28|interactive", 
# 	"IsAuditTable": false, 
# 	"SoftDeleteSource": "", 
# 	"ProjectName": "CLEANSED ISU DATA", 
# 	"ProjectId": 12, 
# 	"TargetType": "BLOB Storage (json)", 
# 	"TargetName": "isu_0UCINSTALLA_ATTR_2", 
# 	"TargetLocation": "isudata/0UCINSTALLA_ATTR_2", 
# 	"TargetServer": "daf-sa-lake-sastoken", 
# 	"DataLoadMode": "INCREMENTAL", 
# 	"DeltaExtract": true, 
# 	"CDCSource": false, 
# 	"TruncateTarget": false, 
# 	"UpsertTarget": true, 
# 	"AppendTarget": false, 
# 	"TrackChanges": false, 
# 	"LoadToSqlEDW": true, 
# 	"TaskName": "isu_0UCINSTALLA_ATTR_2", 
# 	"ControlStageId": 2, 
# 	"TaskId": 152, 
# 	"StageSequence": 200, 
# 	"StageName": "Raw to Cleansed", 
# 	"SourceId": 152, 
# 	"TargetId": 152, 
# 	"ObjectGrain": "Day", 
# 	"CommandTypeId": 8, 
# 	"Watermarks": "2000-01-01 00:00:00", 
# 	"WatermarksDT": "2000-01-01T00:00:00", 
# 	"WatermarkColumn": "_FileDateTimeStamp", 
# 	"BusinessKeyColumn": "installationNumber", 
# 	"PartitionColumn": null, 
# 	"UpdateMetaData": null, 
# 	"SourceTimeStampFormat": "", 
# 	"WhereClause": "",
# 	"Command": "/build/cleansed/ISU Data/0UCINSTALLA_ATTR_2", 
# 	"LastSuccessfulExecutionTS": "2000-01-01 08:42:06.000",
# 	"LastLoadedFile": null
# }
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
df = spark.sql(f"""
     WITH stage AS (
          Select 
               *, 
               ROW_NUMBER() OVER (
                    PARTITION BY ANLAGE 
                    ORDER BY _FileDateTimeStamp DESC, DI_SEQUENCE_NUMBER DESC, _DLRawZoneTimeStamp DESC
               ) AS _RecordVersion 
          FROM {delta_raw_tbl_name} 
          WHERE _DLRawZoneTimestamp >= '{LastSuccessfulExecutionTS}'
     ) 
          SELECT  
               case 
                    when stg.ANLAGE = 'na' 
                    then '' 
                    else stg.ANLAGE 
               end                                                     as installationNumber, -- 2.2
               stg.SPARTE                                              as divisionCode, -- 1.2
               div.division                                            as division, 
               stg.VSTELLE                                             as premise, 
               stg.BEZUG                                               as reference, 
               stg.ABLESARTST                                          as meterReadingControlCode, 
               mrc.meterReadingControl                                 as meterReadingControl, 
               stg.SERVICE                                             as serviceTypeCode, 
               ser.serviceType                                         as serviceType , 
               stg.ETIMEZONE                                           as timeZone, 
               ToValidDate(stg.ERDAT)                                  as createdDate, 
               stg.ERNAM                                               as createdBy, 
               ToValidDate(stg.AEDAT)                                  as lastChangedDate, 
               stg.AENAM                                               as lastChangedBy, 
               stg.BEGRU                                               as authorizationGroupCode, 
               CASE 
                   WHEN stg.LOEVM IS NULL 
                   OR TRIM(stg.LOEVM) = '' 
                   THEN '0' 
                   ELSE '1' 
               END                                                     as deletedFlag, 
               stg.ZZ_HAUS                                             as propertyNumber, 
               stg.ZZ_PROPTYPE                                         as industry, 
               stg.ZZ_ADRNR                                            as addressNumber, 
               stg.ZZ_OBJNR                                            as objectNumber, 
               stg.ABLSPERR                                            as meterReadingBlockedReason, -- 2.2
               stg.BAPERTYP                                            as basePeriodCategory, -- 2.2
               stg.NODISCONCT                                          as guaranteedSupplyReason, -- 2.2
               stg.DEREGSTAT                                           as deregulationStatus, -- 2.2
               stg.ANLART                                              as installationType, -- 2.2 
               cast('1900-01-01' as TimeStamp)                         as _RecordStart, 
               cast('9999-12-31' as TimeStamp)                         as _RecordEnd, 
               CASE 
                   WHEN stg.LOEVM IS NULL 
                   OR TRIM(stg.LOEVM) = '' 
                   THEN '0' 
                   ELSE '1' 
               END                                                     as _RecordDeleted, 
               '1'                                                     as _RecordCurrent, 
               cast('{CurrentTimeStamp}' as TimeStamp)                 as _DLCleansedZoneTimeStamp 
     FROM stage stg 
     LEFT OUTER JOIN {ADS_DATABASE_CLEANSED}.isu_0DIVISION_TEXT div ON 
          div.divisionCode = stg.SPARTE and 
          div._RecordDeleted = 0 and
          div._RecordCurrent = 1 
     LEFT OUTER JOIN {ADS_DATABASE_CLEANSED}.isu_0UC_SERTYPE_TEXT ser ON 
          ser.serviceTypeCode = stg.SERVICE and 
          ser._RecordDeleted = 0 and 
          ser._RecordCurrent = 1 
     LEFT OUTER JOIN {ADS_DATABASE_CLEANSED}.isu_TE438T mrc ON 
          mrc.meterReadingControlCode = stg.ABLESARTST and 
          mrc._RecordDeleted = 0 and 
          mrc._RecordCurrent = 1 
     WHERE stg._RecordVersion = 1 AND stg.ANLAGE IS NOT NULL
"""
)

#print(f'Number of rows: {df.count()}')

# COMMAND ----------

# DBTITLE 1,11. Update/Rename Columns and Load into a Dataframe
#Update/rename Column
#Pass 'MANDATORY' as second argument to function ToValidDate() on key columns to ensure correct value settings for those columns
# df_cleansed = spark.sql(f"SELECT  \
#                             case when stg.ANLAGE = 'na' then '' else stg.ANLAGE end as installationId, \
#                             stg.SPARTE as divisionCode, \
#                             div.division, \
#                             stg.VSTELLE as premise, \
#                             stg.BEZUG as reference, \
#                             stg.ABLESARTST as meterReadingControlCode, \
#                             mrc.meterReadingControl as meterReadingControl, \
#                             stg.SERVICE as serviceTypeCode, \
#                             ser.serviceType, \
#                             stg.ETIMEZONE as timeZone, \
#                             ToValidDate(stg.ERDAT) as createdDate, \
#                             stg.ERNAM as createdBy, \
#                             ToValidDate(stg.AEDAT) as lastChangedDate, \
#                             stg.AENAM as lastChangedBy, \
#                             stg.BEGRU as authorizationGroupCode, \
#                             stg.LOEVM as deletedIndicator, \
#                             stg.ZZ_HAUS as propertyNumber, \
#                             stg.ZZ_PROPTYPE as industry, \
#                             stg.ZZ_ADRNR as addressNumber, \
#                             stg.ZZ_OBJNR as objectNumber, \
#                             stg._RecordStart, \
#                             stg._RecordEnd, \
#                             stg._RecordDeleted, \
#                             stg._RecordCurrent \
#                         FROM {ADS_DATABASE_STAGE}.{source_object} stg \
#                         LEFT OUTER JOIN {ADS_DATABASE_CLEANSED}.isu_0DIVISION_TEXT div on div.divisionCode = stg.SPARTE \
#                                                                                                     and div._RecordDeleted = 0 and div._RecordCurrent = 1 \
#                         LEFT OUTER JOIN {ADS_DATABASE_CLEANSED}.isu_0UC_SERTYPE_TEXT ser on ser.serviceTypeCode = stg.SERVICE \
#                                                                                                     and ser._RecordDeleted = 0 and ser._RecordCurrent = 1 \
#                         LEFT OUTER JOIN {ADS_DATABASE_CLEANSED}.isu_TE438T mrc ON mrc.meterReadingControlCode = stg.ABLESARTST \
#                                                                                                     and mrc._RecordDeleted = 0 and mrc._RecordCurrent = 1")

# print(f'Number of rows: {df_cleansed.count()}')

# COMMAND ----------

# Create schema for the cleanse table
newSchema = StructType([
                        StructField("installationNumber", StringType(), False),
                        StructField("divisionCode", StringType(), True),
                        StructField("division", StringType(), True),
                        StructField("premise", StringType(), True),
                        StructField("reference", StringType(), True),
                        StructField("meterReadingControlCode", StringType(), True),
                        StructField("meterReadingControl", StringType(), True),
                        StructField("serviceTypeCode", StringType(), True),
                        StructField("serviceType", StringType(), True),
                        StructField("timeZone", StringType(), True),
                        StructField("createdDate", DateType(), True),
                        StructField("createdBy", StringType(), True),
                        StructField("lastChangedDate", DateType(), True),
                        StructField("lastChangedBy", StringType(), True),
                        StructField("authorizationGroupCode", StringType(), True),
                        StructField("deletedFlag", StringType(), True),
                        StructField("propertyNumber", StringType(), True),
                        StructField("industry", StringType(), True),
                        StructField("addressNumber", StringType(), True),
                        StructField("objectNumber", StringType(), True),
                        StructField('meterReadingBlockedReason', StringType(), True),
                        StructField('basePeriodCategory', StringType(), True),
                        StructField('guaranteedSupplyReason', StringType(), True),
                        StructField('deregulationStatus', StringType(), True),
                        StructField('installationType', StringType(), True),
                        StructField('_RecordStart',TimestampType(),False),
                        StructField('_RecordEnd',TimestampType(),False),
                        StructField('_RecordDeleted',IntegerType(),False),
                        StructField('_RecordCurrent',IntegerType(),False),
                        StructField('_DLCleansedZoneTimeStamp',TimestampType(),False)
                      ])


# COMMAND ----------

# DBTITLE 1,12. Save Data frame into Cleansed Delta table (Final)
DeltaSaveDataFrameToDeltaTable(df, target_table, ADS_DATALAKE_ZONE_CLEANSED, ADS_DATABASE_CLEANSED, data_lake_folder, ADS_WRITE_MODE_MERGE, newSchema, track_changes, is_delta_extract, business_key, AddSKColumn = False, delta_column = "", start_counter = "0", end_counter = "0")

# COMMAND ----------

# DBTITLE 1,13. Exit Notebook
dbutils.notebook.exit("1")
