# Databricks notebook source
# DBTITLE 1,Generate parameter and source object name for unit testing
import json
#For unit testing...
#Use this string in the Param widget: 
# {"SourceType":"BLOB Storage (json)","SourceServer":"daf-sa-blob-sastoken","SourceGroup":"isudata","SourceName":"isu_0UC_DEVICEH_ATTR","SourceLocation":"isudata/0UC_DEVICEH_ATTR","AdditionalProperty":"","Processor":"databricks-token|1018-021846-1a1ycoqc|Standard_DS3_v2|8.3.x-scala2.12|2:8|interactive","IsAuditTable":false,"SoftDeleteSource":"","ProjectName":"CLEANSED ISU DATA","ProjectId":12,"TargetType":"BLOB Storage (json)","TargetName":"isu_0UC_DEVICEH_ATTR","TargetLocation":"isudata/0UC_DEVICEH_ATTR","TargetServer":"daf-sa-lake-sastoken","DataLoadMode":"INCREMENTAL","DeltaExtract":true,"CDCSource":false,"TruncateTarget":false,"UpsertTarget":true,"AppendTarget":null,"TrackChanges":false,"LoadToSqlEDW":true,"TaskName":"isu_0UC_DEVICEH_ATTR","ControlStageId":2,"TaskId":228,"StageSequence":200,"StageName":"Raw to Cleansed","SourceId":228,"TargetId":228,"ObjectGrain":"Day","CommandTypeId":8,"Watermarks":"2000-01-01 00:00:00","WatermarksDT":"2000-01-01T00:00:00","WatermarkColumn":"_FileDateTimeStamp","BusinessKeyColumn":"equipmentNumber,validToDate","PartitionColumn":null,"UpdateMetaData":null,"SourceTimeStampFormat":"","WhereClause":"","Command":"/build/cleansed/ISU Data/0UC_DEVICEH_ATTR","LastSuccessfulExecutionTS":"2000-01-01T23:46:12.39","LastLoadedFile":null}

#Use this string in the Source Object widget
#isu_0UC_DEVICEH_ATTR

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
df = spark.sql(f"WITH stageUpsert AS \
                      (select *, 'U' as _upsertFlag from (Select *, ROW_NUMBER() OVER (PARTITION BY EQUNR,BIS ORDER BY _FileDateTimeStamp DESC, DI_SEQUENCE_NUMBER DESC, _DLRawZoneTimeStamp DESC) AS _RecordVersion FROM {delta_raw_tbl_name} \
                      WHERE _DLRawZoneTimestamp >= '{LastSuccessfulExecutionTS}' and DI_OPERATION_TYPE !='X' ) where _RecordVersion = 1), \
                      stageDelete AS \
                      (select *, 'D' as _upsertFlag from ( \
Select *, ROW_NUMBER() OVER (PARTITION BY EQUNR,AB,BIS ORDER BY _FileDateTimeStamp DESC, DI_SEQUENCE_NUMBER DESC, _DLRawZoneTimeStamp DESC) AS _RecordVersion FROM {delta_raw_tbl_name} WHERE _DLRawZoneTimestamp >= '{LastSuccessfulExecutionTS}') where  _RecordVersion = 1 and DI_OPERATION_TYPE ='X'), \
                      stage AS (select * from stageUpsert union select * from stageDelete) \
                           SELECT \
                                case when dev.EQUNR = 'na' then '' else dev.EQUNR end as equipmentNumber, \
                                ToValidDate(dev.BIS,'MANDATORY') as validToDate, \
                                ToValidDate(dev.AB) as validFromDate, \
                                dev.KOMBINAT as deviceCategoryCombination, \
                                dev.LOGIKNR as logicalDeviceNumber, \
                                dev.ZWGRUPPE as registerGroupCode, \
                                c.registerGroup, \
                                ToValidDate(dev.EINBDAT) as installationDate, \
                                ToValidDate(dev.AUSBDAT) as deviceRemovalDate, \
                                dev.GERWECHS as activityReasonCode, \
                                b.activityReason, \
                                dev.DEVLOC as deviceLocation, \
                                dev.WGRUPPE as windingGroup, \
                                (CASE WHEN dev.LOEVM = 'X' THEN 'Y' ELSE 'N' END) as deletedFlag, \
                                dev.UPDMOD as bwDeltaProcess, \
                                cast(dev.AMCG_CAP_GRP as Integer) as advancedMeterCapabilityGroup, \
                                cast(dev.MSG_ATTR_ID as Integer) as messageAttributeId, \
                                dev.ZZMATNR as materialNumber, \
                                dev.ZANLAGE as installationId, \
                                dev.ZADDRNUMBER as addressNumber, \
                                dev.ZCITY1 as cityName, \
                                dev.ZHOUSE_NUM1 as houseNumber, \
                                dev.ZSTREET as streetName, \
                                dev.ZPOST_CODE1 as postalCode, \
                                dev.ZTPLMA as superiorFunctionalLocationNumber, \
                                dev.ZZ_POLICE_EVENT as policeEventNumber, \
                                dev.ZAUFNR as orderNumber, \
                                dev.ZERNAM as createdBy, \
                                'EQUNR|BIS' as sourceKeyDesc, \
                                concat_ws('|',dev.EQUNR,dev.BIS) as sourceKey, \
                                'BIS' as rejectColumn, \
                                cast('1900-01-01' as TimeStamp) as _RecordStart, \
                                cast('9999-12-31' as TimeStamp) as _RecordEnd, \
                                (CASE WHEN _upsertFlag = 'U' THEN '0' ELSE '1' END) as _RecordDeleted, \
                                '1' as _RecordCurrent, \
                                cast('{CurrentTimeStamp}' as TimeStamp) as _DLCleansedZoneTimeStamp \
                        FROM stage dev \
                            left outer join {ADS_DATABASE_CLEANSED}.isu_0UC_GERWECHS_TEXT b on dev.GERWECHS = b.activityReasonCode \
                            left outer join {ADS_DATABASE_CLEANSED}.isu_0UC_REGGRP_TEXT c on dev.ZWGRUPPE = c.registerGroupCode \
                        ").cache()

print(f'Number of rows: {df.count()}')

# COMMAND ----------

newSchema = StructType([
                          StructField('equipmentNumber',StringType(),False),
                          StructField('validToDate',DateType(),False),
                          StructField('validFromDate',DateType(),True),
                          StructField('deviceCategoryCombination',StringType(),True),
                          StructField('logicalDeviceNumber',StringType(),True),                                      
                          StructField('registerGroupCode',StringType(),True),
                          StructField('registerGroup',StringType(),True),
                          StructField('installationDate',DateType(),True),
                          StructField('deviceRemovalDate',DateType(),True),
                          StructField('activityReasonCode',StringType(),True),
                          StructField('activityReason',StringType(),True),
                          StructField('deviceLocation',StringType(),True),
                          StructField('windingGroup',StringType(),True),
                          StructField('deletedFlag',StringType(),True),
                          StructField('bwDeltaProcess',StringType(),True),
                          StructField('advancedMeterCapabilityGroup',IntegerType(),True),
                          StructField('messageAttributeId',IntegerType(),True),
                          StructField('materialNumber',StringType(),True),
                          StructField('installationId',StringType(),True),
                          StructField('addressNumber',StringType(),True),
                          StructField('cityName',StringType(),True),
                          StructField('houseNumber',StringType(),True),
                          StructField('streetName',StringType(),True),
                          StructField('postalCode',StringType(),True),
                          StructField('superiorFunctionalLocationNumber',StringType(),True),
                          StructField('policeEventNumber',StringType(),True),
                          StructField('orderNumber',StringType(),True),
                          StructField('createdBy',StringType(),True),
                          StructField('_RecordStart',TimestampType(),False),
                          StructField('_RecordEnd',TimestampType(),False),
                          StructField('_RecordDeleted',IntegerType(),False),
                          StructField('_RecordCurrent',IntegerType(),False),
                          StructField('_DLCleansedZoneTimeStamp',TimestampType(),False)
                        ])
                                      


# COMMAND ----------

# DBTITLE 1,Handle Invalid Records
reject_df =df.where("validToDate = '1000-01-01'").cache()
cleansed_df = df.subtract(reject_df)
cleansed_df = cleansed_df.drop("sourceKeyDesc","sourceKey","rejectColumn")

# COMMAND ----------

# DBTITLE 1,12. Save Data frame into Cleansed Delta table (Final)
DeltaSaveDataFrameToDeltaTable(cleansed_df.filter("_RecordDeleted = '0'"), target_table, ADS_DATALAKE_ZONE_CLEANSED, ADS_DATABASE_CLEANSED, data_lake_folder, ADS_WRITE_MODE_MERGE, newSchema, track_changes, is_delta_extract, business_key, AddSKColumn = False, delta_column = "", start_counter = "0", end_counter = "0")

# COMMAND ----------

# DBTITLE 1,13 Update _RecordDeleted and _RecordCurrent Flags
# Load deleted records to replace the existing Deleted records implementation logic
cleansed_df.filter("_RecordDeleted=1").createOrReplaceTempView("isu_device_deleted_records")
spark.sql(f" \
    MERGE INTO cleansed.isu_0UC_DEVICEH_ATTR \
    using isu_device_deleted_records \
    on isu_0UC_DEVICEH_ATTR.equipmentNumber = isu_device_deleted_records.equipmentNumber \
    and isu_0UC_DEVICEH_ATTR.validFromDate = isu_device_deleted_records.validFromDate \
    and isu_0UC_DEVICEH_ATTR.validToDate = isu_device_deleted_records.validToDate \
    WHEN MATCHED THEN UPDATE SET \
    deviceCategoryCombination=isu_device_deleted_records.deviceCategoryCombination \
    ,logicalDeviceNumber=isu_device_deleted_records.logicalDeviceNumber \
    ,registerGroupCode=isu_device_deleted_records.registerGroupCode \
    ,registerGroup=isu_device_deleted_records.registerGroup \
    ,installationDate=isu_device_deleted_records.installationDate \
    ,deviceRemovalDate=isu_device_deleted_records.deviceRemovalDate \
    ,activityReasonCode=isu_device_deleted_records.activityReasonCode \
    ,activityReason=isu_device_deleted_records.activityReason \
    ,deviceLocation=isu_device_deleted_records.deviceLocation \
    ,windingGroup=isu_device_deleted_records.windingGroup \
    ,deletedFlag=isu_device_deleted_records.deletedFlag \
    ,bwDeltaProcess=isu_device_deleted_records.bwDeltaProcess \
    ,advancedMeterCapabilityGroup=isu_device_deleted_records.advancedMeterCapabilityGroup \
    ,messageAttributeId=isu_device_deleted_records.messageAttributeId \
    ,materialNumber=isu_device_deleted_records.materialNumber \
    ,installationId=isu_device_deleted_records.installationId \
    ,addressNumber=isu_device_deleted_records.addressNumber \
    ,cityName=isu_device_deleted_records.cityName \
    ,houseNumber=isu_device_deleted_records.houseNumber \
    ,streetName=isu_device_deleted_records.streetName \
    ,postalCode=isu_device_deleted_records.postalCode \
    ,superiorFunctionalLocationNumber=isu_device_deleted_records.superiorFunctionalLocationNumber \
    ,policeEventNumber=isu_device_deleted_records.policeEventNumber \
    ,orderNumber=isu_device_deleted_records.orderNumber \
    ,createdBy=isu_device_deleted_records.createdBy \
    ,_DLCleansedZoneTimeStamp = cast('{CurrentTimeStamp}' as TimeStamp) \
    ,_RecordDeleted=1 \
    ,_RecordCurrent=1 \
    ")

# COMMAND ----------

# DBTITLE 1,13.3 Save Reject Data Frame into Rejected Database
if reject_df.count() > 0:
    source_key = 'EQUNR|BIS'
    DeltaSaveDataFrameToRejectTable(reject_df,target_table,business_key,source_key,LastSuccessfulExecutionTS)
    reject_df.unpersist()
df.unpersist()

# COMMAND ----------

# DBTITLE 1,14. Exit Notebook
dbutils.notebook.exit("1")
