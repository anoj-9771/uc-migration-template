# Databricks notebook source
# DBTITLE 1,Generate parameter and source object name for unit testing
import json
#For unit testing...
#Use this string in the Param widget: 
#{"SourceType": "BLOB Storage (json)", "SourceServer": "daf-sa-lake-sastoken", "SourceGroup": "isu", "SourceName": "isu_0UC_DEVICEH_ATTR", "SourceLocation": "isu/0UC_DEVICEH_ATTR", "AdditionalProperty": "", "Processor": "databricks-token|0711-011053-turfs581|Standard_DS3_v2|8.3.x-scala2.12|2:8|interactive", "IsAuditTable": false, "SoftDeleteSource": "", "ProjectName": "SAP DATA", "ProjectId": 2, "TargetType": "BLOB Storage (json)", "TargetName": "isu_0UC_DEVICEH_ATTR", "TargetLocation": "isu/0UC_DEVICEH_ATTR", "TargetServer": "daf-sa-lake-sastoken", "DataLoadMode": "FULL-EXTRACT", "DeltaExtract": false, "CDCSource": false, "TruncateTarget": false, "UpsertTarget": true, "AppendTarget": null, "TrackChanges": false, "LoadToSqlEDW": true, "TaskName": "isu_0UC_DEVICEH_ATTR", "ControlStageId": 2, "TaskId": 46, "StageSequence": 200, "StageName": "Raw to Cleansed", "SourceId": 46, "TargetId": 46, "ObjectGrain": "Day", "CommandTypeId": 8, "Watermarks": "", "WatermarksDT": null, "WatermarkColumn": "", "BusinessKeyColumn": "EQUNR,BIS", "UpdateMetaData": null, "SourceTimeStampFormat": "", "Command": "", "LastLoadedFile": null}

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
df_cleansed = spark.sql(f"select cast(y as double) as y, \
                                  cast(n2 as integer) as n2,\
                                  cast(x as double) as x,\
                                  to_timestamp(start_time) as start_time,\
                                  to_timestamp(valid_time) as valid_time,\
                                  proj,\
                                  cast(y_bounds as double) as y_bounds,\
                                  cast(x_bounds as double) as x_bounds,\
                                  cast(replace(precipitation, 'na', '') as double) as precipitation, \
                                  _DLCleansedZoneTimeStamp, \
                                  _RecordStart,\
                                  _RecordEnd, \
                                  _RecordDeleted, \
                                  _RecordCurrent \
                                  from {ADS_DATABASE_STAGE}.{source_object}")
# df_cleansed = df_cleansed.na.fill(value='0',subset=["precipitation"])
#display(df_cleansed)
# print(f'Number of rows: {df_cleansed.count()}')

# COMMAND ----------

df_cleansed.printSchema()

# COMMAND ----------

# DBTITLE 1,12. Update dataframe - extract IoT hub data
newSchema = StructType([
                          StructField('x',DoubleType(),True),
                          StructField('n2',IntegerType(),True),
                          StructField('x',DoubleType(),True),
                          StructField('start_time',TimestampType(),True),
                          StructField('valid_time',TimestampType(),True),
                          StructField('proj',StringType(),True),                                      
                          StructField('y_bounds',DoubleType(),True),
                          StructField('x_bounds',DoubleType(),True),
                          StructField('precipitation',DoubleType(),True),
                          StructField('_DLCleansedZoneTimeStamp',TimestampType(),True),
                          StructField('_RecordStart',TimestampType(),True),
                          StructField('_RecordEnd',TimestampType(),True),
                          StructField('_RecordDeleted',IntegerType(),True),
                          StructField('_RecordCurrent',IntegerType(),True)
                        ])
                                      
df_updated_column = spark.createDataFrame(df_cleansed.rdd, schema=newSchema)
df_cleansed_updated = df_cleansed
# display(df_cleansed_updated)


# COMMAND ----------

# #Generating sample data for testig. Remove it
# from functools import reduce
# df_d1t1 = df_cleansed_updated
# df_d1t1 = df_d1t1.withColumn("start_time", lit('2020-12-13T09:10:00.000+0000')).withColumn("valid_time", lit('2020-12-13T10:10:00.000+0000'))
# df_d1t2 = df_d1t1.withColumn("start_time", lit('2020-12-13T10:10:00.000+0000')).withColumn("valid_time", lit('2020-12-13T11:10:00.000+0000'))
# df_d1t3 = df_d1t2.withColumn("start_time", lit('2020-12-13T11:10:00.000+0000')).withColumn("valid_time", lit('2020-12-13T12:10:00.000+0000'))
# # df_d1t4 = df_d1t3.withColumn("start_time", lit('2020-12-13T12:10:00.000+0000')).withColumn("valid_time", lit('2020-12-13T13:10:00.000+0000'))\
# # df_d1t5 = df_d1t4.withColumn("start_time", lit('2020-12-13T13:10:00.000+0000')).withColumn("valid_time", lit('2020-12-13T14:10:00.000+0000'))\
# # df_d1t6 = df_d1t5.withColumn("start_time", lit('2020-12-13T14:10:00.000+0000')).withColumn("valid_time", lit('2020-12-15T23:10:00.000+0000'))\
# # df_d1t7 = df_d1t6.withColumn("start_time", lit('2020-12-13T15:10:00.000+0000')).withColumn("valid_time", lit('2020-12-16T16:10:00.000+0000'))\
# # df_d1t8 = df_d1t7.withColumn("start_time", lit('2020-12-13T16:10:00.000+0000')).withColumn("valid_time", lit('2020-12-13T17:10:00.000+0000'))\
# # df_d1t9 = df_d1t8.withColumn("start_time", lit('2020-12-13T17:10:00.000+0000')).withColumn("valid_time", lit('2020-12-13T18:10:00.000+0000'))\
# # df_d1t10 = df_d1t9.withColumn("start_time", lit('2020-12-13T18:10:00.000+0000')).withColumn("valid_time", lit('2020-12-13T19:10:00.000+0000'))\
# # df_d1t11 = df_d1t10.withColumn("start_time", lit('2020-12-13T19:10:00.000+0000')).withColumn("valid_time", lit('2020-12-13T20:10:00.000+0000'))\
# # df_d1t12 = df_d1t11.withColumn("start_time", lit('2020-12-13T20:10:00.000+0000')).withColumn("valid_time", lit('2020-12-13T21:10:00.000+0000'))

# df_d2t1 = df_cleansed_updated
# df_d2t1 = df_d2t1.withColumn("start_time", lit('2020-12-14T09:10:00.000+0000')).withColumn("valid_time", lit('2020-12-14T10:10:00.000+0000'))
# df_d2t2 = df_d2t1.withColumn("start_time", lit('2020-12-14T10:10:00.000+0000')).withColumn("valid_time", lit('2020-12-14T11:10:00.000+0000'))
# df_d2t3 = df_d2t2.withColumn("start_time", lit('2020-12-14T11:10:00.000+0000')).withColumn("valid_time", lit('2020-12-14T12:10:00.000+0000'))
# # df_d2t4 = df_d2t3.withColumn("start_time", lit('2020-12-14T12:10:00.000+0000')).withColumn("valid_time", lit('2020-12-14T13:10:00.000+0000'))\
# # df_d2t5 = df_d2t4.withColumn("start_time", lit('2020-12-14T13:10:00.000+0000')).withColumn("valid_time", lit('2020-12-14T14:10:00.000+0000'))\
# # df_d2t6 = df_d2t5.withColumn("start_time", lit('2020-12-14T14:10:00.000+0000')).withColumn("valid_time", lit('2020-12-15T23:10:00.000+0000'))\
# # df_d2t7 = df_d2t6.withColumn("start_time", lit('2020-12-14T15:10:00.000+0000')).withColumn("valid_time", lit('2020-12-16T16:10:00.000+0000'))\
# # df_d2t8 = df_d2t7.withColumn("start_time", lit('2020-12-14T16:10:00.000+0000')).withColumn("valid_time", lit('2020-12-14T17:10:00.000+0000'))\
# # df_d2t9 = df_d2t8.withColumn("start_time", lit('2020-12-14T17:10:00.000+0000')).withColumn("valid_time", lit('2020-12-14T18:10:00.000+0000'))\
# # df_d2t10 = df_d2t9.withColumn("start_time", lit('2020-12-14T18:10:00.000+0000')).withColumn("valid_time", lit('2020-12-14T19:10:00.000+0000'))\
# # df_d2t11 = df_d2t10.withColumn("start_time", lit('2020-12-14T19:10:00.000+0000')).withColumn("valid_time", lit('2020-12-14T20:10:00.000+0000'))\
# # df_d2t12 = df_d2t11.withColumn("start_time", lit('2020-12-14T20:10:00.000+0000')).withColumn("valid_time", lit('2020-12-14T21:10:00.000+0000'))

# dfs = [df_cleansed_updated,df_d1t1,df_d1t2,df_d1t3,df_d2t1,df_d2t2,df_d2t3]
# df = reduce(DataFrame.unionAll, dfs)
# df_cleansed_updated = df
# # df_combine = df_day1.union(df_day2)               
# # df_final = df_updated_column.union(df_combine)
# # df_cleansed_updated.count()


# COMMAND ----------

# display(df.filter(df.start_time > '2020-12-13T21:10:00.000+0000'))
# df = df_updated_column

# COMMAND ----------

# DBTITLE 1,12. Save Data frame into Cleansed Delta table (Final)
#Save Data frame into Cleansed Delta table (final)
DeltaSaveDataframeDirect(df_cleansed_updated, source_group, target_table, ADS_DATABASE_CLEANSED, ADS_CONTAINER_CLEANSED, "overwrite", "")

# COMMAND ----------

# dfcount = spark.sql("select count(*) from cleansed.bom_bom715")
# dfcount.show()

# COMMAND ----------

df_bom = spark.sql("select * from cleansed.bom_bom715")
df_bom = df_cleansed_updated.withColumn("date_only", to_date(col("start_time")))\
                                         .withColumn("hour_only", hour(col("start_time")))
df_bom = df_bom.filter(df_bom.hour_only.between(9,23))
#display(df_bom)
# df_bom = df_cleansed_updated.withColumn("date_only", to_date(col("start_time")))
# display(df_bom.filter(df_bom.hour_only !=21))

# COMMAND ----------

# gdf = df_bom.groupBy("y", "n2", "x", "proj", "y_bounds", "x_bounds", "date_only","hour_only")  
# df_sum = gdf.agg(sum(col("precipitation")).alias("sum_precipitation")).where(col("hour_only").between(9,23))
gdf = df_bom.groupBy("y", "n2", "x", "proj", "y_bounds", "x_bounds", "date_only")  
df_sum = gdf.agg(sum(col("precipitation")).alias("sum_precipitation"))
df_sum = df_sum.withColumn("is_wet_weather", when((col("sum_precipitation") >= 10),True).otherwise(False))
# df.groupBy(someExpr).agg(somAgg).where(somePredicate) 
# display(df_sum.filter(df_sum.sum_precipitation > 10))
#display(df_sum)
# df_sum.count()

# COMMAND ----------

# DBTITLE 1,11. For Test - Remove it -Update/Rename Columns and Load into a Dataframe
# # sqlContext.createDataFrame(rdd, samplingRatio=0.2)
# df_updated_column = spark.createDataFrame(df_cleansed.rdd, samplingRatio=0.2)
# display(df_updated_column)

# #Update/rename Column
# df_cleansed = spark.sql(f"SELECT \
#                             case when dev.EQUNR = 'na' then '' else dev.EQUNR end as equipmentNumber, \
#                             case when dev.BIS = 'na' then to_date('1900-01-01','yyyy-MM-dd') else to_date(dev.BIS, 'yyyy-MM-dd') end as validToDate, \
#                             to_date(dev.AB, 'yyyy-MM-dd') as validFromDate, \
#                             dev.KOMBINAT as deviceCategoryCombination, \
#                             cast(dev.LOGIKNR as Long) as logicalDeviceNumber, \
#                             dev.ZWGRUPPE as registerGroupCode, \
#                             c.registerGroup, \
#                             to_date(dev.EINBDAT, 'yyyy-MM-dd') as installationDate, \
#                             to_date(dev.AUSBDAT, 'yyyy-MM-dd') as deviceRemovalDate, \
#                             dev.GERWECHS as activityReasonCode, \
#                             b.activityReason, \
#                             dev.DEVLOC as deviceLocation, \
#                             dev.WGRUPPE as windingGroup, \
#                             dev.LOEVM as deletedIndicator, \
#                             dev.UPDMOD as bwDeltaProcess, \
#                             cast(dev.AMCG_CAP_GRP as Integer) as advancedMeterCapabilityGroup, \
#                             cast(dev.MSG_ATTR_ID as Integer) as messageAttributeId, \
#                             dev.ZZMATNR as materialNumber, \
#                             dev.ZANLAGE as installationId, \
#                             dev.ZADDRNUMBER as addressNumber, \
#                             dev.ZCITY1 as cityName, \
#                             dev.ZHOUSE_NUM1 as houseNumber, \
#                             dev.ZSTREET as streetName, \
#                             dev.ZPOST_CODE1 as postalCode, \
#                             dev.ZTPLMA as superiorFunctionalLocationNumber, \
#                             dev.ZZ_POLICE_EVENT as policeEventNumber, \
#                             dev.ZAUFNR as orderNumber, \
#                             dev.ZERNAM as createdBy, \
#                             dev._RecordStart, \
#                             dev._RecordEnd, \
#                             dev._RecordDeleted, \
#                             dev._RecordCurrent \
#                         FROM {ADS_DATABASE_STAGE}.{source_object} dev \
#                             left outer join cleansed.isu_0UC_GERWECHS_TEXT b on dev.GERWECHS = b.activityReasonCode \
#                             left outer join cleansed.isu_0UC_REGGRP_TEXT c on dev.ZWGRUPPE = c.registerGroupCode")

# display(df_cleansed)
# print(f'Number of rows: {df_cleansed.count()}')

# COMMAND ----------

# newSchema = StructType([
#                           StructField('equipmentNumber',StringType(),False),
#                           StructField('validToDate',DateType(),False),
#                           StructField('validFromDate',DateType(),True),
#                           StructField('deviceCategoryCombination',StringType(),True),
#                           StructField('logicalDeviceNumber',LongType(),True),                                      
#                           StructField('registerGroupCode',StringType(),True),
#                           StructField('registerGroup',StringType(),True),
#                           StructField('installationDate',DateType(),True),
#                           StructField('deviceRemovalDate',DateType(),True),
#                           StructField('activityReasonCode',StringType(),True),
#                           StructField('activityReason',StringType(),True),
#                           StructField('deviceLocation',StringType(),True),
#                           StructField('windingGroup',StringType(),True),
#                           StructField('deletedIndicator',StringType(),True),
#                           StructField('bwDeltaProcess',StringType(),True),
#                           StructField('advancedMeterCapabilityGroup',IntegerType(),True),
#                           StructField('messageAttributeId',IntegerType(),True),
#                           StructField('materialNumber',StringType(),True),
#                           StructField('installationId',StringType(),True),
#                           StructField('addressNumber',StringType(),True),
#                           StructField('cityName',StringType(),True),
#                           StructField('houseNumber',StringType(),True),
#                           StructField('streetName',StringType(),True),
#                           StructField('postalCode',StringType(),True),
#                           StructField('superiorFunctionalLocationNumber',StringType(),True),
#                           StructField('policeEventNumber',StringType(),True),
#                           StructField('orderNumber',StringType(),True),
#                           StructField('createdBy',StringType(),True),
#                           StructField('_RecordStart',TimestampType(),False),
#                           StructField('_RecordEnd',TimestampType(),False),
#                           StructField('_RecordDeleted',IntegerType(),False),
#                           StructField('_RecordCurrent',IntegerType(),False)
#                         ])
                                      
# df_updated_column = spark.createDataFrame(df_cleansed.rdd, schema=newSchema)
# display(df_updated_column)

# COMMAND ----------

# DBTITLE 1,13. Exit Notebook
dbutils.notebook.exit("1")
