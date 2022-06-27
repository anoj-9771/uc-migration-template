# Databricks notebook source
# DBTITLE 1,Generate parameter and source object name for unit testing
import json
hydraTable = 'TLOTPARCEL'
businessKeys = 'systemKey'

runParm = '{"SourceType":"BLOB Storage (csv)","SourceServer":"daf-sa-lake-sastoken","SourceGroup":"hydradata","SourceName":"hydra_####","SourceLocation":"hydradata/####","AdditionalProperty":"","Processor":"databricks-token|1103-023442-me8nqcm9|Standard_DS3_v2|8.3.x-scala2.12|2:8|interactive","IsAuditTable":false,"SoftDeleteSource":"","ProjectName":"CLEANSED DATA HYDRA","ProjectId":2,"TargetType":"BLOB Storage (csv)","TargetName":"hydra_####","TargetLocation":"hydradata/####","TargetServer":"daf-sa-lake-sastoken","DataLoadMode":"TRUNCATE-LOAD","DeltaExtract":false,"CDCSource":false,"TruncateTarget":true,"UpsertTarget":false,"AppendTarget":false,"TrackChanges":false,"LoadToSqlEDW":true,"TaskName":"hydra_####","ControlStageId":2,"TaskId":40,"StageSequence":200,"StageName":"Raw to Cleansed","SourceId":40,"TargetId":40,"ObjectGrain":"Day","CommandTypeId":8,"Watermarks":"","WatermarksDT":null,"WatermarkColumn":"","BusinessKeyColumn":"yyyy","PartitionColumn":null,"UpdateMetaData":null,"SourceTimeStampFormat":"","Command":"/build/cleansed/HYDRA Data/####","LastLoadedFile":null,"LastSuccessfulExecutionTS":"2022-05-23T12:49:06"}'

s = json.loads(runParm)
for parm in ['SourceName','SourceLocation','TargetName','TargetLocation','TaskName','BusinessKeyColumn','Command']:
    s[parm] = s[parm].replace('####',hydraTable).replace('yyyy',businessKeys)
runParm = json.dumps(s)

# COMMAND ----------

print('Use the following as parameters for unit testing:')
print(f'hydra_{hydraTable.lower()}')
print(runParm)

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
df = spark.sql(f"WITH stage AS \
                          (Select *, ROW_NUMBER() OVER (PARTITION BY System_Key ORDER BY _DLRawZoneTimestamp DESC) AS _RecordVersion \
                          FROM {delta_raw_tbl_name} \
                          WHERE _DLRawZoneTimestamp >= '{LastSuccessfulExecutionTS}'), \
                      waterNetwork as \
                          (select level30 as deliverySystem, \
                                  level40 as distributionSystem, \
                                  level50 as supplyZone, \
                                  coalesce(level60,'n/a')  as pressureArea, \
                                  case when product = 'Water'  then 'Y' else 'N' end as isPotableWaterNetwork, \
                                  case when product = 'RecycledWater'  then 'Y' else 'N' end as isRecycledWaterNetwork \
                           from {ADS_DATABASE_CLEANSED}.hydra_TSYSTEMAREA \
                           where product in ('Water','RecycledWater') \
                           and   _RecordDeleted = 0 \
                           and   _RecordCurrent = 1), \
                       sewerNetwork as \
                           (select level30 as sewerNetwork, \
                                   level40 as sewerCatchment, \
                                   level50 as SCAMP \
                            from {ADS_DATABASE_CLEANSED}.hydra_TSYSTEMAREA \
                            where product = 'WasteWater' \
                            and   _RecordDeleted = 0 \
                            and   _RecordCurrent = 1), \
                       stormWaterNetwork as \
                           (select level30 as stormWaterNetwork, \
                                   level40 as stormWaterCatchment \
                            from {ADS_DATABASE_CLEANSED}.hydra_TSYSTEMAREA \
                            where product = 'StormWater' \
                            and   _RecordDeleted = 0 \
                            and   _RecordCurrent = 1) \
                       SELECT cast(System_Key as int) AS systemKey, \
                            cast(Property_Number as int) AS propertyNumber, \
                            case when LGA = 'N/A' then null else initcap(LGA) end as LGA, \
                            case when Address = ' ' then null else initcap(Address) end as propertyAddress, \
                            case when Suburb = 'N/A' then null else initcap(Suburb) end AS suburb, \
                            case when Land_Use = 'N/A' then null else initcap(Land_Use) end as landUse, \
                            case when Superior_Land_Use = 'N/A' then null else initcap(Superior_Land_Use) end as superiorLandUse, \
                            cast(Area_m2 as int) as areaSize, \
                            'm2' as areaSizeUnit, \
                            cast(Lon as dec(9,6)) as longitude, \
                            cast(Lat as dec(9,6)) as latitude, \
                            cast(MGA56_X as long) as x_coordinate_MGA56, \
                            cast(MGA56_Y as long) as y_coordinate_MGA56, \
                            case when Water_Pressure_Zone = 'N/A' then null else wnp.deliverySystem end as waterDeliverySystem, \
                            case when Water_Pressure_Zone = 'N/A' then null else wnp.distributionSystem end as waterDistributionSystem, \
                            case when Water_Pressure_Zone = 'N/A' then null else wnp.supplyZone end as waterSupplyZone, \
                            case when Water_Pressure_Zone = 'N/A' then null else wnp.pressureArea end as waterPressureArea, \
                            case when Sewer_SCAMP = 'N/A' then null else sn.sewerNetwork end as sewerNetwork, \
                            case when Sewer_SCAMP = 'N/A' then null else sn.sewerCatchment end as sewerCatchment, \
                            case when Sewer_SCAMP = 'N/A' then null else sn.SCAMP end as SCAMP, \
                            case when Recycled_Supply_Zone = 'N/A' then null else wnr.deliverySystem end as recycledDeliverySystem, \
                            case when Recycled_Supply_Zone = 'N/A' then null else wnr.distributionSystem end as recycledDistributionSystem, \
                            case when Recycled_Supply_Zone = 'N/A' then null else wnr.supplyZone end as recycledSupplyZone, \
                            case when Stormwater_Catchment = 'N/A' then null else swn.stormWaterNetwork end as stormWaterNetwork, \
                            case when Stormwater_Catchment = 'N/A' then null else swn.stormwaterCatchment end as stormWaterCatchment, \
                            cast('1900-01-01' as TimeStamp) as _RecordStart, \
                            cast('9999-12-31' as TimeStamp) as _RecordEnd, \
                            '0' as _RecordDeleted, \
                            '1' as _RecordCurrent, \
                            cast('{CurrentTimeStamp}' as TimeStamp) as _DLCleansedZoneTimeStamp \
                    from stage left outer join waterNetwork wnp on wnp.pressureArea = Water_Pressure_Zone \
                               left outer join waterNetwork wnr on wnr.supplyZone = Recycled_Supply_Zone \
                               left outer join sewerNetwork sn on sn.SCAMP = Sewer_SCAMP \
                               left outer join stormWaterNetwork swn on swn.stormWaterCatchment = Stormwater_Catchment \
                    where _RecordVersion = 1 ")

#print(f'Number of rows: {df.count()}')

# COMMAND ----------

newSchema = StructType([
	StructField('systemKey',IntegerType(),False),
    StructField('propertyNumber',IntegerType(),True),
    StructField('LGA',StringType(),True),
	StructField('propertyAddress',StringType(),True),
    StructField('suburb',StringType(),True),
    StructField('landUse',StringType(),True),
    StructField('superiorLandUse',StringType(),True),
    StructField('areaSize',IntegerType(),True),
    StructField('areaSizeUnit',StringType(),True),
    StructField('longitude',DecimalType(9,6),False),
    StructField('latitude',DecimalType(9,6),False),
    StructField('x_coordinate_MGA56',LongType(),False),
    StructField('y_coordinate_MGA56',LongType(),False),
    StructField('waterDeliverySystem',StringType(),True),
    StructField('waterDistributionSystem',StringType(),True),
    StructField('waterSupplyZone',StringType(),True),
    StructField('waterPressureArea',StringType(),True),
    StructField('sewerNetwork',StringType(),True),
    StructField('sewerCatchment',StringType(),True),
    StructField('SCAMP',StringType(),True),
    StructField('recycledDeliverySystem',StringType(),True),
    StructField('recycledDistributionSystem',StringType(),True),
    StructField('recycledSupplyZone',StringType(),True),
    StructField('stormWaterNetwork',StringType(),True),
    StructField('stormWaterCatchment',StringType(),True),
    StructField('_RecordStart',TimestampType(),False),
    StructField('_RecordEnd',TimestampType(),False),
    StructField('_RecordDeleted',IntegerType(),False),
    StructField('_RecordCurrent',IntegerType(),False)
])


# COMMAND ----------

# DBTITLE 1,12. Save Data frame into Cleansed Delta table (Final)
DeltaSaveDataFrameToDeltaTable(df, target_table, ADS_DATALAKE_ZONE_CLEANSED, ADS_DATABASE_CLEANSED, data_lake_folder, ADS_WRITE_MODE_MERGE, newSchema, track_changes, is_delta_extract, business_key, AddSKColumn = False, delta_column = "", start_counter = "0", end_counter = "0")

# COMMAND ----------

# DBTITLE 1,13. Exit Notebook
dbutils.notebook.exit("1")
