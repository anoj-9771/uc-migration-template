# Databricks notebook source
# DBTITLE 1,Generate parameter and source object name for unit testing
import json
hydraTable = 'TLOTPARCEL'

runParm = '{"SourceType":"Flat File","SourceServer":"saswcnonprod01landingdev-sastoken","SourceGroup":"hydra","SourceName":"hydra_hydra/####_csv","SourceLocation":"hydra/####.csv","AdditionalProperty":"","Processor":"databricks-token|0705-044124-gored835|Standard_DS3_v2|8.3.x-scala2.12|2:8|interactive","IsAuditTable":false,"SoftDeleteSource":"","ProjectName":"hydra Data","ProjectId":2,"TargetType":"BLOB Storage (csv)","TargetName":"hydra_hydra/####_csv","TargetLocation":"hydra/####","TargetServer":"daf-sa-lake-sastoken","DataLoadMode":"TRUNCATE-LOAD","DeltaExtract":false,"CDCSource":false,"TruncateTarget":true,"UpsertTarget":false,"AppendTarget":null,"TrackChanges":false,"LoadToSqlEDW":true,"TaskName":"hydra_hydra/####_csv","ControlStageId":1,"TaskId":4,"StageSequence":100,"StageName":"Source to Raw","SourceId":4,"TargetId":4,"ObjectGrain":"Day","CommandTypeId":5,"Watermarks":"","WatermarksDT":null,"WatermarkColumn":"","BusinessKeyColumn":"","UpdateMetaData":null,"SourceTimeStampFormat":"","Command":"","LastLoadedFile":null}'

s = json.loads(runParm)
for parm in ['SourceName','SourceLocation','TargetName','TargetLocation','TaskName']:
    s[parm] = s[parm].replace('####',hydraTable)
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
df_cleansed = spark.sql("SELECT cast(System_Key as int) AS systemKey, \
        cast(Property_Number as int) AS propertyNumber, \
		case when LGA = 'N/A' then null else initcap(LGA) end as LGA, \
		case when Address = ' ' then null else " + 
        ("initcap(Address) " if ADS_ENVIRONMENT not in ['dev','test'] else "'1 Mumble St, Somewhere NSW 2000'") + " end as propertyAddress, \
		case when Suburb = 'N/A' then null else initcap(Suburb) end AS suburb, \
        case when Land_Use = 'N/A' then null else initcap(Land_Use) end as landUse, \
        case when Superior_Land_Use = 'N/A' then null else initcap(Superior_Land_Use) end as superiorLandUse, \
        cast(Area_m2 as int) as areaSize, \
        'm2' as areaSizeUnit, " +
        ("cast(Lon as float) as longitude, cast(Lat as float) as latitude, cast(MGA56_X as float) as x_coordinate_MGA56, cast(MGA56_Y as float) as y_coordinate_MGA56, " if ADS_ENVIRONMENT not in ['dev','test'] else "\
          cast(Lon as float)+17 as longitude, cast(Lat as float)+23 as latitude, cast(MGA56_X as float)+11235 as x_coordinate_MGA56, cast(MGA56_Y as float)+33245 as y_coordinate_MGA56, ") + "\
		case when Water_Delivery_System = 'N/A' then null else Water_Delivery_System end as waterDeliverySystem, \
		case when Water_Distribution_System = 'N/A' then null else Water_Distribution_System end as waterDistributionSystem, \
		case when Water_Supply_Zone = 'N/A' then null else Water_Supply_Zone end as waterSupplyZone, \
        case when Water_Pressure_Zone = 'N/A' then null else Water_Pressure_Zone end as waterPressureZone, \
        case when Sewer_Network = 'N/A' then null else Sewer_Network end as sewerNetwork, \
        case when Sewer_Catchment = 'N/A' then null else Sewer_Catchment end as sewerCatchment, \
        case when Sewer_SCAMP = 'N/A' then null else Sewer_SCAMP end as sewerScamp, \
        case when Recycled_Delivery_System = 'N/A' then null else Recycled_Delivery_System end as recycledDeliverySystem, \
        case when Recycled_Distribution_System = 'N/A' then null else Recycled_Distribution_System end as recycledDistributionSystem, \
        case when Recycled_Supply_Zone = 'N/A' then null else Recycled_Supply_Zone end as recycledSupplyZone, \
        case when Stormwater_Catchment = 'N/A' then null else Stormwater_Catchment end as stormwaterCatchment, \
		_RecordStart, \
		_RecordEnd, \
		_RecordDeleted, \
		_RecordCurrent \
	FROM CLEANSED.STG_HYDRA_TLOTPARCEL")

display(df_cleansed)

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
    StructField('longitude',FloatType(),False),
    StructField('latitude',FloatType(),False),
    StructField('x_coordinate_MGA56',FloatType(),False),
    StructField('y_coordinate_MGA56',FloatType(),False),
    StructField('waterDeliverySystem',StringType(),True),
    StructField('waterDistributionSystem',StringType(),True),
    StructField('waterSupplyZone',StringType(),True),
    StructField('waterPressureZone',StringType(),True),
    StructField('sewerNetwork',StringType(),True),
    StructField('sewerCatchment',StringType(),True),
    StructField('sewerScamp',StringType(),True),
    StructField('recycledDeliverySystem',StringType(),True),
    StructField('recycledDistributionSystem',StringType(),True),
    StructField('recycledSupplyZone',StringType(),True),
    StructField('stormwaterCatchment',StringType(),True),
    StructField('_RecordStart',TimestampType(),False),
    StructField('_RecordEnd',TimestampType(),False),
    StructField('_RecordDeleted',IntegerType(),False),
    StructField('_RecordCurrent',IntegerType(),False)
])

df_updated_column = spark.createDataFrame(df_cleansed.rdd, schema=newSchema)
display(df_updated_column)

# COMMAND ----------

# DBTITLE 1,12. Save Data frame into Cleansed Delta table (Final)
#Save Data frame into Cleansed Delta table (final)
DeltaSaveDataframeDirect(df_updated_column, "t", source_object, ADS_DATABASE_CLEANSED, ADS_CONTAINER_CLEANSED, "overwrite", "")

# COMMAND ----------

# DBTITLE 1,13. Exit Notebook
dbutils.notebook.exit("1")

# COMMAND ----------


