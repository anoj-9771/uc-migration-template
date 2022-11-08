# Databricks notebook source
# DBTITLE 1,Generate parameter and source object name for unit testing
import json
accessTable = 'Z309_THPROPERTYADDRESS'
businessKeys = 'N_PROP'

runParm = '{"SourceType":"BLOB Storage (csv)","SourceServer":"daf-sa-lake-sastoken","SourceGroup":"accessdata","SourceName":"access_####","SourceLocation":"accessdata/####","AdditionalProperty":"","Processor":"databricks-token|1103-023442-me8nqcm9|Standard_DS3_v2|8.3.x-scala2.12|2:8|interactive","IsAuditTable":false,"SoftDeleteSource":"","ProjectName":"CLEANSED DATA ACCESS","ProjectId":2,"TargetType":"BLOB Storage (csv)","TargetName":"access_####","TargetLocation":"accessdata/####","TargetServer":"daf-sa-lake-sastoken","DataLoadMode":"TRUNCATE-LOAD","DeltaExtract":false,"CDCSource":false,"TruncateTarget":true,"UpsertTarget":false,"AppendTarget":false,"TrackChanges":false,"LoadToSqlEDW":true,"TaskName":"access_####","ControlStageId":2,"TaskId":40,"StageSequence":200,"StageName":"Raw to Cleansed","SourceId":40,"TargetId":40,"ObjectGrain":"Day","CommandTypeId":8,"Watermarks":"","WatermarksDT":null,"WatermarkColumn":"","BusinessKeyColumn":"yyyy","PartitionColumn":null,"UpdateMetaData":null,"SourceTimeStampFormat":"","Command":"/build/cleansed/accessdata/####","LastLoadedFile":null}'

s = json.loads(runParm)
for parm in ['SourceName','SourceLocation','TargetName','TargetLocation','TaskName','BusinessKeyColumn','Command']:
    s[parm] = s[parm].replace('####',accessTable).replace('yyyy',businessKeys)

runParm = json.dumps(s)

# COMMAND ----------

print('Use the following as parameters for unit testing:')
print(f'access_{accessTable.lower()}')
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
dbutils.widgets.text("source_object", f'access_{accessTable.lower()}', "Source Object")
dbutils.widgets.text("start_counter", "", "Start Counter")
dbutils.widgets.text("end_counter", "", "End Counter")
dbutils.widgets.text("delta_column", "", "Delta Column")
dbutils.widgets.text("source_param", runParm, "Param")


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
delta_cleansed_tbl_name = f'{ADS_DATABASE_CLEANSED}.{target_table}'
delta_raw_tbl_name = f'{ADS_DATABASE_RAW}.{ source_object}'

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
df_cleansed = spark.sql(f"SELECT \
	tbl.C_LGA as LGACode, \
	ref1.LGA as LGA, \
	cast(tbl.N_PROP as int) as propertyNumber, \
	tbl.C_STRE_GUID as streetGuideCode, \
	tbl.C_DPID as DPID, \
	tbl.C_FLOR_LVL as floorLevelType, \
	ref2.itemNameAbbreviation as itemNameAbbreviation, \
	tbl.N_FLOR_LVL as floorLevelNumber, \
	tbl.C_FLAT_UNIT as flatUnitType, \
	ref3.itemNameAbbreviation as itemNameAbbreviation, \
	tbl.N_FLAT_UNIT as flatUnitNumber, \
	cast(tbl.N_HOUS_1 as int) as houseNumber1, \
	tbl.T_HOUS_1_SUFX as houseNumber1Suffix, \
	cast(tbl.N_HOUS_2 as int) as houseNumber2, \
	tbl.T_HOUS_2_SUFX as houseNumber2Suffix, \
	tbl.N_LOT as lotNumber, \
	tbl.N_RMB as roadSideMailbox, \
	tbl.T_OTHE_ADDR_INFO as otherAddressInformation, \
	tbl.T_SPEC_DESC as specialDescription, \
	tbl.M_BUIL_1 as buildingName1, \
	tbl.M_BUIL_2 as buildingName2, \
	tbl.C_USER_CREA as createdByUserID, \
	tbl.C_PLAN_CREA as createdByPlan, \
	ToValidDateTime(tbl.H_CREA) as createdTimestamp, \
	tbl.C_MODI_TYPE as modificationType, \
	tbl.C_USER_MODI as modifiedByUserID, \
	tbl.C_PLAN_MODI as modifiedByPlan, \
	ToValidDateTime(tbl.H_MODI) as modifiedTimestamp, \
	tbl._RecordStart, \
	tbl._RecordEnd, \
	tbl._RecordDeleted, \
	tbl._RecordCurrent \
	FROM {ADS_DATABASE_STAGE}.{source_object} tbl \
left outer join cleansed.access_Z309_TLOCALGOVT ref1 on tbl.C_LGA = ref1.LGA \
left outer join cleansed.access_Z309_TDPIDCODE ref2 on tbl.C_FLOR_LVL = ref2.itemNameAbbreviation \
left outer join cleansed.access_Z309_TDPIDCODE ref3 on tbl.C_FLAT_UNIT = ref3.itemNameAbbreviation \
                                ")

# COMMAND ----------

newSchema = StructType([
	StructField('LGACode',StringType(),True),
	StructField('LGA',StringType(),True),
	StructField('propertyNumber',IntegerType(),False),
	StructField('streetGuideCode',StringType(),True),
	StructField('DPID',StringType(),True),
	StructField('floorLevelType',StringType(),True),
	StructField('itemNameAbbreviation',StringType(),True),
	StructField('floorLevelNumber',StringType(),True),
	StructField('flatUnitType',StringType(),True),
	StructField('itemNameAbbreviation',StringType(),True),
	StructField('flatUnitNumber',StringType(),True),
	StructField('houseNumber1',IntegerType(),False),
	StructField('houseNumber1Suffix',StringType(),True),
	StructField('houseNumber2',IntegerType(),False),
	StructField('houseNumber2Suffix',StringType(),True),
	StructField('lotNumber',StringType(),True),
	StructField('roadSideMailbox',StringType(),True),
	StructField('otherAddressInformation',StringType(),True),
	StructField('specialDescription',StringType(),True),
	StructField('buildingName1',StringType(),True),
	StructField('buildingName2',StringType(),True),
	StructField('createdByUserID',StringType(),True),
	StructField('createdByPlan',StringType(),True),
	StructField('createdTimestamp',TimestampType(),True),
	StructField('modificationType',StringType(),True),
	StructField('modifiedByUserID',StringType(),True),
	StructField('modifiedByPlan',StringType(),True),
	StructField('modifiedTimestamp',TimestampType(),True),
	StructField('_RecordStart',TimestampType(),False),
	StructField('_RecordEnd',TimestampType(),False),
	StructField('_RecordDeleted',IntegerType(),False),
	StructField('_RecordCurrent',IntegerType(),False)
])

# COMMAND ----------

# DBTITLE 1,12. Save Data frame into Cleansed Delta table (Final)
#Save Data frame into Cleansed Delta table (final)
DeltaSaveDataframeDirect(df_cleansed, source_group, target_table, ADS_DATABASE_CLEANSED, ADS_CONTAINER_CLEANSED, "overwrite", newSchema, "")

# COMMAND ----------

# DBTITLE 1,13. Exit Notebook
dbutils.notebook.exit("1")