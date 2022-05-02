# Databricks notebook source
# DBTITLE 1,Generate parameter and source object name for unit testing
import json
accessTable = 'Z309_THPROPMETER'
businessKeys = 'N_PROP,D_SUPD,T_TIME_SUPD'

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
	cast(tbl.N_PROP as int) as propertyNumber, \
	cast(tbl.N_PROP_METE as int) as propertyMeterNumber, \
	cast(tbl.N_METE_READ_ROUT as int) as meterReadingRouteNumber, \
	tbl.C_METE_TYPE as meterSizeCode, \
    case when ref1.meterSizeUnit = 'mm' then \
            cast(cast(ref1.meterSize as int) as string)||' '||lower(ref1.meterSizeUnit) \
            else cast(cast(ref1.meterSize as decimal(5,2)) as string)||' '||lower(ref1.meterSizeUnit) end as meterSize, \
	tbl.C_METE_READ_FREQ as meterReadingFrequencyCode, \
	ref2.meterReadingFrequency as meterReadingFrequency, \
	tbl.C_METE_GRID_LOCA as meterGridLocationCode, \
	ref3.meterGridLocation as meterGridLocation, \
	tbl.C_READ_INST_NUM1 as meterReadingInstructionCode1, \
	ref4.meterReadingInstruction as meterReadingInstruction1, \
	tbl.C_READ_INST_NUM2 as meterReadingInstructionCode2, \
	ref5.meterReadingInstruction as meterReadingInstruction2, \
	case when F_METE_ADDI_DESC = '1' then true else false end AS hasAdditionalDescription, \
    case when F_METE_ROUT_PREP = '1' then true else false end AS hasMeterRoutePreparation, \
	case when F_METE_WARN_NOTE = '1' then true else false end AS hasMeterWarningNote, \
	ToValidDate(tbl.D_METE_FIT) as meterFittedDate, \
	cast(tbl.N_METE_READ_SEQU as int) as meterReadingSequenceNumber, \
	case when C_METE_POSI_STAT = 'M' then true else false end as isMasterMeter, \
    case when C_METE_POSI_STAT = 'C' then true else false end as isCheckMeter, \
    case when C_METE_POSI_STAT = 'A' then true else false end as allowAlso, \
	tbl.C_METE_CHAN_REAS as meterChangeReasonCode, \
	ref6.meterExchangeReason as meterExchangeReason, \
	tbl.N_METE_CHAN_ADVI as meterChangeAdviceNumber, \
	ToValidDate(tbl.D_METE_REMO) as meterRemovedDate, \
	tbl.F_METE_CONN as isMeterConnected, \
	tbl.N_METE_MAKE as meterMakerNumber, \
	ToValidDate(tbl.D_PROP_METE_UPDA) as propertyMeterUpdatedDate, \
	tbl.C_METE_CLAS as meterClassCode, \
	ref7.meterClass as meterClass, \
	tbl.C_METE_CATE as meterCategoryCode, \
	ref8.meterCategory as meterCategory, \
	tbl.C_METE_GROU as meterGroupCode, \
	ref9.meterGroup as meterGroup, \
	ToValidDate(tbl.D_SUPD) as rowSupersededDate, \
	tbl.T_TIME_SUPD as rowSupersededTime, \
	tbl.M_PROC as modifiedByProcess, \
	tbl.C_USER_ID as modifiedByUserID, \
	tbl.C_TERM_ID as modifiedByTerminalID, \
	tbl.F_ADJU as rowAdjusted, \
	tbl.C_METE_READ_LOCA as meterReadingLocationCode, \
	ref10.meterGridLocation as meterReadingLocation, \
	tbl.T_METE_SERV as meterServes, \
	tbl._RecordStart, \
	tbl._RecordEnd, \
	tbl._RecordDeleted, \
	tbl._RecordCurrent \
	FROM {ADS_DATABASE_STAGE}.{source_object} tbl \
left outer join cleansed.access_Z309_TMETERTYPE ref1 on tbl.C_METE_TYPE = ref1.meterTypeCode \
left outer join cleansed.access_Z309_TMETEREADFREQ ref2 on tbl.C_METE_READ_FREQ = ref2.meterReadingFrequencyCode \
left outer join cleansed.access_Z309_TMETERGRIDLOCA ref3 on tbl.C_METE_GRID_LOCA = ref3.meterGridLocationCode \
left outer join cleansed.access_Z309_TMETEREADINST ref4 on tbl.C_READ_INST_NUM1 = ref4.meterReadingInstructionCode \
left outer join cleansed.access_Z309_TMETEREADINST ref5 on tbl.C_READ_INST_NUM2 = ref5.meterReadingInstructionCode \
left outer join cleansed.access_Z309_TMETERCHANGEREAS ref6 on tbl.C_METE_CHAN_REAS = ref6.meterExchangeReasonCode \
left outer join cleansed.access_Z309_TMETERCLASS ref7 on tbl.C_METE_CLAS = ref7.meterClassCode \
left outer join cleansed.access_Z309_TMETERCATEGORY ref8 on tbl.C_METE_CATE = ref8.meterCategoryCode \
left outer join cleansed.access_Z309_TMETERGROUP ref9 on tbl.C_METE_GROU = ref9.meterGroupCode \
left outer join cleansed.access_Z309_TMETERGRIDLOCA ref10 on tbl.C_METE_READ_LOCA = ref10.meterGridLocationCode \
                                ")

# COMMAND ----------

newSchema = StructType([
	StructField('propertyNumber',IntegerType(),False),
	StructField('propertyMeterNumber',IntegerType(),False),
	StructField('meterReadingRouteNumber',IntegerType(),False),
	StructField('meterSizeCode',StringType(),True),
	StructField('meterSize',StringType(),True),
	StructField('meterReadingFrequencyCode',StringType(),True),
	StructField('meterReadingFrequency',StringType(),True),
	StructField('meterGridLocationCode',StringType(),True),
	StructField('meterGridLocation',StringType(),True),
	StructField('meterReadingInstructionCode1',StringType(),True),
	StructField('meterReadingInstruction1',StringType(),True),
	StructField('meterReadingInstructionCode2',StringType(),True),
	StructField('meterReadingInstruction2',StringType(),True),
	StructField('hasAdditionalDescription',BooleanType(),False),
	StructField('hasMeterRoutePreparation',BooleanType(),False),
	StructField('hasMeterWarningNote',BooleanType(),False),
	StructField('meterFittedDate',DateType(),True),
	StructField('meterReadingSequenceNumber',IntegerType(),False),
	StructField('isMasterMeter',BooleanType(),False),
	StructField('isCheckMeter',BooleanType(),False),
    StructField('allowAlso',BooleanType(),False),
	StructField('meterChangeReasonCode',StringType(),True),
	StructField('meterExchangeReason',StringType(),True),
	StructField('meterChangeAdviceNumber',StringType(),True),
	StructField('meterRemovedDate',DateType(),True),
	StructField('isMeterConnected',StringType(),True),
	StructField('meterMakerNumber',StringType(),True),
	StructField('propertyMeterUpdatedDate',DateType(),True),
	StructField('meterClassCode',StringType(),True),
	StructField('meterClass',StringType(),True),
	StructField('meterCategoryCode',StringType(),True),
	StructField('meterCategory',StringType(),True),
	StructField('meterGroupCode',StringType(),True),
	StructField('meterGroup',StringType(),True),
	StructField('rowSupersededDate',DateType(),True),
	StructField('rowSupersededTime',StringType(),True),
	StructField('modifiedByProcess',StringType(),True),
	StructField('modifiedByUserID',StringType(),True),
	StructField('modifiedByTerminalID',StringType(),True),
	StructField('rowAdjusted',StringType(),True),
	StructField('meterReadingLocationCode',StringType(),True),
	StructField('meterReadingLocation',StringType(),True),
	StructField('meterServes',StringType(),True),
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

# MAGIC %sql
# MAGIC select * from cleansed.access_Z309_thpropmeter

# COMMAND ----------

# DBTITLE 1,13. Exit Notebook
dbutils.notebook.exit("1")
