# Databricks notebook source
# DBTITLE 1,Generate parameter and source object name for unit testing
import json
accessTable = 'Z309_TPROPMETER' 

runParm = '{"SourceType":"Flat File","SourceServer":"saswcnonprod01landingdev-sastoken","SourceGroup":"access","SourceName":"access_access/####_csv","SourceLocation":"access/####.csv","AdditionalProperty":"","Processor":"databricks-token|0705-044124-gored835|Standard_DS3_v2|8.3.x-scala2.12|2:8|interactive","IsAuditTable":false,"SoftDeleteSource":"","ProjectName":"Access Data","ProjectId":2,"TargetType":"BLOB Storage (csv)","TargetName":"access_access/####_csv","TargetLocation":"access/####","TargetServer":"daf-sa-lake-sastoken","DataLoadMode":"TRUNCATE-LOAD","DeltaExtract":false,"CDCSource":false,"TruncateTarget":true,"UpsertTarget":false,"AppendTarget":null,"TrackChanges":false,"LoadToSqlEDW":true,"TaskName":"access_access/####_csv","ControlStageId":1,"TaskId":4,"StageSequence":100,"StageName":"Source to Raw","SourceId":4,"TargetId":4,"ObjectGrain":"Day","CommandTypeId":5,"Watermarks":"","WatermarksDT":null,"WatermarkColumn":"","BusinessKeyColumn":"","UpdateMetaData":null,"SourceTimeStampFormat":"","Command":"","LastLoadedFile":null}'

s = json.loads(runParm)
for parm in ['SourceName','SourceLocation','TargetName','TargetLocation','TaskName']:
    s[parm] = s[parm].replace('####',accessTable)
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
# MAGIC %run ../includes/include-all-util

# COMMAND ----------

# DBTITLE 1,7. Include User functions (CleansedZone) for the notebook
# MAGIC %run ./utility/transform_data_cleansedzone

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
df_cleansed = spark.sql("SELECT cast(N_PROP as int) AS propertyNumber, \
		cast(N_PROP_METE as int) AS propertyMeterNumber, \
		N_METE_MAKE AS meterMakerNumber, \
		C_METE_TYPE AS meterSizeCode, \
        cast(b.meterSize as string)||lower(b.meterSizeUnit) as meterSize, \
		case when C_METE_POSI_STAT = 'M' then true else false end as isMasterMeter, \
        case when C_METE_POSI_STAT = 'C' then true else false end as isCheckMeter, \
        case when F_METE_CONN = 'D' then false else true end AS isMeterConnected, \
		C_METE_READ_FREQ AS meterReadingFrequencyCode, \
		C_METE_CLAS AS meterClassCode, \
        initcap(c.meterClass) as meterClass, \
        c.waterMeterType, \
        C_METE_CATE AS meterCategoryCode, \
        initcap(d.meterCategory) as meterCategory, \
		C_METE_GROU AS meterGroupCode, \
        replace(initcap(e.meterGroup),'ami','AMI') as meterGroup, \
		C_METE_READ_LOCA AS meterReadingLocationCode, \
		cast(N_METE_READ_ROUT as int) AS meterReadingRouteNumber, \
		initcap(T_METE_SERV) AS meterServes, \
		C_METE_GRID_LOCA AS meterGridLocationCode, \
		C_READ_INST_NUM1 AS readingInstructionCode1, \
		C_READ_INST_NUM2 AS readingInstructionCode2, \
		case when F_METE_ADDI_DESC = 'Y' then true else false end AS hasdditionalDescription, \
		case when F_METE_ROUT_PREP = 'Y' then true else false end AS hasMeterRoutePreparation, \
		case when F_METE_WARN_NOTE = 'Y' then true else false end AS hasMeterWarningNote, \
		to_date(D_METE_FIT, 'yyyyMMdd') AS meterFittedDate, \
		cast(N_METE_READ_SEQU as int) AS meterReadingSequenceNumber, \
		C_METE_CHAN_REAS AS meterChangeReasonCode, \
		N_METE_CHAN_ADVI AS meterChangeAdviceNumber, \
		to_date(D_METE_REMO, 'yyyyMMdd') AS meterRemovedDate, \
		to_date(D_PROP_METE_UPDA, 'yyyyMMdd') AS propertyMeterUpdatedDate, \
		a._RecordStart, \
		a._RecordEnd, \
		a._RecordDeleted, \
		a._RecordCurrent \
	FROM CLEANSED.STG_ACCESS_Z309_TPROPMETER a \
         left outer join CLEANSED.t_access_Z309_TMeterType b on b.meterTypeCode = a.C_METE_TYPE \
         left outer join CLEANSED.t_access_Z309_TMeterClass c on c.meterClassCode = a.C_METE_CLAS \
         left outer join CLEANSED.t_access_Z309_TMeterCategory d on d.meterCategoryCode = a.C_METE_CATE \
         left outer join CLEANSED.t_access_Z309_TMeterGroup e on e.meterGroupCode = a.C_METE_GROU \
         ")

display(df_cleansed)
print(f'Number of rows: {df_cleansed.count()}')

# COMMAND ----------

newSchema = StructType([
	StructField('propertyNumber',IntegerType(),False),
	StructField('propertyMeterNumber',IntegerType(),False),
	StructField('meterMakerNumber',StringType(),True),
    StructField('meterSizeCode',StringType(),False),
	StructField('meterSize',StringType(),False),
    StructField('isMasterMeter',BooleanType(),False),
	StructField('isCheckMeter',BooleanType(),False),
	StructField('isMeterConnected',BooleanType(),False),
	StructField('meterReadingFrequencyCode',StringType(),True),
	StructField('meterClassCode',StringType(),True),
	StructField('meterClass',StringType(),True),
    StructField('waterMeterType',StringType(),True),
	StructField('meterCategoryCode',StringType(),True),
	StructField('meterCategory',StringType(),True),
	StructField('meterGroupCode',StringType(),True),
	StructField('meterGroup',StringType(),True),
	StructField('meterReadingLocationCode',StringType(),True),
	StructField('meterReadingRouteNumber',IntegerType(),True),
	StructField('meterServes',StringType(),True),
	StructField('meterGridLocationCode',StringType(),True),
	StructField('readingInstructionCode1',StringType(),True),
	StructField('readingInstructionCode2',StringType(),True),
	StructField('hasAdditionalDescription',BooleanType(),False),
	StructField('hasMeterRoutePreparation',BooleanType(),False),
	StructField('hasMeterWarningNote',BooleanType(),False),
	StructField('meterFittedDate',DateType(),True),
	StructField('meterReadingSequenceNumber',IntegerType(),False),
	StructField('meterChangeReasonCode',StringType(),True),
	StructField('meterChangeAdviceNumber',StringType(),True),
	StructField('meterRemovedDate',DateType(),True),
	StructField('propertyMeterUpdatedDate',DateType(),True),
	StructField('_RecordStart',TimestampType(),False),
	StructField('_RecordEnd',TimestampType(),False),
	StructField('_RecordDeleted',IntegerType(),False),
	StructField('_RecordCurrent',IntegerType(),False)
])

df_updated_column = spark.createDataFrame(df_cleansed.rdd, schema=newSchema)
display(df_updated_column)
print(f'Number of rows: {df_updated_column.count()}')

# COMMAND ----------

# DBTITLE 1,12. Save Data frame into Cleansed Delta table (Final)
#Save Data frame into Cleansed Delta table (final)
DeltaSaveDataframeDirect(df_updated_column, "t", source_object, ADS_DATABASE_CLEANSED, ADS_CONTAINER_CLEANSED, "overwrite", "")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cleansed.t_access_z309_tpropmeter where waterMeterType is null

# COMMAND ----------

# DBTITLE 1,13. Exit Notebook
dbutils.notebook.exit("1")
