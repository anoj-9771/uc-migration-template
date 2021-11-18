# Databricks notebook source
# DBTITLE 1,Generate parameter and source object name for unit testing
import json
#For unit testing...
#Use this string in the Param widget: 
#{"SourceType": "BLOB Storage (json)", "SourceServer": "daf-sa-lake-sastoken", "SourceGroup": "isu", "SourceName": "isu_0UC_REGIST_ATTR", "SourceLocation": "isu/0UC_REGIST_ATTR", "AdditionalProperty": "", "Processor": "databricks-token|0711-011053-turfs581|Standard_DS3_v2|8.3.x-scala2.12|2:8|interactive", "IsAuditTable": false, "SoftDeleteSource": "", "ProjectName": "ISU DATA", "ProjectId": 2, "TargetType": "BLOB Storage (json)", "TargetName": "isu_0UC_REGIST_ATTR", "TargetLocation": "isu/0UC_REGIST_ATTR", "TargetServer": "daf-sa-lake-sastoken", "DataLoadMode": "FULL-EXTRACT", "DeltaExtract": false, "CDCSource": false, "TruncateTarget": false, "UpsertTarget": true, "AppendTarget": null, "TrackChanges": false, "LoadToSqlEDW": true, "TaskName": "isu_0UC_REGIST_ATTR", "ControlStageId": 2, "TaskId": 46, "StageSequence": 200, "StageName": "Raw to Cleansed", "SourceId": 46, "TargetId": 46, "ObjectGrain": "Day", "CommandTypeId": 8, "Watermarks": "", "WatermarksDT": null, "WatermarkColumn": "", "BusinessKeyColumn": "equipmentNumber,registerNumber,validToDate", "UpdateMetaData": null, "SourceTimeStampFormat": "", "Command": "", "LastLoadedFile": null}

#Use this string in the Source Object widget
#isu_0UC_REGIST_ATTR

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

df_cleansed = spark.sql(f"SELECT \
	case when EQUNR = 'na' then '' else EQUNR end as equipmentNumber, \
	case when ZWNUMMER = 'na' then '' else (cast(ZWNUMMER as int)) end  as registerNumber, \
	case when BIS = 'na' then to_date('1900-01-01','yyyy-MM-dd') else to_date(BIS, 'yyyy-MM-dd') end as validToDate, \
	to_date(AB, 'yyyy-MM-dd') as validFromDate, \
	LOGIKZW as logicalRegisterNumber, \
	SPARTYP as divisionCategoryCode, \
    di.sectorCategory as divisionCategory, \
	ZWKENN as registerIdCode, \
    id.registerId as registerId, \
	ZWART as registerTypeCode, \
	te.registerType as registerType, \
	ZWTYP as registerCategoryCode, \
    dd.domainValueText as registerCategory, \
	BLIWIRK as reactiveApparentOrActiveRegister, \
	MASSREAD as unitOfMeasurementMeterReading, \
	NABLESEN as doNotReadIndicator, \
	cast(HOEKORR as int) as altitudeCorrectionPressure, \
	cast(KZAHLE as int) as setGasLawDeviationFactor, \
	cast(KZAHLT as int) as actualGasLawDeviationFactor, \
	cast(CRGPRESS as int) as gasCorrectionPressure, \
	INTSIZEID as intervalLengthId, \
	LOEVM as deletedIndicator, \
	re._RecordStart, \
	re._RecordEnd, \
	re._RecordDeleted, \
	re._RecordCurrent \
	FROM {ADS_DATABASE_STAGE}.{source_object} re \
    LEFT OUTER JOIN {ADS_DATABASE_CLEANSED}.isu_0UCDIVISCAT_TEXT di ON re.SPARTYP = di.sectorCategoryCode \
                                                                      and di._RecordDeleted = 0 and di._RecordCurrent = 1 \
    LEFT OUTER JOIN {ADS_DATABASE_CLEANSED}.isu_TE065T id ON re.SPARTYP = id.divisionCategoryCode and re.ZWKENN = id.registerIdCode \
                                                                      and id._RecordDeleted = 0 and id._RecordCurrent = 1 \
    LEFT OUTER JOIN {ADS_DATABASE_CLEANSED}.isu_TE523T te ON re.ZWART = te.registerTypeCode \
                                                                      and te._RecordDeleted = 0 and te._RecordCurrent = 1 \
    LEFT OUTER JOIN {ADS_DATABASE_CLEANSED}.isu_DD07T dd ON re.ZWTYP = dd.domainValueSingleUpperLimit and  dd.domainName = 'L_ZWTYP' \
                                                                      and dd._RecordDeleted = 0 and dd._RecordCurrent = 1")


display(df_cleansed)
print(f'Number of rows: {df_cleansed.count()}')

# COMMAND ----------

newSchema = StructType([
	StructField('equipmentNumber',StringType(),False),
	StructField('registerNumber',StringType(),False),
	StructField('validToDate',DateType(),False),
	StructField('validFromDate',DateType(),True),
	StructField('logicalRegisterNumber',StringType(),True),
	StructField('divisionCategoryCode',StringType(),True),
    StructField('divisionCategory',StringType(),True),
	StructField('registerIdCode',StringType(),True),
    StructField('registerId',StringType(),True),
	StructField('registerTypeCode',StringType(),True),
	StructField('registerType',StringType(),True),
	StructField('registerCategoryCode',StringType(),True),
    StructField('registerCategory',StringType(),True),
	StructField('reactiveApparentOrActiveRegister',StringType(),True),
	StructField('unitOfMeasurementMeterReading',StringType(),True),
	StructField('doNotReadIndicator',StringType(),True),
	StructField('altitudeCorrectionPressure',IntegerType(),True),
	StructField('setGasLawDeviationFactor',IntegerType(),True),
	StructField('actualGasLawDeviationFactor',IntegerType(),True),
	StructField('gasCorrectionPressure',IntegerType(),True),
	StructField('intervalLengthId',StringType(),True),
	StructField('deletedIndicator',StringType(),True),
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
DeltaSaveDataframeDirect(df_updated_column, source_group, target_table, ADS_DATABASE_CLEANSED, ADS_CONTAINER_CLEANSED, "overwrite", "")

# COMMAND ----------

# DBTITLE 1,13. Exit Notebook
dbutils.notebook.exit("1")
