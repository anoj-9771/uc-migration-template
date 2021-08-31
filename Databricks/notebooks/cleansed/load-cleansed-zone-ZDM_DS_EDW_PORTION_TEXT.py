# Databricks notebook source
# DBTITLE 1,Generate parameter and source object name for unit testing
import json
#For unit testing...
#Use this string in the Param widget: 
#{"SourceType": "BLOB Storage (json)", "SourceServer": "daf-sa-lake-sastoken", "SourceGroup": "sapisu", "SourceName": "sapisu_ZDM_DS_EDW_PORTION_TEXT", "SourceLocation": "sapisu/ZDM_DS_EDW_PORTION_TEXT", "AdditionalProperty": "", "Processor": "databricks-token|0711-011053-turfs581|Standard_DS3_v2|8.3.x-scala2.12|2:8|interactive", "IsAuditTable": false, "SoftDeleteSource": "", "ProjectName": "SAP REF", "ProjectId": 2, "TargetType": "BLOB Storage (json)", "TargetName": "sapisu_ZDM_DS_EDW_PORTION_TEXT", "TargetLocation": "sapisu/ZDM_DS_EDW_PORTION_TEXT", "TargetServer": "daf-sa-lake-sastoken", "DataLoadMode": "FULL-EXTRACT", "DeltaExtract": false, "CDCSource": false, "TruncateTarget": false, "UpsertTarget": true, "AppendTarget": null, "TrackChanges": false, "LoadToSqlEDW": true, "TaskName": "sapisu_ZDM_DS_EDW_PORTION_TEXT", "ControlStageId": 2, "TaskId": 46, "StageSequence": 200, "StageName": "Raw to Cleansed", "SourceId": 46, "TargetId": 46, "ObjectGrain": "Day", "CommandTypeId": 8, "Watermarks": "", "WatermarksDT": null, "WatermarkColumn": "", "BusinessKeyColumn": "", "UpdateMetaData": null, "SourceTimeStampFormat": "", "Command": "", "LastLoadedFile": null}

#Use this string in the Source Object widget
#SAPISU_ZDM_DS_EDW_PORTION_TEXT

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
delta_cleansed_tbl_name = f'{ADS_DATABASE_CLEANSED}.stg_{source_object}'
delta_raw_tbl_name = f'{ADS_DATABASE_RAW}.stg_{source_object}'


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
df_cleansed = spark.sql("SELECT \
	TERMSCHL as portion, \
	TERMTEXT as scheduleMasterRecord, \
	to_date(TERMERST, 'yyyyMMdd') as billingPeriodEndDate, \
	cast(PERIODEW as int) as periodLengthMonths, \
	PERIODET as periodCategory, \
	to_date(ZUORDDAT, 'yyyyMMdd') as meterReadingAllocationDate, \
	ABSZYK as allowableBudgetBillingCycles, \
	to_date(EROEDAT, 'yyyyMMdd') as createdDate, \
	ERNAM as createdBy, \
	to_date(AENDDATE, 'yyyyMMdd') as lastChangedDate, \
	AENDNAM as lastChangedBy, \
	cast(SPARTENTY1 as int) as divisionCategory1, \
	cast(SPARTENTY2 as int) as divisionCategory2, \
	cast(SPARTENTY3 as int) as divisionCategory3, \
	cast(SPARTENTY4 as int) as divisionCategory4, \
	cast(SPARTENTY5 as int) as divisionCategory5, \
	cast(ABSZYKTER1 as int) as budgetBillingCycle1, \
	cast(ABSZYKTER2 as int) as budgetBillingCycle2, \
	cast(ABSZYKTER3 as int) as budgetBillingCycle3, \
	cast(ABSZYKTER4 as int) as budgetBillingCycle4, \
	cast(ABSZYKTER5 as int) as budgetBillingCycle5, \
	PARASATZ as parameterRecord, \
	IDENT as factoryCalendar, \
	SAPKAL as correctHolidayToWorkDay, \
	cast(PTOLERFROM as int) as lowerLimitBillingPeriod, \
	cast(PTOLERTO as int) as upperLimitBillingPeriod, \
	cast(PERIODED as int) as periodLengthDays, \
	WORK_DAY as isWorkDay, \
	EXTRAPOLWASTE as extrapolationCategory, \
	_RecordStart, \
	_RecordEnd, \
	_RecordDeleted, \
	_RecordCurrent \
	FROM CLEANSED.STG_SAPISU_ZDM_DS_EDW_PORTION_TEXT \
         ")

display(df_cleansed)
print(f'Number of rows: {df_cleansed.count()}')

# COMMAND ----------

newSchema = StructType([
	StructField('portion',StringType(),False,
	StructField('scheduleMasterRecord',StringType(),True,
	StructField('billingPeriodEndDate',DateType(),True,
	StructField('periodLengthMonths',IntegerType(),True,
	StructField('periodCategory',StringType(),True,
	StructField('meterReadingAllocationDate',DateType(),True,
	StructField('allowableBudgetBillingCycles',StringType(),True,
	StructField('createdDate',DateType(),True,
	StructField('createdBy',StringType(),True,
	StructField('lastChangedDate',DateType(),True,
	StructField('lastChangedBy',StringType(),True,
	StructField('divisionCategory1',IntegerType(),True,
	StructField('divisionCategory2',IntegerType(),True,
	StructField('divisionCategory3',IntegerType(),True,
	StructField('divisionCategory4',IntegerType(),True,
	StructField('divisionCategory5',IntegerType(),True,
	StructField('budgetBillingCycle1',IntegerType(),True,
	StructField('budgetBillingCycle2',IntegerType(),True,
	StructField('budgetBillingCycle3',IntegerType(),True,
	StructField('budgetBillingCycle4',IntegerType(),True,
	StructField('budgetBillingCycle5',IntegerType(),True,
	StructField('parameterRecord',StringType(),True,
	StructField('factoryCalendar',StringType(),True,
	StructField('correctHolidayToWorkDay',StringType(),True,
	StructField('lowerLimitBillingPeriod',IntegerType(),True,
	StructField('upperLimitBillingPeriod',IntegerType(),True,
	StructField('periodLengthDays',IntegerType(),True,
	StructField('isWorkDay',StringType(),True,
	StructField('extrapolationCategory',StringType(),True,
	StructField('_RecordStart',TimestampType(),False),
	StructField('_RecordEnd',TimestampType(),False),
	StructField('_RecordDeleted',IntegerType(),False),
	StructField('_RecordCurrent',IntegerType(),False)
])

df_updated_column = spark.createDataFrame(df_cleansed.rdd, schema=newSchema)


# COMMAND ----------

# DBTITLE 1,12. Save Data frame into Cleansed Delta table (Final)
#Save Data frame into Cleansed Delta table (final)
DeltaSaveDataframeDirect(df_updated_column, "t", source_object, ADS_DATABASE_CLEANSED, ADS_CONTAINER_CLEANSED, "overwrite", "")

# COMMAND ----------

# DBTITLE 1,13. Exit Notebook
dbutils.notebook.exit("1")
