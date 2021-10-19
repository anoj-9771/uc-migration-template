# Databricks notebook source
# DBTITLE 1,Generate parameter and source object name for unit testing
import json
#For unit testing...
#Use this string in the Param widget: 
#{"SourceType": "BLOB Storage (json)", "SourceServer": "daf-sa-lake-sastoken", "SourceGroup": "CRM", "SourceName": "CRM_0SRV_REQ_INCI_H", "SourceLocation": "CRM/0SRV_REQ_INCI_H", "AdditionalProperty": "", "Processor": "databricks-token|0711-011053-turfs581|Standard_DS3_v2|8.3.x-scala2.12|2:8|interactive", "IsAuditTable": false, "SoftDeleteSource": "", "ProjectName": "CRMREF", "ProjectId": 2, "TargetType": "BLOB Storage (json)", "TargetName": "CRM_0SRV_REQ_INCI_H", "TargetLocation": "CRM/0SRV_REQ_INCI_H", "TargetServer": "daf-sa-lake-sastoken", "DataLoadMode": "FULL-EXTRACT", "DeltaExtract": false, "CDCSource": false, "TruncateTarget": false, "UpsertTarget": true, "AppendTarget": null, "TrackChanges": false, "LoadToSqlEDW": true, "TaskName": "CRM_0SRV_REQ_INCI_H", "ControlStageId": 2, "TaskId": 46, "StageSequence": 200, "StageName": "Raw to Cleansed", "SourceId": 46, "TargetId": 46, "ObjectGrain": "Day", "CommandTypeId": 8, "Watermarks": "", "WatermarksDT": null, "WatermarkColumn": "", "BusinessKeyColumn": "utilitiesStructuredContract", "UpdateMetaData": null, "SourceTimeStampFormat": "", "Command": "", "LastLoadedFile": null}

#Use this string in the Source Object widget
#CRM_0SRV_REQ_INCI_H

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
df_cleansed = spark.sql("SELECT \
	nan as headerUUID, \
	case when nan = 'na' then '' else nan end as utilitiesStructuredContract, \
	nan as headerType, \
	to_date(nan) as postingDate, \
	nan as transactionDescription, \
	nan as logicalSystem, \
	nan as headerCategory, \
	to_date(nan) as creationDate, \
	nan as createdBy, \
	to_date(nan) as lastChangedDate, \
	nan as changedBy, \
	cast(nan as int) as requestHeaderNumber, \
	nan as scenarioId, \
	nan as templateType, \
	cast(nan as long) as MERGE INTO DATE??, \
	cast(nan as long) as MERGE INTO DATE??, \
	cast(nan as int) as recommendedPriority, \
	cast(nan as int) as urgency, \
	cast(nan as int) as impact, \
	cast(nan as int) as escalation, \
	cast(nan as int) as risk, \
	cast(nan as long) as lastUpdatedAt, \
	nan as activityCategory, \
	cast(nan as int) as activityPriority, \
	nan as activityDirection, \
	nan as soldToParty, \
	nan as salesEmployee, \
	nan as responsibleEmployee, \
	nan as contactPerson, \
	cast(nan as int) as salesOrgResponsible, \
	cast(nan as int) as salesOrg, \
	cast(nan as int) as salesOffice, \
	cast(nan as int) as salesGroup, \
	cast(nan as int) as serviceOrgResponsible, \
	cast(nan as int) as serviceOrg, \
	nan as serviceTeam, \
	to_date(nan) as calendarDay, \
	cast(to_unix_timestamp(nan, 'yyyy-MM-dd hh:mm:ss a') as timestamp) as calendarDatetime, \
	nan as precedingTransactionGUID, \
	nan as precedingDocumentObjectType, \
	nan as precedingActivityGUID, \
	nan as processCategory, \
	nan as processCatalog, \
	nan as processCodeGroup, \
	nan as processCode, \
	nan as precedingObjectType, \
	cast(nan as long) as validTimestamp, \
	nan as catalogCategoryC, \
	nan as catalogC, \
	nan as codeGroupC, \
	nan as codeC, \
	cast(nan as int) as defectCountC, \
	cast(nan as int) as numberOfActivitiesC, \
	nan as coherentAspectIdC, \
	nan as coherentCategoryIdC, \
	nan as dataElementGUIDC, \
	nan as catalogCategoryD, \
	nan as catalogD, \
	nan as codeGroupD, \
	nan as codeD, \
	cast(nan as int) as defectCountD, \
	cast(nan as int) as numberOfActivitiesD, \
	nan as coherentAspectIdD, \
	nan as coherentCategoryIdD, \
	nan as dataElementGUIDD, \
	cast(nan as int) as defectCountE, \
	cast(nan as int) as numberOfActivitiesE, \
	nan as dataElementGUIDE, \
	cast(nan as int) as defectCountT, \
	cast(nan as int) as numberOfActivitiesT, \
	nan as dataElementGUIDT, \
	cast(nan as int) as defectCountW, \
	cast(nan as int) as numberOfActivitiesW, \
	nan as dataElementGUIDW, \
	nan as subjectProfileCategory, \
	nan as dataElementGUID, \
	cast(nan as long) as serviceLifeCycle, \
	nan as serviceLifeCycleUnit, \
	cast(nan as long) as workDuration, \
	nan as workDurationUnit, \
	cast(nan as long) as totalDuration, \
	nan as totalDurationUnit, \
	to_date(nan) as requestStartDate, \
	to_date(nan) as requestEndDate, \
	cast(to_unix_timestamp(nan, 'yyyy-MM-dd hh:mm:ss a') as timestamp) as dueDateTime, \
	cast(to_unix_timestamp(nan, 'yyyy-MM-dd hh:mm:ss a') as timestamp) as completionDateTime, \
	cast(to_unix_timestamp(nan, 'yyyy-MM-dd hh:mm:ss a') as timestamp) as firstEscalateDateTime, \
	cast(to_unix_timestamp(nan, 'yyyy-MM-dd hh:mm:ss a') as timestamp) as secondEscalateDateTime, \
	nan as activityReasonCode, \
	cast(nan as int) as numberOfInteractionRecords, \
	nan as completedBeforeIndicator, \
	nan as problemGUID, \
	nan as notificationNumber, \
	nan as contractiId, \
	nan as podStatus, \
	nan as statusProfile, \
	nan as maximoWorkOrderNumber, \
	nan as source, \
	nan as projectId, \
	nan as issueResponsibility, \
	nan as businessPartnerNumber, \
	nan as agreementNumber, \
	nan as propertyNumber, \
	nan as serviceArea, \
	nan as serviceSubArea, \
	nan as resolutionCode, \
	nan as serviceCategoryCode, \
	nan as rootCauseCode, \
	nan as facilityNameCode, \
	nan as secondaryAnalysisCode, \
	_RecordStart, \
	_RecordEnd, \
	_RecordDeleted, \
	_RecordCurrent \
	FROM CLEANSED.STG_" + source_object \
         )

display(df_cleansed)
print(f'Number of rows: {df_cleansed.count()}')

# COMMAND ----------

newSchema = StructType([
	StructField('headerUUID',StringType(),True),
	StructField('utilitiesStructuredContract',StringType(),False),
	StructField('headerType',StringType(),True),
	StructField('postingDate',DateType(),True),
	StructField('transactionDescription',StringType(),True),
	StructField('logicalSystem',StringType(),True),
	StructField('headerCategory',StringType(),True),
	StructField('creationDate',DateType(),True),
	StructField('createdBy',StringType(),True),
	StructField('lastChangedDate',DateType(),True),
	StructField('changedBy',StringType(),True),
	StructField('requestHeaderNumber',IntegerType(),True),
	StructField('scenarioId',StringType(),True),
	StructField('templateType',StringType(),True),
	StructField('MERGE INTO DATE??',LongType(),True),
	StructField('MERGE INTO DATE??',LongType(),True),
	StructField('recommendedPriority',IntegerType(),True),
	StructField('urgency',IntegerType(),True),
	StructField('impact',IntegerType(),True),
	StructField('escalation',IntegerType(),True),
	StructField('risk',IntegerType(),True),
	StructField('lastUpdatedAt',LongType(),True),
	StructField('activityCategory',StringType(),True),
	StructField('activityPriority',IntegerType(),True),
	StructField('activityDirection',StringType(),True),
	StructField('soldToParty',StringType(),True),
	StructField('salesEmployee',StringType(),True),
	StructField('responsibleEmployee',StringType(),True),
	StructField('contactPerson',StringType(),True),
	StructField('salesOrgResponsible',IntegerType(),True),
	StructField('salesOrg',IntegerType(),True),
	StructField('salesOffice',IntegerType(),True),
	StructField('salesGroup',IntegerType(),True),
	StructField('serviceOrgResponsible',IntegerType(),True),
	StructField('serviceOrg',IntegerType(),True),
	StructField('serviceTeam',StringType(),True),
	StructField('calendarDay',DateType(),True),
	StructField('calendarDatetime',TimestampType(),True),
	StructField('precedingTransactionGUID',StringType(),True),
	StructField('precedingDocumentObjectType',StringType(),True),
	StructField('precedingActivityGUID',StringType(),True),
	StructField('processCategory',StringType(),True),
	StructField('processCatalog',StringType(),True),
	StructField('processCodeGroup',StringType(),True),
	StructField('processCode',StringType(),True),
	StructField('precedingObjectType',StringType(),True),
	StructField('validTimestamp',LongType(),True),
	StructField('catalogCategoryC',StringType(),True),
	StructField('catalogC',StringType(),True),
	StructField('codeGroupC',StringType(),True),
	StructField('codeC',StringType(),True),
	StructField('defectCountC',IntegerType(),True),
	StructField('numberOfActivitiesC',IntegerType(),True),
	StructField('coherentAspectIdC',StringType(),True),
	StructField('coherentCategoryIdC',StringType(),True),
	StructField('dataElementGUIDC',StringType(),True),
	StructField('catalogCategoryD',StringType(),True),
	StructField('catalogD',StringType(),True),
	StructField('codeGroupD',StringType(),True),
	StructField('codeD',StringType(),True),
	StructField('defectCountD',IntegerType(),True),
	StructField('numberOfActivitiesD',IntegerType(),True),
	StructField('coherentAspectIdD',StringType(),True),
	StructField('coherentCategoryIdD',StringType(),True),
	StructField('dataElementGUIDD',StringType(),True),
	StructField('defectCountE',IntegerType(),True),
	StructField('numberOfActivitiesE',IntegerType(),True),
	StructField('dataElementGUIDE',StringType(),True),
	StructField('defectCountT',IntegerType(),True),
	StructField('numberOfActivitiesT',IntegerType(),True),
	StructField('dataElementGUIDT',StringType(),True),
	StructField('defectCountW',IntegerType(),True),
	StructField('numberOfActivitiesW',IntegerType(),True),
	StructField('dataElementGUIDW',StringType(),True),
	StructField('subjectProfileCategory',StringType(),True),
	StructField('dataElementGUID',StringType(),True),
	StructField('serviceLifeCycle',LongType(),True),
	StructField('serviceLifeCycleUnit',StringType(),True),
	StructField('workDuration',LongType(),True),
	StructField('workDurationUnit',StringType(),True),
	StructField('totalDuration',LongType(),True),
	StructField('totalDurationUnit',StringType(),True),
	StructField('requestStartDate',DateType(),True),
	StructField('requestEndDate',DateType(),True),
	StructField('dueDateTime',TimestampType(),True),
	StructField('completionDateTime',TimestampType(),True),
	StructField('firstEscalateDateTime',TimestampType(),True),
	StructField('secondEscalateDateTime',TimestampType(),True),
	StructField('activityReasonCode',StringType(),True),
	StructField('numberOfInteractionRecords',IntegerType(),True),
	StructField('completedBeforeIndicator',StringType(),True),
	StructField('problemGUID',StringType(),True),
	StructField('notificationNumber',StringType(),True),
	StructField('contractiId',StringType(),True),
	StructField('podStatus',StringType(),True),
	StructField('statusProfile',StringType(),True),
	StructField('maximoWorkOrderNumber',StringType(),True),
	StructField('source',StringType(),True),
	StructField('projectId',StringType(),True),
	StructField('issueResponsibility',StringType(),True),
	StructField('businessPartnerNumber',StringType(),True),
	StructField('agreementNumber',StringType(),True),
	StructField('propertyNumber',StringType(),True),
	StructField('serviceArea',StringType(),True),
	StructField('serviceSubArea',StringType(),True),
	StructField('resolutionCode',StringType(),True),
	StructField('serviceCategoryCode',StringType(),True),
	StructField('rootCauseCode',StringType(),True),
	StructField('facilityNameCode',StringType(),True),
	StructField('secondaryAnalysisCode',StringType(),True),
	StructField('_RecordStart',TimestampType(),False),
	StructField('_RecordEnd',TimestampType(),False),
	StructField('_RecordDeleted',IntegerType(),False),
	StructField('_RecordCurrent',IntegerType(),False)
])

df_updated_column = spark.createDataFrame(df_cleansed.rdd, schema=newSchema)


# COMMAND ----------

# DBTITLE 1,12. Save Data frame into Cleansed Delta table (Final)
#Save Data frame into Cleansed Delta table (final)
DeltaSaveDataframeDirect(df_updated_column, source_group, target_table, ADS_DATABASE_CLEANSED, ADS_CONTAINER_CLEANSED, "overwrite", "")
# COMMAND ----------

# DBTITLE 1,13. Exit Notebook
dbutils.notebook.exit("1")
