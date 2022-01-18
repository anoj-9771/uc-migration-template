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
df_cleansed = spark.sql(f"SELECT \
	case when GUID = 'na' then '' else GUID end as headerUUID, \
	OBJECT_ID as utilitiesStructuredContract, \
	PROCESS_TYPE as headerType, \
	to_date(POSTING_DATE,'yyyy-MM-dd') as postingDate, \
	DESCRIPTION_UC as transactionDescription, \
	LOGICAL_SYSTEM as logicalSystem, \
	OBJECT_TYPE as headerCategory, \
	to_date(CREATED_AT,'yyyy-MM-dd') as createdDate, \
    to_timestamp(cast(CREATED_TS as string), 'yyyyMMddHHmmss') as createdDateTime, \
	CREATED_BY as createdBy, \
	to_date(CHANGED_AT,'yyyy-MM-dd') as lastChangedDate, \
    to_timestamp(cast(CHANGED_TS as string), 'yyyyMMddHHmmss') as lastChangedDateTime, \
	CHANGED_BY as changedBy, \
	cast(NUM_OF_HEAD as int) as requestHeaderNumber, \
	SCENARIO as scenarioId, \
	TEMPLATE_TYPE as templateType, \
	REC_PRIORITY as recommendedPriority, \
	URGENCY as urgency, \
	IMPACT as impact, \
	ESCALATION as escalation, \
	RISK as risk, \
	to_timestamp(cast(last_updated_at as string), 'yyyyMMddHHmmss') as lastUpdatedAt, \
	CATEGORY as activityCategory, \
	PRIORITY as activityPriority, \
	DIRECTION as activityDirection, \
	SOLD_TO_PARTY as soldToParty, \
	SALES_EMPLOYEE as salesEmployee, \
	PERSON_RESP as responsibleEmployee, \
	CONTACT_PERSON as contactPerson, \
	SALES_ORG_RESP as salesOrgResponsible, \
	SALES_ORG as salesOrg, \
	SALES_OFFICE as salesOffice, \
	SALES_GROUP as salesGroup, \
	SERVICE_ORG_RESP as serviceOrgResponsible, \
	SERVICE_ORG as serviceOrg, \
	SERVICE_TEAM as serviceTeam, \
	to_date(CALDAY,'yyyy-MM-dd') as calendarDay, \
	to_timestamp(cast(CALDAY_TS as string), 'yyyyMMddHHmmss') as calendarDatetime, \
	PREDEC_OBJKEY as precedingTransactionGUID, \
	PREDEC_OBJTYPE as precedingDocumentObjectType, \
	PRED_ACT_GUID as precedingActivityGUID, \
	PROCESS_CATEGORY as processCategory, \
	PROCESS_CATALOG as processCatalog, \
	PROCESS_CODEGR as processCodeGroup, \
	PROCESS_CODE as processCode, \
	PROCESS_OBJTYPE as precedingObjectType, \
	to_timestamp(cast(QUOT_VALID_TS as string), 'yyyyMMddHHmmss') as validTimestamp, \
	CATALOG_TYPE_C as catalogCategoryC, \
	KATALOGART_C as catalogC, \
	CODEGRUPPE_C as codeGroupC, \
	CODE_C as codeC, \
	cast(DEFQUANTITY_C as int) as defectCountC, \
	COUNTER_C as numberOfActivitiesC, \
	ASP_ID_C as coherentAspectIdC, \
	CAT_ID_C as coherentCategoryIdC, \
	CC_CAT_SUBJECT_C as dataElementGUIDC, \
	CATALOG_TYPE_D as catalogCategoryD, \
	KATALOGART_D as catalogD, \
	CODEGRUPPE_D as codeGroupD, \
	CODE_D as codeD, \
	cast(DEFQUANTITY_D as int) as defectCountD, \
	COUNTER_D as numberOfActivitiesD, \
	ASP_ID_D as coherentAspectIdD, \
	CAT_ID_D as coherentCategoryIdD, \
	CC_CAT_SUBJECT_D as dataElementGUIDD, \
	cast(DEFQUANTITY_E as int) as defectCountE, \
	COUNTER_E as numberOfActivitiesE, \
	CC_CAT_SUBJECT_E as dataElementGUIDE, \
	cast(DEFQUANTITY_T as int) as defectCountT, \
	COUNTER_T as numberOfActivitiesT, \
	CC_CAT_SUBJECT_T as dataElementGUIDT, \
	cast(DEFQUANTITY_W as int) as defectCountW, \
	COUNTER_W as numberOfActivitiesW, \
	CC_CAT_SUBJECT_W as dataElementGUIDW, \
	PROFILE_TYPE as subjectProfileCategory, \
	CC_CAT_SUBJECT as dataElementGUID, \
	cast(LC_SRV_DURATION as long) as serviceLifeCycle, \
	LC_SRV_DUR_UNIT as serviceLifeCycleUnit, \
	cast(WORK_DURATION as long) as workDuration, \
	WORK_DURA_UNIT as workDurationUnit, \
	cast(TOTAL_DURATION as long) as totalDuration, \
	TOTAL_DURA_UNIT as totalDurationUnit, \
	to_timestamp(cast(REQ_START_DATE as string), 'yyyyMMddHHmmss') as requestStartDate, \
	to_timestamp(cast(REQ_END_DATE as string), 'yyyyMMddHHmmss') as requestEndDate, \
	to_timestamp(cast(DUE_DATE as string), 'yyyyMMddHHmmss') as dueDateTime, \
	to_timestamp(cast(COMPLETION_TS as string), 'yyyyMMddHHmmss') as completionDateTime, \
	to_timestamp(cast(ESCALATE_1_TS as string), 'yyyyMMddHHmmss') as firstEscalateDateTime, \
	to_timestamp(cast(ESCALATE_2_TS as string), 'yyyyMMddHHmmss') as secondEscalateDateTime, \
	CC_CAT_ACTREASON as activityReasonCode, \
	cast(NO_OF_IR as int) as numberOfInteractionRecords, \
	IN_COMPL_BEFORE as completedBeforeIndicator, \
	PROBLEM_GUID as problemGUID, \
	NOTIFICATION_NO as notificationNumber, \
	CRM_ISU_CONTRACT as contractiId, \
	STATUS as podStatus, \
	USER_STAT_PROC as statusProfile, \
	ZZ_MAX_REQ_NO as maximoWorkOrderNumber, \
	ZZAFLD000026 as source, \
	ZZAFLD000027 as projectId, \
	ZZAFLD000028 as issueResponsibility, \
	ZZREPORTED_BY as businessPartnerNumber, \
	ZZAGREEMENT_N as agreementNumber, \
	ZZ_PROPERTY_NO as propertyNumber, \
	ZZ_SR_AREA as serviceArea, \
	ZZ_SR_SUB_AREA as serviceSubArea, \
	ZZ_RESOLUTION_CD as resolutionCode, \
	ZZ_SR_CATEGORY_CD as serviceCategoryCode, \
	ZZ_ROOT_CAUSE_CD as rootCauseCode, \
	ZZ_X_FACILITY_NAME_CD as facilityNameCode, \
	ZZ_X_SECONDARY_ANALYSIS_CD as secondaryAnalysisCode, \
	_RecordStart, \
	_RecordEnd, \
	_RecordDeleted, \
	_RecordCurrent \
	FROM {ADS_DATABASE_STAGE}.{source_object}")

display(df_cleansed)
print(f'Number of rows: {df_cleansed.count()}')

# COMMAND ----------

newSchema = StructType([
	StructField('headerUUID',StringType(),False),
	StructField('utilitiesStructuredContract',StringType(),True),
	StructField('headerType',StringType(),True),
	StructField('postingDate',DateType(),True),
	StructField('transactionDescription',StringType(),True),
	StructField('logicalSystem',StringType(),True),
	StructField('headerCategory',StringType(),True),
	StructField('createdDate',DateType(),True),
    StructField('createdDateTime',TimestampType(),True),
	StructField('createdBy',StringType(),True),
	StructField('lastChangedDate',DateType(),True),
    StructField('lastChangedDateTime',TimestampType(),True),
	StructField('changedBy',StringType(),True),
	StructField('requestHeaderNumber',IntegerType(),True),
	StructField('scenarioId',StringType(),True),
	StructField('templateType',StringType(),True),
	StructField('recommendedPriority',StringType(),True),
	StructField('urgency',StringType(),True),
	StructField('impact',StringType(),True),
	StructField('escalation',StringType(),True),
	StructField('risk',StringType(),True),
	StructField('lastUpdatedAt',TimestampType(),True),
	StructField('activityCategory',StringType(),True),
	StructField('activityPriority',StringType(),True),
	StructField('activityDirection',StringType(),True),
	StructField('soldToParty',StringType(),True),
	StructField('salesEmployee',StringType(),True),
	StructField('responsibleEmployee',StringType(),True),
	StructField('contactPerson',StringType(),True),
	StructField('salesOrgResponsible',StringType(),True),
	StructField('salesOrg',StringType(),True),
	StructField('salesOffice',StringType(),True),
	StructField('salesGroup',StringType(),True),
	StructField('serviceOrgResponsible',StringType(),True),
	StructField('serviceOrg',StringType(),True),
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
	StructField('validTimestamp',TimestampType(),True),
	StructField('catalogCategoryC',StringType(),True),
	StructField('catalogC',StringType(),True),
	StructField('codeGroupC',StringType(),True),
	StructField('codeC',StringType(),True),
	StructField('defectCountC',IntegerType(),True),
	StructField('numberOfActivitiesC',StringType(),True),
	StructField('coherentAspectIdC',StringType(),True),
	StructField('coherentCategoryIdC',StringType(),True),
	StructField('dataElementGUIDC',StringType(),True),
	StructField('catalogCategoryD',StringType(),True),
	StructField('catalogD',StringType(),True),
	StructField('codeGroupD',StringType(),True),
	StructField('codeD',StringType(),True),
	StructField('defectCountD',IntegerType(),True),
	StructField('numberOfActivitiesD',StringType(),True),
	StructField('coherentAspectIdD',StringType(),True),
	StructField('coherentCategoryIdD',StringType(),True),
	StructField('dataElementGUIDD',StringType(),True),
	StructField('defectCountE',IntegerType(),True),
	StructField('numberOfActivitiesE',StringType(),True),
	StructField('dataElementGUIDE',StringType(),True),
	StructField('defectCountT',IntegerType(),True),
	StructField('numberOfActivitiesT',StringType(),True),
	StructField('dataElementGUIDT',StringType(),True),
	StructField('defectCountW',IntegerType(),True),
	StructField('numberOfActivitiesW',StringType(),True),
	StructField('dataElementGUIDW',StringType(),True),
	StructField('subjectProfileCategory',StringType(),True),
	StructField('dataElementGUID',StringType(),True),
	StructField('serviceLifeCycle',LongType(),True),
	StructField('serviceLifeCycleUnit',StringType(),True),
	StructField('workDuration',LongType(),True),
	StructField('workDurationUnit',StringType(),True),
	StructField('totalDuration',LongType(),True),
	StructField('totalDurationUnit',StringType(),True),
	StructField('requestStartDate',TimestampType(),True),
	StructField('requestEndDate',TimestampType(),True),
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