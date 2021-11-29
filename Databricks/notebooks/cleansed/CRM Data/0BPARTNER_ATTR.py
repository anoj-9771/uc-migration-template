# Databricks notebook source
# DBTITLE 1,Generate parameter and source object name for unit testing
import json
#For unit testing...
#Use this string in the Param widget: 
#{"SourceType": "BLOB Storage (json)", "SourceServer": "daf-sa-lake-sastoken", "SourceGroup": "CRM", "SourceName": "CRM_0BPARTNER_ATTR", "SourceLocation": "CRM/0BPARTNER_ATTR", "AdditionalProperty": "", "Processor": "databricks-token|0711-011053-turfs581|Standard_DS3_v2|8.3.x-scala2.12|2:8|interactive", "IsAuditTable": false, "SoftDeleteSource": "", "ProjectName": "CRMREF", "ProjectId": 2, "TargetType": "BLOB Storage (json)", "TargetName": "CRM_0BPARTNER_ATTR", "TargetLocation": "CRM/0BPARTNER_ATTR", "TargetServer": "daf-sa-lake-sastoken", "DataLoadMode": "FULL-EXTRACT", "DeltaExtract": false, "CDCSource": false, "TruncateTarget": false, "UpsertTarget": true, "AppendTarget": null, "TrackChanges": false, "LoadToSqlEDW": true, "TaskName": "CRM_0BPARTNER_ATTR", "ControlStageId": 2, "TaskId": 46, "StageSequence": 200, "StageName": "Raw to Cleansed", "SourceId": 46, "TargetId": 46, "ObjectGrain": "Day", "CommandTypeId": 8, "Watermarks": "", "WatermarksDT": null, "WatermarkColumn": "", "BusinessKeyColumn": "businessPartnerNumber", "UpdateMetaData": null, "SourceTimeStampFormat": "", "Command": "", "LastLoadedFile": null}

#Use this string in the Source Object widget
#CRM_0BPARTNER_ATTR

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
	case when PARTNER = 'na' then '' else PARTNER end as businessPartnerNumber, \
	TYPE as businessPartnerCategoryCode, \
	TXTMD as businessPartnerCategory, \
	BPKIND as businessPartnerTypeCode, \
	TEXT40 as businessPartnerType, \
	BU_GROUP as businessPartnerGroupCode, \
	TEXT40 as businessPartnerGroup, \
	BPEXT as externalBusinessPartnerNumber, \
	BU_SORT1 as searchTerm1, \
	BU_SORT2 as searchTerm2, \
	TITLE as titleCode, \
	TITLE_MEDI as title, \
	XDELE as deletedIndicator, \
	XBLCK as centralBlockBusinessPartner, \
	ZZUSER as userId, \
	ZZPAS_INDICATOR as paymentAssistSchemeIndicator, \
	ZZBA_INDICATOR as billAssistIndicator, \
	to_date(ZZAFLD00001Z) as createdDate, \
	ZZ_CONSENT1 as consent1Indicator, \
	ZZWAR_WID as warWidowIndicator, \
	ZZDISABILITY as disabilityIndicator, \
	ZZGCH as goldCardHolderIndicator, \
	ZZDECEASED as deceasedIndicator, \
	ZZPCC as pensionConcessionCardIndicator, \
	ZZELIGIBILITY as eligibilityIndicator, \
	to_date(ZZDT_CHK) as dateOfCheck, \
	to_date(ZZPAY_ST_DT) as paymentStartDate, \
	ZZPEN_TY as pensionType, \
	ZZ_CONSENT2 as consent2Indicator, \
	NAME_ORG1 as organizationName1, \
	NAME_ORG2 as organizationName2, \
	NAME_ORG3 as organizationName3, \
	to_date(FOUND_DAT) as organizationFoundedDate, \
	cast(LOCATION_1 as long) as internationalLocationNumber1, \
	cast(LOCATION_2 as long) as internationalLocationNumber2, \
	cast(LOCATION_3 as long) as internationalLocationNumber3, \
	NAME_LAST as lastName, \
	NAME_FIRST as firstName, \
	NAMEMIDDLE as middleName, \
	TITLE_ACA1 as academicTitleCode, \
	TITLE_MEDI as academicTitle, \
	NICKNAME as nickName, \
	INITIALS as nameInitials, \
	NAMCOUNTRY as countryName, \
	LANGU_CORR as correspondanceLanguage, \
	NATIO as nationality, \
	PERSNUMBER as personNumber, \
	XSEXU as unknownGenderIndicator, \
	BU_LANGU as language, \
	to_date(BIRTHDT) as dateOfBirth, \
	to_date(DEATHDT) as dateOfDeath, \
	cast(PERNO as long) as personnelNumber, \
	NAME_GRP1 as nameGroup1, \
	NAME_GRP2 as nameGroup2, \
	MC_NAME1 as searchHelpLastName, \
	MC_NAME2 as searchHelpFirstName, \
	CRUSR as createdBy, \
	to_date(CRDAT) as createdDate, \
	to_date(CRTIM) as createdTime, \
	CHUSR as changedBy, \
	to_date(CHDAT) as changedDate, \
	to_date(CHTIM) as changedTime, \
	PARTNER_GUID as businessPartnerGUID, \
	ADDRCOMM as communicationAddressNumber, \
	TD_SWITCH as plannedChangeDocument, \
	to_date(VALID_FROM) as validFromDate, \
	to_date(VALID_TO) as validToDate, \
	NATPERS as naturalPersonIndicator, \
	ZZAFLD00000M as kidneyDialysisIndicator, \
	ZZUNIT as patientUnit, \
	ZZTITLE as patientTitleCode, \
	TITLE_MEDI as patientTitle, \
	ZZF_NAME as patientFirstName, \
	ZZSURNAME as patientSurname, \
	ZZAREACODE as patientAreaCode, \
	ZZPHONE as patientPhoneNumber, \
	ZZHOSP_NAME as hospitalName, \
	ZZMACH_TYPE as patientMachineType, \
	to_date(ZZON_DATE) as machineTypeValidFromDate, \
	ZZOFF_REAS as offReason, \
	to_date(ZZOFF_DATE) as machineTypeValidToDate, \
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
	StructField('businessPartnerNumber',StringType(),False),
	StructField('businessPartnerCategoryCode',StringType(),True),
	StructField('businessPartnerCategory',StringType(),True),
	StructField('businessPartnerTypeCode',StringType(),True),
	StructField('businessPartnerType',StringType(),True),
	StructField('businessPartnerGroupCode',StringType(),True),
	StructField('businessPartnerGroup',StringType(),True),
	StructField('externalBusinessPartnerNumber',StringType(),True),
	StructField('searchTerm1',StringType(),True),
	StructField('searchTerm2',StringType(),True),
	StructField('titleCode',StringType(),True),
	StructField('title',StringType(),True),
	StructField('deletedIndicator',StringType(),True),
	StructField('centralBlockBusinessPartner',StringType(),True),
	StructField('userId',StringType(),True),
	StructField('paymentAssistSchemeIndicator',StringType(),True),
	StructField('billAssistIndicator',StringType(),True),
	StructField('createdDate',DateType(),True),
	StructField('consent1Indicator',StringType(),True),
	StructField('warWidowIndicator',StringType(),True),
	StructField('disabilityIndicator',StringType(),True),
	StructField('goldCardHolderIndicator',StringType(),True),
	StructField('deceasedIndicator',StringType(),True),
	StructField('pensionConcessionCardIndicator',StringType(),True),
	StructField('eligibilityIndicator',StringType(),True),
	StructField('dateOfCheck',DateType(),True),
	StructField('paymentStartDate',DateType(),True),
	StructField('pensionType',StringType(),True),
	StructField('consent2Indicator',StringType(),True),
	StructField('organizationName1',StringType(),True),
	StructField('organizationName2',StringType(),True),
	StructField('organizationName3',StringType(),True),
	StructField('organizationFoundedDate',DateType(),True),
	StructField('internationalLocationNumber1',LongType(),True),
	StructField('internationalLocationNumber2',LongType(),True),
	StructField('internationalLocationNumber3',LongType(),True),
	StructField('lastName',StringType(),True),
	StructField('firstName',StringType(),True),
	StructField('middleName',StringType(),True),
	StructField('academicTitleCode',StringType(),True),
	StructField('academicTitle',StringType(),True),
	StructField('nickName',StringType(),True),
	StructField('nameInitials',StringType(),True),
	StructField('countryName',StringType(),True),
	StructField('correspondanceLanguage',StringType(),True),
	StructField('nationality',StringType(),True),
	StructField('personNumber',StringType(),True),
	StructField('unknownGenderIndicator',StringType(),True),
	StructField('language',StringType(),True),
	StructField('dateOfBirth',DateType(),True),
	StructField('dateOfDeath',DateType(),True),
	StructField('personnelNumber',LongType(),True),
	StructField('nameGroup1',StringType(),True),
	StructField('nameGroup2',StringType(),True),
	StructField('searchHelpLastName',StringType(),True),
	StructField('searchHelpFirstName',StringType(),True),
	StructField('createdBy',StringType(),True),
	StructField('createdDate',DateType(),True),
	StructField('createdTime',DateType(),True),
	StructField('changedBy',StringType(),True),
	StructField('changedDate',DateType(),True),
	StructField('changedTime',DateType(),True),
	StructField('businessPartnerGUID',StringType(),True),
	StructField('communicationAddressNumber',StringType(),True),
	StructField('plannedChangeDocument',StringType(),True),
	StructField('validFromDate',DateType(),True),
	StructField('validToDate',DateType(),True),
	StructField('naturalPersonIndicator',StringType(),True),
	StructField('kidneyDialysisIndicator',StringType(),True),
	StructField('patientUnit',StringType(),True),
	StructField('patientTitleCode',StringType(),True),
	StructField('patientTitle',StringType(),True),
	StructField('patientFirstName',StringType(),True),
	StructField('patientSurname',StringType(),True),
	StructField('patientAreaCode',StringType(),True),
	StructField('patientPhoneNumber',StringType(),True),
	StructField('hospitalName',StringType(),True),
	StructField('patientMachineType',StringType(),True),
	StructField('machineTypeValidFromDate',DateType(),True),
	StructField('offReason',StringType(),True),
	StructField('machineTypeValidToDate',DateType(),True),
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
