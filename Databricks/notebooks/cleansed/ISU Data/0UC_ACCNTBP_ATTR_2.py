# Databricks notebook source
# DBTITLE 1,Generate parameter and source object name for unit testing
import json
#For unit testing...
#Use this string in the Param widget: 
#{"SourceType": "BLOB Storage (json)", "SourceServer": "daf-sa-lake-sastoken", "SourceGroup": "isu", "SourceName": "isu_0UC_ACCNTBP_ATTR_2", "SourceLocation": "isu/0UC_ACCNTBP_ATTR_2", "AdditionalProperty": "", "Processor": "databricks-token|0711-011053-turfs581|Standard_DS3_v2|8.3.x-scala2.12|2:8|interactive", "IsAuditTable": false, "SoftDeleteSource": "", "ProjectName": "ISU DATA", "ProjectId": 2, "TargetType": "BLOB Storage (json)", "TargetName": "isu_0UC_ACCNTBP_ATTR_2", "TargetLocation": "isu/0UC_ACCNTBP_ATTR_2", "TargetServer": "daf-sa-lake-sastoken", "DataLoadMode": "FULL-EXTRACT", "DeltaExtract": false, "CDCSource": false, "TruncateTarget": false, "UpsertTarget": true, "AppendTarget": null, "TrackChanges": false, "LoadToSqlEDW": true, "TaskName": "isu_0UC_ACCNTBP_ATTR_2", "ControlStageId": 2, "TaskId": 46, "StageSequence": 200, "StageName": "Raw to Cleansed", "SourceId": 46, "TargetId": 46, "ObjectGrain": "Day", "CommandTypeId": 8, "Watermarks": "", "WatermarksDT": null, "WatermarkColumn": "", "BusinessKeyColumn": "businessPartnerGroupNumber,contractAccountNumber", "UpdateMetaData": null, "SourceTimeStampFormat": "", "Command": "", "LastLoadedFile": null}

#Use this string in the Source Object widget
#isu_0UC_ACCNTBP_ATTR_2

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
	GPART as businessPartnerGroupNumber, \
	VKONT as contractAccountNumber, \
	ABSANFAB as budgetBillingRequestForDebtor, \
	ABSANFBZ as budgetBillingRequestForCashPayer, \
	KEINZAHL as noPaymentFormIndicator, \
	EINZUGSZ as numberOfSuccessfulDirectDebits, \
	RUECKLZ as numberOfDirectDebitReturns, \
	MAHNUNG_Z as sendAdditionalDunningNoticeIndicator, \
	RECHNUNG_Z as sendAdditionalBillIndicator, \
	FORMKEY as applicationForm, \
	AUSGRUP_IN as outsortingCheckGroupCode, \
	OUTCOUNT as manualOutsortingCount, \
	MANOUTS_IN as manualOutsortingReasonCode, \
	SENDCONTROL_MA as shippingControlForAlternativeDunningRecipient, \
	SENDCONTROL_RH as dispatchControlForAlternativeBillRecipient, \
	SENDCONTROL_GP as dispatchControl, \
	KZABSVER as billingProcedureActivationIndicator, \
	JVLTE as participationInYearlyAdvancePayment, \
	to_date(ERDAT, 'yyyy-MM-dd') as createdDate, \
	ERNAM as createdBy, \
	to_date(AEDATP, 'yyyy-MM-dd') as lastChangedDate, \
	AENAMP as changedBy, \
	FDZTG as additionalDaysForCashManagement, \
	GUID as headerUUID, \
	cast(DDLAM as dec(13,0)) as directDebitLimit, \
	DDLNM as numberOfMonthsForDirectDebitLimit, \
	EXVKO as businessPartnerReferenceNumber, \
	OPBUK as companyCodeGroup, \
	STDBK as standardCompanyCode, \
	ABWMA as alternativeDunningRecipient, \
	EBVTY as bankDetailsId, \
	EZAWE as incomingPaymentMethodCode, \
	LOEVM as deletedIndicator, \
	ABWVK as alternativeContractAccountForCollectiveBills, \
	VKPBZ as accountRelationshipCode, \
	TXTSH as accountRelationship, \
	ADRNB as addressNumber, \
	ADRMA as addressNumberForAlternativeDunningRecipient, \
	ABWRH as alternativeInvoiceRecipient, \
	ADRRH as addressNumberForAlternativeBillRecipient, \
	TOGRU as toleranceGroupCode, \
	CCARD_ID as paymentCardId, \
	VERTYP as clearingCategory, \
	CMGRP as collectionManagementMasterDataGroup, \
	STRAT as collectionStrategyCode, \
	ZAHLKOND as paymentCondition, \
	KOFIZ_SD as accountDeterminationId, \
	_RecordStart, \
	_RecordEnd, \
	_RecordDeleted, \
	_RecordCurrent \
	FROM {ADS_DATABASE_STAGE}.{source_object}")

display(df_cleansed)
print(f'Number of rows: {df_cleansed.count()}')

# COMMAND ----------

newSchema = StructType([
	StructField('businessPartnerGroupNumber',StringType(),False),
	StructField('contractAccountNumber',StringType(),False),
	StructField('budgetBillingRequestForDebtor',StringType(),True),
	StructField('budgetBillingRequestForCashPayer',StringType(),True),
	StructField('noPaymentFormIndicator',StringType(),True),
	StructField('numberOfSuccessfulDirectDebits',StringType(),True),
	StructField('numberOfDirectDebitReturns',StringType(),True),
	StructField('sendAdditionalDunningNoticeIndicator',StringType(),True),
	StructField('sendAdditionalBillIndicator',StringType(),True),
	StructField('applicationForm',StringType(),True),
	StructField('outsortingCheckGroupCode',StringType(),True),
	StructField('manualOutsortingCount',StringType(),True),
	StructField('manualOutsortingReasonCode',StringType(),True),
	StructField('shippingControlForAlternativeDunningRecipient',StringType(),True),
	StructField('dispatchControlForAlternativeBillRecipient',StringType(),True),
	StructField('dispatchControl',StringType(),True),
	StructField('billingProcedureActivationIndicator',StringType(),True),
	StructField('participationInYearlyAdvancePayment',StringType(),True),
	StructField('createdDate',DateType(),True),
	StructField('createdBy',StringType(),True),
	StructField('lastChangedDate',DateType(),True),
	StructField('changedBy',StringType(),True),
	StructField('additionalDaysForCashManagement',StringType(),True),
	StructField('headerUUID',StringType(),True),
	StructField('directDebitLimit',DecimalType(13,0),True),
	StructField('numberOfMonthsForDirectDebitLimit',StringType(),True),
	StructField('businessPartnerReferenceNumber',StringType(),True),
	StructField('companyCodeGroup',StringType(),True),
	StructField('standardCompanyCode',StringType(),True),
	StructField('alternativeDunningRecipient',StringType(),True),
	StructField('bankDetailsId',StringType(),True),
	StructField('incomingPaymentMethodCode',StringType(),True),
	StructField('deletedIndicator',StringType(),True),
	StructField('alternativeContractAccountForCollectiveBills',StringType(),True),
	StructField('accountRelationshipCode',StringType(),True),
	StructField('accountRelationship',StringType(),True),
	StructField('addressNumber',StringType(),True),
	StructField('addressNumberForAlternativeDunningRecipient',StringType(),True),
	StructField('alternativeInvoiceRecipient',StringType(),True),
	StructField('addressNumberForAlternativeBillRecipient',StringType(),True),
	StructField('toleranceGroupCode',StringType(),True),
	StructField('paymentCardId',StringType(),True),
	StructField('clearingCategory',StringType(),True),
	StructField('collectionManagementMasterDataGroup',StringType(),True),
	StructField('collectionStrategyCode',StringType(),True),
	StructField('paymentCondition',StringType(),True),
	StructField('accountDeterminationId',StringType(),True),
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