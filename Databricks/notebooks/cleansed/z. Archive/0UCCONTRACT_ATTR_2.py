# Databricks notebook source
# DBTITLE 1,Generate parameter and source object name for unit testing
import json
#For unit testing...
#Use this string in the Param widget: 
#{"SourceType": "BLOB Storage (json)", "SourceServer": "daf-sa-lake-sastoken", "SourceGroup": "sapisu", "SourceName": "sapisu_0UCCONTRACT_ATTR_2", "SourceLocation": "sapisu/0UCCONTRACT_ATTR_2", "AdditionalProperty": "", "Processor": "databricks-token|0711-011053-turfs581|Standard_DS3_v2|8.3.x-scala2.12|2:8|interactive", "IsAuditTable": false, "SoftDeleteSource": "", "ProjectName": "SAP DATA", "ProjectId": 2, "TargetType": "BLOB Storage (json)", "TargetName": "sapisu_0UCCONTRACT_ATTR_2", "TargetLocation": "sapisu/0UCCONTRACT_ATTR_2", "TargetServer": "daf-sa-lake-sastoken", "DataLoadMode": "FULL-EXTRACT", "DeltaExtract": false, "CDCSource": false, "TruncateTarget": false, "UpsertTarget": true, "AppendTarget": null, "TrackChanges": false, "LoadToSqlEDW": true, "TaskName": "sapisu_0UCCONTRACT_ATTR_2", "ControlStageId": 2, "TaskId": 46, "StageSequence": 200, "StageName": "Raw to Cleansed", "SourceId": 46, "TargetId": 46, "ObjectGrain": "Day", "CommandTypeId": 8, "Watermarks": "", "WatermarksDT": null, "WatermarkColumn": "", "BusinessKeyColumn": "", "UpdateMetaData": null, "SourceTimeStampFormat": "", "Command": "", "LastLoadedFile": null}

#Use this string in the Source Object widget
#SAPISU_0UCCONTRACT_ATTR_2

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
	MANDT as clientId, \
	BUKRS as companyCode, \
	select TXTMD
Join using BUKRS/BUKRS 
Where LANGU=E as companyDescription, \
	SPARTE as divisonCode, \
	KOFIZ as contractAccountDeterminationID, \
	select TEXT50
Join using KOFIZ/KOFIZ
Where SPRAS=E as contractAccountDetermination, \
	ABSZYK as allowableBudgetBillingCycles, \
	GEMFAKT as invoiceContractsJointly, \
	ABRSPERR as billBlockingReasonCode, \
	select TEXT30
Join using ABRSPERR/ABRSPERR
where SPRAS='E' as billBlockingReason, \
	ABRFREIG as billReleasingReasonCode, \
	select TEXT30
Join using ABRFREIG/ABRFREIG
where SPRAS='E' as billReleasingReason, \
	VBEZ as contractText, \
	to_date(EINZDAT_ALT, 'yyyyMMdd') as legacyMoveInDate, \
	cast(KFRIST as int) as numberOfCancellations, \
	cast(VERLAENG as int) as numberOfRenewals, \
	cast(PERSNR as int) as personnelNumber, \
	VREFER as contractNumberLegacy, \
	to_date(ERDAT, 'yyyyMMdd') as createdDate, \
	ERNAM as createdBy, \
	to_date(AEDAT, 'yyyyMMdd') as lastChangedDate, \
	AENAM as lastChangedBy, \
	LOEVM as deletedIndicator, \
	FAKTURIERT as isContractInvoiced, \
	cast(PS_PSP_PNR as int) as wbsElement, \
	AUSGRUP as outsortingCheckGroupForBilling, \
	cast(OUTCOUNT as int) as manualOutsortingCount, \
	cast(PYPLS as int) as paymentPlanStartMonth, \
	SERVICEID as serviceProvider, \
	cast(PYPLA as int) as alternativePaymentStartMonth, \
	BILLFINIT as contractTerminatedForBilling, \
	cast(SALESEMPLOYEE as int) as salesEmployee, \
	INVOICING_PARTY as invoicingParty, \
	CANCREASON_NEW as cancellationReasonCRM, \
	ANLAGE as installationId, \
	VKONTO as contractAccountNumber, \
	KZSONDAUSZ as specialMoveOutCase, \
	to_date(EINZDAT, 'yyyyMMdd') as moveInDate, \
	to_date(AUSZDAT, 'yyyyMMdd') as moveOutDate, \
	to_date(ABSSTOPDAT, 'yyyyMMdd') as budgetBillingStopDate, \
	XVERA as isContractTransferred, \
	ZGPART as businessPartnerGroupNumber, \
	to_date(ZDATE_FROM, 'yyyyMMdd') as validFromDate, \
	cast(ZZAGREEMENT_NUM as int) as agreementNumber, \
	VSTELLE as premise, \
	HAUS as propertyNumber, \
	ZZZ_ADRMA as alternativeAddressNumber, \
	ZZZ_IDNUMBER as identificationNumber, \
	ZZ_ADRNR as addressNumber, \
	ZZ_OWNER as objectReferenceId, \
	ZZ_OBJNR as objectNumber, \
	CPERS as collectionsContactPerson, \
	VERTRAG as contractId, \
	_RecordStart, \
	_RecordEnd, \
	_RecordDeleted, \
	_RecordCurrent \
	FROM CLEANSED.STG_SAPISU_0UCCONTRACT_ATTR_2 \
         ")

#display(df_cleansed)
#print(f'Number of rows: {df_cleansed.count()}')

# COMMAND ----------

newSchema = StructType([
	StructField('clientId',StringType(),False,
	StructField('companyCode',StringType(),True,
	StructField('companyDescription',StringType(),True,
	StructField('divisonCode',StringType(),True,
	StructField('contractAccountDeterminationID',StringType(),True,
	StructField('contractAccountDetermination',StringType(),True,
	StructField('allowableBudgetBillingCycles',StringType(),True,
	StructField('invoiceContractsJointly',StringType(),True,
	StructField('billBlockingReasonCode',StringType(),True,
	StructField('billBlockingReason',StringType(),True,
	StructField('billReleasingReasonCode',StringType(),True,
	StructField('billReleasingReason',StringType(),True,
	StructField('contractText',StringType(),True,
	StructField('legacyMoveInDate',DateType(),True,
	StructField('numberOfCancellations',IntegerType(),True,
	StructField('numberOfRenewals',IntegerType(),True,
	StructField('personnelNumber',IntegerType(),True,
	StructField('contractNumberLegacy',StringType(),True,
	StructField('createdDate',DateType(),True,
	StructField('createdBy',StringType(),True,
	StructField('lastChangedDate',DateType(),True,
	StructField('lastChangedBy',StringType(),True,
	StructField('deletedIndicator',StringType(),True,
	StructField('isContractInvoiced',StringType(),True,
	StructField('wbsElement',IntegerType(),True,
	StructField('outsortingCheckGroupForBilling',StringType(),True,
	StructField('manualOutsortingCount',IntegerType(),True,
	StructField('paymentPlanStartMonth',IntegerType(),True,
	StructField('serviceProvider',StringType(),True,
	StructField('alternativePaymentStartMonth',IntegerType(),True,
	StructField('contractTerminatedForBilling',StringType(),True,
	StructField('salesEmployee',IntegerType(),True,
	StructField('invoicingParty',StringType(),True,
	StructField('cancellationReasonCRM',StringType(),True,
	StructField('installationId',StringType(),True,
	StructField('contractAccountNumber',StringType(),True,
	StructField('specialMoveOutCase',StringType(),True,
	StructField('moveInDate',DateType(),True,
	StructField('moveOutDate',DateType(),True,
	StructField('budgetBillingStopDate',DateType(),True,
	StructField('isContractTransferred',StringType(),True,
	StructField('businessPartnerGroupNumber',StringType(),True,
	StructField('validFromDate',DateType(),True,
	StructField('agreementNumber',IntegerType(),True,
	StructField('premise',StringType(),True,
	StructField('propertyNumber',StringType(),True,
	StructField('alternativeAddressNumber',StringType(),True,
	StructField('identificationNumber',StringType(),True,
	StructField('addressNumber',StringType(),True,
	StructField('objectReferenceId',StringType(),True,
	StructField('objectNumber',StringType(),True,
	StructField('collectionsContactPerson',StringType(),True,
	StructField('contractId',StringType(),False,
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
