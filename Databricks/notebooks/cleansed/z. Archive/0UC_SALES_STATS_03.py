# Databricks notebook source
# DBTITLE 1,Generate parameter and source object name for unit testing
import json
#For unit testing...
#Use this string in the Param widget: 
#{"SourceType": "BLOB Storage (json)", "SourceServer": "daf-sa-lake-sastoken", "SourceGroup": "sapisu", "SourceName": "sapisu_0UC_SALES_STATS_03", "SourceLocation": "sapisu/0UC_SALES_STATS_03", "AdditionalProperty": "", "Processor": "databricks-token|0711-011053-turfs581|Standard_DS3_v2|8.3.x-scala2.12|2:8|interactive", "IsAuditTable": false, "SoftDeleteSource": "", "ProjectName": "SAP DATA", "ProjectId": 2, "TargetType": "BLOB Storage (json)", "TargetName": "sapisu_0UC_SALES_STATS_03", "TargetLocation": "sapisu/0UC_SALES_STATS_03", "TargetServer": "daf-sa-lake-sastoken", "DataLoadMode": "FULL-EXTRACT", "DeltaExtract": false, "CDCSource": false, "TruncateTarget": false, "UpsertTarget": true, "AppendTarget": null, "TrackChanges": false, "LoadToSqlEDW": true, "TaskName": "sapisu_0UC_SALES_STATS_03", "ControlStageId": 2, "TaskId": 46, "StageSequence": 200, "StageName": "Raw to Cleansed", "SourceId": 46, "TargetId": 46, "ObjectGrain": "Day", "CommandTypeId": 8, "Watermarks": "", "WatermarksDT": null, "WatermarkColumn": "", "BusinessKeyColumn": "", "UpdateMetaData": null, "SourceTimeStampFormat": "", "Command": "", "LastLoadedFile": null}

#Use this string in the Source Object widget
#SAPISU_0UC_SALES_STATS_03

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
	to_date(BUDAT, 'yyyyMMdd') as postingDate, \
	to_date(CPUDT, 'yyyyMMdd') as documentEnteredDate, \
	FIKEY as reconciliationKeyForGeneralLedger, \
	XCANC as isReversalTransaction, \
	BELNR as billingDocumentNumber, \
	BUKRS as companyCode, \
	SPARTE as divisonCode, \
	GPARTNER as businessPartnerNumber, \
	VKONT as contractAccountNumber, \
	VERTRAG as contractId, \
	ABWVK as alternativeContractAccountForCollectiveBills, \
	ABRVORG as billingTransactionCode, \
	APPLK as applicationArea, \
	HVORG as mainTransactionLineItemCode, \
	KOFIZ as contractAccountDeterminationID, \
	PORTION as portionNumber, \
	ABLEINH as meterReadingUnit, \
	to_date(ZUORDDAA, 'yyyyMMdd') as billingAllocationDate, \
	cast(ANZTAGE as int) as numberOfContractDaysBilled, \
	cast(ANZVERTR as int) as numberOfBilledContracts, \
	cast(CNTINVDOC as int) as numberofInvoicingDocuments, \
	cast(CNTBILLDOC as int) as numberOfBillingDocuments, \
	cast(BELZEILE as int) as billingDocumentLineItemID, \
	BELZART as lineItemTypeCode, \
	AKLASSE as billingClassCode, \
	BRANCHE as industryText, \
	TVORG as subtransactionForDocumentItem, \
	TARIFTYP as rateTypeCode, \
	STATTART as statisticalAnalysisRateType, \
	TARIFNR as rateId, \
	KONDIGR as rateFactGroupCode, \
	STTARIF as statisticalRate, \
	VBRMONAT as consumptionMonth, \
	to_date(AB, 'yyyyMMdd') as validFromDate, \
	to_date(BIS, 'yyyyMMdd') as validToDate, \
	BUCHREL as billingLineItemReleventPostingIndicator, \
	STGRQNT as quantityStatisticsGroupCode, \
	STGRAMT as amountStatisticsGroupCode, \
	ARTMENGE as billedQuantityStatisticsCode, \
	KOKRS as controllingArea, \
	PRCTR as profitCenter, \
	cast(PS_PSP_PNR as int) as wbsElement, \
	WAERS as currencyKey, \
	MASSBILL as billingMeasurementUnitCode, \
	cast(BETRAG as dec(18,6)) as billingLineItemNetAmount, \
	cast(MENGE as dec(18,6)) as billingQuantity, \
	cast(PREISBTR as dec(18,6)) as priceAmount, \
	BELZART_NAME as lineItemType, \
	PRINTDOC as printDocumentId, \
	cast(PRINTDOCLINE as int) as printDocumentLineItemId, \
	to_date(BELEGDAT, 'yyyyMMdd') as billingDocumentCreateDate, \
	ANLAGE as installationId, \
	CITY_CODE as cityCode, \
	STREETCODE as streetCode, \
	COUNTRY as countryShortName, \
	REGION as stateCode, \
	REGPOLIT as politicalRegionCode, \
	SERVICEID as serviceProvider, \
	cast(SALESEMPLOYEE as int) as salesEmployee, \
	INT_UI_BW as internalPointDeliveryKeyBW, \
	MWSKZ as taxSalesCode, \
	cast(SBASW as dec(18,6)) as taxBaseAmount, \
	cast(SBETW as dec(18,6)) as taxAmount, \
	PERIOD as fiscalYear, \
	PERIV as fiscalYearVariant, \
	BILLINGRUNNO as billingRunNumber, \
	SIMRUNID as simulationPeriodID, \
	_RecordStart, \
	_RecordEnd, \
	_RecordDeleted, \
	_RecordCurrent \
	FROM CLEANSED.STG_SAPISU_0UC_SALES_STATS_03 \
         ")

display(df_cleansed)
print(f'Number of rows: {df_cleansed.count()}')

# COMMAND ----------

newSchema = StructType([
	StructField('postingDate',DateType(),True,
	StructField('documentEnteredDate',DateType(),True,
	StructField('reconciliationKeyForGeneralLedger',StringType(),True,
	StructField('isReversalTransaction',StringType(),True,
	StructField('billingDocumentNumber',StringType(),False,
	StructField('companyCode',StringType(),True,
	StructField('divisonCode',StringType(),True,
	StructField('businessPartnerNumber',StringType(),True,
	StructField('contractAccountNumber',StringType(),True,
	StructField('contractId',StringType(),True,
	StructField('alternativeContractAccountForCollectiveBills',StringType(),True,
	StructField('billingTransactionCode',StringType(),True,
	StructField('applicationArea',StringType(),True,
	StructField('mainTransactionLineItemCode',StringType(),True,
	StructField('contractAccountDeterminationID',StringType(),True,
	StructField('portionNumber',StringType(),True,
	StructField('meterReadingUnit',StringType(),True,
	StructField('billingAllocationDate',DateType(),True,
	StructField('numberOfContractDaysBilled',IntegerType(),True,
	StructField('numberOfBilledContracts',IntegerType(),True,
	StructField('numberofInvoicingDocuments',IntegerType(),True,
	StructField('numberOfBillingDocuments',IntegerType(),True,
	StructField('billingDocumentLineItemID',IntegerType(),False,
	StructField('lineItemTypeCode',StringType(),True,
	StructField('billingClassCode',StringType(),True,
	StructField('industryText',StringType(),True,
	StructField('subtransactionForDocumentItem',StringType(),True,
	StructField('rateTypeCode',StringType(),True,
	StructField('statisticalAnalysisRateType',StringType(),True,
	StructField('rateId',StringType(),True,
	StructField('rateFactGroupCode',StringType(),True,
	StructField('statisticalRate',StringType(),True,
	StructField('consumptionMonth',StringType(),True,
	StructField('validFromDate',DateType(),True,
	StructField('validToDate',DateType(),True,
	StructField('billingLineItemReleventPostingIndicator',StringType(),True,
	StructField('quantityStatisticsGroupCode',StringType(),True,
	StructField('amountStatisticsGroupCode',StringType(),True,
	StructField('billedQuantityStatisticsCode',StringType(),True,
	StructField('controllingArea',StringType(),True,
	StructField('profitCenter',StringType(),True,
	StructField('wbsElement',IntegerType(),True,
	StructField('currencyKey',StringType(),True,
	StructField('billingMeasurementUnitCode',StringType(),True,
	StructField('billingLineItemNetAmount',DecimalType(18,6),True,
	StructField('billingQuantity',DecimalType(18,6),True,
	StructField('priceAmount',DecimalType(18,6),True,
	StructField('lineItemType',StringType(),True,
	StructField('printDocumentId',StringType(),True,
	StructField('printDocumentLineItemId',IntegerType(),True,
	StructField('billingDocumentCreateDate',DateType(),True,
	StructField('installationId',StringType(),True,
	StructField('cityCode',StringType(),True,
	StructField('streetCode',StringType(),True,
	StructField('countryShortName',StringType(),True,
	StructField('stateCode',StringType(),True,
	StructField('politicalRegionCode',StringType(),True,
	StructField('serviceProvider',StringType(),True,
	StructField('salesEmployee',IntegerType(),True,
	StructField('internalPointDeliveryKeyBW',StringType(),True,
	StructField('taxSalesCode',StringType(),True,
	StructField('taxBaseAmount',DecimalType(18,6),True,
	StructField('taxAmount',DecimalType(18,6),True,
	StructField('fiscalYear',StringType(),True,
	StructField('fiscalYearVariant',StringType(),True,
	StructField('billingRunNumber',StringType(),True,
	StructField('simulationPeriodID',StringType(),True,
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
