# Databricks notebook source
# DBTITLE 1,Generate parameter and source object name for unit testing
import json
#For unit testing...
#Use this string in the Param widget: 
#{"SourceType": "BLOB Storage (json)", "SourceServer": "daf-sa-lake-sastoken", "SourceGroup": "isu", "SourceName": "isu_0UC_SALES_SIMU_01", "SourceLocation": "isu/0UC_SALES_SIMU_01", "AdditionalProperty": "", "Processor": "databricks-token|0711-011053-turfs581|Standard_DS3_v2|8.3.x-scala2.12|2:8|interactive", "IsAuditTable": false, "SoftDeleteSource": "", "ProjectName": "SAP DATA", "ProjectId": 2, "TargetType": "BLOB Storage (json)", "TargetName": "isu_0UC_SALES_SIMU_01", "TargetLocation": "isu/0UC_SALES_SIMU_01", "TargetServer": "daf-sa-lake-sastoken", "DataLoadMode": "FULL-EXTRACT", "DeltaExtract": false, "CDCSource": false, "TruncateTarget": false, "UpsertTarget": true, "AppendTarget": null, "TrackChanges": false, "LoadToSqlEDW": true, "TaskName": "isu_0UC_SALES_SIMU_01", "ControlStageId": 2, "TaskId": 46, "StageSequence": 200, "StageName": "Raw to Cleansed", "SourceId": 46, "TargetId": 46, "ObjectGrain": "Day", "CommandTypeId": 8, "Watermarks": "", "WatermarksDT": null, "WatermarkColumn": "", "BusinessKeyColumn": "", "UpdateMetaData": null, "SourceTimeStampFormat": "", "Command": "", "LastLoadedFile": null}

#Use this string in the Source Object widget
#isu_0UC_SALES_SIMU_01

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
data_lake_folder = source_group
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

#Get the start time of the last successful cleansed load execution
LastSuccessfulExecutionTS = Params["LastSuccessfulExecutionTS"]
print("LastSuccessfulExecutionTS: " + LastSuccessfulExecutionTS)

#Get current time
#CurrentTimeStamp = spark.sql("select current_timestamp()").first()[0]
CurrentTimeStamp = GeneralLocalDateTime()
CurrentTimeStamp = CurrentTimeStamp.strftime("%Y-%m-%d %H:%M:%S")

#Get business key,track_changes and delta_extract flag
business_key =  Params[PARAMS_BUSINESS_KEY_COLUMN]
track_changes =  Params[PARAMS_TRACK_CHANGES]
is_delta_extract =  Params[PARAMS_DELTA_EXTRACT]


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

# DBTITLE 1,10. Load Raw to Dataframe & Do Transformations
df = spark.sql(f"WITH stage AS \
                      (Select *, ROW_NUMBER() OVER (PARTITION BY SIMRUNID,BELNR,BELZEILE,BIS ORDER BY _FileDateTimeStamp DESC, DI_SEQUENCE_NUMBER DESC, _DLRawZoneTimeStamp DESC) AS _RecordVersion FROM {delta_raw_tbl_name} \
                                      WHERE _DLRawZoneTimestamp >= '{LastSuccessfulExecutionTS}') \
                           SELECT \
                                case when SIMRUNID = 'na' then '' else SIMRUNID end as simulationPeriodId, \
                                case when BELNR = 'na' then '' else BELNR end as billingDocumentNumber, \
                                BUKRS as companyCode, \
                                SPARTE as divisonCode, \
                                VKONT as contractAccountNumber, \
                                VERTRAG as contractId, \
                                ABRVORG as billingTransactionCode, \
                                HVORG as mainTransactionLineItemCode, \
                                KOFIZ as accountDeterminationId, \
                                PORTION as portionNumber, \
                                cast(ANZTAGE as int) as numberOfContractDaysBilled, \
                                cast(ANZVERTR as int) as numberOfBilledContracts, \
                                cast(CNTBILLDOC as int) as numberOfBillingDocuments, \
                                case when BELZEILE = 'na' then '' else BELZEILE end as billingDocumentLineItemId, \
                                BELZART as lineItemTypeCode, \
                                AKLASSE as billingClassCode, \
                                TVORG as subtransactionLineItemCode, \
                                TARIFTYP as rateTypeCode, \
                                STATTART as statisticalAnalysisRateType, \
                                STTARIF as statisticalRate, \
                                VBRMONAT as consumptionMonth, \
                                ToValidDate(AB) as validFromDate, \
                                ToValidDate(BIS,'MANDATORY') as validToDate, \
                                BUCHREL as billingLineItemReleventPostingIndicator, \
                                STGRQNT as quantityStatisticsGroupCode, \
                                STGRAMT as amountStatisticsGroupCode, \
                                ARTMENGE as billedQuantityStatisticsCode, \
                                KOKRS as controllingArea, \
                                PRCTR as profitCenter, \
                                PS_PSP_PNR as wbsElement, \
                                WAERS as currencyKey, \
                                MASSBILL as billingMeasurementUnitCode, \
                                cast(BETRAG as dec(18,6)) as billingLineItemNetAmount, \
                                cast(MENGE as dec(18,6)) as billingQuantity, \
                                ZZAGREEMENT_NUM as agreementNumber, \
                                cast(PREISBTR as dec(18,6)) as price, \
                                CRM_PRODUCT as crmProduct, \
                                BELZART_NAME as lineItemType, \
                                PRINTDOCLINE as printDocumentLineItemId, \
                                ANLAGE as installationId, \
                                CITY_CODE as cityCode, \
                                COUNTRY as countryShortName, \
                                REGION as stateCode, \
                                REGPOLIT as politicalRegionCode, \
                                SALESEMPLOYEE as salesEmployee, \
                                'SIMRUNID|BELNR|BELZEILE|BIS' as sourceKeyDesc, \
                                concat_ws('|',SIMRUNID,BELNR,BELZEILE,BIS) as sourceKey, \
                                'BIS' as rejectColumn, \
                                cast('1900-01-01' as TimeStamp) as _RecordStart, \
                                cast('9999-12-31' as TimeStamp) as _RecordEnd, \
                                '0' as _RecordDeleted, \
                                '1' as _RecordCurrent, \
                                cast('{CurrentTimeStamp}' as TimeStamp) as _DLCleansedZoneTimeStamp \
                        from stage where _RecordVersion = 1 ")

#print(f'Number of rows: {df.count()}')

# COMMAND ----------

# DBTITLE 1,11. Update/Rename Columns and Load into a Dataframe
#Update/rename Column
#Pass 'MANDATORY' as second argument to function ToValidDate() on key columns to ensure correct value settings for those columns
# df_cleansed = spark.sql(f"SELECT \
# 	case when SIMRUNID = 'na' then '' else SIMRUNID end as simulationPeriodId, \
# 	case when BELNR = 'na' then '' else BELNR end as billingDocumentNumber, \
# 	BUKRS as companyCode, \
# 	SPARTE as divisonCode, \
# 	VKONT as contractAccountNumber, \
# 	VERTRAG as contractId, \
# 	ABRVORG as billingTransactionCode, \
# 	HVORG as mainTransactionLineItemCode, \
# 	KOFIZ as accountDeterminationId, \
# 	PORTION as portionNumber, \
# 	cast(ANZTAGE as int) as numberOfContractDaysBilled, \
# 	cast(ANZVERTR as int) as numberOfBilledContracts, \
# 	cast(CNTBILLDOC as int) as numberOfBillingDocuments, \
# 	case when BELZEILE = 'na' then '' else BELZEILE end as billingDocumentLineItemId, \
# 	BELZART as lineItemTypeCode, \
# 	AKLASSE as billingClassCode, \
# 	TVORG as subtransactionLineItemCode, \
# 	TARIFTYP as rateTypeCode, \
# 	STATTART as statisticalAnalysisRateType, \
# 	STTARIF as statisticalRate, \
# 	VBRMONAT as consumptionMonth, \
# 	ToValidDate(AB) as validFromDate, \
# 	ToValidDate(BIS,'MANDATORY') as validToDate, \
# 	BUCHREL as billingLineItemReleventPostingIndicator, \
# 	STGRQNT as quantityStatisticsGroupCode, \
# 	STGRAMT as amountStatisticsGroupCode, \
# 	ARTMENGE as billedQuantityStatisticsCode, \
# 	KOKRS as controllingArea, \
# 	PRCTR as profitCenter, \
# 	PS_PSP_PNR as wbsElement, \
# 	WAERS as currencyKey, \
# 	MASSBILL as billingMeasurementUnitCode, \
# 	cast(BETRAG as dec(18,6)) as billingLineItemNetAmount, \
# 	cast(MENGE as dec(18,6)) as billingQuantity, \
# 	ZZAGREEMENT_NUM as agreementNumber, \
# 	cast(PREISBTR as dec(18,6)) as price, \
# 	CRM_PRODUCT as crmProduct, \
# 	BELZART_NAME as lineItemType, \
# 	PRINTDOCLINE as printDocumentLineItemId, \
# 	ANLAGE as installationId, \
# 	CITY_CODE as cityCode, \
# 	COUNTRY as countryShortName, \
# 	REGION as stateCode, \
# 	REGPOLIT as politicalRegionCode, \
# 	SALESEMPLOYEE as salesEmployee, \
# 	_RecordStart, \
# 	_RecordEnd, \
# 	_RecordDeleted, \
# 	_RecordCurrent \
# 	FROM {ADS_DATABASE_STAGE}.{source_object}")

# print(f'Number of rows: {df_cleansed.count()}')

# COMMAND ----------

newSchema = StructType([
	StructField('simulationPeriodId',StringType(),False,
	StructField('billingDocumentNumber',StringType(),False,
	StructField('companyCode',StringType(),True,
	StructField('divisonCode',StringType(),True,
	StructField('contractAccountNumber',StringType(),True,
	StructField('contractId',StringType(),True,
	StructField('billingTransactionCode',StringType(),True,
	StructField('mainTransactionLineItemCode',StringType(),True,
	StructField('accountDeterminationId',StringType(),True,
	StructField('portionNumber',StringType(),True,
	StructField('numberOfContractDaysBilled',IntegerType(),True,
	StructField('numberOfBilledContracts',IntegerType(),True,
	StructField('numberOfBillingDocuments',IntegerType(),True,
	StructField('billingDocumentLineItemId',StringType(),False,
	StructField('lineItemTypeCode',StringType(),True,
	StructField('billingClassCode',StringType(),True,
	StructField('subtransactionLineItemCode',StringType(),True,
	StructField('rateTypeCode',StringType(),True,
	StructField('statisticalAnalysisRateType',StringType(),True,
	StructField('statisticalRate',StringType(),True,
	StructField('consumptionMonth',StringType(),True,
	StructField('validFromDate',DateType(),True,
	StructField('validToDate',DateType(),False,
	StructField('billingLineItemReleventPostingIndicator',StringType(),True,
	StructField('quantityStatisticsGroupCode',StringType(),True,
	StructField('amountStatisticsGroupCode',StringType(),True,
	StructField('billedQuantityStatisticsCode',StringType(),True,
	StructField('controllingArea',StringType(),True,
	StructField('profitCenter',StringType(),True,
	StructField('wbsElement',StringType(),True,
	StructField('currencyKey',StringType(),True,
	StructField('billingMeasurementUnitCode',StringType(),True,
	StructField('billingLineItemNetAmount',DecimalType(18,6),True,
	StructField('billingQuantity',DecimalType(18,6),True,
	StructField('agreementNumber',StringType(),True,
	StructField('priceAmount',DecimalType(18,6),True,
	StructField('crmProduct',StringType(),True,
	StructField('lineItemType',StringType(),True,
	StructField('printDocumentLineItemId',StringType(),True,
	StructField('installationId',StringType(),True,
	StructField('cityCode',StringType(),True,
	StructField('countryShortName',StringType(),True,
	StructField('stateCode',StringType(),True,
	StructField('politicalRegionCode',StringType(),True,
	StructField('salesEmployee',IntegerType(),True,
	StructField('_RecordStart',TimestampType(),False),
	StructField('_RecordEnd',TimestampType(),False),
	StructField('_RecordDeleted',IntegerType(),False),
	StructField('_RecordCurrent',IntegerType(),False),
    StructField('_DLCleansedZoneTimeStamp',TimestampType(),False)
])


# COMMAND ----------

# DBTITLE 1,Handle Invalid Records
reject_df =df.where("validToDate = '1000-01-01'")
df = df.subtract(reject_df)
df = df.drop("sourceKeyDesc","sourceKey","rejectColumn")

# COMMAND ----------

# DBTITLE 1,12. Save Data frame into Cleansed Delta table (Final)
DeltaSaveDataFrameToDeltaTable(df, target_table, ADS_DATALAKE_ZONE_CLEANSED, ADS_DATABASE_CLEANSED, data_lake_folder, ADS_WRITE_MODE_MERGE, newSchema, track_changes, is_delta_extract, business_key, AddSKColumn = False, delta_column = "", start_counter = "0", end_counter = "0")

# COMMAND ----------

# DBTITLE 1,12.1 Save Reject Data Frame into Rejected Database
if reject_df.count() > 0:
    source_key = 'SIMRUNID|BELNR|BELZEILE|BIS'
    DeltaSaveDataFrameToRejectTable(reject_df,target_table,business_key,source_key,LastSuccessfulExecutionTS)

# COMMAND ----------

# DBTITLE 1,13. Exit Notebook
dbutils.notebook.exit("1")
