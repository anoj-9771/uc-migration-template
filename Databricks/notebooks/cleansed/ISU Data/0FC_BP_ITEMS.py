# Databricks notebook source
# DBTITLE 1,Generate parameter and source object name for unit testing
import json
#For unit testing...
#Use this string in the Param widget: 
#{"SourceType": "BLOB Storage (json)", "SourceServer": "daf-sa-lake-sastoken", "SourceGroup": "ISU", "SourceName": "ISU_0FC_BP_ITEMS", "SourceLocation": "ISU/0FC_BP_ITEMS", "AdditionalProperty": "", "Processor": "databricks-token|0711-011053-turfs581|Standard_DS3_v2|8.3.x-scala2.12|2:8|interactive", "IsAuditTable": false, "SoftDeleteSource": "", "ProjectName": "ISUDATA", "ProjectId": 2, "TargetType": "BLOB Storage (json)", "TargetName": "ISU_0FC_BP_ITEMS", "TargetLocation": "ISU/0FC_BP_ITEMS", "TargetServer": "daf-sa-lake-sastoken", "DataLoadMode": "FULL-EXTRACT", "DeltaExtract": false, "CDCSource": false, "TruncateTarget": false, "UpsertTarget": true, "AppendTarget": null, "TrackChanges": false, "LoadToSqlEDW": true, "TaskName": "ISU_0FC_BP_ITEMS", "ControlStageId": 2, "TaskId": 46, "StageSequence": 200, "StageName": "Raw to Cleansed", "SourceId": 46, "TargetId": 46, "ObjectGrain": "Day", "CommandTypeId": 8, "Watermarks": "", "WatermarksDT": null, "WatermarkColumn": "", "BusinessKeyColumn": "contractAccountNumber,repetitionItem,itemNumber,partialClearingSubitem", "UpdateMetaData": null, "SourceTimeStampFormat": "", "Command": "", "LastLoadedFile": null}

#Use this string in the Source Object widget
#ISU_0FC_BP_ITEMS

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
                                case when OPBEL = 'na' then '' else OPBEL end as contractAccountNumber, \
                                case when OPUPW = 'na' then '' else OPUPW end as repetitionItem, \
                                case when OPUPK = 'na' then '' else OPUPK end as itemNumber, \
                                case when OPUPZ = 'na' then '' else OPUPZ end as partialClearingSubitem, \
                                BUKRS as companyCode, \
                                cc.companyName as company, \
                                AUGST as clearingStatus, \
                                GPART as businessPartnerGroupNumber, \
                                VTREF as contractReferenceSpecification, \
                                VKONT as contractAccountNumber, \
                                ABWBL as ficaDocumentNumber, \
                                ABWTP as ficaDocumentCategory, \
                                APPLK as applicationArea, \
                                HVORG as mainTransactionLineItemCode, \
                                ho.mainTransaction as mainTransactionLineItem, \
                                TVORG as subtransactionLineItemCode, \
                                to.subtransaction as subtransactionLineItem, \
                                KOFIZ as accountDeterminationCode, \
                                fc.accountDetermination as accountDetermination, \
                                SPART as divisionCode, \
                                HKONT as accountGeneralLedger, \
                                MWSKZ as taxSalesCode, \
                                XANZA as downPaymentIndicator, \
                                STAKZ as statisticalItemType, \
                                to_date(C4EYP, 'yyyy-MM-dd') as documentDate, \
                                to_date(CPUDT, 'yyyy-MM-dd') as documentEnteredDate, \
                                WAERS as currencyKey, \
                                to_date(FAEDN, 'yyyy-MM-dd') as paymentDueDate, \
                                to_date(FAEDS, 'yyyy-MM-dd') as cashDiscountDueDate, \
                                to_date(STUDT, 'yyyy-MM-dd') as deferralToDate, \
                                cast(SKTPZ as dec(5,2)) as cashDiscountPercentageRate, \
                                cast(BETRH as dec(13,2)) as amountLocalCurrency, \
                                BLART as documentTypeCode, \
                                cast(SKFBT as dec(13,2)) as amountEligibleCashDiscount, \
                                cast(SBETH as dec(13,2)) as taxAmountLocalCurrency, \
                                cast(SBETW as dec(13,2)) as taxAmount, \
                                to_date(AUGDT, 'yyyy-MM-dd') as clearingDate, \
                                AUGBL as clearingDocument, \
                                to_date(AUGBD, 'yyyy-MM-dd') as clearingDocumentPostingDate, \
                                AUGRD as clearingReason, \
                                AUGWA as clearingCurrency, \
                                cast(AUGBT as dec(13,2)) as clearingAmount, \
                                cast(AUGBS as dec(13,2)) as taxAmount, \
                                cast(AUGSK as dec(13,2)) as cashDiscount, \
                                to_date(AUGVD, 'yyyy-MM-dd') as clearingValueDate, \
                                to_date(ABWTP, 'yyyy-MM-dd') as settlementPeriodLowerLimit, \
                                to_date(ABWBL, 'yyyy-MM-dd') as billingPeriodUpperLimit, \
                                AUGRS as clearingRestriction, \
                                INFOZ as valueAdjustment, \
                                BLART as documentTypeCode, \
                                bl.documentType as documentType, \
                                XPYOR as referenceDocumentNumber, \
                                INKPS as collectionItem, \
                                C4EYE as checkReason, \
                                cast(SCTAX as dec(13,2)) as taxPortion, \
                                cast(STTAX as dec(13,2)) as taxAmountDocument, \
                                ABGRD as writeOffReasonCode, \
                                HERKF as documentOriginCode, \
                                to_date(CPUDT, 'yyyy-MM-dd') as documentEnteredDate, \
                                AWTYP as referenceProcedure, \
                                AWKEY as objectKey, \
                                STORB as reversalDocumentNumber, \
                                FIKEY as reconciliationKeyForGeneralLedger, \
                                XCLOS as furtherPostingIndicator, \
                                bp._RecordStart, \
                                bp._RecordEnd, \
                                bp._RecordDeleted, \
                                bp._RecordCurrent \
                        FROM {ADS_DATABASE_STAGE}.{source_object} bp \
                        LEFT OUTER JOIN {ADS_DATABASE_CLEANSED}.isu_0COMP_CODE_TEXT cc ON bp.BUKRS = cc.companyCode and cc._RecordDeleted = 0 and cc._RecordCurrent = 1 \
                        LEFT OUTER JOIN {ADS_DATABASE_CLEANSED}.isu_0UC_HVORG_TEXT ho ON bp.APPLK = ho.applicationArea and bp.HVORG = ho.mainTransactionLineItemCode \
                                                                                         and ho._RecordDeleted = 0 and ho._RecordCurrent = 1 \
                        LEFT OUTER JOIN {ADS_DATABASE_CLEANSED}.isu_0UC_TVORG_TEXT to ON bp.APPLK = to.applicationArea and bp.HVORG = to.mainTransactionLineItemCode \
                                                                                         and bp.TVORG = to.subtransactionLineItemCode and to._RecordDeleted = 0 and to._RecordCurrent = 1 \
                        LEFT OUTER JOIN {ADS_DATABASE_CLEANSED}.isu_0FC_BLART_TEXT bl ON bp.APPLK = bl.applicationArea and bp.BLART = bl.documentTypeCode \
                                                                                         and bl._RecordDeleted = 0 and bl._RecordCurrent = 1 \
                        LEFT OUTER JOIN {ADS_DATABASE_CLEANSED}.isu_0FCACTDETID_TEXT fc ON bp.KOFIZ = fc.accountDeterminationCode and fc._RecordDeleted = 0 and fc._RecordCurrent = 1")
                        
display(df_cleansed)
print(f'Number of rows: {df_cleansed.count()}')

# COMMAND ----------

newSchema = StructType([
	StructField('contractAccountNumber',StringType(),False),
	StructField('repetitionItem',StringType(),False),
	StructField('itemNumber',StringType(),False),
	StructField('partialClearingSubitem',StringType(),False),
	StructField('companyCode',StringType(),True),
	StructField('company',StringType(),True),
	StructField('clearingStatus',StringType(),True),
	StructField('businessPartnerGroupNumber',StringType(),True),
	StructField('contractReferenceSpecification',StringType(),True),
	StructField('contractAccountNumber',StringType(),True),
	StructField('ficaDocumentNumber',StringType(),True),
	StructField('ficaDocumentCategory',StringType(),True),
	StructField('applicationArea',StringType(),True),
	StructField('mainTransactionLineItemCode',StringType(),True),
	StructField('mainTransactionLineItem',StringType(),True),
	StructField('subtransactionLineItemCode',StringType(),True),
	StructField('subtransactionLineItem',StringType(),True),
	StructField('accountDeterminationCode',StringType(),True),
	StructField('accountDetermination',StringType(),True),
	StructField('divisionCode',StringType(),True),
	StructField('accountGeneralLedger',StringType(),True),
	StructField('taxSalesCode',StringType(),True),
	StructField('downPaymentIndicator',StringType(),True),
	StructField('statisticalItemType',StringType(),True),
	StructField('documentDate',DateType(),True),
	StructField('documentEnteredDate',DateType(),True),
	StructField('currencyKey',StringType(),True),
	StructField('paymentDueDate',DateType(),True),
	StructField('cashDiscountDueDate',DateType(),True),
	StructField('deferralToDate',DateType(),True),
	StructField('cashDiscountPercentageRate',DecimalType(5,2),True),
	StructField('amountLocalCurrency',DecimalType(13,2),True),
	StructField('documentTypeCode',StringType(),True),
	StructField('amountEligibleCashDiscount',DecimalType(13,2),True),
	StructField('taxAmountLocalCurrency',DecimalType(13,2),True),
	StructField('taxAmount',DecimalType(13,2),True),
	StructField('clearingDate',DateType(),True),
	StructField('clearingDocument',StringType(),True),
	StructField('clearingDocumentPostingDate',DateType(),True),
	StructField('clearingReason',StringType(),True),
	StructField('clearingCurrency',StringType(),True),
	StructField('clearingAmount',DecimalType(13,2),True),
	StructField('taxAmount',DecimalType(13,2),True),
	StructField('cashDiscount',DecimalType(13,2),True),
	StructField('clearingValueDate',DateType(),True),
	StructField('settlementPeriodLowerLimit',DateType(),True),
	StructField('billingPeriodUpperLimit',DateType(),True),
	StructField('clearingRestriction',StringType(),True),
	StructField('valueAdjustment',StringType(),True),
	StructField('documentTypeCode',StringType(),True),
	StructField('documentType',StringType(),True),
	StructField('referenceDocumentNumber',StringType(),True),
	StructField('collectionItem',StringType(),True),
	StructField('checkReason',StringType(),True),
	StructField('taxPortion',DecimalType(13,2),True),
	StructField('taxAmountDocument',DecimalType(13,2),True),
	StructField('writeOffReasonCode',StringType(),True),
	StructField('documentOriginCode',StringType(),True),
	StructField('documentEnteredDate',DateType(),True),
	StructField('referenceProcedure',StringType(),True),
	StructField('objectKey',StringType(),True),
	StructField('reversalDocumentNumber',StringType(),True),
	StructField('reconciliationKeyForGeneralLedger',StringType(),True),
	StructField('furtherPostingIndicator',StringType(),True),
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