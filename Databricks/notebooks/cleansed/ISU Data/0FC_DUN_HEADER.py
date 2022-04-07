# Databricks notebook source
# DBTITLE 1,Generate parameter and source object name for unit testing
import json
#For unit testing...
#Use this string in the Param widget: 
#{"SourceType": "BLOB Storage (json)", "SourceServer": "daf-sa-lake-sastoken", "SourceGroup": "ISU", "SourceName": "ISU_0FC_DUN_HEADER", "SourceLocation": "ISU/0FC_DUN_HEADER", "AdditionalProperty": "", "Processor": "databricks-token|0711-011053-turfs581|Standard_DS3_v2|8.3.x-scala2.12|2:8|interactive", "IsAuditTable": false, "SoftDeleteSource": "", "ProjectName": "ISUDATA", "ProjectId": 2, "TargetType": "BLOB Storage (json)", "TargetName": "ISU_0FC_DUN_HEADER", "TargetLocation": "ISU/0FC_DUN_HEADER", "TargetServer": "daf-sa-lake-sastoken", "DataLoadMode": "FULL-EXTRACT", "DeltaExtract": false, "CDCSource": false, "TruncateTarget": false, "UpsertTarget": true, "AppendTarget": null, "TrackChanges": false, "LoadToSqlEDW": true, "TaskName": "ISU_0FC_DUN_HEADER", "ControlStageId": 2, "TaskId": 46, "StageSequence": 200, "StageName": "Raw to Cleansed", "SourceId": 46, "TargetId": 46, "ObjectGrain": "Day", "CommandTypeId": 8, "Watermarks": "", "WatermarksDT": null, "WatermarkColumn": "", "BusinessKeyColumn": "dateId,additionalIdentificationCharacteristic,businessPartnerGroupNumber,contractAccountNumber,dunningNoticeCounter", "UpdateMetaData": null, "SourceTimeStampFormat": "", "Command": "", "LastLoadedFile": null}

#Use this string in the Source Object widget
#ISU_0FC_DUN_HEADER

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
delta_cleansed_tbl_name = f'{ADS_DATABASE_CLEANSED}.{target_table}'
delta_raw_tbl_name = f'{ADS_DATABASE_RAW}.{ source_object}'

#Destination
print(delta_cleansed_tbl_name)
print(delta_raw_tbl_name)


# COMMAND ----------

# DBTITLE 1,10. Load Raw to Dataframe & Do Transformations
df = spark.sql(f"WITH stage AS \
                      (Select *, ROW_NUMBER() OVER (PARTITION BY LAUFD,LAUFI,GPART,VKONT,MAZAE ORDER BY _FileDateTimeStamp DESC, DI_SEQUENCE_NUMBER DESC, _DLRawZoneTimeStamp DESC) AS _RecordVersion FROM {delta_raw_tbl_name} \
                                  WHERE _DLRawZoneTimestamp >= '{LastSuccessfulExecutionTS}') \
                           SELECT \
                                ToValidDate(LAUFD,'MANDATORY') as dateId, \
                                case when LAUFI = 'na' then '' else LAUFI end as additionalIdentificationCharacteristic, \
                                case when GPART = 'na' then '' else GPART end as businessPartnerGroupNumber, \
                                case when VKONT = 'na' then '' else VKONT end as contractAccountNumber, \
                                case when MAZAE = 'na' then '' else MAZAE end as dunningNoticeCounter, \
                                ABWBL as ficaDocumentNumber, \
                                ABWTP as ficaDocumentCategory, \
                                GSBER as businessArea, \
                                ToValidDate(AUSDT) as dateOfIssue, \
                                ToValidDate(MDRKD) as noticeExecutionDate, \
                                VKONTGRP as contractAccountGroup, \
                                cast(ITEMGRP as dec(15,0)) as dunningClosedItemGroup, \
                                STRAT as collectionStrategyCode, \
                                STEP as collectionStepCode, \
                                STEP_LAST as collectionStepLastDunning, \
                                OPBUK as companyCodeGroup, \
                                STDBK as standardCompanyCode, \
                                SPART as divisionCode, \
                                VTREF as contractReferenceSpecification, \
                                VKNT1 as leadingContractAccount, \
                                ABWMA as alternativeDunningRecipient, \
                                MAHNS as dunningLevel, \
                                WAERS as currencyKey, \
                                cast(MSALM as dec(13,2)) as dunningBalance, \
                                cast(RSALM as dec(13,2)) as totalDunningReductions, \
                                CHGID as chargesSchedule, \
                                cast(MGE1M as dec(13,2)) as dunningCharge1, \
                                MG1BL as documentNumber, \
                                MG1TY as chargeType, \
                                cast(MGE2M as dec(13,2)) as dunningCharge2, \
                                cast(MGE3M as dec(13,2)) as dunningCharge3, \
                                cast(MINTM as dec(13,2)) as dunningInterest, \
                                MIBEL as interestPostingDocument, \
                                BONIT as creditWorthiness, \
                                XMSTO as noticeReversedIndicator, \
                                NRZAS as paymentFormNumber, \
                                XCOLL as submittedIndicator, \
                                STAKZ as statisticalItemType, \
                                cast(SUCPC as dec(5,0)) as successRate, \
                                cast('1900-01-01' as TimeStamp) as _RecordStart, \
                                cast('9999-12-31' as TimeStamp) as _RecordEnd, \
                                '0' as _RecordDeleted, \
                                '1' as _RecordCurrent, \
                                cast('{CurrentTimeStamp}' as TimeStamp) as _DLCleansedZoneTimeStamp \
                        from stage where _RecordVersion = 1 ").cache()

print(f'Number of rows: {df.count()}')

# COMMAND ----------

# DBTITLE 1,11. Update/Rename Columns and Load into a Dataframe
#Update/rename Column
#Pass 'MANDATORY' as second argument to function ToValidDate() on key columns to ensure correct value settings for those columns
# df_cleansed = spark.sql(f"SELECT \
# 	ToValidDate(LAUFD,'MANDATORY') as dateId, \
# 	case when LAUFI = 'na' then '' else LAUFI end as additionalIdentificationCharacteristic, \
# 	case when GPART = 'na' then '' else GPART end as businessPartnerGroupNumber, \
# 	case when VKONT = 'na' then '' else VKONT end as contractAccountNumber, \
# 	case when MAZAE = 'na' then '' else MAZAE end as dunningNoticeCounter, \
#     ABWBL as ficaDocumentNumber, \
#     ABWTP as ficaDocumentCategory, \
#     GSBER as businessArea, \
# 	ToValidDate(AUSDT) as dateOfIssue, \
# 	ToValidDate(MDRKD) as noticeExecutionDate, \
# 	VKONTGRP as contractAccountGroup, \
# 	cast(ITEMGRP as dec(15,0)) as dunningClosedItemGroup, \
# 	STRAT as collectionStrategyCode, \
# 	STEP as collectionStepCode, \
# 	STEP_LAST as collectionStepLastDunning, \
# 	OPBUK as companyCodeGroup, \
# 	STDBK as standardCompanyCode, \
# 	SPART as divisionCode, \
# 	VTREF as contractReferenceSpecification, \
# 	VKNT1 as leadingContractAccount, \
# 	ABWMA as alternativeDunningRecipient, \
# 	MAHNS as dunningLevel, \
# 	WAERS as currencyKey, \
# 	cast(MSALM as dec(13,2)) as dunningBalance, \
# 	cast(RSALM as dec(13,2)) as totalDunningReductions, \
# 	CHGID as chargesSchedule, \
# 	cast(MGE1M as dec(13,2)) as dunningCharge1, \
# 	MG1BL as documentNumber, \
# 	MG1TY as chargeType, \
# 	cast(MGE2M as dec(13,2)) as dunningCharge2, \
# 	cast(MGE3M as dec(13,2)) as dunningCharge3, \
# 	cast(MINTM as dec(13,2)) as dunningInterest, \
# 	MIBEL as interestPostingDocument, \
# 	BONIT as creditWorthiness, \
# 	XMSTO as noticeReversedIndicator, \
# 	NRZAS as paymentFormNumber, \
# 	XCOLL as submittedIndicator, \
# 	STAKZ as statisticalItemType, \
# 	cast(SUCPC as dec(5,0)) as successRate, \
# 	_RecordStart, \
# 	_RecordEnd, \
# 	_RecordDeleted, \
# 	_RecordCurrent \
# 	FROM {ADS_DATABASE_STAGE}.{source_object}")

# print(f'Number of rows: {df_cleansed.count()}')

# COMMAND ----------

# newSchema = StructType([
# 	StructField('dateId',DateType(),False),
# 	StructField('additionalIdentificationCharacteristic',StringType(),False),
# 	StructField('businessPartnerGroupNumber',StringType(),False),
# 	StructField('contractAccountNumber',StringType(),False),
# 	StructField('dunningNoticeCounter',StringType(),False),
#     StructField('ficaDocumentNumber',StringType(),True),
#     StructField('ficaDocumentCategory',StringType(),True),
#     StructField('businessArea',StringType(),True),
# 	StructField('dateOfIssue',DateType(),True),
# 	StructField('noticeExecutionDate',DateType(),True),
# 	StructField('contractAccountGroup',StringType(),True),
# 	StructField('dunningClosedItemGroup',DecimalType(15,0),True),
# 	StructField('collectionStrategyCode',StringType(),True),
# 	StructField('collectionStepCode',StringType(),True),
# 	StructField('collectionStepLastDunning',StringType(),True),
# 	StructField('companyCodeGroup',StringType(),True),
# 	StructField('standardCompanyCode',StringType(),True),
# 	StructField('divisionCode',StringType(),True),
# 	StructField('contractReferenceSpecification',StringType(),True),
# 	StructField('leadingContractAccount',StringType(),True),
# 	StructField('alternativeDunningRecipient',StringType(),True),
# 	StructField('dunningLevel',StringType(),True),
# 	StructField('currencyKey',StringType(),True),
# 	StructField('dunningBalance',DecimalType(13,2),True),
# 	StructField('totalDunningReductions',DecimalType(13,2),True),
# 	StructField('chargesSchedule',StringType(),True),
# 	StructField('dunningCharge1',DecimalType(13,2),True),
# 	StructField('documentNumber',StringType(),True),
# 	StructField('chargeType',StringType(),True),
# 	StructField('dunningCharge2',DecimalType(13,2),True),
# 	StructField('dunningCharge3',DecimalType(13,2),True),
# 	StructField('dunningInterest',DecimalType(13,2),True),
# 	StructField('interestPostingDocument',StringType(),True),
# 	StructField('creditWorthiness',StringType(),True),
# 	StructField('noticeReversedIndicator',StringType(),True),
# 	StructField('paymentFormNumber',StringType(),True),
# 	StructField('submittedIndicator',StringType(),True),
# 	StructField('statisticalItemType',StringType(),True),
# 	StructField('successRate',DecimalType(5,0),True),
# 	StructField('_RecordStart',TimestampType(),False),
# 	StructField('_RecordEnd',TimestampType(),False),
# 	StructField('_RecordDeleted',IntegerType(),False),
# 	StructField('_RecordCurrent',IntegerType(),False)
# ])


# COMMAND ----------

# DBTITLE 1,12. Save Data frame into Cleansed Delta table (Final)
DeltaSaveDataFrameToDeltaTableNew(df, target_table, ADS_DATALAKE_ZONE_CLEANSED, ADS_DATABASE_CLEANSED, data_lake_folder, ADS_WRITE_MODE_MERGE, track_changes, is_delta_extract, business_key, AddSKColumn = False, delta_column = "", start_counter = "0", end_counter = "0")
#clear cache
df.unpersist()

# COMMAND ----------

# DBTITLE 1,13. Exit Notebook
dbutils.notebook.exit("1")
