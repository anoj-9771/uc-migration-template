# Databricks notebook source
# DBTITLE 1,Generate parameter and source object name for unit testing
import json
#For unit testing...
#Use this string in the Param widget: 
#{"SourceType":"BLOB Storage (json)","SourceServer":"daf-sa-blob-sastoken","SourceGroup":"isudata","SourceName":"isu_ERCH","SourceLocation":"isudata/ERCH","AdditionalProperty":"","Processor":"databricks-token|1018-021846-1a1ycoqc|Standard_DS3_v2|8.3.x-scala2.12|2:8|interactive","IsAuditTable":false,"SoftDeleteSource":"","ProjectName":"CLEANSED ISU DATA","ProjectId":12,"TargetType":"BLOB Storage (json)","TargetName":"isu_ERCH","TargetLocation":"isudata/ERCH","TargetServer":"daf-sa-lake-sastoken","DataLoadMode":"INCREMENTAL","DeltaExtract":true,"CDCSource":false,"TruncateTarget":false,"UpsertTarget":true,"AppendTarget":null,"TrackChanges":false,"LoadToSqlEDW":true,"TaskName":"isu_ERCH","ControlStageId":2,"TaskId":228,"StageSequence":200,"StageName":"Raw to Cleansed","SourceId":228,"TargetId":228,"ObjectGrain":"Day","CommandTypeId":8,"Watermarks":"2000-01-01 00:00:00","WatermarksDT":"2000-01-01T00:00:00","WatermarkColumn":"_FileDateTimeStamp","BusinessKeyColumn":"billingDocumentNumber","PartitionColumn":null,"UpdateMetaData":null,"SourceTimeStampFormat":"","WhereClause":"","Command":"/build/cleansed/ISU Data/ERCH","LastSuccessfulExecutionTS":"2000-01-01T23:46:12.39","LastLoadedFile":null}

#Use this string in the Source Object widget
#$GROUP_$SOURCE

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
# MAGIC #Load data from Raw Zone to Trusted Zone

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
                      (select *, 'U' as _upsertFlag from (Select *, ROW_NUMBER() OVER (PARTITION BY BELNR ORDER BY _DLRawZoneTimestamp DESC, DELTA_TS DESC) AS _RecordVersion FROM {delta_raw_tbl_name} WHERE _DLRawZoneTimestamp >= '{LastSuccessfulExecutionTS}') where _RecordVersion = 1) \
                           select \
                                  case when BELNR = 'na' then '' else BELNR end as billingDocumentNumber, \
                                  BUKRS as companyCode, \
                                  cc.companyName as companyName, \
                                  SPARTE as divisionCode, \
                                  GPARTNER as businessPartnerGroupNumber, \
                                  VKONT as contractAccountNumber, \
                                  VERTRAG as contractId, \
                                  ToValidDate(BEGABRPE) as  startBillingPeriod, \
                                  ToValidDate(ENDABRPE) as  endBillingPeriod, \
                                  ToValidDate(ABRDATS) as  billingScheduleDate, \
                                  ToValidDate(ADATSOLL) as  meterReadingScheduleDate, \
                                  ToValidDate(PTERMTDAT) as  billingPeriodEndDate, \
                                  ToValidDate(BELEGDAT) as  billingDocumentCreateDate, \
                                  ABWVK as alternativeContractAccountForCollectiveBills, \
                                  BELNRALT as previousDocumentNumber, \
                                  ToValidDate(STORNODAT) as  reversalDate, \
                                  ABRVORG as billingTransactionCode, \
                                  HVORG as mainTransactionCode, \
                                  hvorg_text.mainTransaction as mainTransaction,\
                                  KOFIZ as contractAccountDeterminationId, \
                                  PORTION as portionNumber, \
                                  FORMULAR as formName, \
                                  SIMULATION as billingSimulationIndicator, \
                                  BELEGART as documentTypeCode, \
                                  BERGRUND as backbillingCreditReasonCode, \
                                  ToValidDate(BEGNACH) as  backbillingStartPeriod, \
                                  case when TOBRELEASD = 'X' then 'Y' else 'N' end as documentNotReleasedFlag, \
                                  TXJCD as taxJurisdictionDescription, \
                                  KONZVER as franchiseContractCode, \
                                  EROETIM as billingDocumentCreateTime, \
                                  case when ERCHO_V = 'X' then 'Y' else 'N' end as erchoExistFlag, \
                                  case when ERCHZ_V = 'X' then 'Y' else 'N' end as erchzExistFlag, \
                                  case when ERCHU_V = 'X' then 'Y' else 'N' end as erchuExistFlag, \
                                  case when ERCHR_V = 'X' then 'Y' else 'N' end as erchrExistFlag, \
                                  case when ERCHC_V = 'X' then 'Y' else 'N' end as erchcExistFlag, \
                                  case when ERCHV_V = 'X' then 'Y' else 'N' end as erchvExistFlag, \
                                  case when ERCHT_V = 'X' then 'Y' else 'N' end as erchtExistFlag, \
                                  case when ERCHP_V = 'X' then 'Y' else 'N' end as erchpExistFlag, \
                                  ABRVORG2 as periodEndBillingTransactionCode, \
                                  ABLEINH as meterReadingUnit, \
                                  ENDPRIO as billingEndingPriorityCode, \
                                  ToValidDate(ERDAT) as  createdDate, \
                                  ERNAM as createdBy, \
                                  ToValidDate(AEDAT) as  lastChangedDate, \
                                  AENAM as changedBy, \
                                  BEGRU as authorizationGroupCode, \
                                  (CASE WHEN LOEVM IS NULL THEN 'N' ELSE 'Y' END) as deletedFlag, \
                                  ToValidDate(ABRDATSU) as  suppressedBillingOrderScheduleDate, \
                                  ABRVORGU as suppressedBillingOrderTransactionCode, \
                                  N_INVSEP as jointInvoiceAutomaticDocumentIndicator, \
                                  ABPOPBEL as budgetBillingPlanCode, \
                                  MANBILLREL as manualDocumentReleasedInvoicingIndicator, \
                                  BACKBI as backbillingTypeCode, \
                                  PERENDBI as billingPeriodEndType, \
                                  cast(NUMPERBB as integer) as backbillingPeriodNumber, \
                                  ToValidDate(BEGEND) as  periodEndBillingStartDate, \
                                  ENDOFBB as backbillingPeriodEndIndicator, \
                                  ENDOFPEB as billingPeriodEndIndicator, \
                                  cast(NUMPERPEB as integer) as billingPeriodEndCount, \
                                  SC_BELNR_H as billingDocumentAdjustmentReversalCount, \
                                  SC_BELNR_N as billingDocumentNumberForAdjustmentReversal, \
                                  ToValidDate(ZUORDDAA) as  billingAllocationDate, \
                                  BILLINGRUNNO as billingRunNumber, \
                                  SIMRUNID as simulationPeriodId, \
                                  KTOKLASSE as accountClassCode, \
                                  ORIGDOC as billingDocumentOriginCode, \
                                  NOCANC as billingDonotExecuteIndicator, \
                                  ABSCHLPAN as billingPlanAdjustIndicator, \
                                  MEM_OPBEL as newBillingDocumentNumberForReversedInvoicing, \
                                  ToValidDate(MEM_BUDAT) as  billingPostingDateInDocument, \
                                  EXBILLDOCNO as externalDocumentNumber, \
                                  BCREASON as reversalReasonCode, \
                                  NINVOICE as billingDocumentWithoutInvoicingCode, \
                                  NBILLREL as billingRelevancyIndicator, \
                                  ToValidDate(CORRECTION_DATE) as  errorDetectedDate, \
                                  BASDYPER as basicCategoryDynamicPeriodControlCode, \
                                  ESTINBILL as meterReadingResultEstimatedBillingIndicator, \
                                  ESTINBILLU as suppressedOrderEstimateBillingIndicator, \
                                  ESTINBILL_SAV as originalValueEstimateBillingIndicator, \
                                  ESTINBILL_USAV as suppressedOrderBillingIndicator, \
                                  ACTPERIOD as currentBillingPeriodCategoryCode, \
                                  ACTPERORG as toBeBilledPeriodOriginalCategoryCode, \
                                  EZAWE as incomingPaymentMethodCode, \
                                  DAUBUCH as standingOrderIndicator, \
                                  FDGRP as planningGroupNumber, \
                                  BILLING_PERIOD as billingKeyDate, \
                                  OSB_GROUP as onsiteBillingGroupCode, \
                                  BP_BILL as resultingBillingPeriodIndicator, \
                                  MAINDOCNO as billingDocumentPrimaryInstallationNumber, \
                                  INSTGRTYPE as instalGroupTypeCode, \
                                  INSTROLE as instalGroupRoleCode,  \
                                  dd07t.domainValueText as billingTransaction,\
                                  divText.division as division,\
                                  pText.portionText as portion,\
                                  dd07t2.domainValueText as billingDocumentWithoutInvoicing, \
                                  '' as documentType,\
                                  cast('1900-01-01' as TimeStamp) as _RecordStart, \
                                  cast('9999-12-31' as TimeStamp) as _RecordEnd, \
                                  (CASE WHEN LOEVM IS NULL THEN 'N' ELSE 'Y' END) as _RecordDeleted, \
                                  '1' as _RecordCurrent, \
                                  cast('{CurrentTimeStamp}' as TimeStamp) as _DLCleansedZoneTimeStamp \
                        FROM stage stg \
                               left outer join {ADS_DATABASE_CLEANSED}.isu_0comp_code_text cc on cc.companyCode = stg.BUKRS \
                               left outer join {ADS_DATABASE_CLEANSED}.isu_0division_text divText on divText.divisionCode = stg.SPARTE \
                               left outer join {ADS_DATABASE_CLEANSED}.isu_0uc_portion_text pText on pText.portionNumber = stg.PORTION \
                               left outer join {ADS_DATABASE_CLEANSED}.isu_0uc_hvorg_text hvorg_text on hvorg_text.maintransactionLineItemCode = stg.HVORG \
                               left outer join {ADS_DATABASE_CLEANSED}.isu_dd07t dd07t on dd07t.domainName = 'ABRVORG' and dd07t.domainValueSingleUpperLimit = stg.ABRVORG \
                               left outer join {ADS_DATABASE_CLEANSED}.isu_dd07t dd07t2 on dd07t2.domainName = 'NINVOICE' and dd07t2.domainValueSingleUpperLimit = stg.NINVOICE \
                        ")

# COMMAND ----------

newSchema = StructType([
                          StructField('billingDocumentNumber', StringType(), False),
                          StructField('companyCode', StringType(), True),
                          StructField('companyName', StringType(), True),
                          StructField('divisionCode', StringType(), True),
                          StructField('businessPartnerGroupNumber', StringType(), True),
                          StructField('contractAccountNumber', StringType(), True),
                          StructField('contractId', StringType(), True),
                          StructField('startBillingPeriod', DateType(), True),
                          StructField('endBillingPeriod', DateType(), True),
                          StructField('billingScheduleDate', DateType(), True),
                          StructField('meterReadingScheduleDate', DateType(), True),
                          StructField('billingPeriodEndDate', DateType(), True),
                          StructField('billingDocumentCreateDate', DateType(), True),
                          StructField('alternativeContractAccountForCollectiveBills', StringType(), True),
                          StructField('previousDocumentNumber', StringType(), True),
                          StructField('reversalDate', DateType(), True),
                          StructField('billingTransactionCode', StringType(), True),
                          StructField('mainTransactionCode', StringType(), True),
                          StructField('mainTransaction', StringType(), True),
                          StructField('contractAccountDeterminationId', StringType(), True),
                          StructField('portionNumber', StringType(), True),
                          StructField('formName', StringType(), True),
                          StructField('billingSimulationIndicator', StringType(), True),
                          StructField('documentTypeCode', StringType(), True),
                          StructField('backbillingCreditReasonCode', StringType(), True),
                          StructField('backbillingStartPeriod', DateType(), True),
                          StructField('documentNotReleasedFlag', StringType(), True),
                          StructField('taxJurisdictionDescription', StringType(), True),
                          StructField('franchiseContractCode', StringType(), True),
                          StructField('billingDocumentCreateTime', StringType(), True),
                          StructField('erchoExistFlag', StringType(), True),
                          StructField('erchzExistFlag', StringType(), True),
                          StructField('erchuExistFlag', StringType(), True),
                          StructField('erchrExistFlag', StringType(), True),
                          StructField('erchcExistFlag', StringType(), True),
                          StructField('erchvExistFlag', StringType(), True),
                          StructField('erchtExistFlag', StringType(), True),
                          StructField('erchpExistFlag', StringType(), True), 
                          StructField('periodEndBillingTransactionCode', StringType(), True),
                          StructField('meterReadingUnit', StringType(), True),
                          StructField('billingEndingPriorityCode', StringType(), True),
                          StructField('createdDate', DateType(), True),
                          StructField('createdBy', StringType(), True),
                          StructField('lastChangedDate', DateType(), True),
                          StructField('changedBy', StringType(), True),
                          StructField('authorizationGroupCode', StringType(), True),
                          StructField('deletedFlag', StringType(), True),
                          StructField('suppressedBillingOrderScheduleDate', DateType(), True),
                          StructField('suppressedBillingOrderTransactionCode', StringType(), True),
                          StructField('jointInvoiceAutomaticDocumentIndicator', StringType(), True),
                          StructField('budgetBillingPlanCode', StringType(), True),
                          StructField('manualDocumentReleasedInvoicingIndicator', StringType(), True),
                          StructField('backbillingTypeCode', StringType(), True),
                          StructField('billingPeriodEndType', StringType(), True),
                          StructField('backbillingPeriodNumber', IntegerType(), True),
                          StructField('periodEndBillingStartDate', DateType(), True),
                          StructField('backbillingPeriodEndIndicator', StringType(), True),
                          StructField('billingPeriodEndIndicator', StringType(), True),
                          StructField('billingPeriodEndCount', IntegerType(), True),
                          StructField('billingDocumentAdjustmentReversalCount', StringType(), True),
                          StructField('billingDocumentNumberForAdjustmentReversal', StringType(), True),
                          StructField('billingAllocationDate', DateType(), True),
                          StructField('billingRunNumber', StringType(), True),
                          StructField('simulationPeriodId', StringType(), True),
                          StructField('accountClassCode', StringType(), True),
                          StructField('billingDocumentOriginCode', StringType(), True),
                          StructField('billingDonotExecuteIndicator', StringType(), True),
                          StructField('billingPlanAdjustIndicator', StringType(), True),
                          StructField('newBillingDocumentNumberForReversedInvoicing', StringType(), True),
                          StructField('billingPostingDateInDocument', DateType(), True),
                          StructField('externalDocumentNumber', StringType(), True),
                          StructField('reversalReasonCode', StringType(), True),
                          StructField('billingDocumentWithoutInvoicingCode', StringType(), True),
                          StructField('billingRelevancyIndicator', StringType(), True),
                          StructField('errorDetectedDate', DateType(), True),
                          StructField('basicCategoryDynamicPeriodControlCode', StringType(), True),
                          StructField('meterReadingResultEstimatedBillingIndicator', StringType(), True),
                          StructField('suppressedOrderEstimateBillingIndicator', StringType(), True),
                          StructField('originalValueEstimateBillingIndicator', StringType(), True),
                          StructField('suppressedOrderBillingIndicator', StringType(), True),
                          StructField('currentBillingPeriodCategoryCode', StringType(), True),
                          StructField('toBeBilledPeriodOriginalCategoryCode', StringType(), True),
                          StructField('incomingPaymentMethodCode', StringType(), True),
                          StructField('standingOrderIndicator', StringType(), True),
                          StructField('planningGroupNumber', StringType(), True),
                          StructField('billingKeyDate', StringType(), True),
                          StructField('onsiteBillingGroupCode', StringType(), True),
                          StructField('resultingBillingPeriodIndicator', StringType(), True),
                          StructField('billingDocumentPrimaryInstallationNumber', StringType(), True),
                          StructField('instalGroupTypeCode', StringType(), True),
                          StructField('instalGroupRoleCode', StringType(), True),
                          StructField('billingTransaction', StringType(), True),
                          StructField('division', StringType(), True),
                          StructField('portion', StringType(), True),
                          StructField('billingDocumentWithoutInvoicing', StringType(), True),
                          StructField('documentType', StringType(), True),
                          StructField('_RecordStart', TimestampType(), True),
                          StructField('_RecordEnd', TimestampType(), False),
                          StructField('_RecordDeleted', IntegerType(), False),
                          StructField('_RecordCurrent', IntegerType(), False),
                          StructField('_DLCleansedZoneTimeStamp',TimestampType(),False)
])


# COMMAND ----------

# DBTITLE 1,12.1 Save Non Deleted Records Data frame into Cleansed Delta table (Final)
# Load Non deleted records same as earlier
DeltaSaveDataFrameToDeltaTable(df.filter("_RecordDeleted = '0'"), target_table, ADS_DATALAKE_ZONE_CLEANSED, ADS_DATABASE_CLEANSED, data_lake_folder, ADS_WRITE_MODE_MERGE, newSchema, track_changes, is_delta_extract, business_key, AddSKColumn = False, delta_column = "", start_counter = "0", end_counter = "0")

# COMMAND ----------

# DBTITLE 1,12.2 Save Deleted records Data frame into Cleansed Delta table
# Load deleted records to replace the existing Deleted records implementation logic
DeltaSaveDataFrameToDeltaTable(df.filter("_RecordDeleted = '1'"), target_table, ADS_DATALAKE_ZONE_CLEANSED, ADS_DATABASE_CLEANSED, data_lake_folder, ADS_WRITE_MODE_MERGE, newSchema, track_changes, is_delta_extract, business_key, AddSKColumn = False, delta_column = "", start_counter = "0", end_counter = "0")

# COMMAND ----------

# DBTITLE 1,13. Exit Notebook
dbutils.notebook.exit("1")
