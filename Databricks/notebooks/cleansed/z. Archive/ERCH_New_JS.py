# Databricks notebook source
# DBTITLE 1,Generate parameter and source object name for unit testing
import json
#For unit testing...
#Use this string in the Param widget: 
#$PARAM

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

#-------------------------------------------------------#

# COMMAND ----------

# DBTITLE 1,NEW PARAMAS
#Set DeltaSavetoDeltaTable parameters
source_table = delta_raw_tbl_name
target_table = target_table
target_data_lake_zone = ADS_DATALAKE_ZONE_CLEANSED
target_database = ADS_DATABASE_STAGE
data_lake_folder = data_lake_folder
data_load_mode = data_load_mode
track_changes =  Params[PARAMS_TRACK_CHANGES]
is_delta_extract =  Params[PARAMS_DELTA_EXTRACT]
business_key =  'billingDocumentNumber' #Params[PARAMS_BUSINESS_KEY_COLUMN],
delta_column = delta_column
start_counter = start_counter
end_counter = end_counter

source_table = 'raw.isu_ERCH'
target_table = 'isu_ERCH'
target_data_lake_zone = 'cleansed'
target_database = 'cleansed'
data_lake_folder = 'isudata/stg'
data_load_mode = 'merge'
track_changes =  'False'
is_delta_extract =  'True'
business_key = 'billingDocumentNumber'
delta_column = 'DELTA_TS'
start_counter = '2022-03-02T02:49:1'
end_counter = '2022-03-06T11:47:04'

print(source_table)
print(target_table)
print(target_data_lake_zone)
print(target_database)
print(data_lake_folder)
print(data_load_mode)
print(track_changes)
print(is_delta_extract)
print(business_key)
print(delta_column)
print(start_counter)
print(end_counter)

# COMMAND ----------

# DBTITLE 1,10. (NEW) Load Raw to Dataframe & Do Transformations
df = spark.sql("WITH latestrecord AS (Select *, ROW_NUMBER() OVER (PARTITION BY BELNR ORDER BY DELTA_TS DESC) AS _RecordVersion FROM raw.isu_erch WHERE DELTA_TS >= '2021-11-18T20:20:00') select \
                                  case when BELNR = 'na' then '' else BELNR end as billingDocumentNumber, \
                                  BUKRS as companyCode, \
                                  'abc' as companyName, \
                                  SPARTE as divisonCode, \
                                  DELTA_TS as _DLRawZoneTimeStamp, \
                                  DELTA_TS as _DLCleansedZoneTimeStamp, \
                                  '1' as _RecordCurrent, \
                                  '0' as _RecordDeleted, \
                                  '2022-03-06T11:47:04' as _RecordEnd, \
                                  '2022-03-02T02:49:1' as _RecordStart, \
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
                                  HVORG as mainTransactionLineItemCode, \
                                  KOFIZ as contractAccountDeterminationId, \
                                  PORTION as portionNumber, \
                                  FORMULAR as formName, \
                                  SIMULATION as billingSimulationIndicator, \
                                  BELEGART as documentTypeCode, \
                                  BERGRUND as backbillingCreditReasonCode, \
                                  ToValidDate(BEGNACH) as  backbillingStartPeriod, \
                                  TOBRELEASD as documentNotReleasedIndicator, \
                                  TXJCD as taxJurisdictionDescription, \
                                  KONZVER as franchiseContractCode, \
                                  EROETIM as billingDocumentCreateTime, \
                                  ERCHO_V as erchoExistIndicator, \
                                  ERCHZ_V as erchzExistIndicator, \
                                  ERCHU_V as erchuExistIndicator, \
                                  ERCHR_V as erchrExistIndicator, \
                                  ERCHC_V as erchcExistIndicator, \
                                  ERCHV_V as erchvExistIndicator, \
                                  ERCHT_V as erchtExistIndicator, \
                                  ERCHP_V as erchpExistIndicator, \
                                  ABRVORG2 as periodEndBillingTransactionCode, \
                                  ABLEINH as meterReadingUnit, \
                                  ENDPRIO as billingEndingPriorityCode, \
                                  ToValidDate(ERDAT) as  createdDate, \
                                  ERNAM as createdBy, \
                                  ToValidDate(AEDAT) as  lastChangedDate, \
                                  AENAM as changedBy, \
                                  BEGRU as authorizationGroupCode, \
                                  LOEVM as deletedIndicator, \
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
                                  INSTROLE as instalGroupRoleCode  \
from latestrecord where _RecordVersion = 1 ")
display (df)

# COMMAND ----------

# DBTITLE 1,11. Save Dataframe to Staged & Cleansed
DeltaSaveDataFrameToDeltaTable(
  df, target_table, target_data_lake_zone, target_database, data_lake_folder, data_load_mode, track_changes = False, is_delta_extract = False, business_key = "billingDocumentNumber", AddSKColumn = False, delta_column = "", start_counter = "0", end_counter = "0")

# COMMAND ----------

# DBTITLE 1,13. Exit Notebook
dbutils.notebook.exit("1")

# COMMAND ----------

# DBTITLE 1,----- NEW NOTEBOOK FINISHES HERE ----------
------------------------------------------------------------------