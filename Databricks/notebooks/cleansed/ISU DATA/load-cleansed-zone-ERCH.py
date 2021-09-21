# Databricks notebook source
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
delta_cleansed_tbl_name = "{0}.{1}".format(ADS_DATABASE_CLEANSED, "stg_"+source_object)
delta_raw_tbl_name = "{0}.{1}".format(ADS_DATABASE_RAW, source_object)

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
df_cleansed_column = spark.sql("SELECT  \
                                  BELNR as billingDocumentNumber, \
                                  BUKRS as companyCode, \
                                  cc.companyName as companyName, \
                                  SPARTE as divisonCode, \
                                  GPARTNER as businessPartnerNumber, \
                                  VKONT as contractAccountNumber, \
                                  VERTRAG as contractID, \
                                  to_date(BEGABRPE, 'yyyy-MM-dd') as startBillingPeriod, \
                                  to_date(ENDABRPE, 'yyyy-MM-dd') as endBillingPeriod, \
                                  to_date(ABRDATS, 'yyyy-MM-dd') as billingScheduleDate, \
                                  to_date(ADATSOLL, 'yyyy-MM-dd') as meterReadingScheduleDate, \
                                  to_date(PTERMTDAT, 'yyyy-MM-dd') as billingPeriodEndDate, \
                                  to_date(BELEGDAT, 'yyyy-MM-dd') as billingDocumentCreateDate, \
                                  ABWVK as alternativeContractAccountForCollectiveBills, \
                                  BELNRALT as previousDocumentNumber, \
                                  to_date(STORNODAT, 'yyyy-MM-dd') as reversalDate, \
                                  ABRVORG as billingTransactionCode, \
                                  HVORG as mainTransactionLineItemCode, \
                                  KOFIZ as contractAccountDeterminationID, \
                                  PORTION as portionNumber, \
                                  FORMULAR as formName, \
                                  SIMULATION as billingSimulationIndicator, \
                                  BELEGART as documentTypeCode, \
                                  BERGRUND as backbillingCreditReasonCode, \
                                  to_date(BEGNACH, 'yyyy-MM-dd') as backbillingStartPeriod, \
                                  TOBRELEASD as DocumentNotReleasedIndicator, \
                                  TXJCD as taxJurisdictionDescription, \
                                  KONZVER as franchiseContractCode, \
                                  EROETIM as billingDocumentCreateTime, \
                                  ABRVORG2 as periodEndBillingTransactionCode, \
                                  ABLEINH as meterReadingUnit, \
                                  ENDPRIO as billingEndingPriorityCodfe, \
                                  to_date(ERDAT, 'yyyy-MM-dd hh24:mm:ss') as createdDate, \
                                  ERNAM as createdBy, \
                                  to_date(AEDAT, 'yyyy-MM-dd hh24:mm:ss') as lastChangedDate, \
                                  AENAM as changedBy, \
                                  BEGRU as authorizationGroupCode, \
                                  LOEVM as deletedIndicator, \
                                  to_date(ABRDATSU, 'yyyy-MM-dd') as suppressedBillingOrderScheduleDate, \
                                  ABRVORGU as suppressedBillingOrderTransactionCode, \
                                  N_INVSEP as jointInvoiceAutomaticDocumentIndicator, \
                                  ABPOPBEL as BudgetBillingPlanCode, \
                                  MANBILLREL as manualDocumentReleasedInvoicingIndicator, \
                                  BACKBI as backbillingTypeCode, \
                                  PERENDBI as billingPeriodEndType, \
                                  cast(NUMPERBB as integer) as backbillingPeriodNumber, \
                                  to_date(BEGEND, 'yyyy-MM-dd') as periodEndBillingStartDate, \
                                  ENDOFBB as backbillingPeriodEndIndicator, \
                                  ENDOFPEB as billingPeriodEndIndicator, \
                                  cast(NUMPERPEB as integer) as billingPeriodEndCount, \
                                  SC_BELNR_H as billingDoumentAdjustmentReversalCount, \
                                  SC_BELNR_N as billingDocumentNumberForAdjustmentReverssal, \
                                  to_date(ZUORDDAA, 'yyyy-MM-dd') as billingAllocationDate, \
                                  BILLINGRUNNO as billingRunNumber, \
                                  SIMRUNID as simulationPeriodID, \
                                  KTOKLASSE as accountClassCode, \
                                  ORIGDOC as billingDocumentOriginCode, \
                                  NOCANC as billingDonotExecuteIndicator, \
                                  ABSCHLPAN as billingPlanAdjustIndicator, \
                                  MEM_OPBEL as newBillingDocumentNumberForReversedInvoicing, \
                                  to_date(MEM_BUDAT, 'yyyy-MM-dd') as billingPostingDateInDocument, \
                                  EXBILLDOCNO as externalDocumentNumber, \
                                  BCREASON as reversalReasonCode, \
                                  NINVOICE as billingDocumentWithoutInvoicingCode, \
                                  NBILLREL as billingRelavancyIndicator, \
                                  to_date(CORRECTION_DATE, 'yyyy-MM-dd') as errorDetectedDate, \
                                  BASDYPER as basicCategoryDynamicPeriodControlCode, \
                                  ESTINBILL as meterReadingResultEstimatedBillingIndicator, \
                                  ESTINBILLU as SuppressedOrderEstimateBillingIndicator, \
                                  ESTINBILL_SAV as originalValueEstimateBillingIndicator, \
                                  ESTINBILL_USAV as suppressedOrderBillingIndicator, \
                                  ACTPERIOD as currentBillingPeriodCategoryCode, \
                                  ACTPERORG as toBeBilledPeriodOriginalCategoryCode, \
                                  EZAWE as incomingPaymentMethodCode, \
                                  DAUBUCH as standingOrderIndicator, \
                                  FDGRP as planningGroupNumber, \
                                  to_date(BILLING_PERIOD, 'yyyy-MM-dd') as billingKeyDate, \
                                  OSB_GROUP as onsiteBillingGroupCode, \
                                  BP_BILL as resultingBillingPeriodIndicator, \
                                  MAINDOCNO as billingDocumentPrimaryInstallationNumber, \
                                  INSTGRTYPE as instalGroupTypeCode, \
                                  INSTROLE as instalGroupRoleCode, \
                                  stg._RecordStart, \
                                  stg._RecordEnd, \
                                  stg._RecordDeleted, \
                                  stg._RecordCurrent \
                              FROM CLEANSED.STG_SAPISU_ERCH stg \
                               left outer join cleansed.t_sapisu_0comp_code_text cc on cc.companyCode = stg.BUKRS"
                              )
display(df_cleansed_column)

# COMMAND ----------

newSchema = StructType([
                          StructField('billingDocumentNumber', StringType(), False),
                          StructField('companyCode', StringType(), True),
                          StructField('companyName', StringType(), True),
                          StructField('divisonCode', StringType(), True),
                          StructField('businessPartnerNumber', StringType(), True),
                          StructField('contractAccountNumber', StringType(), True),
                          StructField('contractID', StringType(), True),
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
                          StructField('mainTransactionLineItemCode', StringType(), True),
                          StructField('contractAccountDeterminationID', StringType(), True),
                          StructField('portionNumber', StringType(), True),
                          StructField('formName', StringType(), True),
                          StructField('billingSimulationIndicator', StringType(), True),
                          StructField('documentTypeCode', StringType(), True),
                          StructField('backbillingCreditReasonCode', StringType(), True),
                          StructField('backbillingStartPeriod', DateType(), True),
                          StructField('DocumentNotReleasedIndicator', StringType(), True),
                          StructField('taxJurisdictionDescription', StringType(), True),
                          StructField('franchiseContractCode', StringType(), True),
                          StructField('billingDocumentCreateTime', StringType(), True),
                          StructField('periodEndBillingTransactionCode', StringType(), True),
                          StructField('meterReadingUnit', StringType(), True),
                          StructField('billingEndingPriorityCodfe', StringType(), True),
                          StructField('createdDate', DateType(), True),
                          StructField('createdBy', StringType(), True),
                          StructField('lastChangedDate', DateType(), True),
                          StructField('changedBy', StringType(), True),
                          StructField('authorizationGroupCode', StringType(), True),
                          StructField('deletedIndicator', StringType(), True),
                          StructField('suppressedBillingOrderScheduleDate', DateType(), True),
                          StructField('suppressedBillingOrderTransactionCode', StringType(), True),
                          StructField('jointInvoiceAutomaticDocumentIndicator', StringType(), True),
                          StructField('BudgetBillingPlanCode', StringType(), True),
                          StructField('manualDocumentReleasedInvoicingIndicator', StringType(), True),
                          StructField('backbillingTypeCode', StringType(), True),
                          StructField('billingPeriodEndType', StringType(), True),
                          StructField('backbillingPeriodNumber', IntegerType(), True),
                          StructField('periodEndBillingStartDate', DateType(), True),
                          StructField('backbillingPeriodEndIndicator', StringType(), True),
                          StructField('billingPeriodEndIndicator', StringType(), True),
                          StructField('billingPeriodEndCount', IntegerType(), True),
                          StructField('billingDoumentAdjustmentReversalCount', StringType(), True),
                          StructField('billingDocumentNumberForAdjustmentReverssal', StringType(), True),
                          StructField('billingAllocationDate', DateType(), True),
                          StructField('billingRunNumber', StringType(), True),
                          StructField('simulationPeriodID', StringType(), True),
                          StructField('accountClassCode', StringType(), True),
                          StructField('billingDocumentOriginCode', StringType(), True),
                          StructField('billingDonotExecuteIndicator', StringType(), True),
                          StructField('billingPlanAdjustIndicator', StringType(), True),
                          StructField('newBillingDocumentNumberForReversedInvoicing', StringType(), True),
                          StructField('billingPostingDateInDocument', DateType(), True),
                          StructField('externalDocumentNumber', StringType(), True),
                          StructField('reversalReasonCode', StringType(), True),
                          StructField('billingDocumentWithoutInvoicingCode', StringType(), True),
                          StructField('billingRelavancyIndicator', StringType(), True),
                          StructField('errorDetectedDate', DateType(), True),
                          StructField('basicCategoryDynamicPeriodControlCode', StringType(), True),
                          StructField('meterReadingResultEstimatedBillingIndicator', StringType(), True),
                          StructField('SuppressedOrderEstimateBillingIndicator', StringType(), True),
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
                          StructField('_RecordStart', TimestampType(), True),
                          StructField('_RecordEnd', TimestampType(), False),
                          StructField('_RecordDeleted', IntegerType(), False),
                          StructField('_RecordCurrent', IntegerType(), False)
])

df_updated_column = spark.createDataFrame(df_cleansed_column.rdd, schema=newSchema)
display(df_updated_column)

# COMMAND ----------

# DBTITLE 1,12. Save Data frame into Cleansed Delta table (Final)
#Save Data frame into Cleansed Delta table (final)
DeltaSaveDataframeDirect(df_updated_column, "t", source_object, ADS_DATABASE_CLEANSED, ADS_CONTAINER_CLEANSED, "overwrite", "")


# COMMAND ----------

# DBTITLE 1,13. Exit Notebook
dbutils.notebook.exit("1")
