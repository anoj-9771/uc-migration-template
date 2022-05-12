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
                      (Select *, ROW_NUMBER() OVER (PARTITION BY BELNR, BELZEILE ORDER BY _DLRawZoneTimestamp DESC, DELTA_TS DESC) AS _RecordVersion FROM {delta_raw_tbl_name} WHERE _DLRawZoneTimestamp >= '{LastSuccessfulExecutionTS}') \
                           SELECT  \
                                  case when BELNR = 'na' then '' else BELNR end as billingDocumentNumber, \
                                  case when BELZEILE = 'na' then '' else BELZEILE end as billingDocumentLineItemId, \
                                  CSNO as billingSequenceNumber, \
                                  BELZART as lineItemTypeCode, \
                                  li.lineItemType, \
                                  ABSLKZ as billingLineItemBudgetBillingIndicator, \
                                  DIFFKZ as lineItemDiscountStatisticsIndicator, \
                                  BUCHREL as billingLineItemRelevantPostingIndicator, \
                                  MENGESTREL as billedValueStatisticallyRelevantIndicator, \
                                  BETRSTREL as billingLineItemStatisticallyRelevantAmount, \
                                  STGRQNT as quantityStatisticsGroupCode, \
                                  STGRAMT as amountStatisticsGroupCode, \
                                  PRINTREL as billingLinePrintRelevantIndicator, \
                                  AKLASSE as billingClassCode, \
                                  bc.billingClass as billingClass, \
                                  '' as industryText, \
                                  BRANCHE as industry, \
                                  TVORG as subtransactionForDocumentItem, \
                                  GEGEN_TVORG as offsettingTransactionSubtransactionForDocumentItem, \
                                  LINESORT as presortingBillingLineItems, \
                                  ToValidDate(AB) as  validFromDate, \
                                  ToValidDate(BIS) as  validToDate, \
                                  TIMTYPZA as billingLineItemTimeCategoryCode, \
                                  SCHEMANR as billingSchemaNumber, \
                                  SNO as billingSchemaStepSequenceNumber, \
                                  PROGRAMM as variantProgramNumber, \
                                  MASSBILL as billingMeasurementUnitCode, \
                                  SAISON as seasonNumber, \
                                  TIMBASIS as timeBasisCode, \
                                  TIMTYP as timeCategoryCode, \
                                  FRAN_TYPE as franchiseFeeTypeCode, \
                                  KONZIGR as franchiseFeeGroupNumber, \
                                  stg.TARIFTYP as rateTypeCode, \
                                  rc.TTYPBEZ as rateType, \
                                  TARIFNR as rateId, \
                                  rd.rateDescription, \
                                  KONDIGR as rateFactGroupNumber, \
                                  STTARIF as statisticalRate, \
                                  GEWKEY as weightingKeyId, \
                                  WDHFAKT as referenceValuesForRepetitionFactor, \
                                  TEMP_AREA as temperatureArea, \
                                  DYNCANC01 as reversalDynamicPeriodControl1, \
                                  DYNCANC02 as reversalDynamicPeriodControl2, \
                                  DYNCANC03 as reversalDynamicPeriodControl3, \
                                  DYNCANC04 as reversalDynamicPeriodControl4, \
                                  DYNCANC05 as reversalDynamicPeriodControl5, \
                                  DYNCANC as reverseBackbillingIndicator, \
                                  DYNEXEC as allocateBackbillingIndicator, \
                                  LRATESTEP as rateStepLogicalNumber, \
                                  PEB as periodEndBillingIndicator, \
                                  STAFO as statisticsUpdateGroupCode, \
                                  ARTMENGE as billedQuantityStatisticsCode, \
                                  STATTART as statisticalAnalysisRateType, \
                                  srd.rateType as statisticalAnalysisRateTypeDescription, \
                                  TIMECONTRL as periodControlCode, \
                                  cast(TCNUMTOR as dec(8,4)) as timesliceNumeratorTimePortion, \
                                  cast(TCDENOMTOR as dec(8,4)) as timesliceDenominatorTimePortion, \
                                  TIMTYPQUOT as timesliceTimeCategoryTimePortion, \
                                  AKTIV as meterReadingActiveIndicator, \
                                  KONZVER as franchiseContractIndicator, \
                                  PERTYP as billingPeriodInternalCategoryCode, \
                                  OUCONTRACT as individualContractId, \
                                  cast(V_ABRMENGE as dec(17)) as billingQuantityPlaceBeforeDecimalPoint, \
                                  cast(N_ABRMENGE as dec(14,14)) as billingQuantityPlaceAfterDecimalPoint, \
                                  cast('1900-01-01' as TimeStamp) as _RecordStart, \
                                  cast('9999-12-31' as TimeStamp) as _RecordEnd, \
                                  '0' as _RecordDeleted, \
                                  '1' as _RecordCurrent, \
                                  cast('{CurrentTimeStamp}' as TimeStamp) as _DLCleansedZoneTimeStamp \
                         FROM stage stg \
                                 left outer join {ADS_DATABASE_CLEANSED}.isu_te835t li on li.lineItemTypeCode = stg.BELZART \
                                 left outer join {ADS_DATABASE_CLEANSED}.isu_0uc_aklasse_text bc on bc.billingClassCode = stg.AKLASSE \
                                 left outer join {ADS_DATABASE_CLEANSED}.isu_0uc_tariftyp_text rc on rc.TARIFTYP = stg.TARIFTYP \
                                 left outer join {ADS_DATABASE_CLEANSED}.isu_0uc_tarifnr_text rd on rd.rateId = stg.TARIFNR \
                                 left outer join {ADS_DATABASE_CLEANSED}.isu_0uc_stattart_text srd on srd.rateTypeCode = stg.STATTART \
                         where stg._RecordVersion = 1 ")

#print(f'Number of rows: {df.count()}')

# COMMAND ----------

newSchema = StructType([
                            StructField('billingDocumentNumber', StringType(), False),
                            StructField('billingDocumentLineItemId', StringType(), False),
                            StructField('billingSequenceNumber', StringType(), True),
                            StructField('lineItemTypeCode', StringType(), True),
                            StructField('lineItemType', StringType(), True),
                            StructField('billingLineItemBudgetBillingIndicator', StringType(), True),
                            StructField('lineItemDiscountStatisticsIndicator', StringType(), True),
                            StructField('billingLineItemRelevantPostingIndicator', StringType(), True),
                            StructField('billedValueStatisticallyRelevantIndicator', StringType(), True),
                            StructField('billingLineItemStatisticallyRelevantAmount', StringType(), True),
                            StructField('quantityStatisticsGroupCode', StringType(), True),
                            StructField('amountStatisticsGroupCode', StringType(), True),
                            StructField('billingLinePrintRelevantIndicator', StringType(), True),
                            StructField('billingClassCode', StringType(), True),
                            StructField('billingClass', StringType(), True),
                            StructField('industryText', StringType(), True),
                            StructField('industry', StringType(), True),
                            StructField('subtransactionForDocumentItem', StringType(), True),
                            StructField('offsettingTransactionSubtransactionForDocumentItem', StringType(), True),
                            StructField('presortingBillingLineItems', StringType(), True),
                            StructField('validFromDate', DateType(), True),
                            StructField('validToDate', DateType(), True),
                            StructField('billingLineItemTimeCategoryCode', StringType(), True),
                            StructField('billingSchemaNumber', StringType(), True),
                            StructField('billingSchemaStepSequenceNumber', StringType(), True),
                            StructField('variantProgramNumber', StringType(), True),
                            StructField('billingMeasurementUnitCode', StringType(), True),
                            StructField('seasonNumber', StringType(), True),
                            StructField('timeBasisCode', StringType(), True),
                            StructField('timeCategoryCode', StringType(), True),
                            StructField('franchiseFeeTypeCode', StringType(), True),
                            StructField('franchiseFeeGroupNumber', StringType(), True),
                            StructField('rateTypeCode', StringType(), True),
                            StructField('rateType', StringType(), True),
                            StructField('rateId', StringType(), True),
                            StructField('rateDescription', StringType(), True),
                            StructField('rateFactGroupNumber', StringType(), True),
                            StructField('statisticalRate', StringType(), True),
                            StructField('weightingKeyId', StringType(), True),
                            StructField('referenceValuesForRepetitionFactor', IntegerType(), True),
                            StructField('temperatureArea', StringType(), True),
                            StructField('reversalDynamicPeriodControl1', StringType(), True),
                            StructField('reversalDynamicPeriodControl2', StringType(), True),
                            StructField('reversalDynamicPeriodControl3', StringType(), True),
                            StructField('reversalDynamicPeriodControl4', StringType(), True),
                            StructField('reversalDynamicPeriodControl5', StringType(), True),
                            StructField('reverseBackbillingIndicator', StringType(), True),
                            StructField('allocateBackbillingIndicator', StringType(), True),
                            StructField('rateStepLogicalNumber', StringType(), True),
                            StructField('periodEndBillingIndicator', StringType(), True),
                            StructField('statisticsUpdateGroupCode', StringType(), True),
                            StructField('billedQuantityStatisticsCode', StringType(), True),
                            StructField('statisticalAnalysisRateType', StringType(), True),
                            StructField('statisticalAnalysisRateTypeDescription', StringType(), True),
                            StructField('periodControlCode', StringType(), True),
                            StructField('timesliceNumeratorTimePortion', DecimalType(8,4), True),
                            StructField('timesliceDenominatorTimePortion', DecimalType(8,4), True),
                            StructField('timesliceTimeCategoryTimePortion', StringType(), True),
                            StructField('meterReadingActiveIndicator', StringType(), True),
                            StructField('franchiseContractIndicator', StringType(), True),
                            StructField('billingPeriodInternalCategoryCode', StringType(), True),
                            StructField('individualContractId', StringType(), True),
                            StructField('billingQuantityPlaceBeforeDecimalPoint', DecimalType(17), True),
                            StructField('billingQuantityPlaceAfterDecimalPoint', DecimalType(14,14), True),
                            StructField('_RecordStart', DateType(), False),
                            StructField('_RecordEnd', DateType(), False),
                            StructField('_RecordDeleted', IntegerType(), False),
                            StructField('_RecordCurrent', IntegerType(), False),
                            StructField('_DLCleansedZoneTimeStamp',TimestampType(),False)
                    ])


# COMMAND ----------

# DBTITLE 1,12. Save Data frame into Cleansed Delta table (Final)
DeltaSaveDataFrameToDeltaTable(df, target_table, ADS_DATALAKE_ZONE_CLEANSED, ADS_DATABASE_CLEANSED, data_lake_folder, ADS_WRITE_MODE_MERGE, newSchema, track_changes, is_delta_extract, business_key, AddSKColumn = False, delta_column = "", start_counter = "0", end_counter = "0")

# COMMAND ----------

# DBTITLE 1,13. Exit Notebook
dbutils.notebook.exit("1")
