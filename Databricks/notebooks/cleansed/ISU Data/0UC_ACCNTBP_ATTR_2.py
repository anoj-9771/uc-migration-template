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
df = spark.sql(f"""WITH stage AS 
                      (Select *, ROW_NUMBER() OVER (PARTITION BY GPART,VKONT ORDER BY _FileDateTimeStamp DESC, _DLRawZoneTimeStamp DESC) AS _RecordVersion FROM {delta_raw_tbl_name} WHERE _DLRawZoneTimestamp >= '{LastSuccessfulExecutionTS}') 
                           SELECT 
                                GPART as businessPartnerGroupNumber, 
                                VKONT as contractAccountNumber, 
                                dd07t2.domainValueText as budgetBillingRequestForDebtor, 
                                dd07t1.domainValueText as budgetBillingRequestForCashPayer, 
                                if(KEINZAHL = 'X', 'Y', 'N') as noPaymentFormFlag, 
                                EINZUGSZ as numberOfSuccessfulDirectDebits, 
                                RUECKLZ as numberOfDirectDebitReturns, 
                                if(MAHNUNG_Z = 'X', 'Y', 'N') as sendAdditionalDunningNoticeFlag, 
                                if(RECHNUNG_Z = 'X', 'Y', 'N') as sendAdditionalBillFlag, 
                                efrm.applicationFormDescription as applicationForm, 
                                AUSGRUP_IN as outsortingCheckGroupCode, 
                                OUTCOUNT as manualOutsortingCount, 
                                MANOUTS_IN as manualOutsortingReasonCode, 
                                ToValidDate(ERDAT) as createdDate, 
                                ERNAM as createdBy, 
                                ToValidDate(AEDATP) as lastChangedDate, 
                                AENAMP as changedBy, 
                                FDZTG as additionalDaysForCashManagement, 
                                GUID as headerUUID, 
                                cast(DDLAM as dec(13,0)) as directDebitLimit, 
                                DDLNM as numberOfMonthsForDirectDebitLimit, 
                                EXVKO as businessPartnerReferenceNumber,
                                STDBK as standardCompanyCode, 
                                ABWMA as alternativeDunningRecipient, 
                                EBVTY as bankDetailsId, 
                                EZAWE as incomingPaymentMethodCode, 
                                if(LOEVM = 'X', 'Y', 'N') as deletedFlag, 
                                ABWVK as alternativeContractAccountForCollectiveBills,
                                VKPBZ as accountRelationshipCode, 
                                acc_txt.accountRelationship as accountRelationship, 
                                ADRNB as addressNumber, 
                                ADRMA as addressNumberForAlternativeDunningRecipient, 
                                ABWRH as alternativeInvoiceRecipient, 
                                ADRRH as addressNumberForAlternativeBillRecipient, 
                                TOGRU as toleranceGroupCode, 
                                CCARD_ID as paymentCardId, 
                                TFK111T.clearingCategory as clearingCategory, 
                                TFK041BT.collectionManagementMasterDataGroup as collectionManagementMasterDataGroup, 
                                STRAT as collectionStrategyCode, 
                                ZAHLKOND as paymentConditionCode, 
                                KOFIZ_SD as accountDeterminationCode, 
                                tfk043t.toleranceGroupDescription as toleranceGroup,
                                te128t.manualOutsortingReasonDescription as manualOutsortingReason, 
                                te192t.outsortingCheckGroup as outsortingCheckGroup, 
                                JVLTE as participationInYearlyAdvancePaymentCode,
                                dd07t.domainValueText as participationInYearlyAdvancePayment,
                                KZABSVER as activatebudgetbillingProcedureCode,
                                bbproc.billingProcedure as activatebudgetbillingProcedure,
                                zahlkond.paymentCondition as paymentCondition,
                                0fcactdetid.accountDetermination as accountDetermination, 
                                SENDCONTROL_RH as dispatchControlForAltBillRecipientCode, 
                                esendcontrolt.dispatchControlDescription as dispatchControlForAltBillRecipient,
                                OPBUK as companyGroupCode, 
                                0comp_code.companyName as companyGroupName, 
                                0comp_code1.companyName as standardCompanyName,
                                pymet.paymentMethod as incomingPaymentMethod,
                                tfk047xt.collectionStrategyName as collectionStrategyName,
                                CMGRP as collectionManagementMasterDataGroupCode,
                                SENDCONTROL_MA as shippingControlForAltDunningRecipientCode,
                                esendcontrolt1.dispatchControlDescription as shippingControlForAltDunningRecipient,
                                SENDCONTROL_GP as dispatchControlForOriginalCustomerCode,
                                esendcontrolt2.dispatchControlDescription as dispatchControlForOriginalCustomer,
                                ABSANFBZ as budgetBillingRequestForCashPayerCode,
                                ABSANFAB as budgetBillingRequestForDebtorCode,
                                VERTYP as clearingCategoryCode,
                                FORMKEY as applicationFormCode,
                                cast('1900-01-01' as TimeStamp) as _RecordStart, 
                                cast('9999-12-31' as TimeStamp) as _RecordEnd, 
                                CASE
                                    WHEN LOEVM IS NULL
                                    THEN '0'
                                    ELSE '1'
                                END as _RecordDeleted, 
                                '1' as _RecordCurrent, 
                                cast('{CurrentTimeStamp}' as TimeStamp) as _DLCleansedZoneTimeStamp 
                        FROM stage acc 
                        LEFT OUTER JOIN {ADS_DATABASE_CLEANSED}.isu_0FC_ACCTREL_TEXT acc_txt ON acc.VKPBZ = acc_txt.accountRelationshipCode and acc_txt._RecordDeleted = 0 and acc_txt._RecordCurrent = 1 
                        LEFT OUTER JOIN {ADS_DATABASE_CLEANSED}.isu_tfk043t tfk043t ON tfk043t.toleranceGroupCode = acc.TOGRU 
                        LEFT OUTER JOIN {ADS_DATABASE_CLEANSED}.isu_te128t te128t ON te128t.manualOutsortingReasonCode  = acc.MANOUTS_IN 
                        LEFT OUTER JOIN {ADS_DATABASE_CLEANSED}.isu_te192t te192t ON te192t.outsortingCheckGroupCode  = acc.AUSGRUP_IN 
                        LEFT OUTER JOIN {ADS_DATABASE_CLEANSED}.isu_dd07t dd07t ON dd07t.domainName = 'JVLTE' AND  dd07t.domainValueSingleUpperLimit = acc.JVLTE 
                        LEFT OUTER JOIN {ADS_DATABASE_CLEANSED}.isu_dd07t dd07t1 ON dd07t1.domainName = 'ABSLANFO' AND  dd07t1.domainValueSingleUpperLimit = acc.ABSANFBZ 
                        LEFT OUTER JOIN {ADS_DATABASE_CLEANSED}.isu_dd07t dd07t2 ON dd07t2.domainName = 'ABSLANFO' AND  dd07t2.domainValueSingleUpperLimit = acc.ABSANFAB 
                        LEFT OUTER JOIN {ADS_DATABASE_CLEANSED}.isu_0uc_bbproc_text bbproc ON bbproc.billingProcedureCode  = acc.KZABSVER 
                        LEFT OUTER JOIN {ADS_DATABASE_CLEANSED}.isu_0uc_zahlkond_text zahlkond ON zahlkond.paymentConditionCode = acc.ZAHLKOND 
                        LEFT OUTER JOIN {ADS_DATABASE_CLEANSED}.isu_0fcactdetid_text 0fcactdetid ON 0fcactdetid.accountDeterminationCode  = acc.KOFIZ_SD 
                        LEFT OUTER JOIN {ADS_DATABASE_CLEANSED}.isu_esendcontrolt esendcontrolt ON esendcontrolt.dispatchControlCode  = acc.SENDCONTROL_RH 
                        LEFT OUTER JOIN {ADS_DATABASE_CLEANSED}.isu_0comp_code_text 0comp_code ON acc.OPBUK =0comp_code.companyCode 
                        LEFT OUTER JOIN {ADS_DATABASE_CLEANSED}.isu_0comp_code_text 0comp_code1 ON acc.STDBK =0comp_code1.companyCode 
                        LEFT OUTER JOIN {ADS_DATABASE_CLEANSED}.isu_0fc_pymet_text pymet ON pymet.paymentMethodCode  =  acc.EZAWE 
                        LEFT OUTER JOIN {ADS_DATABASE_CLEANSED}.isu_tfk047xt tfk047xt ON tfk047xt.collectionStrategyCode  = acc.STRAT 
                        LEFT OUTER JOIN {ADS_DATABASE_CLEANSED}.isu_esendcontrolt esendcontrolt1 ON esendcontrolt1.dispatchControlCode  = acc.SENDCONTROL_MA 
                        LEFT OUTER JOIN {ADS_DATABASE_CLEANSED}.isu_esendcontrolt esendcontrolt2 ON esendcontrolt2.dispatchControlCode  = acc.SENDCONTROL_GP 
                        LEFT OUTER JOIN {ADS_DATABASE_CLEANSED}.isu_efrm efrm ON acc.FORMKEY = efrm.applicationForm 
                        LEFT OUTER JOIN {ADS_DATABASE_CLEANSED}.isu_TFK111T TFK111T ON  acc.VERTYP = TFK111T.clearingCategoryCode 
                        LEFT OUTER JOIN {ADS_DATABASE_CLEANSED}.isu_TFK041BT TFK041BT ON TFK041BT.collectionManagementMasterDataGroupCode =acc.CMGRP 
                        where acc._RecordVersion = 1 
                        """
)

#print(f'Number of rows: {df.count()}')

# COMMAND ----------

# DBTITLE 1,11. Update/Rename Columns and Load into a Dataframe
#Update/rename Column
#Pass 'MANDATORY' as second argument to function ToValidDate() on key columns to ensure correct value settings for those columns
# df_cleansed = spark.sql(f"SELECT \
#                                 GPART as businessPartnerGroupNumber, \
#                                 VKONT as contractAccountNumber, \
#                                 ABSANFAB as budgetBillingRequestForDebtor, \
#                                 ABSANFBZ as budgetBillingRequestForCashPayer, \
#                                 KEINZAHL as noPaymentFormIndicator, \
#                                 EINZUGSZ as numberOfSuccessfulDirectDebits, \
#                                 RUECKLZ as numberOfDirectDebitReturns, \
#                                 MAHNUNG_Z as sendAdditionalDunningNoticeIndicator, \
#                                 RECHNUNG_Z as sendAdditionalBillIndicator, \
#                                 FORMKEY as applicationForm, \
#                                 AUSGRUP_IN as outsortingCheckGroupCode, \
#                                 OUTCOUNT as manualOutsortingCount, \
#                                 MANOUTS_IN as manualOutsortingReasonCode, \
#                                 SENDCONTROL_MA as shippingControlForAlternativeDunningRecipient, \
#                                 SENDCONTROL_RH as dispatchControlForAlternativeBillRecipient, \
#                                 SENDCONTROL_GP as dispatchControl, \
#                                 KZABSVER as billingProcedureActivationIndicator, \
#                                 JVLTE as participationInYearlyAdvancePayment, \
#                                 ToValidDate(ERDAT) as createdDate, \
#                                 ERNAM as createdBy, \
#                                 ToValidDate(AEDATP) as lastChangedDate, \
#                                 AENAMP as changedBy, \
#                                 FDZTG as additionalDaysForCashManagement, \
#                                 GUID as headerUUID, \
#                                 cast(DDLAM as dec(13,0)) as directDebitLimit, \
#                                 DDLNM as numberOfMonthsForDirectDebitLimit, \
#                                 EXVKO as businessPartnerReferenceNumber, \
#                                 OPBUK as companyCodeGroup, \
#                                 STDBK as standardCompanyCode, \
#                                 ABWMA as alternativeDunningRecipient, \
#                                 EBVTY as bankDetailsId, \
#                                 EZAWE as incomingPaymentMethodCode, \
#                                 LOEVM as deletedIndicator, \
#                                 ABWVK as alternativeContractAccountForCollectiveBills, \
#                                 VKPBZ as accountRelationshipCode, \
#                                 acc_txt.accountRelationship as accountRelationship, \
#                                 ADRNB as addressNumber, \
#                                 ADRMA as addressNumberForAlternativeDunningRecipient, \
#                                 ABWRH as alternativeInvoiceRecipient, \
#                                 ADRRH as addressNumberForAlternativeBillRecipient, \
#                                 TOGRU as toleranceGroupCode, \
#                                 CCARD_ID as paymentCardId, \
#                                 VERTYP as clearingCategory, \
#                                 CMGRP as collectionManagementMasterDataGroup, \
#                                 STRAT as collectionStrategyCode, \
#                                 ZAHLKOND as paymentConditionCode, \
#                                 KOFIZ_SD as accountDeterminationCode, \
#                                 acc._RecordStart, \
#                                 acc._RecordEnd, \
#                                 acc._RecordDeleted, \
#                                 acc._RecordCurrent \
#                         FROM {ADS_DATABASE_STAGE}.{source_object} acc \
#                         LEFT OUTER JOIN {ADS_DATABASE_CLEANSED}.isu_0FC_ACCTREL_TEXT acc_txt ON acc.VKPBZ = acc_txt.accountRelationshipCode and acc_txt._RecordDeleted = 0 and acc_txt._RecordCurrent = 1")


# print(f'Number of rows: {df_cleansed.count()}')

# COMMAND ----------

newSchema = StructType([
	StructField('businessPartnerGroupNumber',StringType(),False),
	StructField('contractAccountNumber',StringType(),False),
	StructField('budgetBillingRequestForDebtor',StringType(),True),
	StructField('budgetBillingRequestForCashPayer',StringType(),True),
	StructField('noPaymentFormFlag',StringType(),True),
	StructField('numberOfSuccessfulDirectDebits',LongType(),True),
	StructField('numberOfDirectDebitReturns',LongType(),True),
	StructField('sendAdditionalDunningNoticeFlag',StringType(),True),
	StructField('sendAdditionalBillFlag',StringType(),True),
	StructField('applicationForm',StringType(),True),
	StructField('outsortingCheckGroupCode',StringType(),True),
	StructField('manualOutsortingCount',LongType(),True),
	StructField('manualOutsortingReasonCode',StringType(),True),
	StructField('createdDate',DateType(),True),
	StructField('createdBy',StringType(),True),
	StructField('lastChangedDate',DateType(),True),
	StructField('changedBy',StringType(),True),
	StructField('additionalDaysForCashManagement',LongType(),True),
	StructField('headerUUID',StringType(),True),
	StructField('directDebitLimit',DecimalType(13,0),True),
	StructField('numberOfMonthsForDirectDebitLimit',LongType(),True),
	StructField('businessPartnerReferenceNumber',StringType(),True),
	StructField('standardCompanyCode',StringType(),True),
	StructField('alternativeDunningRecipient',StringType(),True),
	StructField('bankDetailsId',StringType(),True),
	StructField('incomingPaymentMethodCode',StringType(),True),
	StructField('deletedFlag',StringType(),True),
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
	StructField('paymentConditionCode',StringType(),True),
	StructField('accountDeterminationCode',StringType(),True),
    StructField('toleranceGroup',StringType(),True),
    StructField('manualOutsortingReason',StringType(),True),
    StructField('outsortingCheckGroup',StringType(),True),
    StructField('participationInYearlyAdvancePaymentCode',LongType(),True),
    StructField('participationInYearlyAdvancePayment',StringType(),True),
    StructField('activatebudgetbillingProcedureCode',StringType(),True),
    StructField('activatebudgetbillingProcedure',StringType(),True),
    StructField('paymentCondition',StringType(),True),
    StructField('accountDetermination',StringType(),True),
    StructField('dispatchControlForAltBillRecipientCode',StringType(),True),
    StructField('dispatchControlForAltBillRecipient',StringType(),True),
    StructField('companyGroupCode',StringType(),True),
    StructField('companyGroupName',StringType(),True),
    StructField('standardCompanyName',StringType(),True),
    StructField('incomingPaymentMethod',StringType(),True),
    StructField('collectionStrategyName',StringType(),True),
    StructField('collectionManagementMasterDataGroupCode',StringType(),True),
    StructField('shippingControlForAltDunningRecipientCode',StringType(),True),
    StructField('shippingControlForAltDunningRecipient',StringType(),True),
    StructField('dispatchControlForOriginalCustomerCode',StringType(),True),
    StructField('dispatchControlForOriginalCustomer',StringType(),True),
    StructField('budgetBillingRequestForCashPayerCode',LongType(),True),
    StructField('budgetBillingRequestForDebtorCode',LongType(),True),
    StructField('clearingCategoryCode',StringType(),True),
    StructField('applicationFormCode',StringType(),True),
	StructField('_RecordStart',TimestampType(),False),
	StructField('_RecordEnd',TimestampType(),False),
	StructField('_RecordDeleted',IntegerType(),False),
	StructField('_RecordCurrent',IntegerType(),False),
    StructField('_DLCleansedZoneTimeStamp',TimestampType(),False)
])

# COMMAND ----------

# DBTITLE 1,12. Save Data frame into Cleansed Delta table (New Records)
DeltaSaveDataFrameToDeltaTable(df, target_table, ADS_DATALAKE_ZONE_CLEANSED, ADS_DATABASE_CLEANSED, data_lake_folder, ADS_WRITE_MODE_OVERWRITE, newSchema, track_changes, is_delta_extract, business_key, AddSKColumn = False, delta_column = "", start_counter = "0", end_counter = "0")

# COMMAND ----------

# DBTITLE 1,13. Exit Notebook
dbutils.notebook.exit("1")
