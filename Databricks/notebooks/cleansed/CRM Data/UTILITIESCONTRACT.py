# Databricks notebook source
# DBTITLE 1,Generate parameter and source object name for unit testing
import json
#For unit testing...
#Use this string in the Param widget: 
#{"SourceType": "BLOB Storage (json)", "SourceServer": "daf-sa-lake-sastoken", "SourceGroup": "CRM", "SourceName": "CRM_UTILITIESCONTRACT", "SourceLocation": "CRM/UTILITIESCONTRACT", "AdditionalProperty": "", "Processor": "databricks-token|0711-011053-turfs581|Standard_DS3_v2|8.3.x-scala2.12|2:8|interactive", "IsAuditTable": false, "SoftDeleteSource": "", "ProjectName": "CRM DATA", "ProjectId": 2, "TargetType": "BLOB Storage (json)", "TargetName": "CRM_UTILITIESCONTRACT", "TargetLocation": "CRM/UTILITIESCONTRACT", "TargetServer": "daf-sa-lake-sastoken", "DataLoadMode": "FULL-EXTRACT", "DeltaExtract": false, "CDCSource": false, "TruncateTarget": false, "UpsertTarget": true, "AppendTarget": null, "TrackChanges": false, "LoadToSqlEDW": true, "TaskName": "CRM_UTILITIESCONTRACT", "ControlStageId": 2, "TaskId": 46, "StageSequence": 200, "StageName": "Raw to Cleansed", "SourceId": 46, "TargetId": 46, "ObjectGrain": "Day", "CommandTypeId": 8, "Watermarks": "", "WatermarksDT": null, "WatermarkColumn": "", "BusinessKeyColumn": "utilitiesContract,contractEndDateE", "UpdateMetaData": null, "SourceTimeStampFormat": "", "Command": "", "LastLoadedFile": null}

#Use this string in the Source Object widget
#CRM_UTILITIESCONTRACT

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
                      (Select *, ROW_NUMBER() OVER (PARTITION BY UTILITIESCONTRACT,CONTRACTENDDATE_E ORDER BY  _FileDateTimeStamp DESC, _DLRawZoneTimeStamp DESC) AS _RecordVersion FROM {delta_raw_tbl_name} \
                                      WHERE _DLRawZoneTimestamp >= '{LastSuccessfulExecutionTS}') \
                           SELECT \
                                ItemUUID as itemUUID, \
                                PodUUID as podUUID, \
                                HeaderUUID as headerUUID, \
                                case when UtilitiesContract = 'na' then '' else UtilitiesContract end as utilitiesContract, \
                                ltrim('0', BusinessPartner) as businessPartnerGroupNumber, \
                                BusinessPartnerFullName as businessPartnerGroupName, \
                                BusinessAgreement as businessAgreement, \
                                BusinessAgreementUUID as businessAgreementUUID, \
                                IncomingPaymentMethod as incomingPaymentMethodCode, \
                                IncomingPaymentMethodName as incomingPaymentMethod, \
                                PaymentTerms as paymentTermsCode, \
                                PaymentTermsName as paymentTerms, \
                                SoldToParty as soldToParty, \
                                SoldToPartyName as businessPartnerFullName, \
                                Division as divisionCode, \
                                DivisionName as division, \
                                ToValidDate(ContractStartDate) as contractStartDate, \
                                ToValidDate(ContractEndDate) as contractEndDate, \
                                ToValidDateTime(CreationDate) as creationDate, \
                                CreatedByUser as createdBy, \
                                ToValidDateTime(LastChangeDate) as lastChangedDate, \
                                LastChangedByUser as changedBy, \
                                ItemCategory as itemCategoryCode, \
                                ItemCategoryName as itemCategory, \
                                Product as productCode, \
                                ProductDescription as product, \
                                ItemType as itemTypeCode, \
                                ItemTypeName as itemType, \
                                ProductType as productTypeCode, \
                                dd.domainValueText as productType, \
                                HeaderType as headerTypeCode, \
                                HeaderTypeName as headerType, \
                                HeaderCategory as headerCategoryCode, \
                                HeaderCategoryName as headerCategoryName, \
                                HeaderDescription as headerDescription, \
                                (CASE WHEN IsDeregulationPod = 'X' THEN 'Y' ELSE 'N' END) as isDeregulationPODFlag, \
                                ltrim('0', UtilitiesPremise) as premise, \
                                NumberOfPersons as numberOfPersons, \
                                CityName as cityName, \
                                StreetName as streetName, \
                                HouseNumber as houseNumber, \
                                PostalCode as postalCode, \
                                Building as building, \
                                AddressTimeZone as addressTimeZone, \
                                CountryName as countryName, \
                                RegionName as regionName, \
                                NumberOfContractChanges as numberOfContractChanges, \
                                IsOpen as isOpen, \
                                IsDistributed as isDistributed, \
                                HasError as hasError, \
                                IsToBeDistributed as isToBeDistributed, \
                                IsIncomplete as isIncomplete, \
                                IsStartedDueToProductChange as isStartedDueToProductChange, \
                                IsInActivation as isInActivation, \
                                IsInDeactivation as isInDeactivation, \
                                (CASE WHEN IsCancelled = 'X' THEN 'Y' ELSE 'N' END) as isCancelledFlag, \
                                CancellationMessageIsCreated as cancellationMessageIsCreated, \
                                SupplyEndCanclnMsgIsCreated as supplyEndCanclnMsgIsCreated, \
                                ActivationIsRejected as activationIsRejected, \
                                DeactivationIsRejected as deactivationIsRejected, \
                                cast(NumberOfContracts as int) as numberOfContracts, \
                                cast(NumberOfActiveContracts as int) as numberOfActiveContracts, \
                                cast(NumberOfIncompleteContracts as int) as numberOfIncompleteContracts, \
                                cast(NumberOfDistributedContracts as int) as numberOfDistributedContracts, \
                                cast(NmbrOfContractsToBeDistributed as int) as numberOfContractsToBeDistributed, \
                                cast(NumberOfBlockedContracts as int) as numberOfBlockedContracts, \
                                cast(NumberOfCancelledContracts as int) as numberOfCancelledContracts, \
                                cast(NumberOfProductChanges as int) as numberOfProductChanges, \
                                cast(NmbrOfContractsWProductChanges as int) as numberOfContractsWProductChanges, \
                                cast(NmbrOfContrWthEndOfSupRjctd as int) as numberOfContrWthEndOfSupRjctd, \
                                cast(NmbrOfContrWthStartOfSupRjctd as int) as numberOfContrWthStartOfSupRjctd, \
                                cast(NmbrOfContrWaitingForEndOfSup as int) as numberOfContrWaitingForEndOfSup, \
                                cast(NmbrOfContrWaitingForStrtOfSup as int) as numberOfContrWaitingForStrtOfSup, \
                                ToValidDate(CreationDate_E) as creationDateE, \
                                ToValidDate(LastChangeDate_E) as lastChangedDateE, \
                                ToValidDate(ContractStartDate_E) as contractStartDateE, \
                                ToValidDate(ContractEndDate_E,'MANDATORY') as contractEndDateE, \
                                'UTILITIESCONTRACT|CONTRACTENDDATE_E' as sourceKeyDesc, \
                                concat_ws('|',UTILITIESCONTRACT,CONTRACTENDDATE_E) as sourceKey, \
                                'CONTRACTENDDATE_E' as rejectColumn, \
                                cast('1900-01-01' as TimeStamp) as _RecordStart, \
                                cast('9999-12-31' as TimeStamp) as _RecordEnd, \
                                '0' as _RecordDeleted, \
                                '1' as _RecordCurrent, \
                                cast('{CurrentTimeStamp}' as TimeStamp) as _DLCleansedZoneTimeStamp \
                        from stage cu \
                         LEFT OUTER JOIN {ADS_DATABASE_CLEANSED}.crm.DD07T dd ON cu.ProductType = dd.domainValueSingleUpperLimit \
                                                                    and dd.domainName = 'CRM_PRODUCT_KIND' and dd._RecordDeleted = 0 and dd._RecordCurrent = 1 \
                        where cu._RecordVersion = 1 ").cache()

print(f'Number of rows: {df.count()}')

# COMMAND ----------

newSchema = StructType([
	StructField('itemUUID',StringType(),True),
	StructField('podUUID',StringType(),True),
	StructField('headerUUID',StringType(),True),
	StructField('utilitiesContract',StringType(),False),
	StructField('businessPartnerGroupNumber',StringType(),True),
	StructField('businessPartnerGroupName',StringType(),True),
	StructField('businessAgreement',StringType(),True),
	StructField('businessAgreementUUID',StringType(),True),
	StructField('incomingPaymentMethodCode',StringType(),True),
	StructField('incomingPaymentMethod',StringType(),True),
	StructField('paymentTermsCode',StringType(),True),
	StructField('paymentTerms',StringType(),True),
	StructField('soldToParty',StringType(),True),
	StructField('businessPartnerFullName',StringType(),True),
	StructField('divisionCode',StringType(),True),
	StructField('division',StringType(),True),
	StructField('contractStartDate',DateType(),True),
	StructField('contractEndDate',DateType(),True),
	StructField('creationDate',TimestampType(),True),
	StructField('createdBy',StringType(),True),
	StructField('lastChangedDate',TimestampType(),True),
	StructField('changedBy',StringType(),True),
	StructField('itemCategoryCode',StringType(),True),
	StructField('itemCategory',StringType(),True),
	StructField('productCode',StringType(),True),
	StructField('product',StringType(),True),
	StructField('itemTypeCode',StringType(),True),
	StructField('itemType',StringType(),True),
	StructField('productTypeCode',StringType(),True),
    StructField('productType',StringType(),True),
	StructField('headerTypeCode',StringType(),True),
	StructField('headerType',StringType(),True),
	StructField('headerCategoryCode',StringType(),True),
	StructField('headerCategoryName',StringType(),True),
	StructField('headerDescription',StringType(),True),
	StructField('isDeregulationPODFlag',StringType(),True),
	StructField('premise',StringType(),True),
	StructField('numberOfPersons',StringType(),True),
	StructField('cityName',StringType(),True),
	StructField('streetName',StringType(),True),
	StructField('houseNumber',StringType(),True),
	StructField('postalCode',StringType(),True),
	StructField('building',StringType(),True),
	StructField('addressTimeZone',StringType(),True),
	StructField('countryName',StringType(),True),
	StructField('regionName',StringType(),True),
	StructField('numberOfContractChanges',StringType(),True),
	StructField('isOpen',StringType(),True),
	StructField('isDistributed',StringType(),True),
	StructField('hasError',StringType(),True),
	StructField('isToBeDistributed',StringType(),True),
	StructField('isIncomplete',StringType(),True),
	StructField('isStartedDueToProductChange',StringType(),True),
	StructField('isInActivation',StringType(),True),
	StructField('isInDeactivation',StringType(),True),
	StructField('isCancelledFlag',StringType(),True),
	StructField('cancellationMessageIsCreated',StringType(),True),
	StructField('supplyEndCanclnMsgIsCreated',StringType(),True),
	StructField('activationIsRejected',StringType(),True),
	StructField('deactivationIsRejected',StringType(),True),
	StructField('numberOfContracts',IntegerType(),True),
	StructField('numberOfActiveContracts',IntegerType(),True),
	StructField('numberOfIncompleteContracts',IntegerType(),True),
	StructField('numberOfDistributedContracts',IntegerType(),True),
	StructField('numberOfContractsToBeDistributed',IntegerType(),True),
	StructField('numberOfBlockedContracts',IntegerType(),True),
	StructField('numberOfCancelledContracts',IntegerType(),True),
	StructField('numberOfProductChanges',IntegerType(),True),
	StructField('numberOfContractsWProductChanges',IntegerType(),True),
	StructField('numberOfContrWthEndOfSupRjctd',IntegerType(),True),
	StructField('numberOfContrWthStartOfSupRjctd',IntegerType(),True),
	StructField('numberOfContrWaitingForEndOfSup',IntegerType(),True),
	StructField('numberOfContrWaitingForStrtOfSup',IntegerType(),True),
	StructField('creationDateE',DateType(),True),
	StructField('lastChangedDateE',DateType(),True),
	StructField('contractStartDateE',DateType(),True),
	StructField('contractEndDateE',DateType(),False),
	StructField('_RecordStart',TimestampType(),False),
	StructField('_RecordEnd',TimestampType(),False),
	StructField('_RecordDeleted',IntegerType(),False),
	StructField('_RecordCurrent',IntegerType(),False),
    StructField('_DLCleansedZoneTimeStamp',TimestampType(),False)
])

# COMMAND ----------

# DBTITLE 1,Handle Invalid Records
reject_df =df.where("contractEndDateE = '1000-01-01'").cache()
cleansed_df = df.subtract(reject_df)
cleansed_df = cleansed_df.drop("sourceKeyDesc","sourceKey","rejectColumn")

# COMMAND ----------

# DBTITLE 1,12. Save Data frame into Cleansed Delta table (Final)
DeltaSaveDataFrameToDeltaTable(cleansed_df, target_table, ADS_DATALAKE_ZONE_CLEANSED, ADS_DATABASE_CLEANSED, data_lake_folder, ADS_WRITE_MODE_OVERWRITE, newSchema, track_changes, is_delta_extract, business_key, AddSKColumn = False, delta_column = "", start_counter = "0", end_counter = "0")

# COMMAND ----------

# DBTITLE 1,12.1 Save Reject Data Frame into Rejected Database
if reject_df.count() > 0:
    source_key = 'UTILITIESCONTRACT|CONTRACTENDDATE_E'
    DeltaSaveDataFrameToRejectTable(reject_df,target_table,business_key,source_key,LastSuccessfulExecutionTS)
    reject_df.unpersist()
df.unpersist()

# COMMAND ----------

# DBTITLE 1,13. Exit Notebook
dbutils.notebook.exit("1")
