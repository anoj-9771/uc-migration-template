# Databricks notebook source
# DBTITLE 1,Generate parameter and source object name for unit testing
import json
#For unit testing...
#Use this string in the Param widget: 
#{"SourceType": "BLOB Storage (json)", "SourceServer": "daf-sa-lake-sastoken", "SourceGroup": "isu", "SourceName": "isu_0BP_DEF_ADDRESS_ATTR", "SourceLocation": "isu/0BP_DEF_ADDRESS_ATTR", "AdditionalProperty": "", "Processor": "databricks-token|0711-011053-turfs581|Standard_DS3_v2|8.3.x-scala2.12|2:8|interactive", "IsAuditTable": false, "SoftDeleteSource": "", "ProjectName": "ISU DATA", "ProjectId": 2, "TargetType": "BLOB Storage (json)", "TargetName": "isu_0BP_DEF_ADDRESS_ATTR", "TargetLocation": "isu/0BP_DEF_ADDRESS_ATTR", "TargetServer": "daf-sa-lake-sastoken", "DataLoadMode": "FULL-EXTRACT", "DeltaExtract": false, "CDCSource": false, "TruncateTarget": false, "UpsertTarget": true, "AppendTarget": null, "TrackChanges": false, "LoadToSqlEDW": true, "TaskName": "isu_0BP_DEF_ADDRESS_ATTR", "ControlStageId": 2, "TaskId": 46, "StageSequence": 200, "StageName": "Raw to Cleansed", "SourceId": 46, "TargetId": 46, "ObjectGrain": "Day", "CommandTypeId": 8, "Watermarks": "", "WatermarksDT": null, "WatermarkColumn": "", "BusinessKeyColumn": "businessPartnerNumber,addressNumber", "UpdateMetaData": null, "SourceTimeStampFormat": "", "Command": "", "LastLoadedFile": null}

#Use this string in the Source Object widget
#isu_0BP_DEF_ADDRESS_ATTR

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
                      (Select *, ROW_NUMBER() OVER (PARTITION BY PARTNER,ADDRNUMBER ORDER BY _FileDateTimeStamp DESC, DI_SEQUENCE_NUMBER DESC, _DLRawZoneTimeStamp DESC) AS _RecordVersion FROM {delta_raw_tbl_name} \
                                     WHERE _DLRawZoneTimestamp >= '{LastSuccessfulExecutionTS}') \
                           SELECT \
                                case when PARTNER = 'na' then '' else PARTNER end as businessPartnerNumber, \
                                PARTNER_GUID as businessPartnerGUID, \
                                case when ADDRNUMBER = 'na' then '' else ADDRNUMBER end as addressNumber, \
                                ToValidDate(DATE_FROM) as validFromDate, \
                                ToValidDate(DATE_TO) as validToDate, \
                                TITLE as titleCode, \
                                NAME1 as businessPartnerName1, \
                                NAME2 as businessPartnerName2, \
                                NAME3 as businessPartnerName3, \
                                NAME_CO as coName, \
                                CITY1 as cityName, \
                                CITY_CODE as cityCode, \
                                POST_CODE1 as postalCode, \
                                POST_CODE2 as poBoxPostalCode, \
                                POST_CODE3 as companyPostalCode, \
                                PO_BOX as poBoxCode, \
                                PO_BOX_NUM as poBoxWithoutNumberIndicator, \
                                PO_BOX_LOC as poBoxCity, \
                                CITY_CODE2 as cityPoBoxCode, \
                                STREET as streetName, \
                                STREETCODE as streetCode, \
                                HOUSE_NUM1 as houseNumber, \
                                HOUSE_NUM2 as houseNumber2, \
                                STR_SUPPL1 as streetType, \
                                STR_SUPPL2 as streetLine3, \
                                STR_SUPPL3 as streetLine4, \
                                LOCATION as streetLine5, \
                                BUILDING as building, \
                                FLOOR as floorNumber, \
                                ROOMNUMBER as apartmentNumber, \
                                COUNTRY as countryShortName, \
                                REGION as stateCode, \
                                PERS_ADDR as personalAddressIndicator, \
                                SORT1 as searchTerm1, \
                                SORT2 as searchTerm2, \
                                TEL_NUMBER as phoneNumber, \
                                FAX_NUMBER as faxNumber, \
                                TIME_ZONE as addressTimeZone, \
                                SMTP_ADDR as emailAddress, \
                                URI_ADDR as uriAddress, \
                                TELDISPLAY as phoneNumberDisplayFormat, \
                                FAXDISPLAY as faxDisplayFormat, \
                                cast(LONGITUDE as dec(15,12)) as longitude, \
                                cast(LATITUDE as dec(15,12)) as latitude, \
                                cast(ALTITUDE as dec(9,3)) as altitude, \
                                PRECISID as precision, \
                                ADDRCOMM as communicationAddressNumber, \
                                ADDR_SHORT as shortFormattedAddress, \
                                ADDR_SHORT_S as shortFormattedAddress2, \
                                LINE0 as addressLine0, \
                                LINE1 as addressLine1, \
                                LINE2 as addressLine2, \
                                LINE3 as addressLine3, \
                                LINE4 as addressLine4, \
                                LINE5 as addressLine5, \
                                LINE6 as addressLine6, \
                                LINE7 as addressLine7, \
                                LINE8 as addressLine8, \
                                ZZMOBILENO as mobileNumber, \
                                ZZTELNO as phoneNumberWithExtension, \
                                PCODE1_EXT as postalCodeExtension, \
                                PCODE2_EXT as poBoxExtension, \
                                DELI_SERV_TYPE as deliveryServiceTypeCode, \
                                DELI_SERV_NUMBER as deliveryServiceNumber, \
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
#                             case when PARTNER = 'na' then '' else PARTNER end as businessPartnerNumber, \
#                             PARTNER_GUID as businessPartnerGUID, \
#                             case when ADDRNUMBER = 'na' then '' else ADDRNUMBER end as addressNumber, \
#                             ToValidDate(DATE_FROM) as validFromDate, \
#                             ToValidDate(DATE_TO) as validToDate, \
#                             TITLE as titleCode, \
#                             NAME1 as businessPartnerName1, \
#                             NAME2 as businessPartnerName2, \
#                             NAME3 as businessPartnerName3, \
#                             NAME_CO as coName, \
#                             CITY1 as cityName, \
#                             CITY_CODE as cityCode, \
#                             POST_CODE1 as postalCode, \
#                             POST_CODE2 as poBoxPostalCode, \
#                             POST_CODE3 as companyPostalCode, \
#                             PO_BOX as poBoxCode, \
#                             PO_BOX_NUM as poBoxWithoutNumberIndicator, \
#                             PO_BOX_LOC as poBoxCity, \
#                             CITY_CODE2 as cityPoBoxCode, \
#                             STREET as streetName, \
#                             STREETCODE as streetCode, \
#                             HOUSE_NUM1 as houseNumber, \
#                             HOUSE_NUM2 as houseNumber2, \
#                             STR_SUPPL1 as streetType, \
#                             STR_SUPPL2 as streetLine3, \
#                             STR_SUPPL3 as streetLine4, \
#                             LOCATION as streetLine5, \
#                             BUILDING as building, \
#                             FLOOR as floorNumber, \
#                             ROOMNUMBER as apartmentNumber, \
#                             COUNTRY as countryShortName, \
#                             REGION as stateCode, \
#                             PERS_ADDR as personalAddressIndicator, \
#                             SORT1 as searchTerm1, \
#                             SORT2 as searchTerm2, \
#                             TEL_NUMBER as phoneNumber, \
#                             FAX_NUMBER as faxNumber, \
#                             TIME_ZONE as addressTimeZone, \
#                             SMTP_ADDR as emailAddress, \
#                             URI_ADDR as uriAddress, \
#                             TELDISPLAY as phoneNumberDisplayFormat, \
#                             FAXDISPLAY as faxDisplayFormat, \
#                             cast(LONGITUDE as dec(15,12)) as longitude, \
#                             cast(LATITUDE as dec(15,12)) as latitude, \
#                             cast(ALTITUDE as dec(9,3)) as altitude, \
#                             PRECISID as precision, \
#                             ADDRCOMM as communicationAddressNumber, \
#                             ADDR_SHORT as shortFormattedAddress, \
#                             ADDR_SHORT_S as shortFormattedAddress2, \
#                             LINE0 as addressLine0, \
#                             LINE1 as addressLine1, \
#                             LINE2 as addressLine2, \
#                             LINE3 as addressLine3, \
#                             LINE4 as addressLine4, \
#                             LINE5 as addressLine5, \
#                             LINE6 as addressLine6, \
#                             LINE7 as addressLine7, \
#                             LINE8 as addressLine8, \
#                             ZZMOBILENO as mobileNumber, \
#                             ZZTELNO as phoneNumberWithExtension, \
#                             PCODE1_EXT as postalCodeExtension, \
#                             PCODE2_EXT as poBoxExtension, \
#                             DELI_SERV_TYPE as deliveryServiceTypeCode, \
#                             DELI_SERV_NUMBER as deliveryServiceNumber, \
#                             _RecordStart, \
#                             _RecordEnd, \
#                             _RecordDeleted, \
#                             _RecordCurrent \
#                         FROM {ADS_DATABASE_STAGE}.{source_object}")

# print(f'Number of rows: {df_cleansed.count()}')

# COMMAND ----------

newSchema = StructType([
                        StructField('businessPartnerNumber', StringType(), False),
                        StructField('businessPartnerGUID', StringType(), True),
                        StructField('addressNumber', StringType(), False),
                        StructField('validFromDate', DateType(), True),
                        StructField('validToDate', DateType(), True),
                        StructField('titleCode', StringType(), True),
                        StructField('businessPartnerName1', StringType(), True),
                        StructField('businessPartnerName2', StringType(), True),
                        StructField('businessPartnerName3', StringType(), True),
                        StructField('coName', StringType(), True),
                        StructField('cityName', StringType(), True),
                        StructField('cityCode', StringType(), True),
                        StructField('postalCode', StringType(), True),
                        StructField('poBoxPostalCode', StringType(), True),
                        StructField('companyPostalCode', StringType(), True),
                        StructField('poBoxCode', StringType(), True),
                        StructField('poBoxWithoutNumberIndicator', StringType(), True),
                        StructField('poBoxCity', StringType(), True),
                        StructField('cityPoBoxCode', StringType(), True),
                        StructField('streetName', StringType(), True),
                        StructField('streetCode', StringType(), True),
                        StructField('houseNumber', StringType(), True),
                        StructField('houseNumber2', StringType(), True),
                        StructField('streetType', StringType(), True),
                        StructField('streetLine3', StringType(), True),
                        StructField('streetLine4', StringType(), True),
                        StructField('streetLine5', StringType(), True),
                        StructField('building', StringType(), True),
                        StructField('floorNumber', StringType(), True),
                        StructField('apartmentNumber', StringType(), True),
                        StructField('countryShortName', StringType(), True),
                        StructField('stateCode', StringType(), True),
                        StructField('personalAddressIndicator', StringType(), True),
                        StructField('searchTerm1', StringType(), True),
                        StructField('searchTerm2', StringType(), True),
                        StructField('phoneNumber', StringType(), True),
                        StructField('faxNumber', StringType(), True),
                        StructField('addressTimeZone', StringType(), True),
                        StructField('emailAddress', StringType(), True),
                        StructField('uriAddress', StringType(), True),
                        StructField('phoneNumberDisplayFormat', StringType(), True),
                        StructField('faxDisplayFormat', StringType(), True),
                        StructField('longitude', DecimalType(15,12), True),
                        StructField('latitude', DecimalType(15,12), True),
                        StructField('altitude', DecimalType(9,3), True),
                        StructField('precision', StringType(), True),
                        StructField('communicationAddressNumber', StringType(), True),
                        StructField('shortFormattedAddress', StringType(), True),
                        StructField('shortFormattedAddress2', StringType(), True),
                        StructField('addressLine0', StringType(), True),
                        StructField('addressLine1', StringType(), True),
                        StructField('addressLine2', StringType(), True),
                        StructField('addressLine3', StringType(), True),
                        StructField('addressLine4', StringType(), True),
                        StructField('addressLine5', StringType(), True),
                        StructField('addressLine6', StringType(), True),
                        StructField('addressLine7', StringType(), True),
                        StructField('addressLine8', StringType(), True),
                        StructField('mobileNumber', StringType(), True),
                        StructField('phoneNumberWithExtension', StringType(), True),
                        StructField('postalCodeExtension', StringType(), True),
                        StructField('poBoxExtension', StringType(), True),
                        StructField('deliveryServiceTypeCode', StringType(), True),
                        StructField('deliveryServiceNumber', StringType(), True),
                        StructField('_RecordStart', TimestampType(), False),
                        StructField('_RecordEnd', TimestampType(), False),
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
