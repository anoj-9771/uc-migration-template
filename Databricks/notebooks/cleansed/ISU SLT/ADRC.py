# Databricks notebook source
# DBTITLE 1,Generate parameter and source object name for unit testing
import json
#For unit testing...
#Use this string in the Param widget: 
#{"SourceType": "BLOB Storage (json)", "SourceServer": "daf-sa-lake-sastoken", "SourceGroup": "ISU", "SourceName": "ISU_ADRC", "SourceLocation": "ISU/ADRC", "AdditionalProperty": "", "Processor": "databricks-token|0711-011053-turfs581|Standard_DS3_v2|8.3.x-scala2.12|2:8|interactive", "IsAuditTable": false, "SoftDeleteSource": "", "ProjectName": "ISU DATA", "ProjectId": 2, "TargetType": "BLOB Storage (json)", "TargetName": "ISU_ADRC", "TargetLocation": "ISU/ADRC", "TargetServer": "daf-sa-lake-sastoken", "DataLoadMode": "FULL-EXTRACT", "DeltaExtract": false, "CDCSource": false, "TruncateTarget": false, "UpsertTarget": true, "AppendTarget": null, "TrackChanges": false, "LoadToSqlEDW": true, "TaskName": "ISU_ADRC", "ControlStageId": 2, "TaskId": 46, "StageSequence": 200, "StageName": "Raw to Cleansed", "SourceId": 46, "TargetId": 46, "ObjectGrain": "Day", "CommandTypeId": 8, "Watermarks": "", "WatermarksDT": null, "WatermarkColumn": "", "BusinessKeyColumn": "addressNumber,validFromDate", "UpdateMetaData": null, "SourceTimeStampFormat": "", "Command": "", "LastLoadedFile": null}

#Use this string in the Source Object widget
#ISU_ADRC

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
#Pass 'MANDATORY' as second argument to function ToValidDate() on key columns to ensure correct value settings for those columns
df_cleansed = spark.sql(f"SELECT \
	ADDR_GROUP as addressGroup, \
	case when ADDRNUMBER = 'na' then '' else ADDRNUMBER end as addressNumber, \
	BUILDING as building, \
	CHCKSTATUS as regionalStructureGrouping, \
	CITY_CODE as cityCode, \
	CITY_CODE2 as cityPoBoxCode, \
	CITY1 as cityName, \
	COUNTRY as countryShortName, \
	ToValidDate(DATE_FROM,'MANDATORY') as validFromDate, \
	ToValidDate(DATE_TO) as validToDate, \
	DEFLT_COMM as communicationMethod, \
	FAX_NUMBER as faxNumber, \
	FLAGCOMM12 as ftpAddressFlag, \
	FLAGCOMM13 as pagerAddressFlag, \
	FLAGCOMM2 as telephoneNumberFlag, \
	FLAGCOMM3 as faxNumberFlag, \
	FLAGCOMM6 as emailAddressFlag, \
	FLOOR as floorNumber, \
	HOUSE_NUM1 as houseNumber, \
	HOUSE_NUM2 as houseNumber2, \
	HOUSE_NUM3 as houseNumber3, \
	LANGU_CREA as originalAddressRecordCreation, \
	LOCATION as streetLine5, \
	MC_CITY1 as searchHelpCityName, \
	MC_NAME1 as searchHelpLastName, \
	MC_STREET as searchHelpStreetName, \
	NAME_CO as coName, \
	NAME1 as name1, \
	NAME2 as name2, \
	NAME3 as name3, \
	PCODE1_EXT as postalCodeExtension, \
	PCODE2_EXT as poBoxExtension, \
	PERS_ADDR as personalAddressIndicator, \
	PO_BOX as poBoxCode, \
	PO_BOX_LOC as poBoxCity, \
	POST_CODE1 as postalCode, \
	POST_CODE2 as poBoxPostalCode, \
	POST_CODE3 as companyPostalCode, \
	REGION as stateCode, \
	ROOMNUMBER as apartmentNumber, \
	SORT1 as searchTerm1, \
	SORT2 as searchTerm2, \
	STR_SUPPL1 as streetType, \
	STR_SUPPL2 as streetLine3, \
	STR_SUPPL3 as streetLine4, \
	STREET as streetName, \
	STREETABBR as streetAbbreviation, \
	STREETCODE as streetCode, \
	TEL_EXTENS as telephoneExtension, \
	TEL_NUMBER as phoneNumber, \
	TIME_ZONE as addressTimeZone, \
	TITLE as titleCode, \
    _RecordStart, \
	_RecordEnd, \
	_RecordDeleted, \
	_RecordCurrent \
	FROM {ADS_DATABASE_STAGE}.{source_object} \
        ")

display(df_cleansed)
print(f'Number of rows: {df_cleansed.count()}')

# COMMAND ----------

newSchema = StructType([
	StructField('addressGroup',StringType(),True),
	StructField('addressNumber',StringType(),False),
	StructField('building',StringType(),True),
	StructField('regionalStructureGrouping',StringType(),True),
	StructField('cityCode',StringType(),True),
	StructField('cityPoBoxCode',StringType(),True),
	StructField('cityName',StringType(),True),
	StructField('countryShortName',StringType(),True),
	StructField('validFromDate',DateType(),False),
	StructField('validToDate',DateType(),True),
	StructField('communicationMethod',StringType(),True),
	StructField('faxNumber',StringType(),True),
	StructField('ftpAddressFlag',StringType(),True),
	StructField('pagerAddressFlag',StringType(),True),
	StructField('telephoneNumberFlag',StringType(),True),
	StructField('faxNumberFlag',StringType(),True),
	StructField('emailAddressFlag',StringType(),True),
	StructField('floorNumber',StringType(),True),
	StructField('houseNumber',StringType(),True),
	StructField('houseNumber2',StringType(),True),
	StructField('houseNumber3',StringType(),True),
	StructField('originalAddressRecordCreation',StringType(),True),
	StructField('streetLine5',StringType(),True),
	StructField('searchHelpCityName',StringType(),True),
	StructField('searchHelpLastName',StringType(),True),
	StructField('searchHelpStreetName',StringType(),True),
	StructField('coName',StringType(),True),
	StructField('name1',StringType(),True),
	StructField('name2',StringType(),True),
	StructField('name3',StringType(),True),
	StructField('postalCodeExtension',StringType(),True),
	StructField('poBoxExtension',StringType(),True),
	StructField('personalAddressIndicator',StringType(),True),
	StructField('poBoxCode',StringType(),True),
	StructField('poBoxCity',StringType(),True),
	StructField('postalCode',StringType(),True),
	StructField('poBoxPostalCode',StringType(),True),
	StructField('companyPostalCode',StringType(),True),
	StructField('stateCode',StringType(),True),
	StructField('apartmentNumber',StringType(),True),
	StructField('searchTerm1',StringType(),True),
	StructField('searchTerm2',StringType(),True),
	StructField('streetType',StringType(),True),
	StructField('streetLine3',StringType(),True),
	StructField('streetLine4',StringType(),True),
	StructField('streetName',StringType(),True),
	StructField('streetAbbreviation',StringType(),True),
	StructField('streetCode',StringType(),True),
	StructField('telephoneExtension',StringType(),True),
	StructField('phoneNumber',StringType(),True),
	StructField('addressTimeZone',StringType(),True),
	StructField('titleCode',StringType(),True),
	StructField('_RecordStart',TimestampType(),False),
	StructField('_RecordEnd',TimestampType(),False),
	StructField('_RecordDeleted',IntegerType(),False),
	StructField('_RecordCurrent',IntegerType(),False)
])

df_updated_column = spark.createDataFrame(df_cleansed.rdd, schema=newSchema)

# COMMAND ----------

# DBTITLE 1,12. Save Data frame into Cleansed Delta table (Final)
#Save Data frame into Cleansed Delta table (final)
DeltaSaveDataframeDirect(df_updated_column, source_group, target_table, ADS_DATABASE_CLEANSED, ADS_CONTAINER_CLEANSED, "overwrite", "")

# COMMAND ----------

# DBTITLE 1,13. Exit Notebook
dbutils.notebook.exit("1")
