# Databricks notebook source
# DBTITLE 1,Generate parameter and source object name for unit testing
import json
#For unit testing...
#Use this string in the Param widget: 
#{"SourceType": "BLOB Storage (json)", "SourceServer": "daf-sa-lake-sastoken", "SourceGroup": "isu", "SourceName": "isu_0BP_RELATIONS_ATTR", "SourceLocation": "isu/0BP_RELATIONS_ATTR", "AdditionalProperty": "", "Processor": "databricks-token|0711-011053-turfs581|Standard_DS3_v2|8.3.x-scala2.12|2:8|interactive", "IsAuditTable": false, "SoftDeleteSource": "", "ProjectName": "ISU DATA", "ProjectId": 2, "TargetType": "BLOB Storage (json)", "TargetName": "isu_0BP_RELATIONS_ATTR", "TargetLocation": "isu/0BP_RELATIONS_ATTR", "TargetServer": "daf-sa-lake-sastoken", "DataLoadMode": "FULL-EXTRACT", "DeltaExtract": false, "CDCSource": false, "TruncateTarget": false, "UpsertTarget": true, "AppendTarget": null, "TrackChanges": false, "LoadToSqlEDW": true, "TaskName": "isu_0BP_RELATIONS_ATTR", "ControlStageId": 2, "TaskId": 46, "StageSequence": 200, "StageName": "Raw to Cleansed", "SourceId": 46, "TargetId": 46, "ObjectGrain": "Day", "CommandTypeId": 8, "Watermarks": "", "WatermarksDT": null, "WatermarkColumn": "", "BusinessKeyColumn": "businessPartnerRelationshipNumber,businessPartnerNumber1,businessPartnerNumber2,validToDate", "UpdateMetaData": null, "SourceTimeStampFormat": "", "Command": "", "LastLoadedFile": null}

#Use this string in the Source Object widget
#isu_0BP_RELATIONS_ATTR

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
delta_cleansed_tbl_name = "{0}.{1}".format(ADS_DATABASE_CLEANSED, target_table)
delta_raw_tbl_name = "{0}.{1}".format(ADS_DATABASE_RAW, source_object)

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
                                case when RELNR = 'na' then '' else RELNR end as businessPartnerRelationshipNumber, \
                                case when PARTNER1 = 'na' then '' else PARTNER1 end as businessPartnerNumber1, \
                                case when PARTNER2 = 'na' then '' else PARTNER2 end as businessPartnerNumber2, \
                                PARTNER1_GUID as businessPartnerGUID1, \
                                PARTNER2_GUID as businessPartnerGUID2, \
                                RELDIR as relationshipDirection, \
                                RELTYP as relationshipTypeCode, \
                                BP_TXT.relationshipType as relationshipType, \
                                case when DATE_TO = 'na' then to_date('1900-01-01', 'yyyy-MM-dd') else to_date(DATE_TO, 'yyyy-MM-dd') end as validToDate, \
                                case when DATE_FROM < '1900-01-01' then to_date('1900-01-01', 'yyyy-MM-dd') else to_date(DATE_FROM, 'yyyy-MM-dd') end as validFromDate, \
                                COUNTRY as countryShortName, \
                                POST_CODE1 as postalCode, \
                                CITY1 as cityName, \
                                STREET as streetName, \
                                HOUSE_NUM1 as houseNumber, \
                                TEL_NUMBER as phoneNumber, \
                                SMTP_ADDR as emailAddress, \
                                cast(CMPY_PART_PER as long) as capitalInterestPercentage, \
                                cast(CMPY_PART_AMO as dec(13,0)) as capitalInterestAmount, \
                                ADDR_SHORT as shortFormattedAddress, \
                                ADDR_SHORT_S as shortFormattedAddress2, \
                                LINE0 as addressLine0, \
                                LINE1 as addressLine1, \
                                LINE2 as addressLine2, \
                                LINE3 as addressLine3, \
                                LINE4 as addressLine4, \
                                LINE5 as addressLine5, \
                                LINE6 as addressLine6, \
                                FLG_DELETED as deletedIndicator, \
                                BP._RecordStart, \
                                BP._RecordEnd, \
                                BP._RecordDeleted, \
                                BP._RecordCurrent \
                          FROM {ADS_DATABASE_STAGE}.{source_object} BP \
                          LEFT OUTER JOIN {ADS_DATABASE_CLEANSED}.isu_0BP_RELTYPES_TEXT BP_TXT \
                                ON BP.RELDIR = BP_TXT.relationshipDirection AND BP.RELTYP =BP_TXT.relationshipTypeCode \
                                AND BP_TXT._RecordDeleted = 0 AND BP_TXT._RecordCurrent = 1")

display(df_cleansed)
print(f'Number of rows: {df_cleansed.count()}')

# COMMAND ----------

newSchema = StructType([
	StructField('businessPartnerRelationshipNumber',StringType(),False),
	StructField('businessPartnerNumber1',StringType(),False),
	StructField('businessPartnerNumber2',StringType(),False),
	StructField('businessPartnerGUID1',StringType(),True),
	StructField('businessPartnerGUID2',StringType(),True),
	StructField('relationshipDirection',StringType(),True),
	StructField('relationshipTypeCode',StringType(),True),
	StructField('relationshipType',StringType(),True),
	StructField('validToDate',DateType(),False),
	StructField('validFromDate',DateType(),True),
	StructField('countryShortName',StringType(),True),
	StructField('postalCode',StringType(),True),
	StructField('cityName',StringType(),True),
	StructField('streetName',StringType(),True),
	StructField('houseNumber',StringType(),True),
	StructField('phoneNumber',StringType(),True),
	StructField('emailAddress',StringType(),True),
	StructField('capitalInterestPercentage',LongType(),True),
	StructField('capitalInterestAmount',DecimalType(13,0),True),
	StructField('shortFormattedAddress',StringType(),True),
	StructField('shortFormattedAddress2',StringType(),True),
	StructField('addressLine0',StringType(),True),
	StructField('addressLine1',StringType(),True),
	StructField('addressLine2',StringType(),True),
	StructField('addressLine3',StringType(),True),
	StructField('addressLine4',StringType(),True),
	StructField('addressLine5',StringType(),True),
	StructField('addressLine6',StringType(),True),
	StructField('deletedIndicator',StringType(),True),
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