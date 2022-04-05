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
                      (Select *, ROW_NUMBER() OVER (PARTITION BY PARTNER ORDER BY _FileDateTimeStamp DESC, DI_SEQUENCE_NUMBER DESC, _DLRawZoneTimeStamp DESC) AS _RecordVersion FROM {delta_raw_tbl_name} WHERE _DLRawZoneTimestamp >= '{LastSuccessfulExecutionTS}') \
                           SELECT \
                                case when BP.PARTNER = 'na' then '' else BP.PARTNER end as businessPartnerNumber, \
                                BP.TYPE as businessPartnerCategoryCode, \
                                BP_TXT.businessPartnerCategory as businessPartnerCategory, \
                                BP.BPKIND as businessPartnerTypeCode, \
                                BPTYPE.businessPartnerType as businessPartnerType, \
                                BP.BU_GROUP as businessPartnerGroupCode, \
                                BPGRP.businessPartnerGroup as businessPartnerGroup, \
                                BP.BPEXT as externalBusinessPartnerNumber, \
                                BP.BU_SORT1 as searchTerm1, \
                                BP.BU_SORT2 as searchTerm2, \
                                BP.TITLE as titleCode, \
                                TITLE.TITLE as title, \
                                BP.XDELE as deletedIndicator, \
                                BP.XBLCK as centralBlockBusinessPartner, \
                                BP.ZZUSER as userId, \
                                BP.ZZPAS_INDICATOR as paymentAssistSchemeIndicator, \
                                BP.ZZBA_INDICATOR as billAssistIndicator, \
                                ToValidDate(BP.ZZAFLD00001Z) as createdOn, \
                                BP.NAME_ORG1 as organizationName1, \
                                BP.NAME_ORG2 as organizationName2, \
                                BP.NAME_ORG3 as organizationName3, \
                                ToValidDate(BP.FOUND_DAT) as organizationFoundedDate, \
                                BP.LOCATION_1 as internationalLocationNumber1, \
                                BP.LOCATION_2 as internationalLocationNumber2, \
                                BP.LOCATION_3 as internationalLocationNumber3, \
                                BP.NAME_LAST as lastName, \
                                BP.NAME_FIRST as firstName, \
                                BP.NAME_LAST2 as atBirthName, \
                                BP.NAMEMIDDLE as middleName, \
                                BP.TITLE_ACA1 as academicTitle, \
                                BP.NICKNAME as nickName, \
                                BP.INITIALS as nameInitials, \
                                BP.NAMCOUNTRY as countryName, \
                                BP.LANGU_CORR as correspondenceLanguage, \
                                BP.NATIO as nationality, \
                                BP.PERSNUMBER as personNumber, \
                                BP.XSEXU as unknownGenderIndicator, \
                                BP.BU_LANGU as language, \
                                ToValidDate(BP.BIRTHDT) as dateOfBirth, \
                                ToValidDate(BP.DEATHDT) as dateOfDeath, \
                                BP.PERNO as personnelNumber, \
                                BP.NAME_GRP1 as nameGroup1, \
                                BP.NAME_GRP2 as nameGroup2, \
                                BP.CRUSR as createdBy, \
                                ToValidDateTime(concat(BP.CRDAT, 'T', coalesce(BP.CRTIM,'00:00:00'))) as createdDateTime, \
                                BP.CHUSR as changedBy, \
                                ToValidDateTime(concat(BP.CHDAT, 'T', coalesce(BP.CHTIM,'00:00:00'))) as changedDateTime, \
                                BP.PARTNER_GUID as businessPartnerGUID, \
                                BP.ADDRCOMM as addressNumber, \
                                ToValidDate(substr(BP.VALID_FROM,0,8)) as validFromDate, \
                                ToValidDate(substr(BP.VALID_TO,0,8)) as validToDate, \
                                BP.NATPERS as naturalPersonIndicator, \
                                cast('1900-01-01' as TimeStamp) as _RecordStart, \
                                cast('9999-12-31' as TimeStamp) as _RecordEnd, \
                                '0' as _RecordDeleted, \
                                '1' as _RecordCurrent, \
                                cast('{CurrentTimeStamp}' as TimeStamp) as _DLCleansedZoneTimeStamp \
                        FROM stage BP \
                               LEFT OUTER JOIN {ADS_DATABASE_CLEANSED}.isu_0BPARTNER_TEXT BP_TXT \
                                 ON BP.PARTNER = BP_TXT.businessPartnerNumber AND BP.TYPE =BP_TXT.businessPartnerCategoryCode \
                                                                              AND BP_TXT._RecordDeleted = 0 AND BP_TXT._RecordCurrent = 1 \
                               LEFT OUTER JOIN {ADS_DATABASE_CLEANSED}.isu_0BPTYPE_TEXT BPTYPE ON BP.BPKIND = BPTYPE.businessPartnerTypeCode \
                                                                              AND BPTYPE._RecordDeleted = 0 AND BPTYPE._RecordCurrent = 1 \
                               LEFT OUTER JOIN {ADS_DATABASE_CLEANSED}.isu_0BP_GROUP_TEXT BPGRP ON BP.BU_GROUP = BPGRP.businessPartnerGroupCode \
                                                                              AND BPGRP._RecordDeleted = 0 AND BPGRP._RecordCurrent = 1 \
                               LEFT OUTER JOIN {ADS_DATABASE_CLEANSED}.isu_TSAD3T TITLE ON BP.TITLE = TITLE.titlecode \
                                                                              AND TITLE._RecordDeleted = 0 AND TITLE._RecordCurrent = 1 \
                        where BP._RecordVersion = 1 ").cache()

print(f'Number of rows: {df.count()}')

# COMMAND ----------

# DBTITLE 1,11. Update/Rename Columns and Load into a Dataframe
#Update/rename Column
#Pass 'MANDATORY' as second argument to function ToValidDate() on key columns to ensure correct value settings for those columns
# df_cleansed = spark.sql(f"SELECT \
#                                 case when BP.PARTNER = 'na' then '' else BP.PARTNER end as businessPartnerNumber, \
#                                 BP.TYPE as businessPartnerCategoryCode, \
#                                 BP_TXT.businessPartnerCategory as businessPartnerCategory, \
#                                 BP.BPKIND as businessPartnerTypeCode, \
#                                 BPTYPE.businessPartnerType as businessPartnerType, \
#                                 BP.BU_GROUP as businessPartnerGroupCode, \
#                                 BPGRP.businessPartnerGroup as businessPartnerGroup, \
#                                 BP.BPEXT as externalBusinessPartnerNumber, \
#                                 BP.BU_SORT1 as searchTerm1, \
#                                 BP.BU_SORT2 as searchTerm2, \
#                                 BP.TITLE as titleCode, \
#                                 TITLE.TITLE as title, \
#                                 BP.XDELE as deletedIndicator, \
#                                 BP.XBLCK as centralBlockBusinessPartner, \
#                                 BP.ZZUSER as userId, \
#                                 BP.ZZPAS_INDICATOR as paymentAssistSchemeIndicator, \
#                                 BP.ZZBA_INDICATOR as billAssistIndicator, \
#                                 ToValidDate(BP.ZZAFLD00001Z) as createdOn, \
#                                 BP.NAME_ORG1 as organizationName1, \
#                                 BP.NAME_ORG2 as organizationName2, \
#                                 BP.NAME_ORG3 as organizationName3, \
#                                 ToValidDate(BP.FOUND_DAT) as organizationFoundedDate, \
#                                 BP.LOCATION_1 as internationalLocationNumber1, \
#                                 BP.LOCATION_2 as internationalLocationNumber2, \
#                                 BP.LOCATION_3 as internationalLocationNumber3, \
#                                 BP.NAME_LAST as lastName, \
#                                 BP.NAME_FIRST as firstName, \
#                                 BP.NAME_LAST2 as atBirthName, \
#                                 BP.NAMEMIDDLE as middleName, \
#                                 BP.TITLE_ACA1 as academicTitle, \
#                                 BP.NICKNAME as nickName, \
#                                 BP.INITIALS as nameInitials, \
#                                 BP.NAMCOUNTRY as countryName, \
#                                 BP.LANGU_CORR as correspondenceLanguage, \
#                                 BP.NATIO as nationality, \
#                                 BP.PERSNUMBER as personNumber, \
#                                 BP.XSEXU as unknownGenderIndicator, \
#                                 BP.BU_LANGU as language, \
#                                 ToValidDate(BP.BIRTHDT) as dateOfBirth, \
#                                 ToValidDate(BP.DEATHDT) as dateOfDeath, \
#                                 BP.PERNO as personnelNumber, \
#                                 BP.NAME_GRP1 as nameGroup1, \
#                                 BP.NAME_GRP2 as nameGroup2, \
#                                 BP.CRUSR as createdBy, \
#                                 ToValidDateTime(concat(BP.CRDAT, 'T', coalesce(BP.CRTIM,'00:00:00'))) as createdDateTime, \
#                                 BP.CHUSR as changedBy, \
#                                 ToValidDateTime(concat(BP.CHDAT, 'T', coalesce(BP.CHTIM,'00:00:00'))) as changedDateTime, \
#                                 BP.PARTNER_GUID as businessPartnerGUID, \
#                                 BP.ADDRCOMM as addressNumber, \
#                                 ToValidDate(substr(BP.VALID_FROM,0,8)) as validFromDate, \
#                                 ToValidDate(substr(BP.VALID_TO,0,8)) as validToDate, \
#                                 BP.NATPERS as naturalPersonIndicator, \
#                                 BP._RecordStart, \
#                                 BP._RecordEnd, \
#                                 BP._RecordDeleted, \
#                                 BP._RecordCurrent \
#                                FROM {ADS_DATABASE_STAGE}.{source_object} BP \
#                                LEFT OUTER JOIN {ADS_DATABASE_CLEANSED}.isu_0BPARTNER_TEXT BP_TXT \
#                                  ON BP.PARTNER = BP_TXT.businessPartnerNumber AND BP.TYPE =BP_TXT.businessPartnerCategoryCode \
#                                                                               AND BP_TXT._RecordDeleted = 0 AND BP_TXT._RecordCurrent = 1 \
#                                LEFT OUTER JOIN {ADS_DATABASE_CLEANSED}.isu_0BPTYPE_TEXT BPTYPE ON BP.BPKIND = BPTYPE.businessPartnerTypeCode \
#                                                                               AND BPTYPE._RecordDeleted = 0 AND BPTYPE._RecordCurrent = 1 \
#                                LEFT OUTER JOIN {ADS_DATABASE_CLEANSED}.isu_0BP_GROUP_TEXT BPGRP ON BP.BU_GROUP = BPGRP.businessPartnerGroupCode \
#                                                                               AND BPGRP._RecordDeleted = 0 AND BPGRP._RecordCurrent = 1 \
#                                LEFT OUTER JOIN {ADS_DATABASE_CLEANSED}.isu_TSAD3T TITLE ON BP.TITLE = TITLE.titlecode \
#                                                                               AND TITLE._RecordDeleted = 0 AND TITLE._RecordCurrent = 1")

# print(f'Number of rows: {df_cleansed.count()}')

# COMMAND ----------

# # Create schema for the cleanse table
# newSchema = StructType(
#   [
#     StructField("businessPartnerNumber", StringType(), False),
#     StructField("businessPartnerCategoryCode", StringType(), True),
#     StructField("businessPartnerCategory", StringType(), True),
#     StructField("businessPartnerTypeCode", StringType(), True),
#     StructField("businessPartnerType", StringType(), True),
#     StructField("businessPartnerGroupCode", StringType(), True),
#     StructField("businessPartnerGroup", StringType(), True),
#     StructField("externalBusinessPartnerNumber", StringType(), True),
#     StructField("searchTerm1", StringType(), True),
#     StructField("searchTerm2", StringType(), True),
#     StructField("titleCode", StringType(), True),
#     StructField("title", StringType(), True),
#     StructField("deletedIndicator", StringType(), True),
#     StructField("centralBlockBusinessPartner", StringType(), True),
#     StructField("userId", StringType(), True),
#     StructField("paymentAssistSchemeIndicator", StringType(), True),
#     StructField("billAssistIndicator", StringType(), True),
#     StructField("createdOn", DateType(), True),
#     StructField("organizationName1", StringType(), True),
#     StructField("organizationName2", StringType(), True),
#     StructField("organizationName3", StringType(), True),
#     StructField("organizationFoundedDate", DateType(), True),
#     StructField("internationalLocationNumber1", StringType(), True),
#     StructField("internationalLocationNumber2", StringType(), True),
#     StructField("internationalLocationNumber3", StringType(), True),
#     StructField("lastName", StringType(), True),
#     StructField("firstName", StringType(), True),
#     StructField("atBirthName", StringType(), True),
#     StructField("middleName", StringType(), True),
#     StructField("academicTitle", StringType(), True),
#     StructField("nickName", StringType(), True),
#     StructField("nameInitials", StringType(), True),
#     StructField("countryName", StringType(), True),
#     StructField("correspondenceLanguage", StringType(), True),
#     StructField("nationality", StringType(), True),
#     StructField("personNumber", StringType(), True),
#     StructField("unknownGenderIndicator", StringType(), True),
#     StructField("language", StringType(), True),
#     StructField("dateOfBirth", DateType(), True),
#     StructField("dateOfDeath", DateType(), True),
#     StructField("personnelNumber", StringType(), True),
#     StructField("nameGroup1", StringType(), True),
#     StructField("nameGroup2", StringType(), True),
#     StructField("createdBy", StringType(), True),
#     StructField("createdDateTime", TimestampType(), True),
#     StructField("changedBy", StringType(), True),
#     StructField("changedDateTime", TimestampType(), True),
#     StructField("businessPartnerGUID", StringType(), True),
#     StructField("addressNumber", StringType(), True),
#     StructField("validFromDate",DateType(), True),
#     StructField("validToDate",DateType(), True),
#     StructField("naturalPersonIndicator", StringType(), True),
#     StructField('_RecordStart',TimestampType(),False),
#     StructField('_RecordEnd',TimestampType(),False),
#     StructField('_RecordDeleted',IntegerType(),False),
#     StructField('_RecordCurrent',IntegerType(),False)
#   ]
# )


# COMMAND ----------

# DBTITLE 1,12. Save Data frame into Cleansed Delta table (Final)
DeltaSaveDataFrameToDeltaTableNew(df, target_table, ADS_DATALAKE_ZONE_CLEANSED, ADS_DATABASE_CLEANSED, data_lake_folder, ADS_WRITE_MODE_MERGE, track_changes, is_delta_extract, business_key, AddSKColumn = False, delta_column = "", start_counter = "0", end_counter = "0")
#clear cache
df.unpersist()

# COMMAND ----------

# DBTITLE 1,13. Exit Notebook
dbutils.notebook.exit("1")
