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

# DBTITLE 1,1. Import libreries/functions
#1.Import libreries/functions
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
# MAGIC %run ../includes/include-all-util

# COMMAND ----------

# DBTITLE 1,7. Include User functions (CleansedZone) for the notebook
# MAGIC %run ./utility/transform_data_cleansedzone

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
df_updated_column_temp = spark.sql("SELECT \
                                BP.PARTNER as businessPartnerNumber,\
                                BP.TYPE as businessPartnerCategoryCode,\
                                BP_TXT.businessPartnerCategory as businessPartnerCategory,\
                                BP.BPKIND as businessPartnerTypeCode,\
                                BPTYPE.businessPartnerType as businessPartnerType,\
                                BP.BU_GROUP as businessPartnerGroupCode,\
                                BPGRP.businessPartnerGroup as businessPartnerGroup,\
                                BP.BPEXT as externalBusinessPartnerNumber,\
                                BP.BU_SORT1 as searchTerm1,\
                                BP.BU_SORT2 as searchTerm2,\
                                BP.TITLE as titleCode,\
                                TITLE.TITLE as title,\
                                BP.XDELE as deletedIndicator,\
                                BP.XBLCK as centralBlockBusinessPartner,\
                                BP.ZZUSER as userId,\
                                BP.ZZPAS_INDICATOR as paymentAssistSchemeIndicator,\
                                BP.ZZBA_INDICATOR as billAssistIndicator,\
                                to_date(BP.ZZAFLD00001Z) as createdOn,\
                                BP.NAME_ORG1 as organizationName1,\
                                BP.NAME_ORG2 as organizationName2,\
                                BP.NAME_ORG3 as organizationName3,\
                                to_date(BP.FOUND_DAT) as organizationFoundedDate,\
                                BP.LOCATION_1 as internationalLocationNumber1,\
                                BP.LOCATION_2 as internationalLocationNumber2,\
                                BP.LOCATION_3 as internationalLocationNumber3,\
                                BP.NAME_LAST as lastName,\
                                BP.NAME_FIRST as firstName,\
                                BP.NAME_LAST2 as atBirthName,\
                                BP.NAMEMIDDLE as middleName,\
                                BP.TITLE_ACA1 as academicTitle,\
                                BP.NICKNAME as nickName,\
                                BP.INITIALS as nameInitials,\
                                BP.NAMCOUNTRY as countryName,\
                                BP.LANGU_CORR as correspondanceLanguage,\
                                BP.NATIO as nationality,\
                                BP.PERSNUMBER as personNumber,\
                                BP.XSEXU as unknownGenderIndicator,\
                                BP.BU_LANGU as language,\
                                to_date(BP.BIRTHDT) as dateOfBirth,\
                                to_date(BP.DEATHDT) as dateOfDeath,\
                                BP.PERNO as personnelNumber,\
                                BP.NAME_GRP1 as nameGroup1,\
                                BP.NAME_GRP2 as nameGroup2,\
                                BP.CRUSR as createdBy,\
                                to_date(BP.CRDAT) as createdDate,\
                                BP.CRTIM as createdTime,\
                                BP.CHUSR as changedBy,\
                                to_date(BP.CHDAT) as changedDate,\
                                BP.CHTIM as changedTime,\
                                BP.PARTNER_GUID as businessPartnerGUID,\
                                BP.ADDRCOMM as addressNumber,\
                                BP.VALID_FROM as validFromDate,\
                                BP.VALID_TO as validToDate,\
                                BP.NATPERS as naturalPersonIndicator,\
                                BP._RecordStart, \
                                BP._RecordEnd, \
                                BP._RecordDeleted, \
                                BP._RecordCurrent \
                               FROM CLEANSED.STG_SAPISU_0BPARTNER_ATTR BP \
                               LEFT OUTER JOIN CLEANSED.T_SAPISU_0BPARTNER_TEXT BP_TXT \
                                 ON BP.PARTNER = BP_TXT.businessPartnerNumber AND BP.TYPE =BP_TXT.businessPartnerCategoryCode \
                               LEFT OUTER JOIN CLEANSED.T_SAPISU_0BPTYPE_TEXT BPTYPE ON BP.TYPE = BPTYPE.businessPartnerTypeCode \
                               LEFT OUTER JOIN CLEANSED.T_SAPISU_0BP_GROUP_TEXT BPGRP ON BP.TYPE = BPGRP.businessPartnerGroupCode \
                               LEFT OUTER JOIN CLEANSED.T_SAPISU_TSAD3T TITLE ON BP.TITLE = TITLE.titlecode")

display(df_updated_column_temp)

# COMMAND ----------

# Create schema for the cleanse table
cleanse_Schema = StructType(
  [
    StructField("businessPartnerNumber", StringType(), False),
    StructField("businessPartnerCategoryCode", StringType(), True),
    StructField("businessPartnerCategory", StringType(), True),
    StructField("businessPartnerTypeCode", StringType(), True),
    StructField("businessPartnerType", StringType(), True),
    StructField("businessPartnerGroupCode", StringType(), True),
    StructField("businessPartnerGroup", StringType(), True),
    StructField("externalBusinessPartnerNumber", StringType(), True),
    StructField("searchTerm1", StringType(), True),
    StructField("searchTerm2", StringType(), True),
    StructField("titleCode", StringType(), True),
    StructField("title", StringType(), True),
    StructField("deletedIndicator", StringType(), True),
    StructField("centralBlockBusinessPartner", StringType(), True),
    StructField("userId", StringType(), True),
    StructField("paymentAssistSchemeIndicator", StringType(), True),
    StructField("billAssistIndicator", StringType(), True),
    StructField("createdOn", DateType(), True),
    StructField("organizationName1", StringType(), True),
    StructField("organizationName2", StringType(), True),
    StructField("organizationName3", StringType(), True),
    StructField("organizationFoundedDate", DateType(), True),
    StructField("internationalLocationNumber1", StringType(), True),
    StructField("internationalLocationNumber2", StringType(), True),
    StructField("internationalLocationNumber3", StringType(), True),
    StructField("lastName", StringType(), True),
    StructField("firstName", StringType(), True),
    StructField("atBirthName", StringType(), True),
    StructField("middleName", StringType(), True),
    StructField("academicTitle", StringType(), True),
    StructField("nickName", StringType(), True),
    StructField("nameInitials", StringType(), True),
    StructField("countryName", StringType(), True),
    StructField("correspondanceLanguage", StringType(), True),
    StructField("nationality", StringType(), True),
    StructField("personNumber", StringType(), True),
    StructField("unknownGenderIndicator", StringType(), True),
    StructField("language", StringType(), True),
    StructField("dateOfBirth", DateType(), True),
    StructField("dateOfDeath", DateType(), True),
    StructField("personnelNumber", StringType(), True),
    StructField("nameGroup1", StringType(), True),
    StructField("nameGroup2", StringType(), True),
    StructField("createdBy", StringType(), True),
    StructField("createdDate", DateType(), True),
    StructField("createdTime", StringType(), True),
    StructField("changedBy", StringType(), True),
    StructField("changedDate", DateType(), True),
    StructField("changedTime", StringType(), True),
    StructField("businessPartnerGUID", StringType(), True),
    StructField("addressNumber", StringType(), True),
    StructField("validFromDate",StringType(), True),
    StructField("validToDate",StringType(), True),
    StructField("naturalPersonIndicator", StringType(), True),
    StructField('_RecordStart',TimestampType(),False),
    StructField('_RecordEnd',TimestampType(),False),
    StructField('_RecordDeleted',IntegerType(),False),
    StructField('_RecordCurrent',IntegerType(),False)
  ]
)
# Apply the new schema to cleanse Data Frame
df_updated_column = spark.createDataFrame(df_updated_column_temp.rdd, schema=cleanse_Schema)
display(df_updated_column)

# COMMAND ----------

# DBTITLE 1,12. Save Data frame into Cleansed Delta table (Final)
#Save Data frame into Cleansed Delta table (final)
DeltaSaveDataframeDirect(df_updated_column, "t", source_object, ADS_DATABASE_CLEANSED, ADS_CONTAINER_CLEANSED, "overwrite", "")

# COMMAND ----------

# DBTITLE 1,13. Exit Notebook
dbutils.notebook.exit("1")
