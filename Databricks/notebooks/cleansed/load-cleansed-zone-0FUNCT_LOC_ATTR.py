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
df_cleansed_column = spark.sql("SELECT  \
                                  TPLNR as functionalLocationNumber, \
                                  FLTYP as functionalLocationCategory, \
                                  IWERK as maintenancePlanningPlant, \
                                  SWERK as maintenancePlant, \
                                  ADRNR as addressNumber, \
                                  KOKRS as controllingArea, \
                                  BUKRS as companyCode, \
                                  cc.companyName as companyName, \
                                  PROID as workBreakdownStructureElement, \
                                  to_date(ERDAT, 'yyyy-MM-dd hh24:mm:ss') as createdDate, \
                                  to_date(AEDAT, 'yyyy-MM-dd hh24:mm:ss') as lastChangedDate, \
                                  ZZ_ZCD_AONR as architecturalObjectCount, \
                                  ZZ_ADRNR as zzaddressNumber, \
                                  ZZ_OWNER as objectReferenceIndicator, \
                                  ZZ_VSTELLE as premiseId, \
                                  ZZ_ANLAGE as installationId, \
                                  ZZ_VKONTO as contractAccountNumber, \
                                  ZZADRMA as alternativeAddressNumber, \
                                  ZZ_OBJNR as objectNumber, \
                                  ZZ_IDNUMBER as identificationNumber, \
                                  ZZ_GPART as businessPartnerNumber, \
                                  ZZ_HAUS as connectionObjectId, \
                                  ZZ_LOCATION as locationDescription, \
                                  ZZ_BUILDING as buildingNumber, \
                                  ZZ_FLOOR as floorNumber, \
                                  ZZ_HOUSE_NUM2 as houseNumber2, \
                                  ZZ_HOUSE_NUM3 as houseNumber3, \
                                  ZZ_HOUSE_NUM1 as houseNumber1, \
                                  ZZ_STREET as streetName, \
                                  ZZ_STR_SUPPL1 as streetLine1, \
                                  ZZ_STR_SUPPL2 as streetLine2, \
                                  ZZ_CITY1 as cityName, \
                                  ZZ_REGION as stateCode, \
                                  ZZ_POST_CODE1 as postCode, \
                                  ZZZ_LOCATION as locationDescriptionSecondary, \
                                  ZZZ_BUILDING as buildingNumberSecondary, \
                                  ZZZ_FLOOR as floorNumberSecondary, \
                                  ZZZ_HOUSE_NUM2 as houseNumber2Secondary, \
                                  ZZZ_HOUSE_NUM3 as houseNumber3Secondary, \
                                  ZZZ_HOUSE_NUM1 as houseNumber1Secondary, \
                                  ZZZ_STREET as streetNameSecondary, \
                                  ZZZ_STR_SUPPL1 as streetLine1Secondary, \
                                  ZZZ_STR_SUPPL2 as streetLine2Secondary, \
                                  ZZZ_CITY1 as cityNameSecondary, \
                                  ZZZ_REGION as stateCodeSecondary, \
                                  ZZZ_POST_CODE1 as postCodeSecondary, \
                                  ZCD_BLD_FEE_DATE as buildingFeeDate, \
                                  stg._RecordStart, \
                                  stg._RecordEnd, \
                                  stg._RecordDeleted, \
                                  stg._RecordCurrent \
                               FROM CLEANSED.STG_SAPISU_0FUNCT_LOC_ATTR stg \
                                 left outer join cleansed.t_sapisu_0comp_code_text cc on cc.companyCode = stg.BUKRS"
                              )
display(df_cleansed_column)

# COMMAND ----------

newSchema = StructType([
                          StructField('functionalLocationNumber', StringType(), False),
                          StructField('functionalLocationCategory', StringType(), True),
                          StructField('maintenancePlanningPlant', StringType(), True),
                          StructField('maintenancePlant', StringType(), True),
                          StructField('addressNumber', StringType(), True),
                          StructField('controllingArea', StringType(), True),
                          StructField('companyCode', StringType(), True),
                          StructField('companyName', StringType(), True),
                          StructField('workBreakdownStructureElement', StringType(), True),
                          StructField('createdDate', DateType(), True),
                          StructField('lastChangedDate', DateType(), True),
                          StructField('architecturalObjectCount', StringType(), True),
                          StructField('zzaddressNumber', StringType(), True),
                          StructField('objectReferenceIndicator', StringType(), True),
                          StructField('premiseId', StringType(), True),
                          StructField('installationId', StringType(), True),
                          StructField('contractAccountNumber', StringType(), True),
                          StructField('alternativeAddressNumber', StringType(), True),
                          StructField('objectNumber', StringType(), True),
                          StructField('identificationNumber', StringType(), True),
                          StructField('businessPartnerNumber', StringType(), True),
                          StructField('connectionObjectId', StringType(), True),
                          StructField('locationDescription', StringType(), True),
                          StructField('buildingNumber', StringType(), True),
                          StructField('floorNumber', StringType(), True),
                          StructField('houseNumber2', StringType(), True),
                          StructField('houseNumber3', StringType(), True),
                          StructField('houseNumber1', StringType(), True),
                          StructField('streetName', StringType(), True),
                          StructField('streetLine1', StringType(), True),
                          StructField('streetLine2', StringType(), True),
                          StructField('cityName', StringType(), True),
                          StructField('stateCode', StringType(), True),
                          StructField('postCode', StringType(), True),
                          StructField('locationDescriptionSecondary', StringType(), True),
                          StructField('buildingNumberSecondary', StringType(), True),
                          StructField('floorNumberSecondary', StringType(), True),
                          StructField('houseNumber2Secondary', StringType(), True),
                          StructField('houseNumber3Secondary', StringType(), True),
                          StructField('houseNumber1Secondary', StringType(), True),
                          StructField('streetNameSecondary', StringType(), True),
                          StructField('streetLine1Secondary', StringType(), True),
                          StructField('streetLine2Secondary', StringType(), True),
                          StructField('cityNameSecondary', StringType(), True),
                          StructField('stateCodeSecondary', StringType(), True),
                          StructField('postCodeSecondary', StringType(), True),
                          StructField('buildingFeeDate', StringType(), True),
                          StructField('_RecordStart',TimestampType(),False),
                          StructField('_RecordEnd',TimestampType(),False),
                          StructField('_RecordDeleted',IntegerType(),False),
                          StructField('_RecordCurrent',IntegerType(),False)
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
