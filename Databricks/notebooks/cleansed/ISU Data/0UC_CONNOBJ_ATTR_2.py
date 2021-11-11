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
                                      case when con.HAUS = 'na' then '' else con.HAUS end as propertyNumber, \
                                      case when con.COUNTRY = 'na' then '' else con.COUNTRY end  as countryShortName, \
                                      con.CITY_CODE as cityCode, \
                                      con.STREETCODE as streetCode, \
                                      stc.STREET as streetName, \
                                      con.POSTALCODE as postCode, \
                                      con.REGION as stateCode, \
                                      con.REGIO_GRP as regionGroup, \
                                      con.REGPOLIT as politicalRegionCode, \
                                      reg.REGNAME as politicalRegion, \
                                      case when con.DATE_FROM is null or year(to_date(con.DATE_FROM, 'yyyy-MM-dd')) < 1900 \
                                                  then to_date('1900-01-01', 'yyyy-MM-dd') else to_date(con.DATE_FROM, 'yyyy-MM-dd') end as validFromDate, \
                                      con.WWTP as connectionObjectGUID, \
                                      con.UPDMOD as updateMode, \
                                      con.LOEVM as deletedIndicator, \
                                      con.REGIOGROUP_PERM as regionGroupPermit, \
                                      con.ZCD_PLAN_TYPE as planTypeCode, \
                                      plt.DESCRIPTION as planType, \
                                      con.ZCD_PROCESS_TYPE as processTypeCode, \
                                      prt.DESCRIPTION as processType, \
                                      con.ZCD_PLAN_NUMBER as planNumber, \
                                      con.ZCD_LOT_TYPE as lotTypeCode, \
                                      con.ZCD_LOT_NUMBER as lotNumber, \
                                      con.ZCD_SECTION_NUMBER as sectionNumber, \
                                      con.ZCD_IND_SRV_AGR as serviceAgreementIndicator, \
                                      con.ZCD_IND_MLIM as mlimIndicator, \
                                      con.ZCD_IND_WICA as wicaIndicator, \
                                      con.ZCD_IND_SOPA as sopaIndicator, \
                                      con.ZCD_AONR as architecturalObjectNumber, \
                                      con.ZCD_AOTYPE as architecturalObjectTypeCode, \
                                      con.ZCD_CASE_NO as caseNumber, \
                                      tiv.XMAOTYPE as architecturalObjectType, \
                                      con.ZCD_BLD_FEE_DATE as buildingFeeDate, \
                                      con.ZCD_CAD_ID as cadID, \
                                      con.ZZPUMP_WW as pumpWateWaterIndicator, \
                                      con.ZZFIRE_SERVICE as fireServiceIndicator, \
                                      con.ZINTRENO as architecturalObjectInternalId, \
                                      con.ZAOID as architecturalObjectId, \
                                      con.ZFIXFITCHARACT as fixtureAndFittingCharacteristic, \
                                      con.HOUSE_ADRNR as houseAddressNumber, \
                                      con.WWTP as WWTP, \
                                      con.SCAMP as SCAMP, \
                                      con.HAUS_STREET as houseStreetName, \
                                      con.HAUS_NUM1 as houseNumber, \
                                      con.HAUS_LGA_NAME as LGA, \
                                      con.HOUS_CITY1 as cityName, \
                                      con.WATER_DELIVERY_SYSTEM as waterDeliverySystem, \
                                      con.WATER_DISTRIBUTION_SYSTEM_WATE as waterDistributionSystem, \
                                      con.WATER_SUPPLY_ZONE as waterSupplyZone, \
                                      con.RECYCLE_WATER_DELIVERY_SYSTEM as receycleWaterDeliverySystem, \
                                      con.RECYCLE_WATER_DISTRIBUTION_SYS as receycleWaterDistributionSystem, \
                                      con.RECYCLE_WATER_SUPPLY_ZONE as recycleWaterSupplyZone, \
                                      con.ZCD_SUP_PROP_TYPE as superiorPropertyTypeCode, \
                                      sp.superiorPropertyType as superiorPropertyType, \
                                      con.ZCD_INF_PROP_TYPE as inferiorPropertyTypeCode, \
                                      ip.inferiorPropertyType as inferiorPropertyType, \
                                      con.Z_OWNER as objectReferenceIndicator, \
                                      con.Z_OBJNR as objectNumber, \
                                      con.ZCD_NO_OF_FLATS as flatCount, \
                                      con._RecordStart, \
                                      con._RecordEnd, \
                                      con._RecordDeleted, \
                                      con._RecordCurrent \
                                    FROM {ADS_DATABASE_STAGE}.{source_object} con \
                                    LEFT OUTER JOIN {ADS_DATABASE_CLEANSED}.isu_ZCD_TINFPRTY_TX ip ON con.ZCD_INF_PROP_TYPE = ip.inferiorPropertyTypeCode \
                                                                                                    and ip._RecordDeleted = 0 and ip._RecordCurrent = 1 \
                                    LEFT OUTER JOIN {ADS_DATABASE_CLEANSED}.isu_ZCD_TSUPPRTYP_TX sp ON con.ZCD_SUP_PROP_TYPE = sp.superiorPropertyTypeCode \
                                                                                                    and sp._RecordDeleted = 0 and sp._RecordCurrent = 1 \
                                    LEFT OUTER JOIN {ADS_DATABASE_CLEANSED}.isu_ZCD_TPLANTYPE_TX plt ON con.ZCD_PLAN_TYPE = plt.PLAN_TYPE \
                                                                                                    and plt._RecordDeleted = 0 and plt._RecordCurrent = 1 \
                                    LEFT OUTER JOIN {ADS_DATABASE_CLEANSED}.isu_TIVBDAROBJTYPET tiv ON con.ZCD_AOTYPE = tiv.AOTYPE \
                                                                                                    and tiv._RecordDeleted = 0 and tiv._RecordCurrent = 1 \
                                    LEFT OUTER JOIN {ADS_DATABASE_CLEANSED}.isu_ZCD_TPROCTYPE_TX prt ON con.ZCD_PROCESS_TYPE = prt.PROCESS_TYPE \
                                                                                                    and prt._RecordDeleted = 0 and prt._RecordCurrent = 1 \
                                    LEFT OUTER JOIN {ADS_DATABASE_CLEANSED}.isu_0CAM_STREETCODE_TEXT stc ON con.STREETCODE = stc.streetCode and con.COUNTRY = stc.COUNTRY \
                                                                                                    and stc._RecordDeleted = 0 and stc._RecordCurrent = 1 \
                                    LEFT OUTER JOIN {ADS_DATABASE_CLEANSED}.isu_TE227T reg ON con.REGPOLIT = reg.REGPOLIT and con.COUNTRY = reg.COUNTRY\
                                                                                                    and reg._RecordDeleted = 0 and reg._RecordCurrent = 1")

display(df_cleansed)
print(f'Number of rows: {df_cleansed.count()}')

# COMMAND ----------

# Create schema for the cleanse table
newSchema = StructType(
                            [
                            StructField("propertyNumber", StringType(), False),
                            StructField("countryShortName", StringType(), False),
                            StructField("cityCode", StringType(), True),
                            StructField("streetCode", StringType(), True),
                            StructField("streetName", StringType(), True),
                            StructField("postCode", StringType(), True),
                            StructField("stateCode", StringType(), True),
                            StructField("regionGroup", StringType(), True),
                            StructField("politicalRegionCode", StringType(), True),
                            StructField("politicalRegion", StringType(), True),
                            StructField("validFromDate", DateType(), True),
                            StructField("connectionObjectGUID", StringType(), True),
                            StructField("updateMode", StringType(), True),
                            StructField("deletedIndicator", StringType(), True),
                            StructField("regionGroupPermit", StringType(), True),
                            StructField("planTypeCode", StringType(), True),
                            StructField("planType", StringType(), True),
                            StructField("processTypeCode", StringType(), True),
                            StructField("processType", StringType(), True),
                            StructField("planNumber", StringType(), True),
                            StructField("lotTypeCode", StringType(), True),
                            StructField("lotNumber", StringType(), True),
                            StructField("sectionNumber", StringType(), True),
                            StructField("serviceAgreementIndicator", StringType(), True),
                            StructField("mlimIndicator", StringType(), True),
                            StructField("wicaIndicator", StringType(), True),
                            StructField("sopaIndicator", StringType(), True),
                            StructField("architecturalObjectNumber", StringType(), True),
                            StructField("architecturalObjectTypeCode", StringType(), True),
                            StructField("caseNumber", StringType(), True),
                            StructField("architecturalObjectType", StringType(), True),
                            StructField("buildingFeeDate", DateType(), True),
                            StructField("cadID", StringType(), True),
                            StructField("pumpWateWaterIndicator", StringType(), True),
                            StructField("fireServiceIndicator", StringType(), True),
                            StructField("architecturalObjectInternalId", StringType(), True),
                            StructField("architecturalObjectId", StringType(), True),
                            StructField("fixtureAndFittingCharacteristic", StringType(), True),
                            StructField("houseAddressNumber", StringType(), True),
                            StructField("WWTP", StringType(), True),
                            StructField("SCAMP", StringType(), True),
                            StructField("houseStreetName", StringType(), True),
                            StructField("houseNumber", StringType(), True),
                            StructField("LGA", StringType(), True),
                            StructField("cityName", StringType(), True),
                            StructField("waterDeliverySystem", StringType(), True),
                            StructField("waterDistributionSystem", StringType(), True),
                            StructField("waterSupplyZone", StringType(), True),
                            StructField("receycleWaterDeliverySystem", StringType(), True),
                            StructField("receycleWaterDistributionSystem", StringType(), True),
                            StructField("recycleWaterSupplyZone", StringType(), True),
                            StructField("superiorPropertyTypeCode", StringType(), True),
                            StructField("superiorPropertyType", StringType(), True),
                            StructField("inferiorPropertyTypeCode", StringType(), True),
                            StructField("inferiorPropertyType", StringType(), True),
                            StructField("objectReferenceIndicator", StringType(), True),
                            StructField("objectNumber", StringType(), True),
                            StructField("flatCount", StringType(), True),
                            StructField('_RecordStart',TimestampType(),False),
                            StructField('_RecordEnd',TimestampType(),False),
                            StructField('_RecordDeleted',IntegerType(),False),
                            StructField('_RecordCurrent',IntegerType(),False)
                            ]
                        )
# Apply the new schema to cleanse Data Frame
df_updated_column = spark.createDataFrame(df_cleansed.rdd, schema=newSchema)
display(df_updated_column)

# COMMAND ----------

# DBTITLE 1,12. Save Data frame into Cleansed Delta table (Final)
#Save Data frame into Cleansed Delta table (final)
DeltaSaveDataframeDirect(df_updated_column, source_group, target_table, ADS_DATABASE_CLEANSED, ADS_CONTAINER_CLEANSED, "overwrite", "")

# COMMAND ----------

# DBTITLE 1,13. Exit Notebook
dbutils.notebook.exit("1")