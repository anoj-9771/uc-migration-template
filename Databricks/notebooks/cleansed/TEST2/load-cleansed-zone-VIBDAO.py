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
# MAGIC %run ../../includes/include-all-util

# COMMAND ----------

# DBTITLE 1,7. Include User functions (CleansedZone) for the notebook
# MAGIC %run ./../utility/transform_data_cleansedzone

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
df_updated_column = spark.sql("SELECT  \
                                  vib.INTRENO as architecturalObjectInternalId , \
                                  vib.AOID as architecturalObjectId , \
                                  vib.AOTYPE as architecturalObjectTypeCode , \
                                  tiv.XMAOTYPE as architecturalObjectType , \
                                  vib.AONR as architecturalObjectNumber , \
                                  to_date(vib.VALIDFROM, 'yyyy-MM-dd') as validFromDate , \
                                  to_date(vib.VALIDTO, 'yyyy-MM-dd') as validToDate , \
                                  vib.PARTAOID as partArchitecturalObjectId , \
                                  vib.OBJNR as objectNumber , \
                                  vib.RERF as firstEnteredBy , \
                                  to_date(vib.DERF, 'yyyy-MM-dd') as firstEnteredOnDate , \
                                  vib.TERF as firstEnteredTime , \
                                  vib.REHER as firstEnteredSource , \
                                  vib.RBEAR as employeeId , \
                                  to_date(vib.DBEAR, 'yyyy-MM-dd') as lastEdittedOnDate , \
                                  vib.TBEAR as lastEdittedTime , \
                                  vib.RBHER as lastEdittedSource , \
                                  vib.RESPONSIBLE as responsiblePerson , \
                                  vib.USEREXCLUSIVE as exclusiveUser , \
                                  to_date(vib.LASTRENO, 'yyyy-MM-dd') as lastRelocationDate , \
                                  vib.MEASSTRC as measurementStructure , \
                                  vib.DOORPLT as shortDescription , \
                                  vib.RSAREA as reservationArea , \
                                  vib.SINSTBEZ as maintenanceDistrict , \
                                  vib.SVERKEHR as businessEntityTransportConnectionsIndicator , \
                                  vib.ZCD_PROPERTY_NO as propertyNumber , \
                                  to_date(vib.ZCD_PROP_CR_DATE, 'yyyy-MM-dd') as propertyCreatedDate , \
                                  cast(vib.ZCD_PROP_LOT_NO as long) as propertyLotNumber , \
                                  cast(vib.ZCD_REQUEST_NO as long) as propertyRequestNumber , \
                                  vib.ZCD_PLAN_TYPE as planTypeCode , \
                                  plt.DESCRIPTION as planType , \
                                  cast(vib.ZCD_PLAN_NUMBER as long) as planNumber , \
                                  vib.ZCD_PROCESS_TYPE as processTypeCode , \
                                  prt.DESCRIPTION as processType , \
                                  vib.ZCD_ADDR_LOT_NO as addressLotNumber , \
                                  vib.ZCD_LOT_TYPE as lotTypeCode , \
                                  vib.ZCD_UNIT_ENTITLEMENT as unitEntitlement , \
                                  vib.ZCD_NO_OF_FLATS as flatCount , \
                                  vib.ZCD_SUP_PROP_TYPE as superiorPropertyTypeCode , \
                                  sp.superiorPropertyType as superiorPropertyType , \
                                  vib.ZCD_INF_PROP_TYPE as inferiorPropertyTypeCode , \
                                  ip.inferiorPropertyType as inferiorPropertyType , \
                                  vib.ZCD_STORM_WATER_ASSESS as stormWaterAssesmentIndicator , \
                                  vib.ZCD_IND_MLIM as mlimIndicator , \
                                  vib.ZCD_IND_WICA as wicaIndicator , \
                                  vib.ZCD_IND_SOPA as sopaIndicator , \
                                  vib.ZCD_IND_COMMUNITY_TITLE as communityTitleIndicator , \
                                  vib.ZCD_SECTION_NUMBER as sectionNumber , \
                                  vib.ZCD_HYDRA_CALC_AREA as hydraCalculatedArea , \
                                  vib.ZCD_HYDRA_AREA_UNIT as hydraAreaUnit , \
                                  vib.ZCD_HYDRA_AREA_FLAG as hydraAreaIndicator , \
                                  vib.ZCD_CASENO_FLAG as caseNumberIndicator , \
                                  vib.ZCD_OVERRIDE_AREA as overrideArea , \
                                  vib.ZCD_OVERRIDE_AREA_UNIT as overrideAreaUnit , \
                                  to_date(vib.ZCD_CANCELLATION_DATE, 'yyyy-MM-dd') as cancellationDate , \
                                  vib.ZCD_CANC_REASON as cancellationReasonCode , \
                                  vib.ZCD_COMMENTS as comments , \
                                  vib.ZCD_PROPERTY_INFO as propertyInfo , \
                                  'OBJNRTRG - not in source' as targetObjectNumber , \
                                  'DIAGRAM_NO - not in source' as diagramNumber , \
                                  'FIXFITCHARACT - not in source' as fixtureAndFittingCharacteristicCode , \
                                  'Could not be derived' as fixtureAndFittingCharacteristic , \
                                  vib._RecordStart, \
                                  vib._RecordEnd, \
                                  vib._RecordDeleted, \
                                  vib._RecordCurrent \
                              FROM CLEANSED.stg_sapisu_vibdao vib \
                                    LEFT OUTER JOIN CLEANSED.t_sapisu_ZCD_TINFPRTY_TX ip ON vib.ZCD_INF_PROP_TYPE = ip.inferiorPropertyTypeCode \
                                    LEFT OUTER JOIN CLEANSED.t_sapisu_ZCD_TSUPPRTYP_TX sp ON vib.ZCD_SUP_PROP_TYPE = sp.superiorPropertyTypeCode \
                                    LEFT OUTER JOIN CLEANSED.t_sapisu_ZCD_TPLANTYPE_TX plt ON vib.ZCD_PLAN_TYPE = plt.PLAN_TYPE \
                                    LEFT OUTER JOIN CLEANSED.t_sapisu_TIVBDAROBJTYPET tiv ON vib.AOTYPE = tiv.AOTYPE \
                                    LEFT OUTER JOIN CLEANSED.t_sapisu_ZCD_TPROCTYPE_TX prt ON vib.ZCD_PROCESS_TYPE = prt.PROCESS_TYPE \
                              ")

display(df_updated_column)

# COMMAND ----------

# Create schema for the cleanse table
cleanse_Schema = StructType(
                            [
                            StructField("architecturalObjectInternalId", StringType(), True),
                            StructField("architecturalObjectId", StringType(), True),
                            StructField("architecturalObjectTypeCode", StringType(), True),
                            StructField("architecturalObjectType", StringType(), True),
                            StructField("architecturalObjectNumber", StringType(), True),
                            StructField("validFromDate", DateType(), True),
                            StructField("validToDate", DateType(), True),
                            StructField("partArchitecturalObjectId", StringType(), True),
                            StructField("objectNumber", StringType(), True),
                            StructField("firstEnteredBy", StringType(), True),
                            StructField("firstEnteredOnDate", DateType(), True),
                            StructField("firstEnteredTime", StringType(), True),
                            StructField("firstEnteredSource", StringType(), True),
                            StructField("employeeId", StringType(), True),
                            StructField("lastEdittedOnDate", DateType(), True),
                            StructField("lastEdittedTime", StringType(), True),
                            StructField("lastEdittedSource", StringType(), True),
                            StructField("responsiblePerson", StringType(), True),
                            StructField("exclusiveUser", StringType(), True),
                            StructField("lastRelocationDate", DateType(), True),
                            StructField("measurementStructure", StringType(), True),
                            StructField("shortDescription", StringType(), True),
                            StructField("reservationArea", StringType(), True),
                            StructField("maintenanceDistrict", LongType(), True),
                            StructField("businessEntityTransportConnectionsIndicator", StringType(), True),
                            StructField("propertyNumber", StringType(), True),
                            StructField("propertyCreatedDate", DateType(), True),
                            StructField("propertyLotNumber", LongType(), True),
                            StructField("propertyRequestNumber", LongType(), True),
                            StructField("planTypeCode", StringType(), True),
                            StructField("planType", StringType(), True),
                            StructField("planNumber", LongType(), True),
                            StructField("processTypeCode", StringType(), True),
                            StructField("processType", StringType(), True),
                            StructField("addressLotNumber", StringType(), True),
                            StructField("lotTypeCode", StringType(), True),
                            StructField("unitEntitlement", StringType(), True),
                            StructField("flatCount", StringType(), True),
                            StructField("superiorPropertyTypeCode", StringType(), True),
                            StructField("superiorPropertyType", StringType(), True),
                            StructField("inferiorPropertyTypeCode", StringType(), True),
                            StructField("inferiorPropertyType", StringType(), True),
                            StructField("stormWaterAssesmentIndicator", StringType(), True),
                            StructField("mlimIndicator", StringType(), True),
                            StructField("wicaIndicator", StringType(), True),
                            StructField("sopaIndicator", StringType(), True),
                            StructField("communityTitleIndicator", StringType(), True),
                            StructField("sectionNumber", StringType(), True),
                            StructField("hydraCalculatedArea", StringType(), True),
                            StructField("hydraAreaUnit", StringType(), True),
                            StructField("hydraAreaIndicator", StringType(), True),
                            StructField("caseNumberIndicator", StringType(), True),
                            StructField("overrideArea", StringType(), True),
                            StructField("overrideAreaUnit", StringType(), True),
                            StructField("cancellationDate", DateType(), True),
                            StructField("cancellationReasonCode", StringType(), True),
                            StructField("comments", StringType(), True),
                            StructField("propertyInfo", StringType(), True),
                            StructField("targetObjectNumber", StringType(), True),
                            StructField("diagramNumber", StringType(), True),
                            StructField("fixtureAndFittingCharacteristicCode", StringType(), True),
                            StructField("fixtureAndFittingCharacteristic", StringType(), True),
                            StructField('_RecordStart',TimestampType(),False),
                            StructField('_RecordEnd',TimestampType(),False),
                            StructField('_RecordDeleted',IntegerType(),False),
                            StructField('_RecordCurrent',IntegerType(),False)
                            ]
                        )
# Apply the new schema to cleanse Data Frame
df_updated_column = spark.createDataFrame(df_updated_column.rdd, schema=cleanse_Schema)
display(df_updated_column)



# COMMAND ----------

# DBTITLE 1,12. Save Data frame into Cleansed Delta table (Final)
#Save Data frame into Cleansed Delta table (final)
DeltaSaveDataframeDirect(df_updated_column, "t", source_object, ADS_DATABASE_CLEANSED, ADS_CONTAINER_CLEANSED, "overwrite", "")

# COMMAND ----------

# DBTITLE 1,13. Exit Notebook
dbutils.notebook.exit("1")
