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
                      (Select *, ROW_NUMBER() OVER (PARTITION BY INTRENO ORDER BY _DLRawZoneTimeStamp DESC) AS _RecordVersion FROM {delta_raw_tbl_name} WHERE _DLRawZoneTimestamp >= '{LastSuccessfulExecutionTS}') \
                           SELECT \
                                  case when vib.INTRENO = 'na' then '' else vib.INTRENO end as architecturalObjectInternalId , \
                                  vib.AOID as architecturalObjectId , \
                                  vib.AOTYPE as architecturalObjectTypeCode , \
                                  tiv.XMAOTYPE as architecturalObjectType , \
                                  AONR as architecturalObjectNumber , \
                                  ToValidDate(VALIDFROM) as validFromDate , \
                                  ToValidDate(VALIDTO) as validToDate , \
                                  PARTAOID as partArchitecturalObjectId , \
                                  OBJNR as objectNumber , \
                                  RERF as firstEnteredBy , \
                                  ToValidDate(DERF) as firstEnteredOnDate , \
                                  TERF as firstEnteredTime , \
                                  ToValidDateTime(concat(DERF, coalesce(TERF,'00:00:00'))) as firstEnteredDateTime , \
                                  REHER as firstEnteredSource , \
                                  RBEAR as employeeId , \
                                  ToValidDate(DBEAR) as lastEditedOnDate , \
                                  TBEAR as lastEditedTime , \
                                  ToValidDateTime(concat(DBEAR, coalesce(TBEAR,'00:00:00'))) as lastEditedDateTime , \
                                  RBHER as lastEditedSource , \
                                  RESPONSIBLE as responsiblePerson , \
                                  USEREXCLUSIVE as exclusiveUser , \
                                  ToValidDate(LASTRENO) as lastRelocationDate , \
                                  MEASSTRC as measurementStructure , \
                                  DOORPLT as shortDescription , \
                                  RSAREA as reservationArea , \
                                  SINSTBEZ as maintenanceDistrict , \
                                  SVERKEHR as businessEntityTransportConnectionsIndicator , \
                                  ZCD_PROPERTY_NO as propertyNumber , \
                                  ToValidDate(ZCD_PROP_CR_DATE) as propertyCreatedDate , \
                                  ZCD_PROP_LOT_NO as propertyLotNumber , \
                                  ZCD_REQUEST_NO as propertyRequestNumber , \
                                  ZCD_PLAN_TYPE as planTypeCode , \
                                  plt.DESCRIPTION as planType , \
                                  ZCD_PLAN_NUMBER as planNumber , \
                                  ZCD_PROCESS_TYPE as processTypeCode , \
                                  prt.DESCRIPTION as processType , \
                                  ZCD_ADDR_LOT_NO as addressLotNumber , \
                                  ZCD_LOT_TYPE as lotTypeCode , \
                                  ZCD_UNIT_ENTITLEMENT as unitEntitlement , \
                                  ZCD_NO_OF_FLATS as flatCount , \
                                  ZCD_SUP_PROP_TYPE as superiorPropertyTypeCode , \
                                  sp.superiorPropertyType as superiorPropertyType , \
                                  ZCD_INF_PROP_TYPE as inferiorPropertyTypeCode , \
                                  ip.inferiorPropertyType as inferiorPropertyType , \
                                  ZCD_STORM_WATER_ASSESS as stormWaterAssessmentIndicator , \
                                  ZCD_IND_MLIM as mlimIndicator , \
                                  ZCD_IND_WICA as wicaIndicator , \
                                  ZCD_IND_SOPA as sopaIndicator , \
                                  ZCD_IND_COMMUNITY_TITLE as communityTitleIndicator , \
                                  ZCD_SECTION_NUMBER as sectionNumber , \
                                  ZCD_HYDRA_CALC_AREA as hydraCalculatedArea , \
                                  ZCD_HYDRA_AREA_UNIT as hydraAreaUnit , \
                                  ZCD_HYDRA_AREA_FLAG as hydraAreaIndicator , \
                                  ZCD_BAND as hydraBand, \
                                  ZCD_CASENO_FLAG as caseNumberIndicator , \
                                  ZCD_OVERRIDE_AREA as overrideArea , \
                                  ZCD_OVERRIDE_AREA_UNIT as overrideAreaUnit , \
                                  ToValidDate(ZCD_CANCELLATION_DATE) as cancellationDate , \
                                  ZCD_CANC_REASON as cancellationReasonCode , \
                                  ZCD_COMMENTS as comments , \
                                  ZCD_PROPERTY_INFO as propertyInfo , \
                                  cast('1900-01-01' as TimeStamp) as _RecordStart, \
                                  cast('9999-12-31' as TimeStamp) as _RecordEnd, \
                                  '0' as _RecordDeleted, \
                                  '1' as _RecordCurrent, \
                                  cast('{CurrentTimeStamp}' as TimeStamp) as _DLCleansedZoneTimeStamp \
                        FROM stage vib \
                                    LEFT OUTER JOIN {ADS_DATABASE_CLEANSED}.isu_ZCD_TINFPRTY_TX ip ON \
                                   vib.ZCD_INF_PROP_TYPE = ip.inferiorPropertyTypeCode and ip._RecordDeleted = 0 and ip._RecordCurrent = 1 \
                                    LEFT OUTER JOIN {ADS_DATABASE_CLEANSED}.isu_ZCD_TSUPPRTYP_TX sp ON \
                                   vib.ZCD_SUP_PROP_TYPE = sp.superiorPropertyTypeCode and sp._RecordDeleted = 0 and sp._RecordCurrent = 1 \
                                    LEFT OUTER JOIN {ADS_DATABASE_CLEANSED}.isu_ZCD_TPLANTYPE_TX plt ON \
                                   vib.ZCD_PLAN_TYPE = plt.PLAN_TYPE and plt._RecordDeleted = 0 and plt._RecordCurrent = 1 \
                                    LEFT OUTER JOIN {ADS_DATABASE_CLEANSED}.isu_TIVBDAROBJTYPET tiv ON \
                                   vib.AOTYPE = tiv.AOTYPE and tiv._RecordDeleted = 0 and tiv._RecordCurrent = 1 \
                                    LEFT OUTER JOIN {ADS_DATABASE_CLEANSED}.isu_ZCD_TPROCTYPE_TX prt ON \
                                   vib.ZCD_PROCESS_TYPE = prt.PROCESS_TYPE and prt._RecordDeleted = 0 and prt._RecordCurrent = 1 \
                        where vib._RecordVersion = 1 ")

#print(f'Number of rows: {df.count()}')

# COMMAND ----------

#Create schema for the cleanse table
newSchema = StructType(
                            [
                            StructField("architecturalObjectInternalId", StringType(), False),
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
                            StructField("firstEnteredDateTime", TimestampType(), True),  
                            StructField("firstEnteredSource", StringType(), True),
                            StructField("employeeId", StringType(), True),
                            StructField("lastEditedOnDate", DateType(), True),
                            StructField("lastEditedTime", StringType(), True),
                            StructField("lastEditedDateTime", TimestampType(), True),    
                            StructField("lastEditedSource", StringType(), True),
                            StructField("responsiblePerson", StringType(), True),
                            StructField("exclusiveUser", StringType(), True),
                            StructField("lastRelocationDate", DateType(), True),
                            StructField("measurementStructure", StringType(), True),
                            StructField("shortDescription", StringType(), True),
                            StructField("reservationArea", StringType(), True),
                            StructField("maintenanceDistrict", StringType(), True),
                            StructField("businessEntityTransportConnectionsIndicator", StringType(), True),
                            StructField("propertyNumber", StringType(), True),
                            StructField("propertyCreatedDate", DateType(), True),
                            StructField("propertyLotNumber", StringType(), True),
                            StructField("propertyRequestNumber", StringType(), True),
                            StructField("planTypeCode", StringType(), True),
                            StructField("planType", StringType(), True),
                            StructField("planNumber", StringType(), True),
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
                            StructField("stormWaterAssessmentIndicator", StringType(), True),
                            StructField("mlimIndicator", StringType(), True),
                            StructField("wicaIndicator", StringType(), True),
                            StructField("sopaIndicator", StringType(), True),
                            StructField("communityTitleIndicator", StringType(), True),
                            StructField("sectionNumber", StringType(), True),
                            StructField("hydraCalculatedArea", StringType(), True),
                            StructField("hydraAreaUnit", StringType(), True),
                            StructField("hydraAreaIndicator", StringType(), True),
                            StructField("hydraBand", StringType(), True),
                            StructField("caseNumberIndicator", StringType(), True),
                            StructField("overrideArea", StringType(), True),
                            StructField("overrideAreaUnit", StringType(), True),
                            StructField("cancellationDate", DateType(), True),
                            StructField("cancellationReasonCode", StringType(), True),
                            StructField("comments", StringType(), True),
                            StructField("propertyInfo", StringType(), True),
                            StructField('_RecordStart',TimestampType(),False),
                            StructField('_RecordEnd',TimestampType(),False),
                            StructField('_RecordDeleted',IntegerType(),False),
                            StructField('_RecordCurrent',IntegerType(),False),
                            StructField('_DLCleansedZoneTimeStamp',TimestampType(),False)
                            ]
                        )


# COMMAND ----------

# DBTITLE 1,12. Save Data frame into Cleansed Delta table (Final)
DeltaSaveDataFrameToDeltaTable(df, target_table, ADS_DATALAKE_ZONE_CLEANSED, ADS_DATABASE_CLEANSED, data_lake_folder, ADS_WRITE_MODE_MERGE, newSchema, track_changes, is_delta_extract, business_key, AddSKColumn = False, delta_column = "", start_counter = "0", end_counter = "0")

# COMMAND ----------

# DBTITLE 1,13. Exit Notebook
dbutils.notebook.exit("1")
