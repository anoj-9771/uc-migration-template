# Databricks notebook source
# MAGIC %md
# MAGIC { "SourceType": "BLOB Storage (json)", "SourceServer": "saswcnonprod01landingdev-sastoken", "SourceGroup": "sapisu", "SourceName": "sapisu_0EQUIPMENT_ATTR", "SourceLocation": "0EQUIPMENT_ATTR", "AdditionalProperty": "", "Processor": "databricks-token|0711-011053-turfs581|Standard_DS3_v2|8.3.x-scala2.12|2:8|interactive", "IsAuditTable": false, "SoftDeleteSource": "", "ProjectName": "SAP ISU", "ProjectId": 2, "TargetType": "BLOB Storage (json)", "TargetName": "sapisu_0EQUIPMENT_ATTR", "TargetLocation": "sapisu/0EQUIPMENT_ATTR", "TargetServer": "daf-sa-lake-sastoken", "DataLoadMode": "FULL-EXTRACT", "DeltaExtract": false, "CDCSource": false, "TruncateTarget": false, "UpsertTarget": true, "AppendTarget": null, "TrackChanges": false, "LoadToSqlEDW": true, "TaskName": "sapisu_0EQUIPMENT_ATTR", "ControlStageId": 1, "TaskId": 65, "StageSequence": 100, "StageName": "Source to Raw", "SourceId": 65, "TargetId": 65, "ObjectGrain": "Day", "CommandTypeId": 3, "Watermarks": "", "WatermarksDT": null, "WatermarkColumn": "", "BusinessKeyColumn": "EQUNR,DATETO", "UpdateMetaData": null, "SourceTimeStampFormat": "", "Command": "", "LastLoadedFile": null }

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
#delta_raw_tbl_name = "raw.sap_0equipment_attr"

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
                                  EQUI.EQUNR as equipmentNumber,\
                                  to_date(EQUI.DATETO) as validToDate,\
                                  to_date(EQUI.DATEFROM) as validFromDate,\
                                  EQUI.EQART as technicalObjectTypeCode,\
                                  EQUI.INVNR as inventoryNumber,\
                                  EQUI.IWERK as maintenancePlanningPlant,\
                                  EQUI.KOKRS as controllingArea,\
                                  EQUI.TPLNR as functionalLocationNumber,\
                                  EQUI.SWERK as maintenancePlant,\
                                  EQUI.ADRNR as addressNumber,\
                                  EQUI.BUKRS as companyCode,\
                                  COMP.companyName as companyName,\
                                  EQUI.MATNR as materialNumber,\
                                  EQUI.ANSWT as acquisitionValue,\
                                  to_date(EQUI.ANSDT) as acquisitionDate,\
                                  to_date(EQUI.ERDAT) as createdDate,\
                                  to_date(EQUI.AEDAT) as lastChangedDate,\
                                  to_date(EQUI.INBDT) as startUpDate,\
                                  EQUI.PROID as workBreakdownStructureElement,\
                                  EQUI.EQTYP as equipmentCategoryCode, \
                                  EQUI._RecordStart,\
                                  EQUI._RecordEnd,\
                                  EQUI._RecordDeleted,\
                                  EQUI._RecordCurrent \
                                FROM CLEANSED.STG_SAPISU_0EQUIPMENT_ATTR EQUI \
                                LEFT OUTER JOIN CLEANSED.T_SAPISU_0COMP_CODE_TEXT COMP ON EQUI.BUKRS = COMP.companyCode")
display(df_updated_column_temp)

# COMMAND ----------

# Create schema for the cleanse table
cleanse_Schema = StructType(
  [
    StructField("equipmentNumber", StringType(), False),
    StructField("validToDate", DateType(), True),
    StructField("validFromDate", DateType(), True),
    StructField("technicalObjectTypeCode", StringType(), True),
    StructField("inventoryNumber", StringType(), True),
    StructField("maintenancePlanningPlant", StringType(), True),
    StructField("controllingArea", StringType(), True),
    StructField("functionalLocationNumber", StringType(), True),
    StructField("maintenancePlant", StringType(), True),
    StructField("addressNumber", StringType(), True),
    StructField("companyCode", StringType(), True),
    StructField("companyName", StringType(), True),
    StructField("materialNumber", StringType(), True),
    StructField("acquisitionValue", DoubleType(), True),
    StructField("acquisitionDate", DateType(), True),
    StructField("createdDate", DateType(), True),
    StructField("lastChangedDate", DateType(), True),
    StructField("startUpDate", DateType(), True),
    StructField("workBreakdownStructureElement", StringType(), True),
    StructField("equipmentCategoryCode", StringType(), True),    
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
