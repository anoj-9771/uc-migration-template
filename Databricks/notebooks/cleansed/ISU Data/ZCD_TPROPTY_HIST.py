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
                      (Select *, ROW_NUMBER() OVER (PARTITION BY PROPERTY_NO,DATE_FROM ORDER BY _FileDateTimeStamp DESC, _DLRawZoneTimeStamp DESC) AS _RecordVersion FROM {delta_raw_tbl_name} \
                                  WHERE _DLRawZoneTimestamp >= '{LastSuccessfulExecutionTS}' and (DATE_FROM <= DATE_TO)) \
                           SELECT  \
                                case when stg.PROPERTY_NO = 'na' then '' else stg.PROPERTY_NO end as propertyNumber, \
                                stg.SUP_PROP_TYPE as superiorPropertyTypeCode, \
                                supty.superiorPropertyType, \
                                stg.INF_PROP_TYPE as inferiorPropertyTypeCode, \
                                infty.inferiorPropertyType, \
                                ToValidDate(stg.DATE_FROM,'MANDATORY') as validFromDate, \
                                ToValidDate(stg.DATE_TO) as validToDate, \
                                ToValidDateTime(stg.CREATED_ON) as createdDate, \
                                stg.CREATED_BY as createdBy, \
                                ToValidDateTime(stg.CHANGED_ON) as changedDate, \
                                stg.CHANGED_BY as changedBy, \
                                'PROPERTY_NO|DATE_FROM' as sourceKeyDesc, \
                                concat_ws('|',stg.PROPERTY_NO,stg.DATE_FROM) as sourceKey, \
                                'DATE_FROM' as rejectColumn, \
                                cast('1900-01-01' as TimeStamp) as _RecordStart, \
                                cast('9999-12-31' as TimeStamp) as _RecordEnd, \
                                '0' as _RecordDeleted, \
                                '1' as _RecordCurrent, \
                                cast('{CurrentTimeStamp}' as TimeStamp) as _DLCleansedZoneTimeStamp \
                        FROM stage stg \
                          LEFT OUTER JOIN {ADS_DATABASE_CLEANSED}.isu.zcd_tsupprtyp_tx supty on supty.superiorPropertyTypeCode = stg.SUP_PROP_TYPE \
                                                                                                    and supty._RecordDeleted = 0 and supty._RecordCurrent = 1 \
                          LEFT OUTER JOIN {ADS_DATABASE_CLEANSED}.isu.zcd_tinfprty_tx infty on infty.inferiorPropertyTypeCode = stg.INF_PROP_TYPE \
                                                                                                    and infty._RecordDeleted = 0 and infty._RecordCurrent = 1 \
                        where stg._RecordVersion = 1").cache()

print(f'Number of rows: {df.count()}')

# COMMAND ----------

# Create schema for the cleanse table
newSchema = StructType([
                        StructField("propertyNumber", StringType(), False),
                        StructField("superiorPropertyTypeCode", StringType(), True),
                        StructField("superiorPropertyType", StringType(), True),
                        StructField("inferiorPropertyTypeCode", StringType(), True),
                        StructField("inferiorPropertyType", StringType(), True),
                        StructField("validFromDate", DateType(), False),
                        StructField("validToDate", DateType(), True),
                        StructField("createdDate", TimestampType(), True),
                        StructField("createdBy", StringType(), True),
                        StructField("changedDate", TimestampType(), True),
                        StructField("changedBy", StringType(), True),
                        StructField('_RecordStart',TimestampType(),False),
                        StructField('_RecordEnd',TimestampType(),False),
                        StructField('_RecordDeleted',IntegerType(),False),
                        StructField('_RecordCurrent',IntegerType(),False),
                        StructField('_DLCleansedZoneTimeStamp',TimestampType(),False)
                      ])


# COMMAND ----------

# DBTITLE 1,Handle Invalid Records
reject_df =df.where("validFromDate = '1000-01-01'").cache() #2018-01-07
cleansed_df = df.subtract(reject_df)
cleansed_df = cleansed_df.drop("sourceKeyDesc","sourceKey","rejectColumn")

# COMMAND ----------

# from pyspark.sql.functions import concat, col, lit, substring
# from pyspark.sql.functions import DataFrame
# tableName = 'isu_ZCD_TPROPTY_HIST'
# COL_DL_REJECTED_LOAD = '_DLRejectedZoneTimeStamp'
# reject_table = 'rejected.cleansed_rejected'
# lastExecutionTS = LastSuccessfulExecutionTS

# reject_df = reject_df.withColumn('rejectRecordCleansed', to_json(struct(col("*"))))
# reject_df = reject_df.withColumn("tableName",lit(tableName)).withColumn(COL_DL_REJECTED_LOAD,current_timestamp()).select("tableName","rejectColumn","sourceKeyDesc","sourceKey","rejectRecordCleansed")
# rawbusinessKey = 'PROPERTY_NO|DATE_FROM'
# print(reject_df.count())
    
# raw_df = spark.table('raw.isu_zcd_tpropty_hist').where(f"_DLRawZoneTimestamp >= '{lastExecutionTS}'").withColumn("rawSourceKey", concat_ws('|', *(rawbusinessKey.split("|")))).withColumn('rejectRecordRaw', to_json(struct(col("*")))).select("rawSourceKey","rejectRecordRaw")


# reject_df =reject_df.join(raw_df,reject_df.sourceKey==raw_df.rawSourceKey,"left")
# print(reject_df.count())
# display(reject_df)
# display(raw_df)

# COMMAND ----------

# DBTITLE 1,12. Save Data frame into Cleansed Delta table (Final)
DeltaSaveDataFrameToDeltaTable(cleansed_df, target_table, ADS_DATALAKE_ZONE_CLEANSED, ADS_DATABASE_CLEANSED, data_lake_folder, ADS_WRITE_MODE_OVERWRITE, newSchema, track_changes, is_delta_extract, business_key, AddSKColumn = False, delta_column = "", start_counter = "0", end_counter = "0")

# COMMAND ----------

# DBTITLE 1,12.1 Save Reject Data Frame into Rejected Database
if reject_df.count() > 0:
    source_key = 'PROPERTY_NO|DATE_FROM'
    DeltaSaveDataFrameToRejectTable(reject_df,target_table,business_key,source_key,LastSuccessfulExecutionTS)
    reject_df.unpersist()
df.unpersist()       

# COMMAND ----------

# DBTITLE 1,13. Exit Notebook
dbutils.notebook.exit("1")
