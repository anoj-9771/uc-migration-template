# Databricks notebook source
# DBTITLE 1,Generate parameter and source object name for unit testing
import json
accessTable = 'Z309_TMETERREADING'
 
runParm = '{"SourceType":"Flat File","SourceServer":"saswcnonprod01landingdev-sastoken","SourceGroup":"access","SourceName":"access_access/####_csv","SourceLocation":"access/####.csv","AdditionalProperty":"","Processor":"databricks-token|0705-044124-gored835|Standard_DS3_v2|8.3.x-scala2.12|2:8|interactive","IsAuditTable":false,"SoftDeleteSource":"","ProjectName":"Access Data","ProjectId":2,"TargetType":"BLOB Storage (csv)","TargetName":"access_access/####_csv","TargetLocation":"access/####","TargetServer":"daf-sa-lake-sastoken","DataLoadMode":"TRUNCATE-LOAD","DeltaExtract":false,"CDCSource":false,"TruncateTarget":true,"UpsertTarget":false,"AppendTarget":null,"TrackChanges":false,"LoadToSqlEDW":true,"TaskName":"access_access/####_csv","ControlStageId":1,"TaskId":4,"StageSequence":100,"StageName":"Source to Raw","SourceId":4,"TargetId":4,"ObjectGrain":"Day","CommandTypeId":5,"Watermarks":"","WatermarksDT":null,"WatermarkColumn":"","BusinessKeyColumn":"","UpdateMetaData":null,"SourceTimeStampFormat":"","Command":"","LastLoadedFile":null}'

s = json.loads(runParm)
for parm in ['SourceName','SourceLocation','TargetName','TargetLocation','TaskName']:
    s[parm] = s[parm].replace('####',accessTable)
runParm = json.dumps(s)

# COMMAND ----------

print('Use the following as parameters for unit testing:')
print(f'access_{accessTable.lower()}')
print(runParm)

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
df_cleansed = spark.sql("SELECT cast(N_PROP as int) AS propertyNumber, \
		cast(N_PROP_METE as int) AS propertyMeterNumber, \
		cast(N_METE_READ as int) AS meterReadingNumber, \
		C_METE_READ_TOLE AS meterReadingToleranceCode, \
        initcap(d.meterReadingTolerance) as meterReadingTolerance, \
		C_METE_READ_TYPE AS meterReadingTypeCode, \
        initcap(e.meterReadingType) as meterReadingType, \
		C_METE_READ_CONS AS consumptionTypeCode, \
        initcap(f.consumptionType) as consumptionType, \
		C_METE_READ_STAT AS meterReadingStatusCode, \
        initcap(g.meterReadingStatus) as meterReadingStatus, \
		coalesce(C_METE_CANT_READ,'') AS cannotReadCode, \
        initcap(h.cannotReadReason) as cannotReadReason, \
		C_PDE_READ_METH AS PDEReadingMethodCode, \
        initcap(i.PDEReadingMethod) as PDEReadingMethod, \
		cast(Q_METE_READ as decimal(9,0)) AS meterReading, \
        cast(unix_timestamp(D_METE_READ||case when substr(trim(T_METE_READ_TIME),-1) = '-' then substr(T_METE_READ_TIME,1,5)||'0' \
                                              when substr(T_METE_READ_TIME,1,2) not between '00' and '23' or \
                                                   substr(T_METE_READ_TIME,3,2) not between '00' and '59' or \
                                                   substr(T_METE_READ_TIME,5,2) not between '00' and '59' then '120000' \
                                              when T_METE_READ_TIME is null then '120000' \
                                                                            else T_METE_READ_TIME end, 'yyyyMMddHHmmss') as Timestamp) as meterReadingTimestamp, \
		cast(Q_METE_READ_CONS as decimal(9,0)) AS meterReadingConsumption, \
        date_sub(to_date(D_METE_READ,'yyyyMMdd'),int(Q_METE_READ_DAYS) - 1) as readingFromDate, \
        to_date(D_METE_READ,'yyyyMMdd') as readingToDate, \
		cast(Q_METE_READ_DAYS as decimal(7,0)) AS meterReadingDays, \
		case when F_READ_COMM_CODE = '1' then true else false end AS hasReadingCommentCode, \
		case when F_READ_COMM_FREE = '1' then true else false end AS hasReadingCommentFreeFormat, \
		cast(Q_PDE_HIGH_LOW as int) AS PDEHighLow, \
		cast(Q_PDE_REEN_COUN as int) AS PDEReenteredCount, \
		case when F_PDE_AUXI_READ = '0' then true else false end AS isPDEAuxilaryReading, \
		to_date(D_METE_READ_UPDA, 'yyyyMMdd') AS meterReadingUpdatedDate, \
		a._RecordStart, \
		a._RecordEnd, \
		a._RecordDeleted, \
		a._RecordCurrent \
	FROM CLEANSED.STG_ACCESS_Z309_TMETERREADING a \
         left outer join CLEANSED.t_access_Z309_TMETEREADTOLE d on a.C_METE_READ_TOLE = d.meterReadingToleranceCode \
         left outer join CLEANSED.t_access_Z309_TMETEREADTYPE e on a.C_METE_READ_TYPE = e.meterReadingTypeCode \
         left outer join CLEANSED.t_access_Z309_TMETEREADCONTYP f on a.C_METE_READ_CONS = f.consumptionTypeCode \
         left outer join CLEANSED.t_access_Z309_TMRSTATUSTYPE g on a.C_METE_READ_STAT = g.meterReadingStatusCode \
         left outer join CLEANSED.t_access_Z309_TMETERCANTREAD h on coalesce(a.C_METE_CANT_READ,'') = h.cannotReadCode \
         left outer join CLEANSED.t_access_Z309_TPDEREADMETH i on a.C_PDE_READ_METH = i.PDEReadingMethodCode \
         ")



# COMMAND ----------

newSchema = StructType([
	StructField('propertyNumber',IntegerType(),False),
	StructField('propertyMeterNumber',IntegerType(),False),
	StructField('meterReadingNumber',IntegerType(),False),
	StructField('meterReadingToleranceCode',StringType(),True),
    StructField('meterReadingTolerance',StringType(),True),
	StructField('meterReadingTypeCode',StringType(),False),
    StructField('meterReadingType',StringType(),False),
	StructField('consumptionTypeCode',StringType(),True),
    StructField('consumptionType',StringType(),True),
	StructField('meterReadingStatusCode',StringType(),True),
    StructField('meterReadingStatus',StringType(),True),
	StructField('cannotReadCode',StringType(),False),
    StructField('cannotReadReason',StringType(),False),
	StructField('PDEReadingMethodCode',StringType(),True),
    StructField('PDEReadingMethod',StringType(),True),
	StructField('meterReading',DecimalType(9,0),False),
	StructField('meterReadingTimestamp',TimestampType(),True),
	StructField('meterReadingConsumption',DecimalType(9,0),False),
    StructField('readingFromDate',DateType(),False),
    StructField('readingFromTo',DateType(),False),
	StructField('meterReadingDays',DecimalType(7,0),False),
	StructField('hasReadingCommentCode',BooleanType(),False),
	StructField('hasReadingCommentFreeFormat',BooleanType(),False),
	StructField('PDEHighLow',IntegerType(),False),
	StructField('PDEReenteredCount',IntegerType(),False),
	StructField('isPDEAuxilaryReading',BooleanType(),False),
	StructField('meterReadingUpdatedDate',DateType(),True),
	StructField('_RecordStart',TimestampType(),False),
	StructField('_RecordEnd',TimestampType(),False),
	StructField('_RecordDeleted',IntegerType(),False),
	StructField('_RecordCurrent',IntegerType(),False)
])

df_updated_column = spark.createDataFrame(df_cleansed.rdd, schema=newSchema)
display(df_updated_column)
print(f'Number of rows: {df_updated_column.count()}')

# COMMAND ----------

# DBTITLE 1,12. Save Data frame into Cleansed Delta table (Final)
#Save Data frame into Cleansed Delta table (final)
DeltaSaveDataframeDirect(df_updated_column, "t", source_object, ADS_DATABASE_CLEANSED, ADS_CONTAINER_CLEANSED, "overwrite", "")

# COMMAND ----------

# DBTITLE 1,13. Exit Notebook
dbutils.notebook.exit("1")

# COMMAND ----------


