# Databricks notebook source
# MAGIC %md
# MAGIC <b>Note that this is a non-standard notebook as it merges multiple input files into one delta table</b>

# COMMAND ----------

# dbutils.widgets.removeAll() #debug runs only

# COMMAND ----------

# DBTITLE 1,Generate parameter and source object name for unit testing
import json
accessTable = 'Z309_TPROPMETER' 
businessKey = 'N_PROP,N_PROP_METE,N_METE_MAKE'

runParm = '{"SourceType":"BLOB Storage (csv)","SourceServer":"daf-sa-lake-sastoken","SourceGroup":"accessdata","SourceName":"access_####","SourceLocation":"accessdata/####","AdditionalProperty":"","Processor":"databricks-token|0705-044124-gored835|Standard_DS3_v2|8.3.x-scala2.12|2:8|interactive","IsAuditTable":false,"SoftDeleteSource":"","ProjectName":"ACCESSDATA","ProjectId":3,"TargetType":"BLOB Storage (csv)","TargetName":"access_####","TargetLocation":"accessdata/####","TargetServer":"daf-sa-lake-sastoken","DataLoadMode":"TRUNCATE-LOAD","DeltaExtract":false,"CDCSource":false,"TruncateTarget":true,"UpsertTarget":false,"AppendTarget":null,"TrackChanges":false,"LoadToSqlEDW":true,"TaskName":"access_####","ControlStageId":2,"TaskId":808,"StageSequence":200,"StageName":"Raw to Cleansed","SourceId":808,"TargetId":808,"ObjectGrain":"Day","CommandTypeId":8,"Watermarks":"","WatermarksDT":null,"WatermarkColumn":"","BusinessKeyColumn":"N_PROP,N_PROP_METE","UpdateMetaData":null,"SourceTimeStampFormat":"","Command":"/build/cleansed/ACCESS Data/####","LastLoadedFile":null}'

s = json.loads(runParm)
for parm in ['SourceName','SourceLocation','TargetName','TargetLocation','TaskName']:
    s[parm] = s[parm].replace('####',accessTable)

s['BusinessKeyColumn'] = businessKey

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
dbutils.widgets.text("source_object", f'access_{accessTable.lower()}', "Source Object")
dbutils.widgets.text("start_counter", "", "Start Counter")
dbutils.widgets.text("end_counter", "", "End Counter")
dbutils.widgets.text("delta_column", "", "Delta Column")
dbutils.widgets.text("source_param", runParm, "Param")


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
delta_cleansed_tbl_name = f'{ADS_DATABASE_CLEANSED}.{target_table}'
delta_raw_tbl_name = f'{ADS_DATABASE_RAW}.{ source_object}'

#Destination
print(delta_cleansed_tbl_name)
print(delta_raw_tbl_name)


# COMMAND ----------

# DBTITLE 1,10. Load to Cleanse Delta Table from Raw Delta Table
#This method uses the source table to load data into target Delta Table
#this notebook is exceptional as it merges multiple tables, so run this code for each of the tables
tableReplace = ['',".replace('TPROPMETER','TPROPMETER_BI')"]

for replacement in tableReplace:
    DeltaSaveToDeltaTable_Access (
        source_table = eval(repr(delta_raw_tbl_name) + replacement),
        target_table = eval(repr(target_table) + replacement),
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
def runQuery(table, source):
    df_data = spark.sql(f"SELECT cast(N_PROP as int) AS propertyNumber, \
            cast(N_PROP_METE as int) AS propertyMeterNumber, \
            N_METE_MAKE AS meterMakerNumber, \
            '{source}' as dataSource, \
            C_METE_TYPE AS meterSizeCode, \
            case when b.meterSizeUnit = 'mm' then \
                cast(cast(b.meterSize as int) as string)||' '||lower(b.meterSizeUnit) \
                else cast(cast(b.meterSize as decimal(5,2)) as string)||' '||lower(b.meterSizeUnit) end as meterSize, \
            case when C_METE_POSI_STAT = 'M' then true else false end as isMasterMeter, \
            case when C_METE_POSI_STAT = 'C' then true else false end as isCheckMeter, \
            case when C_METE_POSI_STAT = 'A' then true else false end as allowAlso, \
            case when F_METE_CONN = 'D' then false else true end AS isMeterConnected, \
            C_METE_READ_FREQ AS meterReadingFrequencyCode, \
            ref2.meterReadingFrequency as meterReadingFrequency, \
            C_METE_CLAS AS meterClassCode, \
            initcap(c.meterClass) as meterClass, \
            c.waterMeterType, \
            C_METE_CATE AS meterCategoryCode, \
            initcap(d.meterCategory) as meterCategory, \
            C_METE_GROU AS meterGroupCode, \
            replace(initcap(e.meterGroup),'ami','AMI') as meterGroup, \
            C_METE_READ_LOCA AS meterReadingLocationCode, \
            ref10.meterGridLocation as meterReadingLocation, \
            cast(N_METE_READ_ROUT as int) AS meterReadingRouteNumber, \
            initcap(T_METE_SERV) AS meterServes, \
            C_METE_GRID_LOCA AS meterGridLocationCode, \
            ref3.meterGridLocation as meterGridLocation, \
            C_READ_INST_NUM1 AS readingInstructionCode1, \
            ref4.meterReadingInstruction as readingInstruction1, \
            C_READ_INST_NUM2 AS readingInstructionCode2, \
            ref5.meterReadingInstruction as readingInstruction2, \
            case when F_METE_ADDI_DESC = '1' then true else false end AS hasAdditionalDescription, \
            case when F_METE_ROUT_PREP = '1' then true else false end AS hasMeterRoutePreparation, \
            case when F_METE_WARN_NOTE = '1' then true else false end AS hasMeterWarningNote, \
            to_date(D_METE_FIT, 'yyyyMMdd') AS meterFittedDate, \
            cast(N_METE_READ_SEQU as int) AS meterReadingSequenceNumber, \
            C_METE_CHAN_REAS AS meterChangeReasonCode, \
            ref6.meterExchangeReason as meterExchangeReason, \
            N_METE_CHAN_ADVI AS meterChangeAdviceNumber, \
            to_date(D_METE_REMO, 'yyyyMMdd') AS meterRemovedDate, \
            to_date(D_PROP_METE_UPDA, 'yyyyMMdd') AS propertyMeterUpdatedDate, \
            a._RecordStart, \
            a._RecordEnd, \
            a._RecordDeleted, \
            a._RecordCurrent \
        FROM {table} a \
             left outer join CLEANSED.access_Z309_TMeterType b on b.meterTypeCode = a.C_METE_TYPE \
             left outer join cleansed.access_Z309_TMETEREADFREQ ref2 on a.C_METE_READ_FREQ = ref2.meterReadingFrequencyCode \
             left outer join cleansed.access_Z309_TMETERGRIDLOCA ref3 on a.C_METE_GRID_LOCA = ref3.meterGridLocationCode \
             left outer join cleansed.access_Z309_TMETEREADINST ref4 on a.C_READ_INST_NUM1 = ref4.meterReadingInstructionCode \
             left outer join cleansed.access_Z309_TMETEREADINST ref5 on a.C_READ_INST_NUM2 = ref5.meterReadingInstructionCode \
             left outer join cleansed.access_Z309_TMETERCHANGEREAS ref6 on a.C_METE_CHAN_REAS = ref6.meterExchangeReasonCode \
             left outer join CLEANSED.access_Z309_TMeterClass c on c.meterClassCode = a.C_METE_CLAS \
             left outer join CLEANSED.access_Z309_TMeterCategory d on d.meterCategoryCode = a.C_METE_CATE \
             left outer join CLEANSED.access_Z309_TMeterGroup e on e.meterGroupCode = a.C_METE_GROU \
             left outer join cleansed.access_Z309_TMETERGRIDLOCA ref10 on a.C_METE_READ_LOCA = ref10.meterGridLocationCode \
             ")
    
    return df_data

df_current = runQuery(f'{ADS_DATABASE_STAGE}.{source_object}','ACCESS')
df_BI_data = runQuery(f"{ADS_DATABASE_STAGE}.{source_object.replace('TPROPMETER','TPROPMETER_BI')}",'BI')

df_current.createOrReplaceTempView('currentMeters')
df_BI_data.createOrReplaceTempView('BIMeters')
#By way of experiment, only take the meters for which we have no information in ACCESS at all                       
df_extraBIMeters = spark.sql('select propertyNumber, propertyMeterNumber, meterMakerNumber from BIMeters \
                              except \
                              select propertyNumber, propertyMeterNumber, meterMakerNumber \
                              from   currentMeters')

df_extraBIMeters.createOrReplaceTempView('extraBIMeters')

df_BI_complement = spark.sql('select a.* \
                              from   BIMeters a, \
                                     extraBIMeters b \
                              where  a.propertyNumber = b.propertyNumber \
                              and    a.propertyMeterNumber = b.propertyMeterNumber \
                              and    a.meterMakerNumber = b.meterMakerNumber')
                              
#                               and    a.waterMeterType = b.waterMeterType \
#                               and    a.isMasterMeter = b.isMasterMeter \
#                               and    a.isCheckMeter = b.isCheckMeter \
#                               and    a.allowAlso = b.allowAlso \
#                               and    a.meterClass = b.meterClass \
#                               and    a.meterCategory = b.meterCategory \
#                               and    a.propertyMeterUpdatedDate = b.propertyMeterUpdatedDate')
df_BI_complement.createOrReplaceTempView('BIComplement')
#for odd reasons there are records in BI that show a later updated date than ACCESS. Don't include these.
# df_max_update_ACCESS = spark.sql('select propertyNumber, \
#                                          propertyMeterNumber, \
#                                          meterMakerNumber, \
#                                          max(propertyMeterUpdatedDate) as maxPropertyMeterUpdatedDate \
#                                   from   currentMeters \
#                                   group by propertyNumber, propertyMeterNumber, meterMakerNumber')

# df_max_update_ACCESS.createOrReplaceTempView('maxDates')

spark.sql('create or replace temp view allMeters as \
              select * \
              from   currentMeters \
              union all \
              select bi.* \
              from   BIComplement bi') 
#                              left outer join maxDates m on bi.propertyNumber = m.propertyNumber and bi.propertyMeterNumber = m.propertyMeterNumber and bi.meterMakerNumber = m.meterMakerNumber \
#                 where  bi.propertyMeterUpdatedDate < m.maxPropertyMeterUpdatedDate')
# with t1 as( \
#                                     select *, \
#                                            row_number() over(partition by propertyNumber, propertyMeterNumber, meterMakerNumber, waterMeterType, isMasterMeter, isCheckMeter, allowAlso, meterClass, \
#                                                                           meterCategory order by propertyMeterUpdatedDate desc) as rn \
#                                     from   allMeters ), \
#                               t2 as(select *, \
#                                             case when meterRemovedDate is null \
#                                                       then lead(meterfittedDate,1) over (partition by propertyNumber, propertyMeterNumber order by meterFittedDate) \
#                                                       else meterRemovedDate \
#                                             end as altMeterRemovedDate \
#                                     from   t1 \
#                                     where  rn = 1) \                        
#drop original rows if they were updated after having been archived                        
df_cleansed = spark.sql(" select propertyNumber, \
                                 propertyMeterNumber, \
                                 trim(translate(meterMakerNumber,chr(26),' ')) as meterMakerNumber, \
                                 dataSource, \
                                 meterSizeCode, \
                                 meterSize, \
                                 isMasterMeter, \
                                 isCheckMeter, \
                                 allowAlso, \
                                 isMeterConnected, \
                                 meterReadingFrequencyCode, \
                                 meterReadingFrequency, \
                                 meterClassCode, \
                                 meterClass, \
                                 waterMeterType, \
                                 meterCategoryCode, \
                                 meterCategory, \
                                 meterGroupCode, \
                                 meterGroup, \
                                 meterReadingLocationCode, \
                                 meterReadingLocation, \
                                 meterReadingRouteNumber, \
                                 meterServes, \
                                 meterGridLocationCode, \
                                 meterGridLocation, \
                                 readingInstructionCode1, \
                                 readingInstruction1, \
                                 readingInstructionCode2, \
                                 readingInstruction2, \
                                 hasAdditionalDescription, \
                                 hasMeterRoutePreparation, \
                                 hasMeterWarningNote, \
                                 meterFittedDate, \
                                 meterReadingSequenceNumber, \
                                 meterChangeReasonCode, \
                                 meterExchangeReason, \
                                 meterChangeAdviceNumber, \
                                 meterRemovedDate, \
                                 propertyMeterUpdatedDate, \
                                 _RecordStart, \
                                 _RecordEnd, \
                                 _RecordDeleted, \
                                 _RecordCurrent \
                          from   allMeters \
                          ")
                        #altMeterRemovedDate as meterRemovedDate, \
df_cleansed = df_cleansed.drop('rn')
#print(f'Number of rows: {df_cleansed.count()}')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * 
# MAGIC from BIComplement
# MAGIC where propertyNumber = 5568937

# COMMAND ----------

# %sql
# select *
# from mtr
# where propertyNumber = 3100078

# COMMAND ----------

# print('Current Data:',df_current.count())
# print('BI Data:',df_BI_data.count())
# print('BI complement:',df_BI_complement.count())

# COMMAND ----------

newSchema = StructType([
	StructField('propertyNumber',IntegerType(),False),
	StructField('propertyMeterNumber',IntegerType(),False),
	StructField('meterMakerNumber',StringType(),True),
    StructField('dataSource',StringType(),False),
    StructField('meterSizeCode',StringType(),False),
	StructField('meterSize',StringType(),False),
    StructField('isMasterMeter',BooleanType(),False),
	StructField('isCheckMeter',BooleanType(),False),
    StructField('allowAlso',BooleanType(),False),
	StructField('isMeterConnected',BooleanType(),False),
	StructField('meterReadingFrequencyCode',StringType(),True),
    StructField('meterReadingFrequency',StringType(),True),
	StructField('meterClassCode',StringType(),True),
	StructField('meterClass',StringType(),True),
    StructField('waterMeterType',StringType(),True),
	StructField('meterCategoryCode',StringType(),True),
	StructField('meterCategory',StringType(),True),
	StructField('meterGroupCode',StringType(),True),
	StructField('meterGroup',StringType(),True),
	StructField('meterReadingLocationCode',StringType(),True),
    StructField('meterReadingLocation',StringType(),True),
	StructField('meterReadingRouteNumber',IntegerType(),True),
	StructField('meterServes',StringType(),True),
	StructField('meterGridLocationCode',StringType(),True),
    StructField('meterGridLocation',StringType(),True),
	StructField('readingInstructionCode1',StringType(),True),
    StructField('readingInstruction1',StringType(),True),
	StructField('readingInstructionCode2',StringType(),True),
    StructField('readingInstruction2',StringType(),True),
	StructField('hasAdditionalDescription',BooleanType(),False),
	StructField('hasMeterRoutePreparation',BooleanType(),False),
	StructField('hasMeterWarningNote',BooleanType(),False),
	StructField('meterFittedDate',DateType(),True),
	StructField('meterReadingSequenceNumber',IntegerType(),False),
	StructField('meterChangeReasonCode',StringType(),True),
    StructField('meterExchangeReason',StringType(),True),
	StructField('meterChangeAdviceNumber',StringType(),True),
	StructField('meterRemovedDate',DateType(),True),
	StructField('propertyMeterUpdatedDate',DateType(),True),
	StructField('_RecordStart',TimestampType(),False),
	StructField('_RecordEnd',TimestampType(),False),
	StructField('_RecordDeleted',IntegerType(),False),
	StructField('_RecordCurrent',IntegerType(),False)
])

# df_updated_column = spark.createDataFrame(df_cleansed.rdd, schema=newSchema)
# display(df_updated_column)
# print(f'Number of rows: {df_updated_column.count()}')

# COMMAND ----------

# DBTITLE 1,12. Save Data frame into Cleansed Delta table (Final)
#Save Data frame into Cleansed Delta table (final)
DeltaSaveDataframeDirect(df_cleansed, source_group, target_table, ADS_DATABASE_CLEANSED, ADS_CONTAINER_CLEANSED, "overwrite", newSchema, "")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from cleansed.access_Z309_TPROPMETER where dataSource = 'BI'

# COMMAND ----------

# DBTITLE 1,13. Exit Notebook
dbutils.notebook.exit("1")
