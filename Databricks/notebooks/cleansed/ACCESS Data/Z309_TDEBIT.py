# Databricks notebook source
# DBTITLE 1,Generate parameter and source object name for unit testing
import json
accessTable = 'Z309_TDEBIT'

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
delta_cleansed_tbl_name = f'{ADS_DATABASE_CLEANSED}.{target_table}'
delta_raw_tbl_name = f'{ADS_DATABASE_RAW}.{ source_object}'

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
df_cleansed = spark.sql("SELECT C_LGA AS LGACode, \
        b.LGA, \
		cast(N_PROP as int) AS propertyNumber, \
		cast(N_DEBI_REFE as int) AS debitReferenceNumber, \
		C_DEBI_TYPE AS debitTypeCode, \
        c.debitType, \
		C_DEBI_REAS AS debitReasonCode, \
        d.debitReason, " + (" \
		cast(A_DEBI as decimal(15,2)) AS debitAmount, \
		cast(A_DEBI_OUTS as decimal(15,2)) AS debitOutstandingAmount, \
		cast(A_WATE as decimal(15,2)) AS waterAmount, \
		cast(A_SEWE as decimal(15,2)) AS sewerAmount, \
		cast(A_DRAI as decimal(15,2)) AS drainAmount, " if ADS_ENVIRONMENT not in ['dev','test'] else " cast(0 as decimal(15,2)) AS debitAmount, \
		cast(0 as decimal(15,2)) AS debitOutstandingAmount, \
		cast(0 as decimal(15,2)) AS waterAmount, \
		cast(0 as decimal(15,2)) AS sewerAmount, \
		cast(0 as decimal(15,2)) AS drainAmount, ") + f" \
        to_date(D_DEFE_FROM, 'yyyyMMdd') AS debitDeferredFrom, \
		to_date(D_DEFE_TO, 'yyyyMMdd') AS debitDeferredTo, \
		to_date(D_DISP, 'yyyyMMdd') AS dateDebitDisputed, \
		C_RECO_LEVE AS recoveryLevelCode, \
		to_date(D_RECO_LEVE, 'yyyyMMdd') AS dateRecoveryLevelSet, \
        case when F_OCCU = '0' then true else false end as isOwnerDebit, \
        case when F_OCCU = '0' then false else true end as isOccupierDebit, \
		case when F_ARRE = 'Y' then true else false end as isInArrears, \
        case when N_FINA_YEAR is null then case when substr(d_debi_crea,5,2) < '07' then substr(d_debi_crea,1,4) \
                                                                                    else cast(int(substr(D_DEBI_CREA,1,4)) + 1 as string) \
                                           end \
                                      else case when N_FINA_YEAR > '70' then '19'||N_FINA_YEAR \
                                                                        else '20'||N_FINA_YEAR \
                                           end \
        end AS financialYear, \
		C_ISSU AS issuedCode, \
		to_date(D_DEBI_CREA, 'yyyyMMdd') AS debitCreatedDate, \
		to_date(D_DEBI_UPDA, 'yyyyMMdd') AS debitUpdatedDate, \
		to_date(D_ORIG_ISSU, 'yyyyMMdd') AS originalIssueDate, \
		a._RecordStart, \
		a._RecordEnd, \
		a._RecordDeleted, \
		a._RecordCurrent \
	FROM {ADS_DATABASE_STAGE}.{source_object} a \
         left outer join CLEANSED.access_z309_tlocalgovt b on a.c_lga = b.LGACode \
         left outer join CLEANSED.access_z309_tdebittype c on a.c_debi_type = c.debitTypeCode \
         left outer join CLEANSED.access_z309_tdebitreason d on a.c_debi_type = d.debitTypeCode and a.c_debi_reas = d.debitReasonCode \
         ")

display(df_cleansed)

# COMMAND ----------


newSchema = StructType([
	StructField('LGACode',StringType(),False),
    StructField('LGA',StringType(),False),
    StructField('propertyNumber',IntegerType(),False),
    StructField('debitReferenceNumber',IntegerType(),False),
    StructField('debitTypeCode',StringType(),False),
    StructField('debitType',StringType(),False),
    StructField('debitReasonCode',StringType(),False),
    StructField('debitReason',StringType(),False),
    StructField('debitAmount',DecimalType(15,2),False),
	StructField('debitOutstandingAmount',DecimalType(15,2),False),
    StructField('waterAmount',DecimalType(15,2),False),
    StructField('sewerAmount',DecimalType(15,2),False),
    StructField('drainAmount',DecimalType(15,2),False),
    StructField('debitDeferredFrom',DateType(),True),
	StructField('debitDeferredTo',DateType(),True),
    StructField('dateDebitDisputed',DateType(),True),
    StructField('recoveryLevelCode',StringType(),True),
    StructField('dateRecoveryLevelSet',DateType(),True),
    StructField('isOwnerDebit',BooleanType(),False),
    StructField('isOccupierDebit',BooleanType(),False),
    StructField('isInArrears',BooleanType(),False),
    StructField('financialYear',StringType(),False),
    StructField('issuedCode',StringType(),True),
    StructField('debitCreatedDate',DateType(),True),
    StructField('debitUpdatedDate',DateType(),True),
    StructField('originalIssueDate',DateType(),True),
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
DeltaSaveDataframeDirect(df_updated_column, source_group, target_table, ADS_DATABASE_CLEANSED, ADS_CONTAINER_CLEANSED, "overwrite", "")
# COMMAND ----------

# DBTITLE 1,13. Exit Notebook
dbutils.notebook.exit("1")