# Databricks notebook source
# DBTITLE 1,Generate parameter and source object string for unit testing
import json
accessTable = 'Z309_TPROPERTY'
businessKeys = 'N_PROP'

runParm = '{"SourceType":"BLOB Storage (csv)","SourceServer":"daf-sa-lake-sastoken","SourceGroup":"accessdata","SourceName":"access_####","SourceLocation":"accessdata/####","AdditionalProperty":"","Processor":"databricks-token|1103-023442-me8nqcm9|Standard_DS3_v2|8.3.x-scala2.12|2:8|interactive","IsAuditTable":false,"SoftDeleteSource":"","ProjectName":"CLEANSED DATA ACCESS","ProjectId":2,"TargetType":"BLOB Storage (csv)","TargetName":"access_####","TargetLocation":"accessdata/####","TargetServer":"daf-sa-lake-sastoken","DataLoadMode":"TRUNCATE-LOAD","DeltaExtract":false,"CDCSource":false,"TruncateTarget":true,"UpsertTarget":false,"AppendTarget":false,"TrackChanges":false,"LoadToSqlEDW":true,"TaskName":"access_####","ControlStageId":2,"TaskId":40,"StageSequence":200,"StageName":"Raw to Cleansed","SourceId":40,"TargetId":40,"ObjectGrain":"Day","CommandTypeId":8,"Watermarks":"","WatermarksDT":null,"WatermarkColumn":"","BusinessKeyColumn":"yyyy","PartitionColumn":null,"UpdateMetaData":null,"SourceTimeStampFormat":"","Command":"/build/cleansed/accessdata/####","LastLoadedFile":null}'

s = json.loads(runParm)
for parm in ['SourceName','SourceLocation','TargetName','TargetLocation','TaskName','BusinessKeyColumn','Command']:
    s[parm] = s[parm].replace('####',accessTable).replace('yyyy',businessKeys)
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
df_cleansed = spark.sql(f"SELECT cast(N_PROP as int) AS propertyNumber, \
		C_LGA AS LGACode, \
        b.LGA, \
		C_PROP_TYPE AS propertyTypeCode, \
        e.propertyType, \
        f.propertyTypeCode as superiorPropertyTypeCode, \
        f.propertyType as superiorPropertyType, \
        case when D_PROP_TYPE_EFFE is not null \
                  then to_date(D_PROP_TYPE_EFFE,'yyyyMMdd') \
             when D_PROP_RATE_CANC is not null \
                  then to_date(D_PROP_RATE_CANC,'yyyyMMdd') \
                  else to_date(D_PROP_UPDA,'yyyyMMdd') \
        end AS propertyTypeEffectiveFrom, \
        C_RATA_TYPE AS rateabilityTypeCode, \
		initcap(h.rateabilityType) as rateabilityType, \
		cast(Q_RESI_PORT as decimal(5,0)) AS residentialPortionCount, \
        case when F_RATE_INCL = 'R' \
                  then true \
                  else false \
        end AS hasIncludedRatings, \
        case when F_RATE_INCL = 'I' \
                  then true \
                  else false \
        end AS isIncludedInOtherRating, \
        case when F_VALU_INCL = 'Y' \
              then true \
              else false \
        end AS hasIncludedValuations, \
        case when F_METE_INCL = 'M' \
                  then true \
                  else false \
        end AS meterServesOtherProperties, \
        case when F_METE_INCL = 'L' \
                  then true \
                  else false \
        end AS hasMeterOnOtherProperty, \
        case when F_SPEC_METE_ALLO = '1' \
                  then true \
                  else false \
        end AS hasSpecialMeterAllocation, \
        case when F_FREE_SUPP = 'K' \
                  then true \
                  else false \
        end AS hasKidneyFreeSupply, \
        case when F_FREE_SUPP in('Y','1') \
                  then true \
                  else false \
        end AS hasNonKidneyFreeSupply, \
        case when F_SPEC_PROP_DESC = '1' \
                  then true \
                  else false \
        end AS hasSpecialPropertyDescription, \
		C_SEWE_USAG_TYPE AS sewerUsageTypeCode, \
        ref5.sewerUsageType as sewerUsageType, \
		cast(Q_PROP_METE as int) AS propertyMeterCount, \
		cast(Q_PROP_AREA as decimal(9,3)) AS propertyArea, \
		C_PROP_AREA_TYPE AS propertyAreaTypeCode, \
        ref6.propertyAreaType as propertyAreaType, \
		cast(A_PROP_PURC_PRIC as decimal(11,0)) AS purchasePrice, \
		case when d_prop_sale_sett = '00000000' \
                  then null \
             when D_PROP_SALE_SETT = '00181029' \
                  then to_date('20181029','yyyyMMdd') \
                  else to_date(D_PROP_SALE_SETT,'yyyyMMdd') \
        end AS settlementDate, \
		to_date(D_CNTR, 'yyyyMMdd') AS contractDate, \
		C_EXTR_LOT AS extractLotCode, \
        ref7.extractLotDescription as extractLotDescription, \
        to_date(D_PROP_UPDA, 'yyyyMMdd') AS propertyUpdatedDate, \
		T_PROP_LOT AS lotDescription, \
		C_USER_CREA AS createdByUserId, \
		C_PLAN_CREA AS createdByPlan, \
		cast(to_unix_timestamp(H_CREA, 'yyyy-MM-dd hh:mm:ss a') as timestamp) as createdTimestamp, \
        case when substr(hex(c_user_modi),1,2) = '00' then ' ' else C_USER_MODI end AS modifiedByUserId, \
		C_PLAN_MODI AS modifiedByPlan, \
		cast(to_unix_timestamp(H_MODI, 'yyyy-MM-dd hh:mm:ss a') as timestamp) as modifiedTimestamp, \
        a._RecordStart, \
        a._RecordEnd, \
        a._RecordDeleted, \
        a._RecordCurrent \
	FROM {ADS_DATABASE_STAGE}.{source_object} a \
         left outer join cleansed.access_Z309_TLocalGovt b on b.LGACode = a.c_lga and b._RecordCurrent = 1 \
         left outer join cleansed.access_Z309_TPropType e on e.propertyTypeCode = a.c_prop_type and e._RecordCurrent = 1 \
         left outer join cleansed.access_Z309_TPropType f on e.superiorPropertyTypeCode = f.propertyTypeCode and f._RecordCurrent = 1 \
         left outer join cleansed.access_Z309_TRataType h on h.rateabilityTypeCode = a.c_rata_type and h._RecordCurrent = 1 \
         left outer join cleansed.access_Z309_TSEWEUSAGETYPE ref5 on a.C_SEWE_USAG_TYPE = ref5.sewerUsageTypeCode and ref5._RecordCurrent = 1 \
         left outer join cleansed.access_Z309_TPROPAREATYPE ref6 on a.C_PROP_AREA_TYPE = ref6.propertyAreaTypeCode and ref6._RecordCurrent = 1 \
         left outer join cleansed.access_Z309_TEXTRACTLOT ref7 on a.C_EXTR_LOT = ref7.extractLotCode and ref7._RecordCurrent = 1 \
")
#print(f'Number of rows: {df_cleansed.count()}')

# COMMAND ----------

newSchema = StructType([
    StructField('propertyNumber',IntegerType(),False),
    StructField('LGACode',StringType(),False),
    StructField('LGA',StringType(),False),
	StructField('propertyTypeCode',StringType(),False),
	StructField('propertyType',StringType(),False),
    StructField('superiorPropertyTypeCode',StringType(),False),
    StructField('superiorPropertyType',StringType(),False),
    StructField('propertyTypeEffectiveFrom',DateType(),False),
    StructField('rateabilityTypeCode',StringType(),False),
    StructField('rateabilityType',StringType(),False),
	StructField('residentialPortionCount',DecimalType(5,0),False),
    StructField('hasIncludedRatings',BooleanType(),False),
    StructField('isIncludedInOtherRating',BooleanType(),False),
    StructField('hasIncludedValuations',BooleanType(),False),
    StructField('meterServesOtherProperties',BooleanType(),False),
    StructField('hasMeterOnOtherProperty',BooleanType(),False),
    StructField('hasSpecialMeterAllocation',BooleanType(),False),
    StructField('hasKidneyFreeSupply',BooleanType(),False),
    StructField('hasNonKidneyFreeSupply',BooleanType(),False),
    StructField('hasSpecialPropertyDescription',BooleanType(),False),
	StructField('sewerUsageTypeCode',StringType(),True),
	StructField('sewerUsageType',StringType(),True),
	StructField('propertyMeterCount',IntegerType(),False),
	StructField('propertyArea',DecimalType(9,3),False),
	StructField('propertyAreaTypeCode',StringType(),True),
	StructField('propertyAreaType',StringType(),True),
    StructField('purchasePrice',DecimalType(11,0),False),
	StructField('settlementDate',DateType(),True),
	StructField('contractDate',DateType(),True),
	StructField('extractLotCode',StringType(),True),
	StructField('extractLotDescription',StringType(),True),
	StructField('propertyUpdatedDate',DateType(),True),
    StructField('lotDescription',StringType(),True),
	StructField('createdByUserId',StringType(),True),
	StructField('createdByPlan',StringType(),True),
	StructField('createdTimestamp',TimestampType(),True),
	StructField('modifiedByUserId',StringType(),True),
	StructField('modifiedByPlan',StringType(),True),
	StructField('modifiedTimestamp',TimestampType(),True),
    StructField('_RecordStart',TimestampType(),False),
    StructField('_RecordEnd',TimestampType(),False),
    StructField('_RecordDeleted',IntegerType(),False),
    StructField('_RecordCurrent',IntegerType(),False)
])

# df_updated_column = spark.createDataFrame(df_cleansed.rdd, schema=newSchema)
# display(df_updated_column)

# COMMAND ----------

# DBTITLE 1,12. Save Data frame into Cleansed Delta table (Final)
#Save Data frame into Cleansed Delta table (final)
DeltaSaveDataframeDirect(df_cleansed, source_group, target_table, ADS_DATABASE_CLEANSED, ADS_CONTAINER_CLEANSED, "overwrite", newSchema, "")

# COMMAND ----------

# DBTITLE 1,13. Exit Notebook
dbutils.notebook.exit("1")
