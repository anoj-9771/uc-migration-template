# Databricks notebook source
# DBTITLE 1,Generate parameter and source object string for unit testing
import json
accessTable = 'Z309_TPROPERTY'

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
		C_LGA AS LGACode, \
        b.LGA, \
		C_PROP_TYPE AS propertyTypeCode, \
        e.propertyType, \
        f.propertyTypeCode as superiorPropertyTypecode, \
        f.propertyType as superiorPropertyType, \
        case when D_PROP_TYPE_EFFE is not null \
                  then to_date(D_PROP_TYPE_EFFE,'yyyyMMdd') \
             when D_PROP_RATE_CANC is not null \
                  then to_date(D_PROP_RATE_CANC,'yyyyMMdd') \
                  else to_date(D_PROP_UPDA,'yyyyMMdd') \
        end AS propertyTypeEffectiveDate, \
        C_RATA_TYPE AS rateabilityTypeCode, \
		initcap(h.rateabilityType) as rateabilityType, \
		cast(Q_RESI_PORT as decimal(5,0)) AS residentialPortionCount, \
        case when F_RATE_INCL = 'Y' \
                  then true \
                  else false \
        end AS hasIncludedRatings, \
        case when F_VALU_INCL = 'Y' \
                  then true \
                  else false \
        end AS hasIncludedValuations, \
        case when F_METE_INCL = 'Y' \
                  then true \
                  else false \
        end AS hasIncludedMeters, \
        case when F_SPEC_METE_ALLO = 'Y' \
                  then true \
                  else false \
        end AS hasSpecialMeterAllocation, \
        case when F_FREE_SUPP = 'Y' \
                  then true \
                  else false \
        end AS hasFreeSupply, \
        case when F_SPEC_PROP_DESC = 'Y' \
                  then true \
                  else false \
        end AS hasSpecialPropertyDescription, \
		C_SEWE_USAG_TYPE AS sewerUsageTypeCode, \
		cast(Q_PROP_METE as int) AS propertyMeterCount, \
		cast(Q_PROP_AREA as decimal(9,3)) AS propertyArea, \
		C_PROP_AREA_TYPE AS propertyAreaTypeCode, \
		cast(A_PROP_PURC_PRIC as decimal(11,0)) AS propertyPurchasePriceAmount, \
		case when d_prop_sale_sett = '00000000' \
                  then null \
             when D_PROP_SALE_SETT = '00181029' \
                  then to_date('20181029','yyyyMMdd') \
                  else to_date(D_PROP_SALE_SETT,'yyyyMMdd') \
        end AS propertySaleSettledDate, \
		to_date(D_CNTR, 'yyyyMMdd') AS contractDate, \
		C_EXTR_LOT AS extraLotCode, \
        to_date(D_PROP_UPDA, 'yyyyMMdd') AS propertyUpdatedDate, \
		T_PROP_LOT AS lotDescription, \
		C_USER_CREA AS userCreated, \
		C_PLAN_CREA AS planCreated, \
		cast(to_unix_timestamp(H_CREA, 'yyyy-MM-dd hh:mm:ss a') as timestamp) as createdTimestamp, \
		C_USER_MODI AS userModified, \
		C_PLAN_MODI AS planModified, \
		cast(to_unix_timestamp(H_CREA, 'yyyy-MM-dd hh:mm:ss a') as timestamp) as modifiedTimestamp, \
        a._RecordStart, \
        a._RecordEnd, \
        a._RecordDeleted, \
        a._RecordCurrent \
	FROM CLEANSED.STG_ACCESS_Z309_TPROPERTY a \
         left outer join cleansed.t_access_Z309_TLocalGovt b on b.LGACode = a.c_lga \
         left outer join cleansed.t_access_Z309_TPropType e on e.propertyTypeCode = a.c_prop_type \
         left outer join cleansed.t_access_Z309_TPropType f on e.superiorPropertyTypeCode = f.propertyTypeCode \
         left outer join cleansed.t_access_Z309_TRataType h on h.rateabilityTypeCode = a.c_rata_type \
")

display(df_cleansed)

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
    StructField('hasIncludedValuations',BooleanType(),False),
    StructField('hasIncludedMeters',BooleanType(),False),
    StructField('hasSpecialMeterAllocation',BooleanType(),False),
    StructField('hasFreeSupply',BooleanType(),False),
    StructField('hasSpecialPropertyDescription',BooleanType(),False),
	StructField('sewerUsageTypeCode',StringType(),True),
	StructField('propertyMeterCount',IntegerType(),False),
	StructField('propertyArea',DecimalType(9,3),False),
	StructField('propertyAreaTypeCode',StringType(),True),
	StructField('propertyPurchasePriceAmount',DecimalType(11,0),False),
	StructField('propertySaleSettledDate',DateType(),True),
	StructField('contractDate',DateType(),True),
	StructField('extraLotCode',StringType(),True),
	StructField('propertyUpdatedDate',DateType(),True),
    StructField('lotDescription',StringType(),True),
	StructField('userCreated',StringType(),True),
	StructField('planCreated',StringType(),True),
	StructField('createdTimestamp',TimestampType(),True),
	StructField('userModified',StringType(),True),
	StructField('planModified',StringType(),True),
	StructField('modifiedTimestamp',TimestampType(),True),
    StructField('_RecordStart',TimestampType(),False),
    StructField('_RecordEnd',TimestampType(),False),
    StructField('_RecordDeleted',IntegerType(),False),
    StructField('_RecordCurrent',IntegerType(),False)
])

df_updated_column = spark.createDataFrame(df_cleansed.rdd, schema=newSchema)
display(df_updated_column)

# COMMAND ----------

# DBTITLE 1,12. Save Data frame into Cleansed Delta table (Final)
#Save Data frame into Cleansed Delta table (final)
DeltaSaveDataframeDirect(df_updated_column, "t", source_object, ADS_DATABASE_CLEANSED, ADS_CONTAINER_CLEANSED, "overwrite", "")

# COMMAND ----------

# DBTITLE 1,13. Exit Notebook
dbutils.notebook.exit("1")
