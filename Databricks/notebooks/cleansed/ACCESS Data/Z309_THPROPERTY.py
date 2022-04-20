# Databricks notebook source
# DBTITLE 1,Generate parameter and source object name for unit testing
import json
accessTable = 'Z309_THPROPERTY'
businessKeys = 'propertyNumber,dateRowSuperseded,timeRowSuperseded'

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
df_cleansed = spark.sql(f"SELECT \
	cast(tbl.N_PROP as int) as propertyNumber, \
	tbl.C_LGA as LGACode, \
	ref1.LGA as LGA, \
	tbl.C_STRE_GUID as streetGuideCode, \
	tbl.N_ADDR_UNIT as unitNumber, \
	tbl.N_ADDR_STRE as streetNumber, \
	tbl.C_OTHE_ADDR_TYPE as otherAddressTypeCode, \
	ref2.otherAddressType as otherAddressType, \
	tbl.T_OTHE_ADDR_INFO as otherAddressInformation, \
	cast(tbl.N_WATE_SERV_SKET as int) as waterServiceSketchNumber, \
	tbl.N_SEWE_DIAG as sewerDiagramNumber, \
	tbl.C_PROP_TYPE as propertyTypeCode, \
	ref3.propertyType as propertyType, \
	tbl.D_PROP_TYPE_EFFE as propertyTypeEffectiveFrom, \
	tbl.C_RATA_TYPE as rateabilityTypeCode, \
	ref4.rateabilityType as rateabilityType, \
	tbl.D_PROP_RATE_CANC as propertyRatingCancelledDate, \
	cast(tbl.Q_RESI_PORT as int) as residentialPortionCount, \
	tbl.F_RATE_INCL as hasIncludedRatings, \
	tbl.F_VALU_INCL as hasIncludedValuations, \
	tbl.F_METE_INCL as hasIncludedMeters, \
	tbl.F_SPEC_METE_ALLO as hasSpecialMeterAllocation, \
	tbl.F_FREE_SUPP as hasFreeSupply, \
	tbl.C_SEWE_USAG_TYPE as sewerUsageTypeCode, \
	ref5.sewerUsageType as sewerUsageType, \
	tbl.F_SPEC_PROP_DESC as hasSpecialPropertyDescription, \
	cast(tbl.Q_PROP_METE as int) as propertyMeterCount, \
	cast(tbl.N_LAST_STMT_ISSU as int) as lastStatementIssuedNumber, \
	tbl.F_CERT_NO_METE as isCertificateNumberMeter, \
	tbl.N_SEWE_REFE_SHEE as sewerReferenceSheetNumber, \
	cast(tbl.Q_PROP_AREA as dec(9,3)) as propertyArea, \
	tbl.C_PROP_AREA_TYPE as propertyAreaTypeCode, \
	ref6.propertyAreaType as propertyAreaType, \
	cast(tbl.A_PROP_PURC_PRIC as int) as purchasePrice, \
	tbl.D_PROP_SALE_SETT as settlementDate, \
	tbl.D_CNTR as contractDate, \
	tbl.C_EXTR_LOT as extractLotCode, \
	ref7.extractLotDescription as extractLotDescription, \
	tbl.D_PROP_UPDA as propertyUpdatedDate, \
	tbl.T_PROP_LOT as lotDescription, \
	tbl.D_SUPD as dateRowSuperseded, \
	tbl.T_TIME_SUPD as timeRowSuperseded, \
	tbl.M_PROC as modifiedByProcess, \
	tbl.C_USER_ID as modifiedByUserID, \
	tbl.C_TERM_ID as modifiedByTerminalID, \
	tbl.F_ADJU as rowAdjusted, \
	tbl.C_USER_CREA as createdByUserID, \
	tbl.C_PLAN_CREA as createdByPlan, \
	ToValidDateTime(tbl.H_CREA) as createdTimestamp, \
	tbl.C_USER_MODI as modifiedByUserID, \
	tbl.C_PLAN_MODI as modifiedByPlan, \
	ToValidDateTime(tbl.H_MODI) as modifiedTimestamp, \
	_RecordStart, \
	_RecordEnd, \
	_RecordDeleted, \
	_RecordCurrent \
	FROM {ADS_DATABASE_STAGE}.{source_object} tbl \
left outer join Z309.TLOCALGOVT ref1 on tbl.C_LGA = ref1.LGA \
left outer join Z309.TOTHERADDRTYPE ref2 on tbl.C_OTHE_ADDR_TYPE = ref2.otherAddressType \
left outer join Z309.TPROPTYPE ref3 on tbl.C_PROP_TYPE = ref3.propertyType \
left outer join Z309.TRATATYPE ref4 on tbl.C_RATA_TYPE = ref4.rateabilityType \
left outer join Z309.TSEWEUSAGETYPE ref5 on tbl.C_SEWE_USAG_TYPE = ref5.sewerUsageType \
left outer join Z309.TPROPAREATYPE ref6 on tbl.C_PROP_AREA_TYPE = ref6.propertyAreaType \
left outer join Z309.TEXTRACTLOT ref7 on tbl.C_EXTR_LOT = ref7.extractLotDescription \
                                )

# COMMAND ----------

newSchema = StructType([
	StructField('propertyNumber',IntegerType(),True),
	StructField('LGACode',StringType(),True),
	StructField('LGA',StringType(),True),
	StructField('streetGuideCode',StringType(),True),
	StructField('unitNumber',StringType(),True),
	StructField('streetNumber',StringType(),True),
	StructField('otherAddressTypeCode',StringType(),True),
	StructField('otherAddressType',StringType(),True),
	StructField('otherAddressInformation',StringType(),True),
	StructField('waterServiceSketchNumber',IntegerType(),True),
	StructField('sewerDiagramNumber',StringType(),True),
	StructField('propertyTypeCode',StringType(),True),
	StructField('propertyType',StringType(),True),
	StructField('propertyTypeEffectiveFrom',StringType(),True),
	StructField('rateabilityTypeCode',StringType(),True),
	StructField('rateabilityType',StringType(),True),
	StructField('propertyRatingCancelledDate',StringType(),True),
	StructField('residentialPortionCount',IntegerType(),True),
	StructField('hasIncludedRatings',StringType(),True),
	StructField('hasIncludedValuations',StringType(),True),
	StructField('hasIncludedMeters',StringType(),True),
	StructField('hasSpecialMeterAllocation',StringType(),True),
	StructField('hasFreeSupply',StringType(),True),
	StructField('sewerUsageTypeCode',StringType(),True),
	StructField('sewerUsageType',StringType(),True),
	StructField('hasSpecialPropertyDescription',StringType(),True),
	StructField('propertyMeterCount',IntegerType(),True),
	StructField('lastStatementIssuedNumber',IntegerType(),True),
	StructField('isCertificateNumberMeter',StringType(),True),
	StructField('sewerReferenceSheetNumber',StringType(),True),
	StructField('propertyArea',DecimalType(9,3),True),
	StructField('propertyAreaTypeCode',StringType(),True),
	StructField('propertyAreaType',StringType(),True),
	StructField('purchasePrice',IntegerType(),True),
	StructField('settlementDate',StringType(),True),
	StructField('contractDate',StringType(),True),
	StructField('extractLotCode',StringType(),True),
	StructField('extractLotDescription',StringType(),True),
	StructField('propertyUpdatedDate',StringType(),True),
	StructField('lotDescription',StringType(),True),
	StructField('dateRowSuperseded',StringType(),True),
	StructField('timeRowSuperseded',StringType(),True),
	StructField('modifiedByProcess',StringType(),True),
	StructField('modifiedByUserID',StringType(),True),
	StructField('modifiedByTerminalID',StringType(),True),
	StructField('rowAdjusted',StringType(),True),
	StructField('createdByUserID',StringType(),True),
	StructField('createdByPlan',StringType(),True),
	StructField('createdTimestamp',TimestampType(),True),
	StructField('modifiedByUserID',StringType(),True),
	StructField('modifiedByPlan',StringType(),True),
	StructField('modifiedTimestamp',TimestampType(),True),
	StructField('_RecordStart',TimestampType(),False),
	StructField('_RecordEnd',TimestampType(),False),
	StructField('_RecordDeleted',IntegerType(),False),
	StructField('_RecordCurrent',IntegerType(),False)
])

# COMMAND ----------

# DBTITLE 1,12. Save Data frame into Cleansed Delta table (Final)
#Save Data frame into Cleansed Delta table (final)
DeltaSaveDataframeDirect(df_cleansed, source_group, target_table, ADS_DATABASE_CLEANSED, ADS_CONTAINER_CLEANSED, "overwrite", newSchema, "")

# COMMAND ----------

# DBTITLE 1,13. Exit Notebook
dbutils.notebook.exit("1")