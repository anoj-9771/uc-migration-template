# Databricks notebook source
dbutils.widgets.text(name="task", defaultValue="", label="task")

# COMMAND ----------

# MAGIC %run ../Common/common-include-all

# COMMAND ----------

spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")
task = dbutils.widgets.get("task")
j = json.loads(task)
systemCode = j.get("SystemCode")
sourceTable = j.get("SourceTableName")
destinationSchema = j.get("DestinationSchema")
destinationTableName = j.get("DestinationTableName")
cleansedPath = j.get("CleansedPath")
businessKey = j.get("BusinessKeyColumn")
destinationKeyVaultSecret = j.get("DestinationKeyVaultSecret")
extendedProperties = j.get("ExtendedProperties")
dataLakePath = cleansedPath.replace("/cleansed", "/mnt/datalake-cleansed")
rawTableNameMatchSource = None

if extendedProperties:
    extendedProperties = json.loads(extendedProperties)
    rawTableNameMatchSource = extendedProperties.get("RawTableNameMatchSource")
if rawTableNameMatchSource:
    sourceTableName = get_table_name('raw', destinationSchema, sourceTable).lower()
    source_table_name_nonuc = f'raw.{destinationSchema}_{sourceTable}'.lower()
else:
    sourceTableName = get_table_name('raw', destinationSchema, destinationTableName).lower()
    source_table_name_nonuc = f'raw.{destinationSchema}_{destinationTableName}'.lower()
cleansedTableName = get_table_name('cleansed', destinationSchema, destinationTableName).lower()

# COMMAND ----------

#GET LAST CLEANSED LOAD TIMESTAMP
lastLoadTimeStamp = '2022-01-01'
try:
    lastLoadTimeStamp = spark.sql(f"select date_format(max(_DLCleansedZoneTimeStamp),'yyyy-MM-dd HH:mm:ss') as lastLoadTimeStamp from {cleansedTableName}").collect()[0][0]
except Exception as e:
    print(str(e))
                

print(lastLoadTimeStamp)

# COMMAND ----------

#Flag verified dataset to mark existing records for a file timestamp range as deleted
if sourceTable.lower() == "tsv_verified":
    isVerifiedDataset = True
else:
    isVerifiedDataset = False

#if provisional dataset previously existing records should not be updated only new records inserted    
if sourceTable.lower() == "tsv_provisional":
    mergeWithUpdate = False
    source_business_key="GAUGE_ID,VARIABLENAME_UNIT,TIMESTAMP"
else:
    mergeWithUpdate = True


# COMMAND ----------

import pandas as pd
from pyspark.sql.functions import col,lit
from pandas.tseries.offsets import DateOffset

sourceDataFrame = spark.sql(f"select * from {sourceTableName} where _DLRawZoneTimeStamp > '{lastLoadTimeStamp}'")
CleansedSourceCount = sourceDataFrame.count()

recordStart = pd.Timestamp.now()
#if dataset is verified and the source table already exists loop through the newly loaded data to raw file by file and update the records between the min and max timestamp for the gauge
if isVerifiedDataset and spark.sql("show tables in cleansed").where(f"tableName = lower('{destinationSchema}_{destinationTableName}')").count() > 0:
    print("Verified dataset found. Looping through files and setting existing records to deleted where they exist within the range of a new file...")
    gaugeRangeDf = (
        sourceDataFrame.groupby("GAUGE_ID","_InputFileName")
        .agg(
            date_format(min("TIMESTAMP"),'yyyy-MM-dd HH:mm:ss').alias("minTimestamp"), 
            date_format(max("TIMESTAMP"),'yyyy-MM-dd HH:mm:ss').alias("maxTimestamp")
         )
    )
    for i in gaugeRangeDf.collect():
        sql = f"update {cleansedTableName} set _RecordDeleted = 1, _RecordCurrent = 0, _RecordEnd = from_utc_timestamp('{recordStart}','Australia/Sydney') where gaugeId = '{i.GAUGE_ID}' and measurementResultDateTime between '{i.minTimestamp}' and '{i.maxTimestamp}' and _RecordCurrent = 1"
        print(sql)
        spark.sql(sql)

#FIX BAD COLUMNS
sourceDataFrame = sourceDataFrame.toDF(*(c.replace(' ', '_') for c in sourceDataFrame.columns))

if isVerifiedDataset:
    recordStart = recordStart + DateOffset(seconds=1)

if(extendedProperties):
    groupOrderBy = extendedProperties.get("GroupOrderBy")

# GET LATEST RECORD OF THE BUSINESS KEY
if(groupOrderBy):
    
    sourceDataFrame = GetRawLatestRecordBK(sourceDataFrame,source_business_key,groupOrderBy,systemCode)

# APPLY CLEANSED FRAMEWORK
cleanseDataFrame = CleansedTransform(sourceDataFrame, sourceTableName.lower(), systemCode)
cleanseDataFrame = cleanseDataFrame.withColumn("_DLCleansedZoneTimeStamp",to_timestamp(lit(recordStart))) \
                                   .withColumn("_RecordCurrent",lit('1')) \
                                   .withColumn("_RecordDeleted",lit('0')) \
                                   .withColumn("_RecordStart",to_timestamp(lit(recordStart))) \
                                   .withColumn("_RecordEnd",to_timestamp(lit("9999-12-31"), "yyyy-MM-dd"))

# COMMAND ----------

#if verified dataset append all new records as previously existing records have been marked as deleted
if isVerifiedDataset:
    AppendDeltaTable(cleanseDataFrame, cleansedTableName, dataLakePath, j.get("BusinessKeyColumn")) 
else:
    CreateDeltaTable(cleanseDataFrame, cleansedTableName, dataLakePath) if j.get("BusinessKeyColumn") is None else CreateOrMerge(cleanseDataFrame, cleansedTableName, dataLakePath, j.get("BusinessKeyColumn"), True, mergeWithUpdate)        

# COMMAND ----------

CleansedSinkCount = spark.table(cleansedTableName).count()
#print(f"Cleansed Source Count: {CleansedSourceCount} Cleansed Sink Count: {CleansedSinkCount}")
dbutils.notebook.exit({"CleansedSourceCount": CleansedSourceCount, "CleansedSinkCount": CleansedSinkCount})
