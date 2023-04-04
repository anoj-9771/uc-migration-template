# Databricks notebook source
SystemCode = 'datagov'
DestinationTableName = 'australiapublicholidays'

# COMMAND ----------

# MAGIC %run ../Common/common-include-all

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
def DeleteDirectoryRecursive(dirname):
    files=dbutils.fs.ls(dirname)
    for f in files:
        if f.isDir():
            DeleteDirectoryRecursive(f.path)
        dbutils.fs.rm(f.path, recurse=True)
    dbutils.fs.rm(dirname, True)

# COMMAND ----------

def CleanTable(tableNameFqn):
    try:
        detail = spark.sql(f"DESCRIBE DETAIL {tableNameFqn}").collect()[0]
        DeleteDirectoryRecursive(detail.location)
    except:    
        pass
    
    try:
        spark.sql(f"DROP TABLE {tableNameFqn}")
    except:
        pass
CleanTable(f"cleansed.{SystemCode}_{DestinationTableName}")      

# COMMAND ----------

manifest_df = ( 
                spark.table("controldb.dbo_extractloadmanifest")
                    .filter(f"SystemCode = '{SystemCode}'")
                    .filter(f"DestinationTableName = '{DestinationTableName}'")
               )
display(manifest_df)

# COMMAND ----------

for j in manifest_df.collect():
    systemCode = j.SystemCode
    destinationSchema = j.DestinationSchema
    destinationTableName = j.DestinationTableName
    CleansedPath = j.CleansedPath
    businessKey = j.BusinessKeyColumn
    destinationKeyVaultSecret = j.DestinationKeyVaultSecret
    extendedProperties = j.ExtendedProperties
    watermarkColumn = j.WatermarkColumn
    dataLakePath = CleansedPath.replace("/cleansed", "/mnt/datalake-cleansed")
    sourceTableName = f"raw.{destinationSchema}_{destinationTableName}"
    cleansedTableName = f"cleansed.{destinationSchema}_{destinationTableName}"

# COMMAND ----------

#GET LAST CLEANSED LOAD TIMESTAMP
try:
    lastLoadTimeStamp = spark.sql(f"select max(_DLCleansedZoneTimeStamp) as lastLoadTimeStamp from {cleansedTableName}").collect()[0][0]
    print(lastLoadTimeStamp)
except Exception as e:
    if "Table or view not found" in str(e):
        lastLoadTimeStamp = '2022-01-01'            

# COMMAND ----------

sourceDataFrame = spark.sql(f"select * from {sourceTableName} where _DLRawZoneTimeStamp > '{lastLoadTimeStamp}'")
sourceDataFrame = sourceDataFrame.groupby(sourceDataFrame.columns[0:-1]).count().drop("count")
CleansedSourceCount = sourceDataFrame.count()

# FIX BAD COLUMNS
sourceDataFrame = sourceDataFrame.toDF(*(c.replace(' ', '_') for c in sourceDataFrame.columns))

# CLEANSED QUERY FROM RAW TO FLATTEN OBJECT
if(extendedProperties):
    extendedProperties = json.loads(extendedProperties)
    cleansedPath = extendedProperties.get("CleansedQuery")
    if(cleansedPath):
        sourceDataFrame = spark.sql(cleansedPath.replace("{tableFqn}", sourceTableName))
    
# APPLY CLEANSED FRAMEWORK
cleanseDataFrame = CleansedTransform(sourceDataFrame, sourceTableName.lower(), systemCode)
cleanseDataFrame = cleanseDataFrame.withColumn("_DLCleansedZoneTimeStamp",current_timestamp()) \
                                   .withColumn("_RecordCurrent",lit('1')) \
                                   .withColumn("_RecordDeleted",lit('0')) \
                                   .withColumn("_RecordStart",current_timestamp()) \
                                   .withColumn("_RecordEnd",to_timestamp(lit("9999-12-31"), "yyyy-MM-dd"))

# GET LATEST RECORD OF THE BUSINESS KEY
if(extendedProperties):
    groupOrderBy = extendedProperties.get("GroupOrderBy")
    if(groupOrderBy):
        cleanseDataFrame.createOrReplaceTempView("vwCleanseDataFrame")
        cleanseDataFrame = spark.sql(f"select * from (select vwCleanseDataFrame.*, row_number() OVER (Partition By {businessKey} order by {groupOrderBy}) row_num from vwCleanseDataFrame) where row_num = 1 ").drop("row_num")     

tableName = f"{destinationSchema}_{destinationTableName}"
CreateDeltaTable(cleanseDataFrame, f"cleansed.{tableName}", dataLakePath) if businessKey is None else CreateOrMerge(cleanseDataFrame, f"cleansed.{tableName}", dataLakePath, businessKey)

# COMMAND ----------

CleansedSinkCount = spark.table(cleansedTableName).count()
print(f"Cleansed Source Count: {CleansedSourceCount} Cleansed Sink Count: {CleansedSinkCount}")
