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
    sourceTableName = get_table_name('raw',destinationSchema, sourceTable).lower()
    source_table_name_nonuc = f'raw.{destinationSchema}_{sourceTable}'
else:
    sourceTableName = get_table_name('raw', destinationSchema, destinationTableName).lower()
    source_table_name_nonuc = f'raw.{destinationSchema}_{destinationTableName}'
cleansedTableName = get_table_name('cleansed', destinationSchema, destinationTableName).lower()


# COMMAND ----------

#GET LAST CLEANSED LOAD TIMESTAMP
lastLoadTimeStamp = '2022-01-01'
try:
    lastLoadTimeStamp = spark.sql(f"select date_format(max(_DLCleansedZoneTimeStamp),'yyyy-MM-dd HH:mm:ss') as lastLoadTimeStamp from {cleansedTableName}").collect()[0][0]
    print(lastLoadTimeStamp)
except Exception as e:
    print(str(e))

# COMMAND ----------

sourceDataFrame = spark.sql(f"select * from {sourceTableName} where _DLRawZoneTimeStamp > '{lastLoadTimeStamp}'")
sourceDataFrame = sourceDataFrame.groupby(sourceDataFrame.columns[0:-1]).count().drop("count")
CleansedSourceCount = sourceDataFrame.count()

# FIX BAD COLUMNS
sourceDataFrame = sourceDataFrame.toDF(*(c.replace(' ', '_') for c in sourceDataFrame.columns))

# CLEANSED QUERY FROM RAW TO FLATTEN OBJECT
if(extendedProperties):
    cleansedPath = extendedProperties.get("CleansedQuery")
    if(cleansedPath):
        sourceDataFrame = spark.sql(cleansedPath.replace("{tableFqn}", sourceTableName)
                                                .replace("{lastLoadTimeStamp}", lastLoadTimeStamp)
                                    )
    
# APPLY CLEANSED FRAMEWORK
cleanseDataFrame = CleansedTransform(sourceDataFrame, sourceTableName.lower(), systemCode)
cleanseDataFrame = cleanseDataFrame.withColumn("_DLCleansedZoneTimeStamp",current_timestamp()) \
                                   .withColumn("_RecordCurrent",lit('1')) \
                                   .withColumn("_RecordDeleted",lit('0')) \
                                   .withColumn("_RecordStart",current_timestamp()) \
                                   .withColumn("_RecordEnd",to_timestamp(lit("9999-12-31"), "yyyy-MM-dd"))

# HANDLE SOURCE SYSTEM DELETES
if(extendedProperties):
    deleteRecordsTable = extendedProperties.get("deleteRecordsTable")
    deleteRecordsQuery = extendedProperties.get("deleteRecordsQuery")
    groupOrderBy = extendedProperties.get("GroupOrderBy")
    if(deleteRecordsTable):
        deleteRecordsTable = f"cleansed.{deleteRecordsTable}"
        deletedRecordsDataFrame = SourceDeletedRecords(cleansedTableName,businessKey,groupOrderBy,deleteRecordsTable,systemCode,lastLoadTimeStamp)
        if (deletedRecordsDataFrame):
            cleanseDataFrame = cleanseDataFrame.unionByName(deletedRecordsDataFrame, allowMissingColumns=True)                                                  
    elif(deleteRecordsQuery and businessKey):
        if TableExists(cleansedTableName):
            cleanseDataFrame.unionByName(spark.table(cleansedTableName)).createOrReplaceTempView("vwCleanseDataFrame")
        else:    
            cleanseDataFrame.createOrReplaceTempView("vwCleanseDataFrame")
        deletedRecordsDataFrame = spark.sql(deleteRecordsQuery.replace("{lastLoadTimeStamp}", lastLoadTimeStamp)
                                                              .replace("{vwCleanseDataFrame}", "vwCleanseDataFrame")
                                           )
        deletedRecordsDataFrame = deletedRecordsDataFrame.withColumn("_DLCleansedZoneTimeStamp",current_timestamp()) \
                                                         .withColumn("_RecordCurrent",lit('1')) \
                                                         .withColumn("_RecordDeleted",lit('1')) \
                                                         .withColumn("_RecordStart",current_timestamp()) \
                                                         .withColumn("_RecordEnd",to_timestamp(lit("9999-12-31"), "yyyy-MM-dd"))
        cleanseDataFrame = cleanseDataFrame.join(deletedRecordsDataFrame,businessKey.split(','),'leftanti')
        cleanseDataFrame = cleanseDataFrame.unionByName(deletedRecordsDataFrame)                                                         

# GET LATEST RECORD OF THE BUSINESS KEY
    if(groupOrderBy):
        cleanseDataFrame = GetRawLatestRecordBK(cleanseDataFrame,businessKey,groupOrderBy,systemCode)

if extendedProperties and extendedProperties.get("LoadType") == "Append":
    AppendDeltaTable(cleanseDataFrame, cleansedTableName, dataLakePath, j.get("BusinessKeyColumn"))
else:    
    CreateDeltaTable(cleanseDataFrame, cleansedTableName, dataLakePath) if j.get("BusinessKeyColumn") is None else CreateOrMerge(cleanseDataFrame, cleansedTableName, dataLakePath, j.get("BusinessKeyColumn"))

# COMMAND ----------

CleansedSinkCount = spark.table(cleansedTableName).where("_RecordDeleted = 0").count()
#print(f"Cleansed Source Count: {CleansedSourceCount} Cleansed Sink Count: {CleansedSinkCount}")
dbutils.notebook.exit({"CleansedSourceCount": CleansedSourceCount, "CleansedSinkCount": CleansedSinkCount})
