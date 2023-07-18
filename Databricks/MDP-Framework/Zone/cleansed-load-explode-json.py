# Databricks notebook source
dbutils.widgets.text(name="task", defaultValue="", label="task")

# COMMAND ----------

# MAGIC %run ../Common/common-include-all

# COMMAND ----------

spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")
spark.conf.set("spark.sql.ansi.enabled", "true") #enabled to error incorrect sql casting types I.e. string to int will throw an exception
task = dbutils.widgets.get("task")
j = json.loads(task)
systemCode = j.get("SystemCode")
destinationSchema = j.get("DestinationSchema")
destinationTableName = j.get("DestinationTableName")
cleansedPath = j.get("CleansedPath")
businessKey = j.get("BusinessKeyColumn")
destinationKeyVaultSecret = j.get("DestinationKeyVaultSecret")
extendedProperties = j.get("ExtendedProperties")
dataLakePath = cleansedPath.replace("/cleansed", "/mnt/datalake-cleansed")
sourceTableName = get_table_name('raw', destinationSchema, destinationTableName)
cleansedTableName = get_table_name('cleansed', destinationSchema, destinationTableName)

# COMMAND ----------

#GET LAST CLEANSED LOAD TIMESTAMP
lastLoadTimeStamp = '2022-01-01'
try:
    lastLoadTimeStamp = spark.sql(f"select date_format(max(_DLCleansedZoneTimeStamp),'yyyy-MM-dd HH:mm:ss') as lastLoadTimeStamp from {cleansedTableName}").collect()[0][0]
    print(lastLoadTimeStamp)
except Exception as e:
    print(str(e))
                    

# COMMAND ----------

excludeColumns = ""
if systemCode == "hydra":
    excludeColumns = "features_ChildJoins,features_ParentJoins,features_Geometry"
    
sourceDataFrame = spark.sql(f"select * from {sourceTableName} where _DLRawZoneTimeStamp > '{lastLoadTimeStamp}'")
sourceDataFrame = ExpandTable(sourceDataFrame, True, "_", excludeColumns)
sourceDataFrame = sourceDataFrame.toDF(*(RemoveBadCharacters(c) for c in sourceDataFrame.columns))

CleansedSourceCount = sourceDataFrame.count()

if systemCode == "hydra":
    sourceDataFrame = ConvertBlankRecordsToNull(sourceDataFrame)

cleanseDataFrame = CleansedTransform(sourceDataFrame, sourceTableName.lower(), systemCode)
cleanseDataFrame = cleanseDataFrame.withColumn("_DLCleansedZoneTimeStamp",current_timestamp()) \
                                   .withColumn("_RecordCurrent",lit('1')) \
                                   .withColumn("_RecordDeleted",lit('0')) \
                                   .withColumn("_RecordStart",current_timestamp()) \
                                   .withColumn("_RecordEnd",to_timestamp(lit("9999-12-31"), "yyyy-MM-dd"))

CreateDeltaTable(cleanseDataFrame, cleansedTableName) if j.get("BusinessKeyColumn") is None else CreateOrMerge(cleanseDataFrame, cleansedTableName, j.get("BusinessKeyColumn"))

# COMMAND ----------

CleansedSinkCount = spark.table(cleansedTableName).count()
#print(f"Cleansed Source Count: {CleansedSourceCount} Cleansed Sink Count: {CleansedSinkCount}")
dbutils.notebook.exit({"CleansedSourceCount": CleansedSourceCount, "CleansedSinkCount": CleansedSinkCount})
