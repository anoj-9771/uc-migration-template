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
destinationSchema = j.get("DestinationSchema")
destinationTableName = j.get("DestinationTableName")
cleansedPath = j.get("CleansedPath")
businessKey = j.get("BusinessKeyColumn")
destinationKeyVaultSecret = j.get("DestinationKeyVaultSecret")
extendedProperties = j.get("ExtendedProperties")
dataLakePath = cleansedPath.replace("/cleansed", "/mnt/datalake-cleansed")
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

excludeColumns = ""
whereClause = "1 = 1"
if systemCode == "hydra":
    excludeColumns = "ChildJoins"
    whereClause = 'ChildJoins is not null'
    
sourceDataFrame = spark.sql(f"select * from {sourceTableName} where _DLRawZoneTimeStamp > '{lastLoadTimeStamp}'")

sourceDataFrame = sourceDataFrame.where(whereClause)
    
sourceDataFrame = ExpandTable(sourceDataFrame, True, "_", excludeColumns)

cleanseDataFrame = CleansedTransform(sourceDataFrame, sourceTableName.lower(), systemCode)
cleanseDataFrame = cleanseDataFrame.withColumn("_DLCleansedZoneTimeStamp",current_timestamp()) \
                                   .withColumn("_RecordCurrent",lit('1')) \
                                   .withColumn("_RecordDeleted",lit('0')) \
                                   .withColumn("_RecordStart",current_timestamp()) \
                                   .withColumn("_RecordEnd",to_timestamp(lit("9999-12-31"), "yyyy-MM-dd"))

cleanseDataFrame.display()

tableName = f"{destinationSchema}_{destinationTableName}"
CreateDeltaTable(cleanseDataFrame, f"cleansed.{tableName}", dataLakePath) if j.get("BusinessKeyColumn") is None else CreateOrMerge(cleanseDataFrame, f"cleansed.{tableName}", dataLakePath, j.get("BusinessKeyColumn"))

# COMMAND ----------

CleansedSinkCount = spark.table(cleansedTableName).count()
#print(f"Cleansed Source Count: {CleansedSourceCount} Cleansed Sink Count: {CleansedSinkCount}")
dbutils.notebook.exit({"CleansedSourceCount": CleansedSourceCount, "CleansedSinkCount": CleansedSinkCount})