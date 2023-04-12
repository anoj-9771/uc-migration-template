# Databricks notebook source
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
sourceTableName = get_table_name('raw', destinationSchema, destinationTableName)
cleansedTableName = get_table_name('cleansed', destinationSchema, destinationTableName)
loadType = ""

# COMMAND ----------

sourceDataFrame = spark.table(sourceTableName)

# CLEANSED QUERY FROM RAW TO FLATTEN OBJECT
if(extendedProperties):
  extendedProperties = json.loads(extendedProperties)
  loadType = extendedProperties.get("LoadType")
  cleansedQuery = extendedProperties.get("CleansedQuery")
  transformMethod = extendedProperties.get("TransformMethod")
  if(cleansedQuery):
    sourceDataFrame = spark.sql(cleansedQuery.replace("{tableFqn}", sourceTableName))
  if(transformMethod):
    sourceDataFrame = getattr(sys.modules[__name__], f"{transformMethod}")(spark.table(sourceTableName))  

# FIX BAD COLUMNS
sourceDataFrame = sourceDataFrame.toDF(*(RemoveBadCharacters(c) for c in sourceDataFrame.columns))

# APPLY CLEANSED FRAMEWORK
sourceDataFrame = CleansedTransform(sourceDataFrame, sourceTableName, systemCode)
 

if loadType == "Append":
     AppendDeltaTable(sourceDataFrame, cleansedTableName, dataLakePath)
else:
    CreateDeltaTable(sourceDataFrame, cleansedTableName, dataLakePath) if j.get("BusinessKeyColumn") is None else CreateOrMerge(sourceDataFrame, cleansedTableName, dataLakePath, j.get("BusinessKeyColumn"))
