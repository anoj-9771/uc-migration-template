# Databricks notebook source
#Define Widgets/Parameters
dbutils.widgets.text("task", "", "Task")

# COMMAND ----------

# MAGIC %run ../Common/common-include-all

# COMMAND ----------

spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")
task = dbutils.widgets.get("task")
print(task)
j = json.loads(task)
systemCode = j.get("SystemCode")
destinationSchema = j.get("DestinationSchema")
destinationTableName = j.get("DestinationTableName")
CleansedPath = j.get("CleansedPath")
businessKey = j.get("BusinessKeyColumn")
destinationKeyVaultSecret = j.get("DestinationKeyVaultSecret")
extendedProperties = j.get("ExtendedProperties")
sourceQuery = j.get("SourceQuery")
watermarkColumn = j.get("WatermarkColumn")
dataLakePath = CleansedPath.replace("/cleansed", "/mnt/datalake-cleansed")
sourceTableName = get_table_name('raw', destinationSchema, destinationTableName)
cleansedTableName = get_table_name('cleansed', destinationSchema, destinationTableName)

# COMMAND ----------

#GET LAST CLEANSED LOAD TIMESTAMP
lastLoadTimeStamp ='20000101000001'
try:
    lastLoadTimeStamp = spark.sql(f"select max(extract_datetime) as extract_datetime from {cleansedTableName}").collect()[0][0]
except Exception as e:
    print(str(e))
                  
print(lastLoadTimeStamp)

# COMMAND ----------

sourceDataFrame = spark.table(sourceTableName).where(f"extract_datetime > {lastLoadTimeStamp}")
if sourceDataFrame.count() <= 0:
    try:
        CleansedSinkCount = spark.table(cleansedTableName).count()
        print("Exiting Notebook as no records to process")
        dbutils.notebook.exit({"CleansedSinkCount": CleansedSinkCount})
    except Exception as e:
        print(str(e))
        dbutils.notebook.exit({"CleansedSinkCount": 0})
print(sourceDataFrame.count())

# FIX BAD COLUMNS
sourceDataFrame = sourceDataFrame.toDF(*(c.replace(' ', '_') for c in sourceDataFrame.columns))

# EXTENDED PROPERTIES
sourceRecordDeletion=""
if(extendedProperties):
    extendedProperties = json.loads(extendedProperties)
    sourceRecordDeletion = extendedProperties.get("SourceRecordDeletion") if extendedProperties.get("SourceRecordDeletion") else ""
    
# APPLY CLEANSED FRAMEWORK
sourceDataFrame = CleansedTransform(sourceDataFrame, sourceTableName.lower(), systemCode)
sourceDataFrame = sourceDataFrame.withColumn("_DLCleansedZoneTimeStamp",current_timestamp())

# HANDLE SAP ISU & SAP CRM DATA
rawDataFrame = sourceDataFrame
sourceDataFrame = SapCleansedPreprocess(sourceDataFrame,businessKey,sourceRecordDeletion,watermarkColumn) if sourceQuery[0:3].lower() in ('crm','isu') else sourceDataFrame

#UPSERT CLEANSED TABLE
CreateDeltaTable(sourceDataFrame, cleansedTableName, dataLakePath) if j.get("BusinessKeyColumn") is None else CreateOrMerge(sourceDataFrame, cleansedTableName, dataLakePath, j.get("BusinessKeyColumn"))
    
# HANDLE SAP ISU & SAP CRM DATA (FOR DELETED RECORDS)
if sourceRecordDeletion.lower() == "true":
    if rawDataFrame.where("di_operation_type == 'X' OR di_operation_type == 'D'").count() > 0:
        sourceDataFrame = SapCleansedPostprocess(rawDataFrame,businessKey,sourceRecordDeletion,watermarkColumn)
        CreateOrMerge(sourceDataFrame, cleansedTableName, dataLakePath, j.get("BusinessKeyColumn")) if sourceDataFrame.count() > 0 else None

# COMMAND ----------

CleansedSinkCount = spark.table(cleansedTableName).count()
dbutils.notebook.exit({"CleansedSinkCount": CleansedSinkCount})

# COMMAND ----------

# spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
# spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")
# task = dbutils.widgets.get("task")
# print(task)
# j = json.loads(task)
# systemCode = j.get("SystemCode")
# destinationSchema = j.get("DestinationSchema")
# destinationTableName = j.get("DestinationTableName")
# CleansedPath = j.get("CleansedPath")
# businessKey = j.get("BusinessKeyColumn")
# destinationKeyVaultSecret = j.get("DestinationKeyVaultSecret")
# extendedProperties = j.get("ExtendedProperties")
# sourceQuery = j.get("SourceQuery")
# watermarkColumn = j.get("WatermarkColumn")
# dataLakePath = CleansedPath.replace("/cleansed", "/mnt/datalake-cleansed")
# sourceTableName = get_table_name('raw', destinationSchema, destinationTableName)
# stgTableName = f"raw.stg_{destinationSchema}_{destinationTableName}"

# COMMAND ----------

# sourceDataFrame = spark.table(stgTableName)

# # FIX BAD COLUMNS
# sourceDataFrame = sourceDataFrame.toDF(*(c.replace(' ', '_') for c in sourceDataFrame.columns))
# sourceRecordDeletion=""
# # CLEANSED QUERY FROM RAW TO FLATTEN OBJECT
# if(extendedProperties):
#     extendedProperties = json.loads(extendedProperties)
#     sourceRecordDeletion = extendedProperties.get("SourceRecordDeletion") if extendedProperties.get("SourceRecordDeletion") else ""
#     cleansedQuery = extendedProperties.get("CleansedQuery")
#     if(cleansedQuery):
#         sourceDataFrame = spark.sql(cleansedQuery.replace("{tableFqn}", stgTableName)) 
# # APPLY CLEANSED FRAMEWORK
# sourceDataFrame = CleansedTransform(sourceDataFrame, stgTableName.lower(), systemCode) 

# # HANDLE SAP ISU & SAP CRM DATA
# rawDataFrame = sourceDataFrame
# sourceDataFrame = SapCleansedPreprocess(sourceDataFrame,businessKey,sourceRecordDeletion,watermarkColumn) if sourceQuery[0:3].lower() in ('crm','isu') else sourceDataFrame
    
# tableName = f"{destinationSchema}_{destinationTableName}"
# CreateDeltaTable(sourceDataFrame, f"cleansed.{tableName}", dataLakePath) if j.get("BusinessKeyColumn") is None else CreateOrMerge(sourceDataFrame, f"cleansed.{tableName}", dataLakePath, j.get("BusinessKeyColumn"))
    
# # HANDLE SAP ISU & SAP CRM DATA (FOR DELETED RECORDS)

# if sourceRecordDeletion.lower() == "true":
#     if rawDataFrame.where("di_operation_type == 'X' OR di_operation_type == 'D'").count() > 0:
#         sourceDataFrame = SapCleansedPostprocess(rawDataFrame,businessKey,sourceRecordDeletion,watermarkColumn)
#         CreateOrMerge(sourceDataFrame, f"cleansed.{tableName}", dataLakePath, j.get("BusinessKeyColumn")) if sourceDataFrame.count() > 0 else None
