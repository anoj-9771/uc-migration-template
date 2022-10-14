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
sourceQuery = 'slt' if j.get("SourceQuery") is None else j.get("SourceQuery") 
watermarkColumn = j.get("WatermarkColumn")
dataLakePath = CleansedPath.replace("/cleansed", "/mnt/datalake-cleansed")
sourceTableName = f"raw.{destinationSchema}_{destinationTableName}"
cleansedTableName = f"cleansed.{destinationSchema}_{destinationTableName}"

# COMMAND ----------

#GET LAST CLEANSED LOAD TIMESTAMP
try:
    lastLoadTimeStamp = spark.sql(f"select max(_DLCleansedZoneTimeStamp) as extract_datetime from {cleansedTableName}").collect()[0][0]
except Exception as e:
    if "Table or view not found" in str(e):
            lastLoadTimeStamp ='2000-01-01T01:00:00.000'      
print(lastLoadTimeStamp)

# COMMAND ----------

sourceDataFrame = spark.table(sourceTableName).where(f"_DLRawZoneTimeStamp > '{lastLoadTimeStamp}'")
if sourceDataFrame.count() <= 0:
    try:
        CleansedSinkCount = spark.table(cleansedTableName).count()
        print("Exiting Notebook as no records to process")
        dbutils.notebook.exit({"CleansedSinkCount": CleansedSinkCount})
    except Exception as e:
        if "Table or view not found" in str(e):
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

# HANDLE SAP ISU, SAP CRM & SAP SLT DATA
rawDataFrame = sourceDataFrame
sourceDataFrame = SapPreprocessCleansed(sourceDataFrame,businessKey,sourceRecordDeletion,sourceQuery,watermarkColumn) if sourceQuery[0:3].lower() in ('crm','isu','slt') else sourceDataFrame

#UPSERT CLEANSED TABLE
CreateDeltaTable(sourceDataFrame, cleansedTableName, dataLakePath) if j.get("BusinessKeyColumn") is None else CreateOrMerge(sourceDataFrame, cleansedTableName, dataLakePath, j.get("BusinessKeyColumn"))
    
# HANDLE SAP ISU, SAP CRM & SAP SLT (FOR DELETED RECORDS)
if sourceRecordDeletion.lower() == "true":
    whereClause = "di_operation_type == 'X' OR di_operation_type == 'D'" if sourceQuery[0:3].lower() in ('crm','isu') else "is_deleted == 'Y'"
    if rawDataFrame.where(whereClause).count() > 0:
        sourceDataFrame = SapPostprocessCleansed(rawDataFrame,businessKey,sourceRecordDeletion,sourceQuery,watermarkColumn)
        CreateOrMerge(sourceDataFrame, cleansedTableName, dataLakePath, j.get("BusinessKeyColumn")) if sourceDataFrame.count() > 0 else None

# COMMAND ----------

CleansedSinkCount = spark.table(cleansedTableName).count()
dbutils.notebook.exit({"CleansedSinkCount": CleansedSinkCount})
