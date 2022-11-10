# Databricks notebook source
# MAGIC %run ../Common/common-include-all

# COMMAND ----------

dbutils.widgets.text("task","")
dbutils.widgets.text("rawPath","")

# COMMAND ----------

task = dbutils.widgets.get("task")
rawPath = dbutils.widgets.get("rawPath").replace("/raw", "/mnt/datalake-raw")

# COMMAND ----------

j = json.loads(task)

schemaName = j.get("DestinationSchema")
tableName = j.get("DestinationTableName")
rawTargetPath = j.get("RawPath")
rawFolderPath = "/".join(rawPath.split("/")[0:-1])

fileFormat = ""
fileOptions = ""

if("xml" in rawTargetPath):
    extendedProperties = json.loads(j.get("ExtendedProperties"))
    rowTag = extendedProperties.get("rowTag")
    fileFormat = "XML"
    fileOptions = {"ignoreNamespace":"true", "rowTag":f"{rowTag}"}
elif ("csv" in rawTargetPath):
    fileFormat = "CSV"
    fileOptions = {"header":"true", "inferSchema":"true", "multiline":"true"}
elif ("json" in rawTargetPath):
    spark.conf.set("spark.sql.caseSensitive", "true")
    fileFormat = "JSON"
    fileOptions = {"multiline":"true", "inferSchema":"true"}
else:
    fileFormat = "PARQUET"

# COMMAND ----------

if (fileOptions):
    df = spark.read.options(**fileOptions).format(fileFormat).load(rawPath)
else:
    df = spark.read.format(fileFormat).load(rawPath)
    
df = df.withColumn("_DLRawZoneTimeStamp",current_timestamp())
tableFqn = f"raw.{schemaName}_{tableName}"
dataLakePath = "/".join(rawPath.split("/")[0:5])+"/delta"
AppendDeltaTable(df, tableFqn, dataLakePath)