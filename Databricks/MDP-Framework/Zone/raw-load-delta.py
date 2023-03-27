# Databricks notebook source
# MAGIC %run ../Common/common-include-all

# COMMAND ----------

dbutils.widgets.text("task","")
dbutils.widgets.text("rawPath","")

# COMMAND ----------

task = dbutils.widgets.get("task")
#rawPath = dbutils.widgets.get("rawPath").replace("/raw", "/mnt/datalake-raw")

# COMMAND ----------

j = json.loads(task)

rawPath = j.get("RawPath").replace("/raw", "/mnt/datalake-raw")

extendedProperties = j.get("ExtendedProperties")

schemaName = j.get("DestinationSchema")
rawTableNameMatchSource = None

if extendedProperties:
    extendedProperties = json.loads(extendedProperties)
    rawTableNameMatchSource = extendedProperties.get("RawTableNameMatchSource")

if rawTableNameMatchSource: 
    tableName = j.get("SourceTableName").lower()
else:
    tableName = j.get("DestinationTableName").lower()
    
rawTargetPath = j.get("RawPath")
systemCode = j.get("SystemCode")
rawFolderPath = "/".join(rawPath.split("/")[0:-1])

fileFormat = ""
fileOptions = ""

if("xml" in rawTargetPath):
    rowTag = extendedProperties.get("rowTag")
    fileFormat = "XML"
    fileOptions = {"ignoreNamespace":"true", "rowTag":f"{rowTag}"}
elif ("csv" in rawTargetPath):
    fileFormat = "CSV"
    fileOptions = {"header":"true", "inferSchema":"true", "multiline":"true"}
elif ("json" in rawTargetPath):
    spark.conf.set("spark.sql.caseSensitive", "true")
    fileFormat = "JSON"
    
    if systemCode[0:3].lower()=='slt':
        fileOptions = {"multiline":"false", "inferSchema":"true"}
    else:
        fileOptions = {"multiline":"true", "inferSchema":"true"}
else:
    fileFormat = "PARQUET"

# COMMAND ----------

if (fileOptions):
    df = spark.read.options(**fileOptions).format(fileFormat).load(rawPath)
else:
    df = spark.read.format(fileFormat).load(rawPath)
    
df = df.withColumn("_DLRawZoneTimeStamp",current_timestamp())
df = df.toDF(*(RemoveBadCharacters(c) for c in df.columns))

tableFqn = get_table_name('raw', schemaName, tableName).lower()
dataLakePath = "/".join(rawPath.split("/")[0:5])+"/delta"
AppendDeltaTable(df, tableFqn, dataLakePath)
