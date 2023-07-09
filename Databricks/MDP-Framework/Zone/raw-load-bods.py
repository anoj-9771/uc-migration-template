# Databricks notebook source
# MAGIC %run ../Common/common-include-all

# COMMAND ----------

task = dbutils.widgets.get("task")
j = json.loads(task)
rawFolderPath = j.get("RawPath").replace("/raw", "/mnt/datalake-raw")  
schemaName = j.get("DestinationSchema")
tableName = j.get("DestinationTableName")
rawTargetPath = j.get("RawPath")
watermarkColumn = j.get("WatermarkColumn")
sourceQuery = j.get("SourceQuery")
rawManifestPath = rawFolderPath.replace(tableName, f"{tableName}_MANIFEST")

# COMMAND ----------

# FILE DOESN'T EXIST
try:
    dbutils.fs.ls(rawFolderPath)
except:
    dbutils.notebook.exit({"SinkRowCount": 0, "Warning" : "File doesn't exist!"})

# MANIFEST DOESN'T EXIST
try:
    dbutils.fs.ls(rawManifestPath)
except:
    dbutils.notebook.exit({"SinkRowCount": 0, "Warning" : "Manifest doesn't exist!"})

# READ MANIFEST, IF DELTA_RECORD_COUNT=0
manifestRow = spark.read.format("JSON").load(rawManifestPath)
if manifestRow.count() > 0:
    dbutils.notebook.exit({"SinkRowCount": 0, "Warning" : "No rows!"})
if manifestRow.collect()[0].DELTA_RECORD_COUNT == 0:
    dbutils.notebook.exit({"SinkRowCount": 0, "Warning" : "Delta count is 0!"})

# COMMAND ----------

df = (spark.read
    .format("JSON")
    .option("inferSchema", True)
    .option("allowUnquotedFieldNames", True)
    .option("allowSingleQuotes", True)
    .option("allowBackslashEscapingAnyCharacter", True)
    .option("allowUnquotedControlChars", True)
    .option("recursiveFileLookup", True)
)
try:
    raise Exception("")
    df = (df.option("mode", "FAILFAST")
            .load(rawFolderPath))
except Exception:
    print("Problem - trying multiline = true")
    df = (df.option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", "corrupt_record")
            .load(rawFolderPath))

# COMMAND ----------

# DBTITLE 1,Append/Overwrite Delta Table
df=df.withColumn("_DLRawZoneTimeStamp",current_timestamp())
tableFqn = get_table_name('raw', schemaName, tableName)
dataLakePath = "/".join(rawPath.split("/")[0:5])+"/delta"
AppendDeltaTable(df, tableFqn, dataLakePath)
SinkRowCount = spark.table(tableFqn).count()
dbutils.notebook.exit({"SinkRowCount": SinkRowCount})
