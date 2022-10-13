# Databricks notebook source
#Define Widgets/Parameters
dbutils.widgets.text("task", "", "Task")
dbutils.widgets.text("rawPath", "", "rawPath")

# COMMAND ----------

# MAGIC %run ../Common/common-include-all

# COMMAND ----------

task = dbutils.widgets.get("task")
rawPath = dbutils.widgets.get("rawPath").replace("/raw", "/mnt/datalake-raw")

# COMMAND ----------

j = json.loads(task)
 
schemaName = j.get("DestinationSchema")
tableName = j.get("DestinationTableName")
rawTargetPath = j.get("RawPath")
watermarkColumn = j.get("WatermarkColumn")
sourceQuery = j.get("SourceQuery")
rawFolderPath = rawPath
 
if ("json" in rawTargetPath):
    fileFormat = "JSON"
    fileOptions = ", multiline \"true\",recursiveFileLookup \"true\", inferSchema \"true\", allowUnquotedFieldNames \"true\", allowSingleQuotes \"true\", allowBackslashEscapingAnyCharacter \"true\", allowUnquotedControlChars \"true\", columnNameOfCorruptRecord \"true\""
else:
    fileFormat = "PARQUET"

# COMMAND ----------

try:
    df = spark.read\
        .format(fileFormat) \
        .option("inferSchema","true")\
        .option("allowUnquotedFieldNames","true")\
        .option("allowSingleQuotes","true")\
        .option("allowBackslashEscapingAnyCharacter","true")\
        .option("allowUnquotedControlChars","true")\
        .option("recursiveFileLookup",True)\
        .option("mode","FAILFAST")\
        .load(rawFolderPath)
except Exception:
    print('Problem - trying multiline = true')
    df = spark.read\
        .format(fileFormat) \
        .option("multiline", "true")\
        .option("inferSchema","true")\
        .option("allowUnquotedFieldNames","true")\
        .option("allowSingleQuotes","true")\
        .option("allowBackslashEscapingAnyCharacter","true")\
        .option("allowUnquotedControlChars","true")\
        .option("recursiveFileLookup",True)\
        .option("mode","PERMISSIVE")\
        .option("columnNameOfCorruptRecord","corrupt_record")\
        .load(rawFolderPath)
finally:
    current_record_count = df.count()
    print("Records read : " + str(current_record_count))

# COMMAND ----------

# DBTITLE 1,Exit Notebook If No Records
if current_record_count == 0 or len(df.columns) <= 1:
    print("Exiting Notebook as no records to process")
    dbutils.notebook.exit({"SinkRowCount": 0})

# COMMAND ----------

# DBTITLE 1,Append/Overwrite Delta Table
df=df.withColumn("_DLRawZoneTimeStamp",current_timestamp())
tableFqn = f"raw.{schemaName}_{tableName}"
dataLakePath = "/".join(rawPath.split("/")[0:5])+"/delta"
if watermarkColumn:
    AppendDeltaTable(df, tableFqn, dataLakePath)
else:
    CreateDeltaTable(df, tableFqn, dataLakePath)

# COMMAND ----------

SinkRowCount = spark.table(tableFqn).count()
dbutils.notebook.exit({"SinkRowCount": SinkRowCount})
