# Databricks notebook source
# MAGIC %run ../Common/common-include-all

# COMMAND ----------

dbutils.widgets.text("task","")

# COMMAND ----------

task = dbutils.widgets.get("task")

# COMMAND ----------

j = json.loads(task)

schemaName = j.get("DestinationSchema")
tableName = j.get("DestinationTableName")
rawPath = j.get("RawPath").replace("/raw", "/mnt/datalake-raw")
rawTargetPath = j.get("RawPath")
# changed for UC migration - new function in common-helpers
# rawFolderPath = "/".join(rawPath.split("/")[0:-1])
rawFolderPath = get_raw_folder_path(rawPath)

fileFormat = ""
fileOptions = ""

if("xml" in rawTargetPath):
    extendedProperties = json.loads(j.get("ExtendedProperties"))
    rowTag = extendedProperties.get("rowTag")
    fileFormat = "xml"
    fileOptions = f"ignoreNamespace \"true\", rowTag \"{rowTag}\""
elif ("csv" in rawTargetPath):
    fileFormat = "CSV"
    fileOptions = ", header \"true\", inferSchema \"true\", multiline \"true\""
elif ("json" in rawTargetPath):
    spark.conf.set("spark.sql.caseSensitive", "true")
    fileFormat = "JSON"
    fileOptions = ", multiline \"true\", inferSchema \"true\""
else:
    fileFormat = "PARQUET"

# COMMAND ----------

tableFqn = get_table_name('raw', schemaName, tableName)
sql = f"DROP TABLE IF EXISTS {tableFqn};"
spark.sql(sql)

# COMMAND ----------

if fileFormat == 'xml':
    df = spark.read.format(fileFormat).option('rowTag',f'{rowTag}').option('ignoreNamespace','True').load(f'{rawFolderPath}')
    df.write.saveAsTable(f'{tableFqn}')
else:
    sql = f"CREATE TABLE {tableFqn} USING {fileFormat} OPTIONS (path=\"{rawFolderPath}\" {fileOptions});"
    spark.sql(sql)

# print(spark.table(f"{tableFqn}").count())

# COMMAND ----------


