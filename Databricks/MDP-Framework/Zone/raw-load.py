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
rawFolderPath = "/".join(rawPath.split("/")[0:-1])

fileFormat = ""
fileOptions = ""

if("xml" in rawTargetPath):
    extendedProperties = json.loads(j.get("ExtendedProperties"))
    rowTag = extendedProperties.get("rowTag")
    fileFormat = "XML"
    fileOptions = f", ignoreNamespace \"true\", rowTag \"{rowTag}\""
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
sql = f"CREATE TABLE {tableFqn} USING {fileFormat} OPTIONS (path \"{rawFolderPath}\" {fileOptions});"
print(sql)
spark.sql(sql)

print(spark.table(f"{tableFqn}").count())
