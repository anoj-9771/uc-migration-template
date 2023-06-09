# Databricks notebook source
import json
from datetime import datetime

# COMMAND ----------

# MAGIC %run ../includes/include-all-util

# COMMAND ----------

dbutils.widgets.text("taskDetails", "")
dbutils.widgets.text("rawPath", "")
dbutils.widgets.text("extractDateTime", "")

# COMMAND ----------

taskDetails = dbutils.widgets.get("taskDetails")
rawPath     = dbutils.widgets.get("rawPath")
extractDateTime = dbutils.widgets.get("extractDateTime")
print(extractDateTime)

# COMMAND ----------

def RemoveBadCharacters(text, replacement=""):
    [text := text.replace(c, replacement) for c in "/%� ,;{}()?\n\t=-"]
    return text

# COMMAND ----------

j = json.loads(taskDetails)

fileFormat      = "PARQUET"
rawPath         = rawPath.replace("raw/", "/mnt/datalake-raw/")
targetTable     = j.get("TargetName")
targetTableFqn  = f"{ADS_DATABASE_RAW}.{targetTable}"
extractDateTime = datetime.strptime(extractDateTime, '%Y%m%d%H%M%S')

print('rawTargetPath    : ', rawPath)
print('targetTableFqn   : ', targetTableFqn)
print('taskDetails      : ', j)
print('extractDateTime  : ', extractDateTime)

# COMMAND ----------


df = spark.read.format(fileFormat).load(rawPath)
df = df.toDF(*(RemoveBadCharacters(c) for c in df.columns))
df = df.withColumn("_DLRawZoneTimeStamp", df._DLRawZoneTimeStamp.cast('timestamp'))
df.write \
            .format("delta") \
            .option("mergeSchema", "true") \
            .mode("append") \
            .saveAsTable(targetTableFqn)
if (not(TableExists(targetTableFqn))):
    spark.sql(f"CREATE TABLE IF NOT EXISTS {targetTableFqn}") 


# COMMAND ----------

output = {"TargetTableRecordCount": -1} 

if TableExists(targetTableFqn): 
  sql_query = "SELECT COUNT(*) FROM {0} WHERE _DLRawZoneTimeStamp = '{1}'".format(targetTableFqn, extractDateTime)
  df_dl = spark.sql(sql_query)
  output["TargetTableRecordCount"] = df_dl.collect()[0][0]
else:
  output["TargetTableRecordCount"] = -1

print(output)

# COMMAND ----------

dbutils.notebook.exit(json.dumps(output))
