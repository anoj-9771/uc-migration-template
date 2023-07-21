# Databricks notebook source
import json as j
from pyspark.sql.types import *
import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %run ../Common/common-include-all

# COMMAND ----------

dbutils.widgets.text("task","")

# COMMAND ----------

task = dbutils.widgets.get("task")

# COMMAND ----------

j = json.loads(task)

schemaName = j.get("DestinationSchema")
tableName = j.get("DestinationTableName")
tableFqn = get_table_name('raw', schemaName, tableName)

rawPath = j.get("RawPath").replace("/raw", "/mnt/datalake-raw").split(f"{tableName}/")[0] + tableName
landingPath = j.get("SourceQuery")
fileFormat = j.get("RawPath").split(".")[1]

dataLakePath = "/".join(rawPath.split("/")[0:5])+"/delta/"

separator = ""
charset = ""


if j.get("ExtendedProperties") is not None:
    extendedProperties = json.loads(j.get("ExtendedProperties"))
    separator = extendedProperties.get("separator")
    charset = extendedProperties.get("charset")
    geometryDataExists = extendedProperties.get("geometryDataExists")
    

#default csv delimiter to comma delimmited if not explicitly stated via extended properties
if fileFormat == "csv" and not(separator):
    separator = ","
if fileFormat == "txt":
    fileFormat = "text"

if fileFormat != "json":
    if charset:
        fileOptions = {"cloudFiles.format":f"{fileFormat}", "sep":f"{separator}", "multiline":"true", "cloudFiles.schemaLocation":f"{rawPath}/schema", "cloudFiles.inferColumnTypes":"True","charset":f"{charset}"}
    else:
        fileOptions = {"cloudFiles.format":f"{fileFormat}", "sep":f"{separator}", "multiline":"true", "cloudFiles.schemaLocation":f"{rawPath}/schema", "cloudFiles.inferColumnTypes":"True"}
else:
    if geometryDataExists:
        if charset:
            fileOptions = {"cloudFiles.format":f"{fileFormat}", "sep":f"{separator}", "multiline":"true", "cloudFiles.schemaLocation":f"{rawPath}/schema", "cloudFiles.inferColumnTypes":"True","allowBackslashEscapingAnyCharacter":"True","charset":f"{charset}","cloudFiles.schemaHints":"features.element.geometry.geometries.element.coordinates ARRAY<ARRAY<DOUBLE>>"}
        else:
            fileOptions = {"cloudFiles.format":f"{fileFormat}", "sep":f"{separator}", "multiline":"true", "cloudFiles.schemaLocation":f"{rawPath}/schema", "cloudFiles.inferColumnTypes":"True","allowBackslashEscapingAnyCharacter":"True","cloudFiles.schemaHints":"features.element.geometry.geometries.element.coordinates ARRAY<ARRAY<DOUBLE>>"}        

    else:
        if charset:
            fileOptions = {"cloudFiles.format":f"{fileFormat}", "sep":f"{separator}", "multiline":"true", "cloudFiles.schemaLocation":f"{rawPath}/schema", "cloudFiles.inferColumnTypes":"True","allowBackslashEscapingAnyCharacter":"True","charset":f"{charset}"}
        else:
            fileOptions = {"cloudFiles.format":f"{fileFormat}", "sep":f"{separator}", "multiline":"true", "cloudFiles.schemaLocation":f"{rawPath}/schema", "cloudFiles.inferColumnTypes":"True","allowBackslashEscapingAnyCharacter":"True"}        

# COMMAND ----------

df = (
        spark.readStream
        .format("cloudFiles")
        .options(**fileOptions)
        .load(landingPath)
     )

df = df.toDF(*(RemoveBadCharacters(c) for c in df.columns))
df = df.withColumn('_DLRawZoneTimeStamp', F.current_timestamp()).withColumn('_InputFileName', F.input_file_name())

query = AppendDeltaTableStream(df, tableFqn)
