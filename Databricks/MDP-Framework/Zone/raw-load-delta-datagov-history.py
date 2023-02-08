# Databricks notebook source
# MAGIC %run ../Common/common-include-all

# COMMAND ----------

files = dbutils.fs.ls("/mnt/datalake-raw/dataGov/AustraliaPublicHolidays/history")
display(files)

# COMMAND ----------

schemaName = "datagov"
tableName = "AustraliaPublicHolidays"
tableFqn = f"raw.{schemaName}_{tableName}"

systemCode = "datagov"
dataLakePath = "/mnt/datalake-raw/dataGov/AustraliaPublicHolidays/delta"
rawFolderPath = "/mnt/datalake-raw/dataGov/AustraliaPublicHolidays/history"

rawPath = "/mnt/datalake-raw/dataGov/AustraliaPublicHolidays/history"

fileFormat = "CSV"
fileOptions = {"header":"true", "inferSchema":"true", "multiline":"true"}

for f in files:
    if "csv" in f.name:
        print(f.path)
        rawPath = f.path.replace("dbfs:","")
        df = spark.read.options(**fileOptions).format(fileFormat).load(rawPath)

        df = df.withColumn("_DLRawZoneTimeStamp",current_timestamp())
        df = df.toDF(*(RemoveBadCharacters(c) for c in df.columns))

        AppendDeltaTable(df, tableFqn, dataLakePath)
