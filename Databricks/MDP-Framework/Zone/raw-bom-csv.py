# Databricks notebook source
# MAGIC %run ../Common/common-include-all

# COMMAND ----------

task = dbutils.widgets.get("task")

# COMMAND ----------

j = json.loads(task)
rawPath = j.get("RawPath").replace("/raw", "/mnt/datalake-raw")
schemaName = j.get("DestinationSchema")
tableName = j.get("DestinationTableName")
tableFqn = f"raw.{schemaName}_{tableName}"

# COMMAND ----------

def RemoveBadCharacters(text, replacement):
    [text := text.replace(c, replacement) for c in "/%� ,;{}()\n\t="]
    return text

# COMMAND ----------

import csv
from pyspark.sql.types import StringType

sql = f"DROP TABLE IF EXISTS {tableFqn};"
spark.sql(sql)

startAtColumn = 2
df = (sc.textFile(rawPath)
          .mapPartitions(lambda line: csv.reader(line)).filter(lambda line: len(line)>=startAtColumn)
          .toDF()
     )
firstColumnName = df.columns[0]
df = df.drop(df.columns[0])
header = df.rdd.collect()[0]

filteredData = df.where(f"_{startAtColumn} != '{df.rdd.collect()[0][0]}'").drop(firstColumnName)

df = spark.createDataFrame(filteredData.rdd, schema=StructType([StructField(RemoveBadCharacters(h, "_"), StringType(), True) for h in header]))
#display(df)

df.write.saveAsTable(tableFqn)
display(spark.table(tableFqn))

# COMMAND ----------


