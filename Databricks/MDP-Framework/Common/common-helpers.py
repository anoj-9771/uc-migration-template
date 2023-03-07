# Databricks notebook source
import json
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

def ExpandTable(df, includeParentNames = False, sep = "_", excludeColumns = ""):
    newDf = df
    for i in df.dtypes:
        columnName = i[0]
        
        #skip explosion if column name in comma delimitted exclude column list
        if columnName in excludeColumns.split(","):
            continue
        
        #if the column is a structure data type loop through and explode appending the parent column name to the root object
        if i[1].startswith("struct"):
            newDf = newDf.selectExpr("*", f"`{columnName}`.*")
            if includeParentNames:
                for c in newDf.selectExpr(f"`{columnName}`.*").columns:
                    newDf = newDf.withColumnRenamed(c, f"{columnName}{sep}{c}".replace("__", "_"))
            newDf = newDf.drop(columnName)
            return ExpandTable(newDf, includeParentNames, sep, excludeColumns)
        
        if i[1].startswith("array") and "struct" in i[1]:
            explodedDf = newDf.withColumn(f"{columnName}", expr(f"explode(`{columnName}`)"))
            newDf = explodedDf.selectExpr("*", f"`{columnName}`.*")
            for c in explodedDf.selectExpr(f"`{columnName}`.*").columns:
                newDf = newDf.withColumnRenamed(c, f"{columnName}{sep}{c}".replace("__", "_"))
            newDf = newDf.drop(columnName, columnName)
            return ExpandTable(newDf, includeParentNames, sep, excludeColumns)
    return newDf

# COMMAND ----------

def LoadJsonFile(path):
    f = open(path)
    data = json.load(f)
    f.close()
    return data

# COMMAND ----------

def DataFrameFromFilePath(path):
    fsSchema = StructType([
        StructField('path', StringType()),
        StructField('name', StringType()),
        StructField('size', LongType()),
        StructField('modificationTime', LongType())
    ])
    list = dbutils.fs.ls(path)
    df = spark.createDataFrame(list, fsSchema).withColumn("modificationTime", expr("from_unixtime(modificationTime / 1000)"))
    return df
