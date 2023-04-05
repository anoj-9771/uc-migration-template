# Databricks notebook source
import json
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

def ExpandTable(df, includeParentNames = False, sep = "_", excludeColumns = "", originalDataFrame = True):
    newDf = df
    replacementToken = "$original$"
    
    #rename original columns that are not structure types to avoid column renaming where the same column name exists at the top level of a json object and within a sub json column 
    if originalDataFrame:
        for i in df.dtypes:
            columnName = i[0]
            if not "struct" in i[1]:
                newDf = newDf.withColumnRenamed(columnName, f"{columnName}{replacementToken}")
    
    for i in df.dtypes:
        columnName = i[0]
        
        #skip explosion if column name in comma delimitted exclude column list
        if columnName.lower() in excludeColumns.lower().split(","):
            continue
        
        #if the column is a structure data type loop through and explode appending the parent column name to the root object
        if i[1].startswith("struct"):
            newDf = newDf.selectExpr("*", f"`{columnName}`.*")
            if includeParentNames:
                for c in newDf.selectExpr(f"`{columnName}`.*").columns:
                    newDf = newDf.withColumnRenamed(c, f"{columnName}{sep}{c}".replace("__", "_"))
            newDf = newDf.drop(columnName)
            return ExpandTable(newDf, includeParentNames, sep, excludeColumns, False)
        
        if i[1].startswith("array") and "struct" in i[1]:
            explodedDf = newDf.withColumn(f"{columnName}", expr(f"explode(`{columnName}`)"))
            newDf = explodedDf.selectExpr("*", f"`{columnName}`.*")
            for c in explodedDf.selectExpr(f"`{columnName}`.*").columns:
                newDf = newDf.withColumnRenamed(c, f"{columnName}{sep}{c}".replace("__", "_"))
            newDf = newDf.drop(columnName, columnName)
            return ExpandTable(newDf, includeParentNames, sep, excludeColumns, False)
    
    #fix the colum renaming after initial explosion has happened
    for i in newDf.dtypes:
        columnName = i[0]
        if replacementToken in columnName:
            newDf = newDf.withColumnRenamed(columnName, columnName.replace(replacementToken,""))
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


# COMMAND ----------



def get_table_name(layer:str, j_schema: str, j_table: str) -> str:
    """gets correct table namespace based on the UC migration/databricks-env secret being available in keyvault."""
    try:
        env = dbutils.secrets.get('ADS', 'databricks-env')
        return f"{env}{layer}.{j_schema}.{j_table}"
    except Exception as e:
        return f"{layer}.{j_schema}_{j_table}"

# COMMAND ----------

def ConvertBlankRecordsToNull(df):
    for i in df.dtypes:
        if "struct" not in i[1]:
            df = df.withColumn(i[0],when(col(i[0])=="" ,None).otherwise(col(i[0])))
    return df
