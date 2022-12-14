# Databricks notebook source
def TableExists(tableFqn):
  return spark._jsparkSession.catalog().tableExists(tableFqn.split(".")[0], tableFqn.split(".")[1])

# COMMAND ----------

def GetDeltaTablePath(tableFqn):
  df = spark.sql(f"DESCRIBE TABLE EXTENDED {tableFqn}").where("col_name = 'Location'")
  return df.rdd.collect()[0].data_type

# COMMAND ----------

def CreateDeltaTable(dataFrame, targetTableFqn, dataLakePath):
  dataLakePath = dataLakePath.lower()
  dataFrame.write \
    .format("delta") \
    .option("mergeSchema", "true") \
    .mode("overwrite") \
    .save(dataLakePath)
  spark.sql(f"CREATE TABLE IF NOT EXISTS {targetTableFqn} USING DELTA LOCATION \'{dataLakePath}\'")

# COMMAND ----------

def AppendDeltaTable(dataFrame, targetTableFqn, dataLakePath):
  dataLakePath = dataLakePath.lower()
  dataFrame.write \
    .format("delta") \
    .option("mergeSchema", "true") \
    .mode("append") \
    .save(dataLakePath)
  if (not(TableExists(targetTableFqn))):
    spark.sql(f"CREATE TABLE IF NOT EXISTS {targetTableFqn} USING DELTA LOCATION \'{dataLakePath}\'")

# COMMAND ----------

def AliasDataFrameColumns(dataFrame, prefix):
  return dataFrame.select(*(col(x).alias(prefix + x) for x in dataFrame.columns))

# COMMAND ----------

def DeleteDirectoryRecursive(dirname):
    files=dbutils.fs.ls(dirname)
    for f in files:
        if f.isDir():
            DeleteDirectoryRecursive(f.path)
        dbutils.fs.rm(f.path, recurse=True)
    dbutils.fs.rm(dirname, True)

# COMMAND ----------

def CleanTable(tableNameFqn):
    try:
        detail = spark.sql(f"DESCRIBE DETAIL {tableNameFqn}").rdd.collect()[0]
        DeleteDirectoryRecursive(detail.location)
    except:    
        pass
    
    try:
        spark.sql(f"DROP TABLE {tableNameFqn}")
    except:
        pass


# COMMAND ----------

def TableExists(tableFqn):
  return spark._jsparkSession.catalog().tableExists(tableFqn.split(".")[0], tableFqn.split(".")[1])

# COMMAND ----------

def GetTableRowCount(tableFqn):
  return spark.table(tableFqn).count() if TableExists(tableFqn) else 0

# COMMAND ----------

def RemoveBadCharacters(text, replacement=""):
    [text := text.replace(c, replacement) for c in "/%� ,;{}()\n\t=-"]
    return text
