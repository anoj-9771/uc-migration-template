# Databricks notebook source
def TableExists(tableFqn):
  return spark._jsparkSession.catalog().tableExists(tableFqn.split(".")[0], tableFqn.split(".")[1])

# COMMAND ----------

def GetDeltaTablePath(tableFqn):
  df = spark.sql(f"DESCRIBE TABLE EXTENDED {tableFqn}").where("col_name = 'Location'")
  return df.collect()[0].data_type

# COMMAND ----------

def CreateDeltaTable(dataFrame, targetTableFqn, dataLakePath, businessKeys = None, createTableConstraints=True):
    dataFrame.write \
             .format("delta") \
             .option("mergeSchema", "true") \
             .mode("overwrite") \
             .save(dataLakePath)
    CreateDeltaTableConstraints(targetTableFqn, dataLakePath, businessKeys, createTableConstraints)

# COMMAND ----------

def CreateDeltaTableR1W4(dataFrame, targetTableFqn, dataLakePath, businessKeys = None, createTableConstraints=True):
    dataFrame.write \
             .format("delta") \
             .option("delta.minReaderVersion", "1") \
             .option("delta.minWriterVersion", "4") \
             .option("mergeSchema", "true") \
             .mode("overwrite") \
             .save(dataLakePath)
    CreateDeltaTableConstraints(targetTableFqn, dataLakePath, businessKeys, createTableConstraints)

# COMMAND ----------

#A streaming variant of AppendDeltaTable function. The other difference is that the table creation has been decoupled. We should look at saving the dataframe to table directly.
def AppendDeltaTableStream(dataFrame, targetTableFqn, dataLakePath, businessKeys = None, triggerType = "once"):
    if triggerType == "once":
        kwArgs = {"once":True}
    elif triggerType == "continuous" or triggerType == "processingTime":
        kwArgs = {f"{triggerType}":"5 seconds"}
    else:
        print("Trigger type must be one of: 'once', 'continuous', 'processingTime'.")
        return
    query = (
        dataFrame
        .writeStream
        .format("delta")
        .outputMode("append")
        .option("mergeSchema", "true")
        .option("checkpointLocation", f"{rawPath}/checkpoints")
        .option("path", dataLakePath)
        .queryName(targetTableFqn.split(".")[1] + "_stream")
        .trigger(**kwArgs)
        .table(targetTableFqn)
    )
    #Unable to use "CreateDeltaTableConstraints" functio as above code automatically creates the raw table. Below code checks if the business keys columns are nullable on the table. If so they're set to not null
    if (businessKeys):
        columnSchema = StructType([
            StructField('name', StringType(), True)
            ,StructField('description', StringType(), True)
            ,StructField('dataType', StringType(), True)
            ,StructField('nullable', StringType(), True)
            ,StructField('isPartition', StringType(), True)
            ,StructField('isBucket', StringType(), True)
        ])
        columns = spark.catalog.listColumns(targetTableFqn.split(".")[1], targetTableFqn.split(".")[0])
        df = spark.createDataFrame(columns, columnSchema)
        for businessKey in businessKeys.split(","):
            if df.where(f"name = '{businessKey}' and nullable = 'true'").count() > 0:
                spark.sql(f"ALTER TABLE {targetTableFqn} ALTER COLUMN {businessKey} SET NOT NULL")
                
    return query

# COMMAND ----------

def AppendDeltaTable(dataFrame, targetTableFqn, dataLakePath, businessKeys = None):
    dataFrame.write \
             .format("delta") \
             .option("mergeSchema", "true") \
             .mode("append") \
             .save(dataLakePath)
    if (not(TableExists(targetTableFqn))):
        CreateDeltaTableConstraints(targetTableFqn, dataLakePath, businessKeys)

# COMMAND ----------

def delete_files_recursive(file_path: str) -> None:
    """delete the given folder including all it's contents"""
    for file_folder in dbutils.fs.ls(file_path):
        if len(dbutils.fs.ls(file_folder.path[5:])) == 1:
            dbutils.fs.rm(f'{file_folder.path[5:]}', True)
        else:
            delete_files_recursive(file_folder.path[5:])
    dbutils.fs.rm(file_path, True)

# COMMAND ----------

def CreateDeltaTableConstraints(targetTableFqn, dataLakePath, businessKeys = None, createTableConstraints = True):
    spark.sql(f"CREATE TABLE IF NOT EXISTS {targetTableFqn} USING DELTA LOCATION \'{dataLakePath}\'")
    if businessKeys is not None and createTableConstraints:
        for businessKey in businessKeys.split(","):
            spark.sql(f"ALTER TABLE {targetTableFqn} ALTER COLUMN {businessKey} SET NOT NULL")

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
        detail = spark.sql(f"DESCRIBE DETAIL {tableNameFqn}").collect()[0]
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
    [text := text.replace(c, replacement) for c in "/%� ,;{}()?\n\t=-"]
    return text
