# Databricks notebook source
def is_uc():
    """check if the current databricks environemnt is Unity Catalog enabled"""
    try:
        dbutils.secrets.get('ADS', 'databricks-env')
        return True
    except Exception as e:
        return False

# COMMAND ----------


def TableExists(tableFqn):
    return (
        spark.sql(f"show tables in {'.'.join(tableFqn.split('.')[:-1])} like '{tableFqn.split('.')[-1]}'").count() == 1
    )

# COMMAND ----------

def GetDeltaTablePath(tableFqn):
  df = spark.sql(f"DESCRIBE TABLE EXTENDED {tableFqn}").where("col_name = 'Location'")
  return df.collect()[0].data_type

# COMMAND ----------

    
def CreateDeltaTable(dataFrame, targetTableFqn, dataLakePath, businessKeys = None, createTableConstraints=True):
    #with introduction of Unity Catalog, function needs to be able to create new schema
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {targetTableFqn.split('.')[0]}.{targetTableFqn.split('.')[1]}")
    
    (dataFrame.write 
        .option("mergeSchema", "true") 
        .option("delta.minReaderVersion", "3") 
        .option("delta.minWriterVersion", "7") 
        .mode("overwrite") 
        .saveAsTable(targetTableFqn))
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
        .queryName(targetTableFqn.split(".")[1] + "_stream")
        .trigger(**kwArgs)
        .table(targetTableFqn)
    )
    #Unable to use "CreateDeltaTableConstraints" function as above code automatically creates the raw table. Below code checks if the business keys columns are nullable on the table. If so they're set to not null
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
    #with introduction of Unity Catalog, function needs to be able to create new schema
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {targetTableFqn.split('.')[0]}.{targetTableFqn.split('.')[1]}")

    #although below code block is to "append", in case of no table existing, a new one will be created
    dataFrame.write \
            .option("mergeSchema", "true") \
            .mode("append") \
            .saveAsTable(targetTableFqn)
    
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

def GetTableRowCount(tableFqn):
  return spark.table(tableFqn).count() if TableExists(tableFqn) else 0

# COMMAND ----------

def RemoveBadCharacters(text, replacement=""):
    [text := text.replace(c, replacement) for c in "/%� ,;{}()?\n\t=-"]
    return text

# COMMAND ----------

import json
def getClusterEnv():
    clustTags = json.loads(spark.conf.get("spark.databricks.clusterUsageTags.clusterAllTags"))
    env = [x['value'] for x in clustTags if x['key'] == 'Environment'][0]
    return env
