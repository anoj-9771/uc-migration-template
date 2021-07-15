# Databricks notebook source
import pathlib
import base64
import hashlib

# COMMAND ----------

def DatalakeWriteFile(df,container, folder, file, fileFormat, writeMode, colSeparator):
  # ##########################################################################################################################  
  # Function: DatalakeWriteFile
  # Writes the input dataframe to a file into Azure Datalake
  # 
  # Parameters:
  # df= input dataframe
  # container = File System/Container of Azure Data Lake Storage
  # folder = folder name
  # file = file name including extension
  # fileFormat = File extension. Supported formats are csv/txt/parquet/orc/json  
  # writeMode= mode of writing the curated file. Allowed values - append/overwrite/ignore/error/errorifexists
  # colSeparator = Column separator for text files
  # 
  # Returns:
  # A dataframe of the raw file
  # ########################################################################################################################## 
  containerMount = DataLakeGetMountPoint(container)
  filePath = "{dataLakeMount}/{file}".format(dataLakeMount=containerMount, file=folder+"/"+file)
  if "csv" in fileFormat:
    df.write.csv(filePath,mode=writeMode,sep=colSeparator,header="true", nullValue="0", timestampFormat ="yyyy-MM-dd HH:mm:ss")
  if "txt" in fileFormat:
    path =  "/dbfs{dataLakeMount}/{file}".format(dataLakeMount=containerMount, file=folder)
    filePath =  "/dbfs{dataLakeMount}/{file}".format(dataLakeMount=containerMount, file=folder+"/"+file)
    pathlib.Path(path).mkdir(parents=True, exist_ok=True)
    if colSeparator is not None:
      df.toPandas().to_csv(filePath, header=False, index=False, sep=colSeparator)
    elif colSeparator is None:
      df.toPandas().to_csv(filePath, header=False, index=False)
  elif "parquet" in fileFormat:
    df.write.parquet(filePath,mode=writeMode)
  elif "orc" in fileFormat:
    df.write.orc(filePath,mode=writeMode)
  elif "json" in fileFormat:
    df.write.json(filePath, mode=writeMode)
  else:
    df.write.save(path=filePath,format=fileFormat,mode=writeMode)
  return

# COMMAND ----------

def DataLakeWriteDeltaFqn(dataFrame, tableNameFqn, dataLakePath):
  # WRITE TO DATA LAKE
  dataFrame.write \
  .format('delta') \
  .option('mergeSchema', 'true') \
  .mode("append") \
  .save(dataLakePath)
  
  # CREATE DELTA TABLE
  query = "CREATE TABLE IF NOT EXISTS {tableFqn} USING DELTA LOCATION \'{dataLakePath}\'".format(tableFqn=tableNameFqn, dataLakePath=dataLakePath)
  spark.sql(query)

# COMMAND ----------

def DataLakeFileExists(path):
  try:
    dbutils.fs.ls(path)
    return True
  except Exception as e:
    if 'java.io.FileNotFoundException' in str(e):
      return False
    else:
      raise

# COMMAND ----------

def DataLakeFileHash(path):
  #HASH FILE
  df = spark.read.format("binaryFile").load(path)
  content = base64.b64encode(df.rdd.collect()[0].content)
  h = hashlib.sha256()
  h.update(content)
  return h.hexdigest()
