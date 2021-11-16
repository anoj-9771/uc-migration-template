# Databricks notebook source
# MAGIC %run ./global-variables-python

# COMMAND ----------

def SynapseGetBlobStorageStageFolder():
  BLOB_ACCESS_KEY = dbutils.secrets.get(scope = ADS_KV_ACCOUNT_SCOPE, key = "daf-sa-lake-key1")

  BLOB_CONTAINER = "synapsestaging"
  TEMPFOLDER = "abfss://" + BLOB_CONTAINER + "@" + ADS_DATA_LAKE_ACCOUNT + ".dfs.core.windows.net" + "/temp"
  

  # Set up the Blob Storage account access key in the notebook session conf.
  spark.conf.set("fs.azure.account.key." +  ADS_DATA_LAKE_ACCOUNT + ".dfs.core.windows.net" + "", BLOB_ACCESS_KEY)
  spark.conf.set("spark.sql.parquet.writeLegacyFormat", "true")
  
  sc._jsc.hadoopConfiguration().set("fs.azure.account.key." +  ADS_DATA_LAKE_ACCOUNT + ".dfs.core.windows.net" + "", BLOB_ACCESS_KEY)
  
  return TEMPFOLDER

# COMMAND ----------

def SynapseGetConnectionURL():  
  
  DB_SERVER_FULL = ADS_SYNAPSE_DB_SERVER + ".sql.azuresynapse.net"
  DB_USER_NAME = "svc_synapse1@" + ADS_SYNAPSE_DB_SERVER

  #SQL Data Warehouse related settings
  DB_PASSWORD = dbutils.secrets.get(scope = ADS_KV_ACCOUNT_SCOPE, key = ADS_KV_SYN_DB_PWD_SECRET_KEY)
  JDBC_PORT =  "1433"
  JDBC_EXTRA_OPTIONS = "encrypt=true;trustServerCertificate=true;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
  SQL_DW_URL = "jdbc:sqlserver://" + DB_SERVER_FULL + ":" + JDBC_PORT + ";database=" + ADS_SYN_DATABASE_NAME + ";user=" + DB_USER_NAME+";password=" + DB_PASSWORD + ";" + JDBC_EXTRA_OPTIONS
  SQL_DW_URL_SMALL = "jdbc:sqlserver://" + DB_SERVER_FULL + ":" + JDBC_PORT + ";database=" + ADS_SYN_DATABASE_NAME + ";user=" + DB_USER_NAME+";password=" + DB_PASSWORD  
  return SQL_DW_URL

# COMMAND ----------

# MAGIC %scala
# MAGIC Class.forName("com.databricks.spark.sqldw.DefaultSource")

# COMMAND ----------

def SynapseLoadDataToDB(dataframe, tablename, writemode):
  
  SQL_DW_URL = SynapseGetConnectionURL()
  TEMPFOLDER = SynapseGetBlobStorageStageFolder()

  print(TEMPFOLDER)
  print(SQL_DW_URL)
  
  print("Writing data to table " + tablename + " with mode " + writemode)

  dataframe.write \
  .format("com.databricks.spark.sqldw") \
  .option("url", SQL_DW_URL) \
  .option("useAzureMSI", "true") \
  .option("dbtable", tablename) \
  .option("tempdir", TEMPFOLDER) \
  .mode(writemode) \
  .save()

# COMMAND ----------

def SynapseExecuteSQLRead(query):
  
  SQL_DW_URL = SynapseGetConnectionURL()
  TEMPFOLDER = SynapseGetBlobStorageStageFolder()

  print(TEMPFOLDER)
  print(SQL_DW_URL)
  
  df = spark.read \
  .format("com.databricks.spark.sqldw") \
  .option("url", SQL_DW_URL) \
  .option("useAzureMSI", "true") \
  .option("tempdir", TEMPFOLDER) \
  .option("query",query)\
  .load()

  print("Finished : ScalaExecuteSQLQuery : Execution query: " + query)
  return df
