# Databricks notebook source
# MAGIC %run ./global-variables-python

# COMMAND ----------

def SynapseGetBlobStorageStageFolder():
  BLOB_ACCESS_KEY = dbutils.secrets.get(scope = ADS_KV_ACCOUNT_SCOPE, key = "blobstorage-key")

  BLOB_CONTAINER = "stage"
  TEMPFOLDER = "wasbs://" + BLOB_CONTAINER + "@" + ADS_BLOB_STORAGE_ACCOUNT + "/temp"

  # Set up the Blob Storage account access key in the notebook session conf.
  spark.conf.set("fs.azure.account.key." + ADS_BLOB_STORAGE_ACCOUNT + "", BLOB_ACCESS_KEY)
  spark.conf.set("spark.sql.parquet.writeLegacyFormat", "true")
  
  return TEMPFOLDER

# COMMAND ----------

def SynapseGetConnectionURL():
  
  DB_SERVER_FULL = ADS_DB_SERVER + ".database.windows.net"
  DB_USER_NAME = "sqladmin@" + ADS_DB_SERVER

  #SQL Data Warehouse related settings
  DB_PASSWORD = dbutils.secrets.get(scope = ADS_KV_ACCOUNT_SCOPE, key = ADS_KV_DB_PWD_SECRET_KEY)
  JDBC_PORT =  "1433"
  JDBC_EXTRA_OPTIONS = "encrypt=true;trustServerCertificate=true;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
  SQL_DW_URL = "jdbc:sqlserver://" + DB_SERVER_FULL + ":" + JDBC_PORT + ";database=" + ADS_DATABASE_NAME + ";user=" + DB_USER_NAME+";password=" + DB_PASSWORD + ";" + JDBC_EXTRA_OPTIONS
  SQL_DW_URL_SMALL = "jdbc:sqlserver://" + DB_SERVER_FULL + ":" + JDBC_PORT + ";database=" + ADS_DATABASE_NAME + ";user=" + DB_USER_NAME+";password=" + DB_PASSWORD
  
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

  df.write \
  .format("com.databricks.spark.sqldw") \
  .option("url", SQL_DW_URL) \
  .option("forward_spark_azure_storage_credentials", "true") \
  .option("dbtable", tablename) \
  .option("tempdir", TEMPFOLDER) \
  .mode(writemode) \
  .save()