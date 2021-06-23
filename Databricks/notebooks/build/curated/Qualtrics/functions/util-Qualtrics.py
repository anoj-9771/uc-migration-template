# Databricks notebook source
# MAGIC %run ../../../includes/include-all-util

# COMMAND ----------

QUALTRICS_DATABASE_NAME = "qualtrics"
QUALTRICS_DATALAKE_FOLDER = "qualtrics"
QUALTRICS_AZURE_SCHEMA = "tsQualtrics"

# COMMAND ----------

# DBTITLE 1,Function to Save DataFrame to Curated and Azure SQL
def QualtricsSaveDataFrameToCurated(df, table_name, save_to_azure = True):


  print("Saving to Delta Table")
  #Save data frame to Databse (Curated Zone)
  DeltaSaveDataFrameToCurated(df, QUALTRICS_DATABASE_NAME, QUALTRICS_DATALAKE_FOLDER, table_name, ADS_WRITE_MODE_OVERWRITE, QUALTRICS_AZURE_SCHEMA, save_to_azure)


# COMMAND ----------

def QualtricsSaveDataFrame(df, table_name, timestamp):
  data_lake_mount_point = DataLakeGetMountPoint(ADS_DATABASE_CURATED)
  print(data_lake_mount_point)

  folder ="{zone}/{database}/reports/{folders}".format(
              zone = data_lake_mount_point, 
              database = QUALTRICS_DATABASE_NAME.lower(), 
              table = table_name.lower(), 
              folders = timestamp.strftime("year=%Y/month=%m/day=%d/hour=%H/minute=%M"))
  
  file = "CX_{reportname}_{ts}.txt".format(reportname = table_name, ts = timestamp.strftime("%Y%m%d"))
  #Remove TS from file name.
  file = "CX_{reportname}_.txt".format(reportname = table_name)
  
  print("Creating folder at dbfs:{}".format(folder))
  dbutils.fs.mkdirs("dbfs:{}".format(folder))
 
  report_uri = "/dbfs{}/{}".format(folder, file)
  print(report_uri)
  
  import csv
  df.toPandas().to_csv(report_uri, header=True, index=False, sep=",", quoting=csv.QUOTE_ALL)
  
  print("File created at " + report_uri)
  
  QualtricsSaveDataFrameToCurated(df, table_name, False)
  
