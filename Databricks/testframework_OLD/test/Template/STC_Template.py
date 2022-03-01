# Databricks notebook source
# DBTITLE 1,Connection
storage_account_name = dbutils.secrets.get(scope="AccessData",key="deltalakestorage")
storage_account_access_key = dbutils.secrets.get(scope="AccessData",key="deltalakestoragekey")
container_name = "adls2-test"
file_location = "wasbs://adls2-test@swcdevdataanalytics.blob.core.windows.net/sample-test/sourcejson_test.json"
file_type = "json"

# COMMAND ----------

spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
  storage_account_access_key)


# COMMAND ----------

# DBTITLE 1,Read the data
df = spark.read.format(file_type).option("inferSchema", "true").load(file_location)

# COMMAND ----------

# DBTITLE 1,[Source] Creating Temporary Table
df.createOrReplaceTempView("Sales")

# COMMAND ----------

# DBTITLE 1,Displaying Source Records
# MAGIC %sql
# MAGIC select * from Sales

# COMMAND ----------

# DBTITLE 1,Connection setup for files loaded in Blob (Target)
storage_account_name = dbutils.secrets.get(scope="AccessData",key="BlobStorageAccount")
storage_account_access_key = dbutils.secrets.get(scope="AccessData",key="BlobStorageKey")
container_name = "samplesource-test"
file_location = "wasbs://samplesource-test@swcdataanalyticscons.blob.core.windows.net/sourcejson_test.json"
file_type = "json"

# COMMAND ----------

spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
  storage_account_access_key)

# COMMAND ----------

# DBTITLE 1,Loading data into Dataframe
blobdf = spark.read.format(file_type).option("inferSchema", "true").load(file_location)

# COMMAND ----------

# DBTITLE 1,Creating Temporary Table for Target
blobdf.createOrReplaceTempView("SalesTarget")

# COMMAND ----------

# DBTITLE 1,Displaying Target Records
# MAGIC %sql
# MAGIC select * from SalesTarget

# COMMAND ----------

# DBTITLE 1,Checking Source and Target Count
# MAGIC %sql
# MAGIC --Verify Source and Target table count
# MAGIC select count (*) as RecordCount, 'Salestarget' as TableName from SalesTarget
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Sales' as TableName from Sales

# COMMAND ----------

# DBTITLE 1,Compare Source and Target Data
# MAGIC %sql
# MAGIC select * from Sales
# MAGIC except
# MAGIC select * from SalesTarget

# COMMAND ----------

# DBTITLE 1,Compare Source and Target Data
# MAGIC %sql
# MAGIC select * from SalesTarget
# MAGIC except
# MAGIC select * from Sales

# COMMAND ----------

# DBTITLE 1,Load Count Result into DataFrame
countdf = spark.sql("select count (*)  as RecordCount, 'SalesTarget' as TableName from SalesTarget union all select count (*) as RecordCount, 'Sales' as TableName from Sales")



# COMMAND ----------

display(countdf)

# COMMAND ----------

# DBTITLE 1,Writing Count Result in Database
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS TestDbSales

# COMMAND ----------

# DBTITLE 1,Database Table Property for Results
countdf.write.format("json").saveAsTable("TestDbSales" + "." + "Salesjson")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE SalesDetails

# COMMAND ----------

target_folder_path = "wasbs://adls2-test@swcdevdataanalytics.blob.core.windows.net/result-test/salesResult"
countdf.write.format("json").save(target_folder_path)

# COMMAND ----------

remove_folder_path = "wasbs://adls2-test@swcdevdataanalytics.blob.core.windows.net/result-test/salesResult"
dbutils.fs.rm(remove_folder_path, True)

# COMMAND ----------

remove_folder_path = "wasbs://adls2-test@swcdevdataanalytics.blob.core.windows.net/result-test"
dbutils.fs.rm(remove_folder_path, True)
