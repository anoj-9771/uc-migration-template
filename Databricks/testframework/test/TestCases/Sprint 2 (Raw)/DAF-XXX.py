# Databricks notebook source
# DBTITLE 1,[Config] Connection Setup
storage_account_name = "saswcnonprod01landingtst"
storage_account_access_key = dbutils.secrets.get(scope="Test-Access",key="test-blob-key")
container_name = "bods"
file_location = "wasbs://test@saswcnonprod01landingtst.blob.core.windows.net/sourcejson_test.json"
file_type = "json"
print(storage_account_name)

# COMMAND ----------

spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
  storage_account_access_key)


# COMMAND ----------

# DBTITLE 1,[Source] Loading data into Dataframe
df = spark.read.format(file_type).option("inferSchema", "true").load(file_location)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# DBTITLE 1,[Source] Creating Temporary Table
df.createOrReplaceTempView("Source")

# COMMAND ----------

# DBTITLE 1,[Source] Displaying Records
# MAGIC %sql
# MAGIC select * from Source

# COMMAND ----------

# DBTITLE 1,[Source] Apply transformation rules in Source
--Placeholder for scripts developed separately

# COMMAND ----------

# DBTITLE 1,[Target] Connection setup for files loaded in data lake
storage_account_name = "sadaftest01"
storage_account_access_key = dbutils.secrets.get(scope="Test-Access",key="test-datalake-key")
container_name = "raw"
file_location = "wasbs://raw@sadaftest01.blob.core.windows.net/"
file_type = "json"

# COMMAND ----------

spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
  storage_account_access_key)

# COMMAND ----------

# DBTITLE 1,[Target] Loading data into Dataframe
blobdf = spark.read.format(file_type).option("inferSchema", "true").load(file_location)

# COMMAND ----------

# DBTITLE 1,[Target] Creating Temporary Table
blobdf.createOrReplaceTempView("SalesTarget")

# COMMAND ----------

# DBTITLE 1,[Target] Displaying Records
# MAGIC %sql
# MAGIC select * from SalesTarget

# COMMAND ----------

# DBTITLE 1,[Verification] Checking Source and Target Count
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Salestarget' as TableName from SalesTarget
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Sales' as TableName from Sales

# COMMAND ----------

# DBTITLE 1,[Verification] Comparing Source and Target
# MAGIC %sql
# MAGIC select
# MAGIC a.BU_SORT1 as Source_BU_SORT1,
# MAGIC a.BU_SORT2 as Source_BU_SORT2,
# MAGIC b.BU_SORT1 as Target_BU_SORT1,
# MAGIC b.BU_SORT2 as Target_BU_SORT2,
# MAGIC case when a.BU_SORT1 = b.BU_SORT1 then 'Pass' else 'Fail' end as Result_BU_SORT1,
# MAGIC case when a.BU_SORT2 = b.BU_SORT2 then 'Pass' else 'Fail' end as Result_BU_SORT2,
# MAGIC 'executed as part of the fix' as comment
# MAGIC from SalesTarget a
# MAGIC join Sales b on a.DI_SEQUENCE_NUMBER = b.DI_SEQUENCE_NUMBER

# COMMAND ----------

# DBTITLE 1,[Result] Storing Results in DB
# MAGIC %sql
# MAGIC select * into DAF-123_EXTRACTOR_15072021
# MAGIC ( dataframe above
# MAGIC )

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Source and Target Data
# MAGIC %sql
# MAGIC select * from SalesTarget
# MAGIC except
# MAGIC select * from Sales

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Target and Source Data
# MAGIC %sql
# MAGIC select * from Sales
# MAGIC except
# MAGIC select * from SalesTarget

# COMMAND ----------

# DBTITLE 1,[Result] Load Count Result into DataFrame
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

# DBTITLE 1,[Template] Drop Table Command
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
