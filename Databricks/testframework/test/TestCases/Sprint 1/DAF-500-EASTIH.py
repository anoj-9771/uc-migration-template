# Databricks notebook source
# DBTITLE 1,[Config] Connection Setup
storage_account_name = "saswcnonprod01landingtst"
storage_account_access_key = dbutils.secrets.get(scope="Test-Access",key="test-blob-key")
container_name = "archive"
file_location = "wasbs://archive@saswcnonprod01landingtst.blob.core.windows.net/archive/sap/eastih/json/year=2021/month=07/day=21/EASTIH_20210720153501.json"
file_type = "json"
print(storage_account_name)

# COMMAND ----------

spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
  storage_account_access_key)


# COMMAND ----------

# DBTITLE 1,[Source] Loading data into Dataframe
df = spark.read.format(file_type).option("header", "true").load(file_location)

# COMMAND ----------

# DBTITLE 1,[Source] Schema Check
df.printSchema()

# COMMAND ----------

# DBTITLE 1,[Source] Creating Temporary Table
df.createOrReplaceTempView("Source")

# COMMAND ----------

# DBTITLE 1,[Source] Displaying Records
# MAGIC %sql
# MAGIC select * from Source

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from raw.sap_eastih

# COMMAND ----------

# DBTITLE 0,[Result] Load Count Result into DataFrame
lakedf = spark.sql("select * from raw.sap_eastih")

# COMMAND ----------

lakedf.printSchema()

# COMMAND ----------

# DBTITLE 1,[Target] Creating Temporary Table
lakedf.createOrReplaceTempView("Target")

# COMMAND ----------

# DBTITLE 1,[Verification] Checking Source and Target Count
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from Target
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from Source

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Source
# MAGIC except
# MAGIC select AEDAT, AENAM, AUTOEND, ERDAT, ERNAM,EXTRACT_DATETIME,EXTRACT_RUN_ID,INDEXNR,MANDT,PRUEFGR,ZWZUART FROM Target

# COMMAND ----------

countdf = spark.sql("select * from Source")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS csvextracts

# COMMAND ----------

df.write.format("csv").saveAsTable("csvextracts" + "." + "eastih")

# COMMAND ----------

countdf.write.format("json").saveAsTable("csvextracts" + "." + "eastihjson")

# COMMAND ----------

# MAGIC %sql select * from csvextracts.eastih

# COMMAND ----------

# MAGIC %sql 
# MAGIC select INDEXNR from csvextracts.eastihjson

# COMMAND ----------

# MAGIC %sql
# MAGIC select MANDT, PRUEFGR, ZWZUART, AUTOEND, ERDAT, ERNAM, AEDAT, AENAM  from Source

# COMMAND ----------

spark.sql("""select * from Source""") 

# COMMAND ----------

# MAGIC %sql
# MAGIC select MANDT, Source.INDEXNR, PRUEFGR, ZWZUART, AUTOEND, ERNAM, AEDAT, AENAM  from Source
# MAGIC MANDT, INDEXNR, PRUEFGR, ZWZUART, AUTOEND, ERDAT, ERNAM, AEDAT, AENAM 

# COMMAND ----------

# MAGIC %sql
# MAGIC select MANDT, INDEXNR, PRUEFGR, ZWZUART, AUTOEND, ERDAT, ERNAM, AEDAT, AENAM FROM Target
# MAGIC except 
# MAGIC select * from Source
