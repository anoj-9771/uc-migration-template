# Databricks notebook source
# DBTITLE 1,[Config] Connection Setup
storage_account_name = "saswcnonprod01landingtst"
storage_account_access_key = dbutils.secrets.get(scope="Test-Access",key="test-blob-key")
container_name = "archive"
file_location = "wasbs://archive@saswcnonprod01landingtst.blob.core.windows.net/archive/sapisu/eastih/json/year=2021/month=08/day=05/EASTIH_20210804141617.json"
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
# MAGIC select * from raw.sapisu_eastih

# COMMAND ----------

# DBTITLE 0,[Result] Load Count Result into DataFrame
lakedf = spark.sql("select * from raw.sapisu_eastih")

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

# DBTITLE 1,[Verification]Source and Target
# MAGIC %sql
# MAGIC select AEDAT, AENAM, AUTOEND, ERDAT, ERNAM,INDEXNR,MANDT,PRUEFGR,ZWZUART,EXTRACT_DATETIME,EXTRACT_RUN_ID  from Source
# MAGIC except
# MAGIC select AEDAT, AENAM, AUTOEND, ERDAT, ERNAM,INDEXNR,MANDT,PRUEFGR,ZWZUART,EXTRACT_DATETIME,EXTRACT_RUN_ID FROM Target

# COMMAND ----------

# DBTITLE 1,[Verification]Target and Source 
# MAGIC %sql
# MAGIC select AEDAT, AENAM, AUTOEND, ERDAT, ERNAM,INDEXNR,MANDT,PRUEFGR,ZWZUART,EXTRACT_DATETIME,EXTRACT_RUN_ID FROM Target
# MAGIC except
# MAGIC select AEDAT, AENAM, AUTOEND, ERDAT, ERNAM,INDEXNR,MANDT,PRUEFGR,ZWZUART,EXTRACT_DATETIME,EXTRACT_RUN_ID from Source
