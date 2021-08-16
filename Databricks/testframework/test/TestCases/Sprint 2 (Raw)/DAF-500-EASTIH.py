# Databricks notebook source
# DBTITLE 1,[Config] Connection Setup
storage_account_name = "saswcnonprod01landingtst"
storage_account_access_key = dbutils.secrets.get(scope="Test-Access",key="test-blob-key")
container_name = "archive"
file_location1 = "wasbs://archive@saswcnonprod01landingtst.blob.core.windows.net/archive/sap/eastih/json/year=2021/month=07/day=15/EASTIH_20210713104234.json"
file_location2 = "wasbs://archive@saswcnonprod01landingtst.blob.core.windows.net/archive/sap/eastih/json/year=2021/month=07/day=21/EASTIH_20210720153501.json"
file_type = "json"
print(storage_account_name)

# COMMAND ----------

spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
  storage_account_access_key)


# COMMAND ----------

# DBTITLE 1,[Source] Loading data into Dataframe
df1 = spark.read.format(file_type).option("header", "true").load(file_location1)
df2 = spark.read.format(file_type).option("header", "true").load(file_location2)


# COMMAND ----------

# DBTITLE 1,[Source] Schema Check
df1.printSchema()

# COMMAND ----------

# DBTITLE 1,[Source] Creating Temporary Table
df1.createOrReplaceTempView("Source1")

# COMMAND ----------

df2.printSchema()

# COMMAND ----------

df2.createOrReplaceTempView("Source2")

# COMMAND ----------

# DBTITLE 1,[Source] Displaying Records
# MAGIC %sql
# MAGIC select * from Source1

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Source2

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
# MAGIC select count (*) as RecordCount, 'Source1' as TableName from Source1
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source2' as TableName from Source2

# COMMAND ----------

# MAGIC %sql
# MAGIC select AEDAT, AENAM, AUTOEND, ERDAT, ERNAM,INDEXNR,MANDT,PRUEFGR,ZWZUART from Source1
# MAGIC union all
# MAGIC select AEDAT, AENAM, AUTOEND, ERDAT, ERNAM,INDEXNR,MANDT,PRUEFGR,ZWZUART from Source2
# MAGIC except
# MAGIC select AEDAT, AENAM, AUTOEND, ERDAT, ERNAM,INDEXNR,MANDT,PRUEFGR,ZWZUART FROM Target

# COMMAND ----------

# MAGIC %sql
# MAGIC select AEDAT, AENAM, AUTOEND, ERDAT, ERNAM,INDEXNR,MANDT,PRUEFGR,ZWZUART FROM Target
# MAGIC except
# MAGIC (select AEDAT, AENAM, AUTOEND, ERDAT, ERNAM,INDEXNR,MANDT,PRUEFGR,ZWZUART from Source1
# MAGIC union all
# MAGIC select AEDAT, AENAM, AUTOEND, ERDAT, ERNAM,INDEXNR,MANDT,PRUEFGR,ZWZUART from Source2)
