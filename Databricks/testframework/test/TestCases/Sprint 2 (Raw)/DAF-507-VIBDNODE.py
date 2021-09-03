# Databricks notebook source
# DBTITLE 1,[Config] Connection Setup
storage_account_name = "saswcnonprod01landingtst"
storage_account_access_key = dbutils.secrets.get(scope="Test-Access",key="test-blob-key")
container_name = "archive"
file_location = "wasbs://archive@saswcnonprod01landingtst.blob.core.windows.net/archive/sap/vibdnode/json/year=2021/month=07/day=21/VIBDNODE_20210720152842.json"
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
# MAGIC select * from raw.sap_vibdnode

# COMMAND ----------

# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from raw.sap_vibdnode
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from Source

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Source and Target Data
# MAGIC %sql
# MAGIC select 
# MAGIC AONR_AO,AONR_PA,AOTYPE_AO,AOTYPE_PA,EXTRACT_DATETIME,EXTRACT_RUN_ID,INTRENO,MANDT,PARENT,TREE from Source
# MAGIC except
# MAGIC select 
# MAGIC AONR_AO,AONR_PA,AOTYPE_AO,AOTYPE_PA,EXTRACT_DATETIME,EXTRACT_RUN_ID,INTRENO,MANDT,PARENT,TREE from raw.sap_vibdnode

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Target and Source Data
# MAGIC %sql
# MAGIC select 
# MAGIC AONR_AO,AONR_PA,AOTYPE_AO,AOTYPE_PA,EXTRACT_DATETIME,EXTRACT_RUN_ID,INTRENO,MANDT,PARENT,TREE from raw.sap_vibdnode
# MAGIC except
# MAGIC select 
# MAGIC AONR_AO,AONR_PA,AOTYPE_AO,AOTYPE_PA,EXTRACT_DATETIME,EXTRACT_RUN_ID,INTRENO,MANDT,PARENT,TREE from Source

# COMMAND ----------

# DBTITLE 0,[Result] Load Count Result into DataFrame
lakedf = spark.sql("select * from raw.sap_vibdnode")

# COMMAND ----------

# DBTITLE 1,[Target] Schema Check
lakedf.printSchema()
