# Databricks notebook source
# DBTITLE 1,[Config] Connection Setup
storage_account_name = "saswcnonprod01landingtst"
storage_account_access_key = dbutils.secrets.get(scope="Test-Access",key="test-blob-key")
container_name = "archive"
file_location1 = "wasbs://archive@saswcnonprod01landingtst.blob.core.windows.net/archive/sap/0bptype_text/json/year=2021/month=07/day=15/0BPTYPE_TEXT_20210713164053.json"
file_location2 = "wasbs://archive@saswcnonprod01landingtst.blob.core.windows.net/archive/sap/0bptype_text/json/year=2021/month=07/day=21/0BPTYPE_TEXT_20210720152842.json"
file_type = "json"
print(storage_account_name)

# COMMAND ----------

spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
  storage_account_access_key)


# COMMAND ----------

# DBTITLE 1,[Source] Loading data into Dataframe
df = spark.read.format(file_type).option("inferSchema", "true").load(file_location1)
blobdf = spark.read.format(file_type).option("inferSchema", "true").load(file_location2)

# COMMAND ----------

# DBTITLE 1,[Source] Schema Check
df.printSchema()

# COMMAND ----------

blobdf.printSchema()

# COMMAND ----------

# DBTITLE 1,[Source] Creating Temporary Table
df.createOrReplaceTempView("Source1")
blobdf.createOrReplaceTempView("Source2")

# COMMAND ----------

# DBTITLE 1,[Source] Displaying Records
# MAGIC %sql
# MAGIC select * from Source1

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Source2

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from raw.sap_0bptype_text

# COMMAND ----------

# MAGIC %sql
# MAGIC select BPKIND ,
# MAGIC 'null' as EXTRACT_DATETIME
# MAGIC 'null' as EXTRACT_RUN_ID
# MAGIC   ODQ_CHANGEMODE ,
# MAGIC   ODQ_ENTITYCNTR ,
# MAGIC   SPRAS ,
# MAGIC   TEXT40  from Source1
# MAGIC union all
# MAGIC select BPKIND ,
# MAGIC   EXTRACT_DATETIME ,
# MAGIC   EXTRACT_RUN_ID ,
# MAGIC   ODQ_CHANGEMODE ,
# MAGIC   ODQ_ENTITYCNTR ,
# MAGIC   SPRAS ,
# MAGIC   TEXT40 from Source2
# MAGIC except
# MAGIC select   BPKIND ,
# MAGIC 
# MAGIC  from raw.sap_0bptype_text

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from raw.sap_0bptype_text

# COMMAND ----------

# DBTITLE 0,[Result] Load Count Result into DataFrame
lakedf = spark.sql("select * from raw.sap_0bptype_text")

# COMMAND ----------

lakedf.printSchema()

# COMMAND ----------

lakedf.createOrReplaceTempView("Target")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Target

# COMMAND ----------

# MAGIC %sql
# MAGIC select BPKIND, ODQ_CHANGEMODE,ODQ_ENTITYCNTR, SPRAS, TEXT40 from Source1
# MAGIC except
# MAGIC select BPKIND, ODQ_CHANGEMODE,ODQ_ENTITYCNTR, SPRAS, TEXT40 from Target

# COMMAND ----------

# DBTITLE 1,[Target] Connection setup for files loaded in data lake
storage_account_name = "sadaftest01"
storage_account_access_key = dbutils.secrets.get(scope="Test-Access",key="test-datalake-key")
container_name = "raw"
file_location = "wasbs://raw@sadaftest01.blob.core.windows.net/sap/0uc_mtr_doc/json/year=2021/month=07/day=16/bods/0UC_MTR_DOC.json_2021-07-16_153813_227.json.gz"
file_type = "json"

# COMMAND ----------

# DBTITLE 1,[Target] Loading data into Dataframe
lakedf = spark.read.format(file_type).option("inferSchema", "true").load(file_location)

# COMMAND ----------

spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
  storage_account_access_key)

# COMMAND ----------

# DBTITLE 1,[Target] Creating Temporary Table
lakedf.createOrReplaceTempView("Target")

# COMMAND ----------

# DBTITLE 1,[Target] Schema Check
lakedf.printSchema()

# COMMAND ----------

# DBTITLE 1,[Target] Displaying Records
# MAGIC %sql
# MAGIC select * from Target

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Source and Target Data
# MAGIC %sql
# MAGIC select * from Source
# MAGIC minus
# MAGIC select   BPKIND ,
# MAGIC   EXTRACT_DATETIME ,
# MAGIC   EXTRACT_RUN_ID ,
# MAGIC   ODQ_CHANGEMODE ,
# MAGIC   ODQ_ENTITYCNTR ,
# MAGIC   SPRAS ,
# MAGIC   TEXT40 
# MAGIC  from Target

# COMMAND ----------

# DBTITLE 1,[Verification] Checking Source and Target Count
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from Target
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from Source

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Target and Source Data
# MAGIC %sql
# MAGIC select   BPKIND ,
# MAGIC   EXTRACT_DATETIME ,
# MAGIC   EXTRACT_RUN_ID ,
# MAGIC   ODQ_CHANGEMODE ,
# MAGIC   ODQ_ENTITYCNTR ,
# MAGIC   SPRAS ,
# MAGIC   TEXT40 
# MAGIC from Target
# MAGIC except
# MAGIC select * from Source

# COMMAND ----------

# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Source' as TableName from Source

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from raw.sap_0UC_MTR_DOC
