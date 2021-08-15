# Databricks notebook source
# DBTITLE 1,[Config] Connection Setup
storage_account_name = "saswcnonprod01landingtst"
storage_account_access_key = dbutils.secrets.get(scope="Test-Access",key="test-blob-key")
container_name = "archive"
file_location = "wasbs://archive@saswcnonprod01landingtst.blob.core.windows.net/archive/sapisu/0uc_device_attr/json/year=2021/month=08/day=05/0UC_DEVICE_ATTR_20210805103817.json"
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
# MAGIC select * from raw.sapisu_0uc_device_attr

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select count (*) as RecordCount, 'Source' as TableName from Source
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Target' as TableName from raw.sapisu_0uc_device_attr

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Source and Target Data
# MAGIC %sql
# MAGIC select 
# MAGIC   EQUNR ,
# MAGIC   EXTRACT_DATETIME ,
# MAGIC   EXTRACT_RUN_ID ,
# MAGIC   GERAET ,
# MAGIC   GROES ,
# MAGIC   HERST ,
# MAGIC   KOMGRP ,
# MAGIC   KZSTICH ,
# MAGIC   LOEVM ,
# MAGIC   MATNR ,
# MAGIC   ODQ_CHANGEMODE ,
# MAGIC   ODQ_ENTITYCNTR ,
# MAGIC   PLOMBDAT ,
# MAGIC   SERGE ,
# MAGIC   VLZEIT_DEV ,
# MAGIC   ZWNABR ,
# MAGIC   ZZOBJNR ,
# MAGIC   ZZTYPBZ 
# MAGIC  from Source
# MAGIC except
# MAGIC select 
# MAGIC   EQUNR ,
# MAGIC   EXTRACT_DATETIME ,
# MAGIC   EXTRACT_RUN_ID ,
# MAGIC   GERAET ,
# MAGIC   GROES ,
# MAGIC   HERST ,
# MAGIC   KOMGRP ,
# MAGIC   KZSTICH ,
# MAGIC   LOEVM ,
# MAGIC   MATNR ,
# MAGIC   ODQ_CHANGEMODE ,
# MAGIC   ODQ_ENTITYCNTR ,
# MAGIC   PLOMBDAT ,
# MAGIC   SERGE ,
# MAGIC   VLZEIT_DEV ,
# MAGIC   ZWNABR ,
# MAGIC   ZZOBJNR ,
# MAGIC   ZZTYPBZ 
# MAGIC  from raw.sapisu_0uc_device_attr

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Target and Source Data
# MAGIC %sql
# MAGIC select 
# MAGIC   EQUNR ,
# MAGIC   EXTRACT_DATETIME ,
# MAGIC   EXTRACT_RUN_ID ,
# MAGIC   GERAET ,
# MAGIC   GROES ,
# MAGIC   HERST ,
# MAGIC   KOMGRP ,
# MAGIC   KZSTICH ,
# MAGIC   LOEVM ,
# MAGIC   MATNR ,
# MAGIC   ODQ_CHANGEMODE ,
# MAGIC   ODQ_ENTITYCNTR ,
# MAGIC   PLOMBDAT ,
# MAGIC   SERGE ,
# MAGIC   VLZEIT_DEV ,
# MAGIC   ZWNABR ,
# MAGIC   ZZOBJNR ,
# MAGIC   ZZTYPBZ 
# MAGIC  from raw.sapisu_0uc_device_attr
# MAGIC except
# MAGIC select 
# MAGIC   EQUNR ,
# MAGIC   EXTRACT_DATETIME ,
# MAGIC   EXTRACT_RUN_ID ,
# MAGIC   GERAET ,
# MAGIC   GROES ,
# MAGIC   HERST ,
# MAGIC   KOMGRP ,
# MAGIC   KZSTICH ,
# MAGIC   LOEVM ,
# MAGIC   MATNR ,
# MAGIC   ODQ_CHANGEMODE ,
# MAGIC   ODQ_ENTITYCNTR ,
# MAGIC   PLOMBDAT ,
# MAGIC   SERGE ,
# MAGIC   VLZEIT_DEV ,
# MAGIC   ZWNABR ,
# MAGIC   ZZOBJNR ,
# MAGIC   ZZTYPBZ 
# MAGIC  from Source

# COMMAND ----------

# DBTITLE 0,[Result] Load Count Result into DataFrame
lakedf = spark.sql("select * from raw.sapisu_0uc_device_attr")

# COMMAND ----------

# DBTITLE 1,[Target] Schema Check
lakedf.printSchema()
