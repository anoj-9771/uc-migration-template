# Databricks notebook source
# DBTITLE 1,[Config] Connection Setup
storage_account_name = "saswcnonprod01landingtst"
storage_account_access_key = dbutils.secrets.get(scope="Test-Access",key="test-blob-key")
container_name = "archive"
file_location = "wasbs://archive@saswcnonprod01landingtst.blob.core.windows.net/archive/sapisu/0uc_mtr_doc/json/year=2021/month=08/day=05/0UC_MTR_DOC_20210804141617.json"
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
# MAGIC select * from raw.sapisu_0uc_mtr_doc

# COMMAND ----------

# DBTITLE 0,[Result] Load Count Result into DataFrame
lakedf = spark.sql("select * from raw.sapisu_0uc_mtr_doc")

# COMMAND ----------

# DBTITLE 1,[Target] Schema Check
lakedf.printSchema()

# COMMAND ----------

lakedf.createOrReplaceTempView("Target")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from raw.sapisu_0uc_mtr_doc
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from Source

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Source and Target Data
# MAGIC %sql
# MAGIC select 
# MAGIC   * from Source
# MAGIC except
# MAGIC select  
# MAGIC  ABLBELNR ,
# MAGIC   ABLESART ,
# MAGIC   ABLESER ,
# MAGIC   ABLESTYP ,
# MAGIC   ABLHINW ,
# MAGIC   ABLSTAT ,
# MAGIC   ADAT ,
# MAGIC   ADATSOLL ,
# MAGIC   AEDAT ,
# MAGIC   AKTIV ,
# MAGIC   AMS ,
# MAGIC   DI_OPERATION_TYPE ,
# MAGIC   DI_SEQUENCE_NUMBER ,
# MAGIC   EQUNR ,
# MAGIC   EXTRACT_DATETIME ,
# MAGIC   EXTRACT_RUN_ID ,
# MAGIC   ISTABLART ,
# MAGIC   LOEVM ,
# MAGIC   MASSREAD ,
# MAGIC   MDEUPL ,
# MAGIC   MRESULT ,
# MAGIC   MR_BILL ,
# MAGIC   ODQ_CHANGEMODE ,
# MAGIC   ODQ_ENTITYCNTR ,
# MAGIC   POPCODE ,
# MAGIC   PRUEFPKT ,
# MAGIC   SOURCESYST ,
# MAGIC   TRANSSTAT ,
# MAGIC   TRANSTSTAMP ,
# MAGIC   UPDMOD ,
# MAGIC   ZADATTATS ,
# MAGIC   ZGERNR ,
# MAGIC   ZPREV_ADT ,
# MAGIC   ZPREV_MRESULT ,
# MAGIC   ZWNABR ,
# MAGIC   ZWNUMMER ,
# MAGIC   ZZ_COMM_CODE ,
# MAGIC   ZZ_FREE_TEXT ,
# MAGIC   ZZ_NO_READ_CODE ,
# MAGIC   ZZ_PHOTO_IND 
# MAGIC 
# MAGIC  from raw.sapisu_0uc_mtr_doc

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Target and Source Data
# MAGIC %sql
# MAGIC select  
# MAGIC    ABLBELNR ,
# MAGIC   ABLESART ,
# MAGIC   ABLESER ,
# MAGIC   ABLESTYP ,
# MAGIC   ABLHINW ,
# MAGIC   ABLSTAT ,
# MAGIC   ADAT ,
# MAGIC   ADATSOLL ,
# MAGIC   AEDAT ,
# MAGIC   AKTIV ,
# MAGIC   AMS ,
# MAGIC   DI_OPERATION_TYPE ,
# MAGIC   DI_SEQUENCE_NUMBER ,
# MAGIC   EQUNR ,
# MAGIC   EXTRACT_DATETIME ,
# MAGIC   EXTRACT_RUN_ID ,
# MAGIC   ISTABLART ,
# MAGIC   LOEVM ,
# MAGIC   MASSREAD ,
# MAGIC   MDEUPL ,
# MAGIC   MRESULT ,
# MAGIC   MR_BILL ,
# MAGIC   ODQ_CHANGEMODE ,
# MAGIC   ODQ_ENTITYCNTR ,
# MAGIC   POPCODE ,
# MAGIC   PRUEFPKT ,
# MAGIC   SOURCESYST ,
# MAGIC   TRANSSTAT ,
# MAGIC   TRANSTSTAMP ,
# MAGIC   UPDMOD ,
# MAGIC   ZADATTATS ,
# MAGIC   ZGERNR ,
# MAGIC   ZPREV_ADT ,
# MAGIC   ZPREV_MRESULT ,
# MAGIC   ZWNABR ,
# MAGIC   ZWNUMMER ,
# MAGIC   ZZ_COMM_CODE ,
# MAGIC   ZZ_FREE_TEXT ,
# MAGIC   ZZ_NO_READ_CODE ,
# MAGIC   ZZ_PHOTO_IND 
# MAGIC from raw.sapisu_0uc_mtr_doc
# MAGIC  except
# MAGIC  select 
# MAGIC   * from Source
