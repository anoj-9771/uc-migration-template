# Databricks notebook source
# DBTITLE 1,[Config] Connection Setup
storage_account_name = "saswcnonprod01landingtst"
storage_account_access_key = dbutils.secrets.get(scope="Test-Access",key="test-blob-key")
container_name = "archive"
file_location = "wasbs://archive@saswcnonprod01landingtst.blob.core.windows.net/archive/archive/sap/zcd_tpropty_hist/json/year=2021/month=07/day=21/ZCD_TPROPTY_HIST_20210720143959.json"
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
df.createOrReplaceTempView("SourceTable")

# COMMAND ----------

# DBTITLE 1,[Source] Displaying Records
# MAGIC %sql
# MAGIC select * from SourceTable

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from raw.sap_zcd_tpropty_hist

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Source and Target Data
# MAGIC %sql
# MAGIC select * from Source
# MAGIC except
# MAGIC select * from raw.sap_ercho

# COMMAND ----------

# DBTITLE 0,[Result] Load Count Result into DataFrame
lakedf = spark.sql("select * from raw.sap_ercho")

# COMMAND ----------

# DBTITLE 1,[Target] Schema Check
lakedf.printSchema()

# COMMAND ----------

lakedf.createOrReplaceTempView("Target")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from Target
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from SourceTable

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Target and Source Data
# MAGIC %sql
# MAGIC select MANDT,BELNR,  OUTCNSO, VALIDATION, MANOUTSORT, FREI_AM, FREI_VON, DEVIATION, SIMULATION, OUTCOUNT, COLOGRP_INST from SourceTable
# MAGIC except
# MAGIC select MANDT,BELNR, OUTCNSO, VALIDATION, MANOUTSORT, FREI_AM, FREI_VON, DEVIATION, SIMULATION, OUTCOUNT, COLOGRP_INST from Target

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from SourceTable
# MAGIC except
# MAGIC select MANDT,BELNR, OUTCNSO, VALIDATION, MANOUTSORT, FREI_AM, FREI_VON, DEVIATION, SIMULATION, OUTCOUNT, COLOGRP_INST from Target

# COMMAND ----------

# MAGIC %sql
# MAGIC select MANOUTSORT from SourceTable
# MAGIC except
# MAGIC select MANOUTSORT from Target