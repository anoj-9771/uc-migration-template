# Databricks notebook source
# DBTITLE 1,[Config] Connection Setup
storage_account_name = "saswcnonprod01landingtst"
storage_account_access_key = dbutils.secrets.get(scope="Test-Access",key="test-blob-key")
container_name = "test"
file_location = "wasbs://test@saswcnonprod01landingtst.blob.core.windows.net/ERCHO_27July.csv"
file_type = "csv"
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
# MAGIC select * from raw.sap_ercho

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
