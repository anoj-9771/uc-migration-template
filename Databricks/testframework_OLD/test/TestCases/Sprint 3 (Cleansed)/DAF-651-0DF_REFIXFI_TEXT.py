# Databricks notebook source
# DBTITLE 1,[Config] Connection Setup
storage_account_name = "saswcnonprod01landingtst"
storage_account_access_key = dbutils.secrets.get(scope="Test-Access",key="test-blob-key")
container_name = "archive"
file_location = "wasbs://archive@saswcnonprod01landingtst.blob.core.windows.net/archive/sapisu/0df_refixfi_text/json/year=2021/month=08/day=05/0DF_REFIXFI_TEXT_20210804121857.json"
file_type = "json"
print(storage_account_name)

# COMMAND ----------

spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
  storage_account_access_key)


# COMMAND ----------

# DBTITLE 1,[Source] Schema Check
df.printSchema()

# COMMAND ----------

# DBTITLE 0,[Result] Load Count Result into DataFrame
lakedf = spark.sql("select * from cleansed.t_sapisu_0df_refixfi_text")

# COMMAND ----------

# DBTITLE 1,[Target] Schema Check
lakedf.printSchema()

# COMMAND ----------

# DBTITLE 1,[Source] Loading data into Dataframe
df = spark.read.format(file_type).option("inferSchema", "true").load(file_location)

# COMMAND ----------

# DBTITLE 1,[Source] Creating Temporary Table
df.createOrReplaceTempView("Source")

# COMMAND ----------

# DBTITLE 1,[Source] Displaying Records
# MAGIC %sql
# MAGIC select * from Source

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cleansed.t_sapisu_0df_refixfi_text

# COMMAND ----------

# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from cleansed.t_sapisu_0df_refixfi_text
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from Source

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Source and Target Data
# MAGIC %sql
# MAGIC select 
# MAGIC   FIXFITCHARACT as fixtureAndFittingCharacteristicCode,
# MAGIC   XFIXFITCHARACT as fixtureAndFittingCharacteristic
# MAGIC   from Source
# MAGIC except
# MAGIC select  
# MAGIC   fixtureAndFittingCharacteristicCode,
# MAGIC   fixtureAndFittingCharacteristic
# MAGIC  from cleansed.t_sapisu_0df_refixfi_text

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Target and Source Data
# MAGIC %sql
# MAGIC select  
# MAGIC   fixtureAndFittingCharacteristicCode,
# MAGIC   fixtureAndFittingCharacteristic
# MAGIC from cleansed.t_sapisu_0df_refixfi_text
# MAGIC except
# MAGIC select 
# MAGIC   FIXFITCHARACT as fixtureAndFittingCharacteristicCode,
# MAGIC   XFIXFITCHARACT as fixtureAndFittingCharacteristic
# MAGIC from Source