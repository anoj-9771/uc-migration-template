# Databricks notebook source
# DBTITLE 1,[Config] Connection Setup
storage_account_name = "saswcnonprod01landingtst"
storage_account_access_key = dbutils.secrets.get(scope="Test-Access",key="test-blob-key")
container_name = "archive"
file_location = "wasbs://archive@saswcnonprod01landingtst.blob.core.windows.net/sapisu/20210902/20210902_00:23:52/TIVBDAROBJTYPET_20210831104548.json"
file_location2 = "wasbs://archive@saswcnonprod01landingtst.blob.core.windows.net/sapisu/20210902/20210902_00:23:52/TIVBDAROBJTYPET_20210831110701.json"
file_type = "json"
print(storage_account_name)

# COMMAND ----------

spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
  storage_account_access_key)


# COMMAND ----------

# DBTITLE 1,[Source] Loading data into Dataframe
df = spark.read.format(file_type).option("inferSchema", "true").load(file_location)
df2 = spark.read.format(file_type).option("inferSchema", "true").load(file_location2)

# COMMAND ----------

# DBTITLE 1,[Source] Schema Check - Refer to Raw2Cleansed Mapping
df.printSchema()
df2.printSchema()

# COMMAND ----------

# DBTITLE 0,[Result] Load Count Result into DataFrame
lakedf = spark.sql("select * from cleansed.t_sapisu_tivbdarobjtypet")

# COMMAND ----------

# DBTITLE 1,[Target] Schema Check
lakedf.printSchema()

# COMMAND ----------

# DBTITLE 1,[Source] Creating Temporary Table
df.createOrReplaceTempView("Source")
df2.createOrReplaceTempView("Source2")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from Source2
# MAGIC union all
# MAGIC select * from source

# COMMAND ----------

# DBTITLE 1,[Source] with mapping
# MAGIC %sql
# MAGIC 
# MAGIC select 
# MAGIC AOTYPE,
# MAGIC XMAOTYPE
# MAGIC from(
# MAGIC select 
# MAGIC AOTYPE,
# MAGIC XMAOTYPE,
# MAGIC EXTRACT_DATETIME,
# MAGIC EXTRACT_RUN_ID,
# MAGIC SPRAS,
# MAGIC MANDT,
# MAGIC row_number() over (partition by AOTYPE, XMAOTYPE order by EXTRACT_DATETIME desc) as rn 
# MAGIC from(
# MAGIC SELECT * from Source2
# MAGIC union all
# MAGIC select * from source
# MAGIC )a
# MAGIC )b where b.rn = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC *
# MAGIC FROM
# MAGIC cleansed.t_sapisu_tivbdarobjtypet

# COMMAND ----------

lakedf.createOrReplaceTempView("Target")

# COMMAND ----------

# DBTITLE 1,[Verification] Count Checks
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from cleansed.t_sapisu_tivbdarobjtypet
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from (select 
# MAGIC AOTYPE,
# MAGIC XMAOTYPE
# MAGIC from(
# MAGIC select 
# MAGIC AOTYPE,
# MAGIC XMAOTYPE,
# MAGIC EXTRACT_DATETIME,
# MAGIC EXTRACT_RUN_ID,
# MAGIC SPRAS,
# MAGIC MANDT,
# MAGIC row_number() over (partition by AOTYPE, XMAOTYPE order by EXTRACT_DATETIME desc) as rn 
# MAGIC from(
# MAGIC SELECT * from Source2
# MAGIC union all
# MAGIC select * from source
# MAGIC )a
# MAGIC )b where b.rn = 1)

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT AOTYPE, COUNT (*) as count
# MAGIC FROM cleansed.t_sapisu_tivbdarobjtypet
# MAGIC GROUP BY AOTYPE
# MAGIC HAVING COUNT (*) > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT * FROM (
# MAGIC SELECT
# MAGIC *,
# MAGIC row_number() OVER(PARTITION BY AOTYPE order by AOTYPE) as rn
# MAGIC FROM cleansed.t_sapisu_tivbdarobjtypet
# MAGIC )a where a.rn > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Source and Target Data
# MAGIC %sql
# MAGIC select 
# MAGIC AOTYPE,
# MAGIC XMAOTYPE
# MAGIC from(
# MAGIC select 
# MAGIC AOTYPE,
# MAGIC XMAOTYPE,
# MAGIC EXTRACT_DATETIME,
# MAGIC EXTRACT_RUN_ID,
# MAGIC SPRAS,
# MAGIC MANDT,
# MAGIC row_number() over (partition by AOTYPE, XMAOTYPE order by EXTRACT_DATETIME desc) as rn 
# MAGIC from(
# MAGIC SELECT * from Source2
# MAGIC union all
# MAGIC select * from source
# MAGIC )a
# MAGIC )b where b.rn = 1
# MAGIC except
# MAGIC select
# MAGIC AOTYPE,
# MAGIC XMAOTYPE
# MAGIC from
# MAGIC cleansed.t_sapisu_tivbdarobjtypet

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Target and Source Data
# MAGIC %sql
# MAGIC select
# MAGIC AOTYPE,
# MAGIC XMAOTYPE
# MAGIC from
# MAGIC cleansed.t_sapisu_tivbdarobjtypet
# MAGIC except
# MAGIC select 
# MAGIC AOTYPE,
# MAGIC XMAOTYPE
# MAGIC from(
# MAGIC select 
# MAGIC AOTYPE,
# MAGIC XMAOTYPE,
# MAGIC EXTRACT_DATETIME,
# MAGIC EXTRACT_RUN_ID,
# MAGIC SPRAS,
# MAGIC MANDT,
# MAGIC row_number() over (partition by AOTYPE, XMAOTYPE order by EXTRACT_DATETIME desc) as rn 
# MAGIC from(
# MAGIC SELECT * from Source2
# MAGIC union all
# MAGIC select * from source
# MAGIC )a
# MAGIC )b where b.rn = 1
