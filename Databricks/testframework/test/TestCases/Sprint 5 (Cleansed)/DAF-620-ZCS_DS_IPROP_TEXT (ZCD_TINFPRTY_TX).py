# Databricks notebook source
# DBTITLE 1,[Config] Connection Setup
storage_account_name = "saswcnonprod01landingtst"
storage_account_access_key = dbutils.secrets.get(scope="Test-Access",key="test-blob-key")
container_name = "archive"
file_location = "wasbs://archive@saswcnonprod01landingtst.blob.core.windows.net/sapisu/20210901/20210901_13:17:34/ZCD_TINFPRTY_TX_20210831104548.json"
file_location2 = "wasbs://archive@saswcnonprod01landingtst.blob.core.windows.net/sapisu/20210901/20210901_13:17:34/ZCD_TINFPRTY_TX_20210831110701.json"
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
lakedf = spark.sql("select * from cleansed.t_sapisu_zcd_tinfprty_tx")

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
# MAGIC INFERIOR_PROP_TYPE as inferiorPropertyTypeCode,
# MAGIC DESCRIPTION as inferiorPropertyType
# MAGIC from(
# MAGIC select 
# MAGIC DESCRIPTION, 
# MAGIC EXTRACT_DATETIME,
# MAGIC EXTRACT_RUN_ID,
# MAGIC INFERIOR_PROP_TYPE,
# MAGIC LANGU,
# MAGIC MANDT,
# MAGIC row_number() over (partition by INFERIOR_PROP_TYPE, MANDT order by EXTRACT_DATETIME desc) as rn 
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
# MAGIC cleansed.t_sapisu_zcd_tinfprty_tx

# COMMAND ----------

lakedf.createOrReplaceTempView("Target")

# COMMAND ----------

# DBTITLE 1,[Verification] Count Checks
# MAGIC 
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from cleansed.t_sapisu_zcd_tinfprty_tx
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from (select 
# MAGIC INFERIOR_PROP_TYPE as inferiorPropertyTypeCode,
# MAGIC DESCRIPTION as inferiorPropertyType
# MAGIC from(
# MAGIC select 
# MAGIC DESCRIPTION, 
# MAGIC EXTRACT_DATETIME,
# MAGIC EXTRACT_RUN_ID,
# MAGIC INFERIOR_PROP_TYPE,
# MAGIC LANGU,
# MAGIC MANDT,
# MAGIC row_number() over (partition by INFERIOR_PROP_TYPE, MANDT order by EXTRACT_DATETIME desc) as rn 
# MAGIC from(
# MAGIC SELECT * from Source2
# MAGIC union all
# MAGIC select * from source
# MAGIC )a
# MAGIC )b where b.rn = 1)a

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT inferiorPropertyTypeCode, COUNT (*) as count
# MAGIC FROM cleansed.t_sapisu_zcd_tinfprty_tx
# MAGIC GROUP BY inferiorPropertyTypeCode
# MAGIC HAVING COUNT (*) > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT * FROM (
# MAGIC SELECT
# MAGIC *,
# MAGIC row_number() OVER(PARTITION BY inferiorPropertyTypeCode order by inferiorPropertyTypeCode) as rn
# MAGIC FROM cleansed.t_sapisu_zcd_tinfprty_tx
# MAGIC )a where a.rn > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Source and Target Data
# MAGIC %sql
# MAGIC SELECT
# MAGIC INFERIOR_PROP_TYPE as inferiorPropertyTypeCode,
# MAGIC DESCRIPTION as inferiorPropertyType
# MAGIC from(
# MAGIC select 
# MAGIC DESCRIPTION, 
# MAGIC EXTRACT_DATETIME,
# MAGIC EXTRACT_RUN_ID,
# MAGIC INFERIOR_PROP_TYPE,
# MAGIC LANGU,
# MAGIC MANDT,
# MAGIC row_number() over (partition by INFERIOR_PROP_TYPE, MANDT order by EXTRACT_DATETIME desc) as rn 
# MAGIC from(
# MAGIC SELECT * from Source2
# MAGIC union all
# MAGIC select * from source
# MAGIC )a
# MAGIC )b where b.rn = 1
# MAGIC except
# MAGIC select
# MAGIC inferiorPropertyTypeCode,
# MAGIC inferiorPropertyType
# MAGIC from
# MAGIC cleansed.t_sapisu_zcd_tinfprty_tx

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Target and Source Data
# MAGIC %sql
# MAGIC select
# MAGIC inferiorPropertyTypeCode,
# MAGIC inferiorPropertyType
# MAGIC from
# MAGIC cleansed.t_sapisu_zcd_tinfprty_tx
# MAGIC EXCEPT
# MAGIC SELECT
# MAGIC INFERIOR_PROP_TYPE as inferiorPropertyTypeCode,
# MAGIC DESCRIPTION as inferiorPropertyType
# MAGIC from(
# MAGIC select 
# MAGIC DESCRIPTION, 
# MAGIC EXTRACT_DATETIME,
# MAGIC EXTRACT_RUN_ID,
# MAGIC INFERIOR_PROP_TYPE,
# MAGIC LANGU,
# MAGIC MANDT,
# MAGIC row_number() over (partition by INFERIOR_PROP_TYPE, MANDT order by EXTRACT_DATETIME desc) as rn 
# MAGIC from(
# MAGIC SELECT * from Source2
# MAGIC union all
# MAGIC select * from source
# MAGIC )a
# MAGIC )b where b.rn = 1
