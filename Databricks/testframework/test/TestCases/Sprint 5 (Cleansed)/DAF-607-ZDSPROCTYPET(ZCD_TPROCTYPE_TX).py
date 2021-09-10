# Databricks notebook source
# DBTITLE 1,[Config] Connection Setup
storage_account_name = "saswcnonprod01landingtst"
storage_account_access_key = dbutils.secrets.get(scope="Test-Access",key="test-blob-key")
container_name = "archive"
file_location = "wasbs://archive@saswcnonprod01landingtst.blob.core.windows.net/sapisu/20210902/20210902_00:23:52/ZCD_TPROCTYPE_TX_20210831104548.json"
file_location2 = "wasbs://archive@saswcnonprod01landingtst.blob.core.windows.net/sapisu/20210902/20210902_00:23:52/ZCD_TPROCTYPE_TX_20210831110701.json"
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
lakedf = spark.sql("select * from cleansed.t_sapisu_zcd_tproctype_tx")

# COMMAND ----------

# DBTITLE 1,[Target] Schema Check
lakedf.printSchema()

# COMMAND ----------

# DBTITLE 1,[Source] Creating Temporary Table
df.createOrReplaceTempView("Source")
df2.createOrReplaceTempView("Source2")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from Source2 WHERE LANGU = 'E'
# MAGIC union all
# MAGIC select * from source WHERE LANGU = 'E'

# COMMAND ----------

# DBTITLE 1,[Source] with mapping
# MAGIC %sql
# MAGIC 
# MAGIC select 
# MAGIC PROCESS_TYPE,
# MAGIC PROCESs_MODE,
# MAGIC DESCRIPTION
# MAGIC from(
# MAGIC select 
# MAGIC DESCRIPTION, 
# MAGIC EXTRACT_DATETIME,
# MAGIC EXTRACT_RUN_ID,
# MAGIC LANGU,
# MAGIC MANDT,
# MAGIC PROCESS_MODE,
# MAGIC PROCESS_TYPE,
# MAGIC row_number() over (partition by PROCESS_TYPE, MANDT order by EXTRACT_DATETIME desc) as rn 
# MAGIC from(
# MAGIC SELECT * from Source2 WHERE LANGU = 'E'
# MAGIC union all
# MAGIC select * from source WHERE LANGU = 'E'
# MAGIC )a
# MAGIC )b where b.rn = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC *
# MAGIC FROM
# MAGIC cleansed.t_sapisu_ZCD_TPROCTYPE_TX

# COMMAND ----------

lakedf.createOrReplaceTempView("Target")

# COMMAND ----------

# DBTITLE 1,[Verification] Count Checks
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from cleansed.t_sapisu_zcd_tproctype_tx
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from (
# MAGIC --BEGIN SOURCE QUERY
# MAGIC select 
# MAGIC PROCESS_TYPE,
# MAGIC PROCESs_MODE,
# MAGIC DESCRIPTION
# MAGIC from(
# MAGIC select 
# MAGIC DESCRIPTION, 
# MAGIC EXTRACT_DATETIME,
# MAGIC EXTRACT_RUN_ID,
# MAGIC LANGU,
# MAGIC MANDT,
# MAGIC PROCESS_MODE,
# MAGIC PROCESS_TYPE,
# MAGIC row_number() over (partition by PROCESS_TYPE, MANDT order by EXTRACT_DATETIME desc) as rn 
# MAGIC from(
# MAGIC SELECT * from Source2 WHERE LANGU = 'E'
# MAGIC union all
# MAGIC select * from source WHERE LANGU = 'E'
# MAGIC )a
# MAGIC )b where b.rn = 1
# MAGIC --END SOURCE QUERY
# MAGIC 
# MAGIC )a

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT process_type, COUNT (*) as count
# MAGIC FROM cleansed.t_sapisu_zcd_tproctype_tx
# MAGIC GROUP BY process_type
# MAGIC HAVING COUNT (*) > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT * FROM (
# MAGIC SELECT
# MAGIC *,
# MAGIC row_number() OVER(PARTITION BY process_type order by process_type) as rn
# MAGIC FROM cleansed.t_sapisu_zcd_tproctype_tx
# MAGIC )a where a.rn > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Source and Target Data
# MAGIC %sql
# MAGIC select 
# MAGIC PROCESS_TYPE,
# MAGIC PROCESs_MODE,
# MAGIC DESCRIPTION
# MAGIC from(
# MAGIC select 
# MAGIC DESCRIPTION, 
# MAGIC EXTRACT_DATETIME,
# MAGIC EXTRACT_RUN_ID,
# MAGIC LANGU,
# MAGIC MANDT,
# MAGIC PROCESS_MODE,
# MAGIC PROCESS_TYPE,
# MAGIC row_number() over (partition by PROCESS_TYPE, MANDT order by EXTRACT_DATETIME desc) as rn 
# MAGIC from(
# MAGIC SELECT * from Source2 WHERE LANGU = 'E'
# MAGIC union all
# MAGIC select * from source WHERE LANGU = 'E'
# MAGIC )a
# MAGIC )b where b.rn = 1
# MAGIC except
# MAGIC select
# MAGIC PROCESS_TYPE,
# MAGIC PROCESs_MODE,
# MAGIC DESCRIPTION
# MAGIC from
# MAGIC cleansed.t_sapisu_zcd_tproctype_tx

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Target and Source Data
# MAGIC %sql
# MAGIC select
# MAGIC PROCESS_TYPE,
# MAGIC PROCESs_MODE,
# MAGIC DESCRIPTION
# MAGIC from
# MAGIC cleansed.t_sapisu_zcd_tproctype_tx
# MAGIC except
# MAGIC select 
# MAGIC PROCESS_TYPE,
# MAGIC PROCESs_MODE,
# MAGIC DESCRIPTION
# MAGIC from(
# MAGIC select 
# MAGIC DESCRIPTION, 
# MAGIC EXTRACT_DATETIME,
# MAGIC EXTRACT_RUN_ID,
# MAGIC LANGU,
# MAGIC MANDT,
# MAGIC PROCESS_MODE,
# MAGIC PROCESS_TYPE,
# MAGIC row_number() over (partition by PROCESS_TYPE, MANDT order by EXTRACT_DATETIME desc) as rn 
# MAGIC from(
# MAGIC SELECT * from Source2 WHERE LANGU = 'E'
# MAGIC union all
# MAGIC select * from source WHERE LANGU = 'E'
# MAGIC )a
# MAGIC )b where b.rn = 1
