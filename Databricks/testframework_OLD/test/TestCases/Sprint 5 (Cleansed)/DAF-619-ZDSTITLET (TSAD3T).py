# Databricks notebook source
# DBTITLE 1,[Config] Connection Setup
storage_account_name = "saswcnonprod01landingtst"
storage_account_access_key = dbutils.secrets.get(scope="Test-Access",key="test-blob-key")
container_name = "archive"
file_location = "wasbs://archive@saswcnonprod01landingtst.blob.core.windows.net/sapisu/20210901/20210901_13:17:34/TSAD3T_20210831124243.json"
file_location2 = "wasbs://archive@saswcnonprod01landingtst.blob.core.windows.net/sapisu/20210901/20210901_17:24:57/TSAD3T_20210901162623.json"
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
lakedf = spark.sql("select * from cleansed.t_sapisu_tsad3t")

# COMMAND ----------

# DBTITLE 1,[Target] Schema Check
lakedf.printSchema()

# COMMAND ----------

# DBTITLE 1,[Source] Creating Temporary Table
df.createOrReplaceTempView("Source")
df2.createOrReplaceTempView("Source2")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Source

# COMMAND ----------

# DBTITLE 1,[Source] with mapping
# MAGIC %sql
# MAGIC select 
# MAGIC title as titlecode,
# MAGIC title_medi as title
# MAGIC from(
# MAGIC select 
# MAGIC client, 
# MAGIC extract_datetime,
# MAGIC extract_run_id,
# MAGIC langu,
# MAGIC title,
# MAGIC title_medi,
# MAGIC row_number() over (partition by client, title order by extract_datetime desc) as rn 
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
# MAGIC cleansed.t_sapisu_tsad3t

# COMMAND ----------

lakedf.createOrReplaceTempView("Target")

# COMMAND ----------

# DBTITLE 1,[Verification] Count Checks
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from cleansed.t_sapisu_tsad3t
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from (select 
# MAGIC title as titlecode,
# MAGIC title_medi as title
# MAGIC from(
# MAGIC select 
# MAGIC client, 
# MAGIC extract_datetime,
# MAGIC extract_run_id,
# MAGIC langu,
# MAGIC title,
# MAGIC title_medi,
# MAGIC row_number() over (partition by client, title order by extract_datetime desc) as rn 
# MAGIC from(
# MAGIC SELECT * from Source2 where langu = 'E'
# MAGIC union all
# MAGIC select * from source where langu = 'E'
# MAGIC )a
# MAGIC )b where b.rn = 1)a

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT titlecode, COUNT (*) as count
# MAGIC FROM cleansed.t_sapisu_tsad3t
# MAGIC GROUP BY titlecode
# MAGIC HAVING COUNT (*) > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Source and Target Data
# MAGIC %sql
# MAGIC select 
# MAGIC title as titlecode,
# MAGIC title_medi as title
# MAGIC from(
# MAGIC select 
# MAGIC client, 
# MAGIC extract_datetime,
# MAGIC extract_run_id,
# MAGIC langu,
# MAGIC title,
# MAGIC title_medi,
# MAGIC row_number() over (partition by client, title order by extract_datetime desc) as rn 
# MAGIC from(
# MAGIC SELECT * from Source2 where langu ='E'
# MAGIC union all
# MAGIC select * from source where langu ='E'
# MAGIC )a
# MAGIC )b where b.rn = 1
# MAGIC EXCEPT
# MAGIC select
# MAGIC *
# MAGIC FROM
# MAGIC cleansed.t_sapisu_tsad3t

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Target and Source Data
# MAGIC %sql
# MAGIC select
# MAGIC *
# MAGIC FROM
# MAGIC cleansed.t_sapisu_tsad3t
# MAGIC EXCEPT
# MAGIC 
# MAGIC select 
# MAGIC title as titlecode,
# MAGIC title_medi as title
# MAGIC from(
# MAGIC select 
# MAGIC client, 
# MAGIC extract_datetime,
# MAGIC extract_run_id,
# MAGIC langu,
# MAGIC title,
# MAGIC title_medi,
# MAGIC row_number() over (partition by client, title order by extract_datetime desc) as rn 
# MAGIC from(
# MAGIC SELECT * from Source2 where langu = 'E'
# MAGIC union all
# MAGIC select * from source where langu = 'E'
# MAGIC )a
# MAGIC )b where b.rn = 1
