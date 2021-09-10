# Databricks notebook source
# DBTITLE 1,[Config] Connection Setup
storage_account_name = "saswcnonprod01landingtst"
storage_account_access_key = dbutils.secrets.get(scope="Test-Access",key="test-blob-key")
container_name = "archive"
file_location = "wasbs://archive@saswcnonprod01landingtst.blob.core.windows.net/sapisu/20210902/20210902_00:23:52/TE227T_20210831124243.json"
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

# DBTITLE 1,[Source] Schema Check - Refer to Raw2Cleansed Mapping
df.printSchema()


# COMMAND ----------

# DBTITLE 0,[Result] Load Count Result into DataFrame
lakedf = spark.sql("select * from cleansed.t_sapisu_te227t")

# COMMAND ----------

# DBTITLE 1,[Target] Schema Check
lakedf.printSchema()

# COMMAND ----------

# DBTITLE 1,[Source] Creating Temporary Table
df.createOrReplaceTempView("Source")


# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from source

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct SPRAS from Source

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC *
# MAGIC FROM
# MAGIC cleansed.t_sapisu_te227t

# COMMAND ----------

lakedf.createOrReplaceTempView("Target")

# COMMAND ----------

# DBTITLE 1,[Verification] Count Checks
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from cleansed.t_sapisu_te227t
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from Source

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT REGPOLIT, COUNT (*) as count
# MAGIC FROM cleansed.t_sapisu_te227t
# MAGIC GROUP BY REGPOLIT
# MAGIC HAVING COUNT (*) > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT * FROM (
# MAGIC SELECT
# MAGIC *,
# MAGIC row_number() OVER(PARTITION BY REGPOLIT order by REGPOLIT) as rn
# MAGIC FROM cleansed.t_sapisu_te227t
# MAGIC )a where a.rn > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Source and Target Data
# MAGIC %sql
# MAGIC select
# MAGIC COUNTRY,
# MAGIC REGPOLIT,
# MAGIC REGNAME
# MAGIC from
# MAGIC Source
# MAGIC except
# MAGIC select
# MAGIC COUNTRY,
# MAGIC REGPOLIT,
# MAGIC REGNAME
# MAGIC from
# MAGIC cleansed.t_sapisu_te227t

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Target and Source Data
# MAGIC %sql
# MAGIC select
# MAGIC COUNTRY,
# MAGIC REGPOLIT,
# MAGIC REGNAME
# MAGIC from
# MAGIC cleansed.t_sapisu_te227t
# MAGIC 
# MAGIC except
# MAGIC select
# MAGIC COUNTRY,
# MAGIC REGPOLIT,
# MAGIC REGNAME
# MAGIC from
# MAGIC Source
