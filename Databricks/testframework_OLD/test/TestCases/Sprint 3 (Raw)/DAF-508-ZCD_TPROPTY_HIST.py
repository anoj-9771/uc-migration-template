# Databricks notebook source
# DBTITLE 1,[Config] Connection Setup
storage_account_name = "saswcnonprod01landingtst"
storage_account_access_key = dbutils.secrets.get(scope="Test-Access",key="test-blob-key")
container_name = "archive"
file_location = "wasbs://archive@saswcnonprod01landingtst.blob.core.windows.net/archive/sapisu/zcd_tpropty_hist/json/year=2021/month=08/day=05/ZCD_TPROPTY_HIST_20210804141617.json"
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

# DBTITLE 0,[Result] Load Count Result into DataFrame
lakedf = spark.sql("select * from raw.sapisu_zcd_tpropty_hist")

# COMMAND ----------

# DBTITLE 1,[Target] Schema Check
lakedf.printSchema()

# COMMAND ----------

# DBTITLE 1,[Source] Creating Temporary Table
df.createOrReplaceTempView("Source")

# COMMAND ----------

# DBTITLE 1,[Source] Displaying Records
# MAGIC %sql
# MAGIC select * from Source

# COMMAND ----------

# DBTITLE 1,[Target] Displaying Records
# MAGIC %sql
# MAGIC select * from raw.sapisu_zcd_tpropty_hist

# COMMAND ----------

# DBTITLE 1,[Verification] Record Count Check
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from raw.sapisu_zcd_tpropty_hist
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from Source

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Source and Target Data
# MAGIC %sql
# MAGIC select  *                     
# MAGIC from Source
# MAGIC except
# MAGIC select
# MAGIC   CHANGED_BY, 
# MAGIC   CHANGED_ON ,
# MAGIC   CREATED_BY ,
# MAGIC   CREATED_ON ,
# MAGIC   DATE_FROM ,
# MAGIC   DATE_TO ,
# MAGIC   EXTRACT_DATETIME ,
# MAGIC   EXTRACT_RUN_ID ,
# MAGIC   INF_PROP_TYPE ,
# MAGIC   MANDT ,
# MAGIC   PROPERTY_NO ,
# MAGIC   SUP_PROP_TYPE 
# MAGIC                            
# MAGIC from raw.sapisu_zcd_tpropty_hist

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Target and Source Data
# MAGIC %sql
# MAGIC select
# MAGIC   CHANGED_BY, 
# MAGIC   CHANGED_ON ,
# MAGIC   CREATED_BY ,
# MAGIC   CREATED_ON ,
# MAGIC   DATE_FROM ,
# MAGIC   DATE_TO ,
# MAGIC   EXTRACT_DATETIME ,
# MAGIC   EXTRACT_RUN_ID ,
# MAGIC   INF_PROP_TYPE ,
# MAGIC   MANDT ,
# MAGIC   PROPERTY_NO ,
# MAGIC   SUP_PROP_TYPE                           
# MAGIC from raw.sapisu_zcd_tpropty_hist
# MAGIC except
# MAGIC select
# MAGIC *                            
# MAGIC from Source
