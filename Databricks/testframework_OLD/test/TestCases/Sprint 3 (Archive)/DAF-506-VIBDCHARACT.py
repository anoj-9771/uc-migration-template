# Databricks notebook source
# DBTITLE 1,[Config] Connection Setup
storage_account_name = "saswcnonprod01landingtst"
storage_account_access_key = dbutils.secrets.get(scope="Test-Access",key="test-blob-key")
container_name = "archive"
file_location = "wasbs://archive@saswcnonprod01landingtst.blob.core.windows.net/archive/sapisu/vibdcharact/json/year=2021/month=08/day=05/VIBDCHARACT_20210805103817.json"
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
# MAGIC select * from raw.sapisu_vibdcharact

# COMMAND ----------

# DBTITLE 0,[Result] Load Count Result into DataFrame
lakedf = spark.sql("select * from raw.sapisu_vibdcharact")

# COMMAND ----------

# DBTITLE 1,[Target] Schema Check
lakedf.printSchema()

# COMMAND ----------

lakedf.createOrReplaceTempView("Target")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from Target
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from Source

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Source and Target Data
# MAGIC %sql
# MAGIC select 
# MAGIC   * from Source
# MAGIC except
# MAGIC select  
# MAGIC    ADDITIONALINFO ,
# MAGIC   AMOUNTPERAREA ,
# MAGIC   CHARACTAMTABS ,
# MAGIC   CHARACTAMTAREA ,
# MAGIC   CHARACTCOUNT ,
# MAGIC   CHARACTPERCENT ,
# MAGIC   CURRENCY ,
# MAGIC   EXTRACT_DATETIME ,
# MAGIC   EXTRACT_RUN_ID ,
# MAGIC   FFCTACCURATE ,
# MAGIC   FIXFITCHARACT ,
# MAGIC   INTRENO ,
# MAGIC   MANDT ,
# MAGIC   MODERNMEASURE ,
# MAGIC   RESULTVAL ,
# MAGIC   SUPPLEMENTINFO ,
# MAGIC   VALIDFROM ,
# MAGIC   VALIDTO ,
# MAGIC   WEIGHT 
# MAGIC 
# MAGIC  from Target

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Target and Source Data
# MAGIC %sql
# MAGIC select  
# MAGIC  ADDITIONALINFO ,
# MAGIC   AMOUNTPERAREA ,
# MAGIC   CHARACTAMTABS ,
# MAGIC   CHARACTAMTAREA ,
# MAGIC   CHARACTCOUNT ,
# MAGIC   CHARACTPERCENT ,
# MAGIC   CURRENCY ,
# MAGIC   EXTRACT_DATETIME ,
# MAGIC   EXTRACT_RUN_ID ,
# MAGIC   FFCTACCURATE ,
# MAGIC   FIXFITCHARACT ,
# MAGIC   INTRENO ,
# MAGIC   MANDT ,
# MAGIC   MODERNMEASURE ,
# MAGIC   RESULTVAL ,
# MAGIC   SUPPLEMENTINFO ,
# MAGIC   VALIDFROM ,
# MAGIC   VALIDTO ,
# MAGIC   WEIGHT 
# MAGIC   from Target
# MAGIC  except
# MAGIC  select 
# MAGIC   * from Source
