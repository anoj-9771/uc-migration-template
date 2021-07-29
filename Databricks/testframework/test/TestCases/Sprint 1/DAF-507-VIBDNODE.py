# Databricks notebook source
# DBTITLE 1,[Config] Connection Setup
storage_account_name = "saswcnonprod01landingtst"
storage_account_access_key = dbutils.secrets.get(scope="Test-Access",key="test-blob-key")
container_name = "test"
file_location = "wasbs://test@saswcnonprod01landingtst.blob.core.windows.net/VIBDNODE_28072021.csv"
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
df.createOrReplaceTempView("Source")

# COMMAND ----------

# DBTITLE 1,[Source] Displaying Records
# MAGIC %sql
# MAGIC select * from Source

# COMMAND ----------

# MAGIC %sql
# MAGIC select MANDT, `INTRENO        `, TREE, `AONR_AO      `   from Source

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from raw.sap_vibdnode

# COMMAND ----------

# MAGIC %sql
# MAGIC select count (*) as RecordCount from raw.sap_vibdnode
# MAGIC union all
# MAGIC select count (*) from Source

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC INTRENO from Source limit 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct  from (
# MAGIC select MANDT,INTRENO,TREE,AOTYPE_AO,AONR_AO,PARENT,AOTYPE_PA,AONR_PA from raw.sap_vibdnode
# MAGIC order by INTRENO asc)
# MAGIC limit 100000

# COMMAND ----------

# MAGIC %sql
# MAGIC (select MANDT,
# MAGIC INTRENO,
# MAGIC case when TREE='' then 'null',
# MAGIC AOTYPE_AO,
# MAGIC AONR_AO,
# MAGIC PARENT,
# MAGIC AOTYPE_PA,
# MAGIC AONR_PA 
# MAGIC from Source limit 1)
# MAGIC 
# MAGIC except
# MAGIC 
# MAGIC (select * from (
# MAGIC select MANDT,INTRENO,TREE,AOTYPE_AO,AONR_AO,PARENT,AOTYPE_PA,AONR_PA from raw.sap_vibdnode
# MAGIC order by INTRENO asc)
# MAGIC limit 1)

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Source and Target Data
# MAGIC %sql
# MAGIC select * from Source
# MAGIC 
# MAGIC except
# MAGIC 
# MAGIC select * from (
# MAGIC select MANDT,INTRENO,TREE,AOTYPE_AO,AONR_AO,PARENT,AOTYPE_PA,AONR_PA from raw.sap_vibdnode
# MAGIC order by INTRENO asc)
# MAGIC limit 100000
# MAGIC  

# COMMAND ----------

# DBTITLE 0,[Result] Load Count Result into DataFrame
lakedf = spark.sql("select * from raw.sap_vibdnode")

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

# DBTITLE 1,[Verification] Compare Target and Source Data
# MAGIC %sql
# MAGIC select * from Source
# MAGIC except
# MAGIC select * from Target
