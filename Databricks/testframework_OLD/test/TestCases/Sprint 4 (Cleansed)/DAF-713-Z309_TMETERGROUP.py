# Databricks notebook source
# DBTITLE 1,[Config] Connection Setup
storage_account_name = "sadaftest01"
storage_account_access_key = dbutils.secrets.get(scope="Test-Access",key="test-datalake-key")
container_name = "raw"
file_location = "wasbs://raw@sadaftest01.blob.core.windows.net/landing/accessarchive/Z309_TMETERGROUP.csv"
file_type = "csv"
print(storage_account_name)

# COMMAND ----------

spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
  storage_account_access_key)


# COMMAND ----------

# DBTITLE 1,[Source] loading to a dataframe
 df = spark.read.format("csv").option('delimiter','|').option('header','true').load("wasbs://raw@sadaftest01.blob.core.windows.net/landing/accessarchive/Z309_TMETERGROUP.csv")

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.createOrReplaceTempView("Source")

# COMMAND ----------

# DBTITLE 1,[Source] displaying records
# MAGIC %sql
# MAGIC select * from Source

# COMMAND ----------

# DBTITLE 1,[Source] displaying records after mapping
# MAGIC %sql
# MAGIC select 
# MAGIC C_METE_GROU as meterGroupCode
# MAGIC ,T_METE_GROU as meterGroup,
# MAGIC case when D_METE_GROU_EFFE <> 'null' then 
# MAGIC CONCAT(LEFT(D_METE_GROU_EFFE,4),'-',SUBSTRING(D_METE_GROU_EFFE,5,2),'-',RIGHT(D_METE_GROU_EFFE,2))
# MAGIC else D_METE_GROU_EFFE end as meterGroupEffectiveDate,
# MAGIC case when D_METE_GROU_CANC <> 'null' then 
# MAGIC CONCAT(LEFT(D_METE_GROU_CANC,4),'-',SUBSTRING(D_METE_GROU_CANC,5,2),'-',RIGHT(D_METE_GROU_CANC,2))
# MAGIC else D_METE_GROU_CANC end as meterGroupCancelledDate
# MAGIC 
# MAGIC from Source

# COMMAND ----------

# DBTITLE 1,[Target] displaying records
# MAGIC %sql
# MAGIC select * from cleansed.t_access_z309_tmetergroup

# COMMAND ----------

# DBTITLE 0,[Result] Load Count Result into DataFrame
cleansedf = spark.sql("select * from cleansed.t_access_z309_tmetergroup")

# COMMAND ----------

# DBTITLE 1,[Target] Schema Check
cleansedf.printSchema()

# COMMAND ----------

cleansedf.createOrReplaceTempView("Target")

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate checks
# MAGIC %sql
# MAGIC SELECT meterGroupCode, COUNT (*) as count
# MAGIC FROM cleansed.t_access_z309_tmetergroup
# MAGIC GROUP BY meterGroupCode
# MAGIC HAVING COUNT (*) > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Records count check
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from cleansed.t_access_z309_tmetergroup
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from Source

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Source and Target Data
# MAGIC %sql
# MAGIC select 
# MAGIC C_METE_GROU as meterGroupCode
# MAGIC ,T_METE_GROU as meterGroup,
# MAGIC case when D_METE_GROU_EFFE <> 'null' then 
# MAGIC CONCAT(LEFT(D_METE_GROU_EFFE,4),'-',SUBSTRING(D_METE_GROU_EFFE,5,2),'-',RIGHT(D_METE_GROU_EFFE,2))
# MAGIC else D_METE_GROU_EFFE end as meterGroupEffectiveDate,
# MAGIC case when D_METE_GROU_CANC <> 'null' then 
# MAGIC CONCAT(LEFT(D_METE_GROU_CANC,4),'-',SUBSTRING(D_METE_GROU_CANC,5,2),'-',RIGHT(D_METE_GROU_CANC,2))
# MAGIC else D_METE_GROU_CANC end as meterGroupCancelledDate
# MAGIC from Source
# MAGIC except
# MAGIC 
# MAGIC select 
# MAGIC meterGroupCode,
# MAGIC UPPER(meterGroup),
# MAGIC meterGroupEffectiveDate,
# MAGIC meterGroupCancelledDate
# MAGIC from cleansed.t_access_z309_tmetergroup

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Target and Source Data
# MAGIC %sql
# MAGIC select 
# MAGIC meterGroupCode,
# MAGIC UPPER(meterGroup),
# MAGIC meterGroupEffectiveDate,
# MAGIC meterGroupCancelledDate
# MAGIC from cleansed.t_access_z309_tmetergroup
# MAGIC except
# MAGIC select 
# MAGIC C_METE_GROU as meterGroupCode
# MAGIC ,T_METE_GROU as meterGroup,
# MAGIC case when D_METE_GROU_EFFE <> 'null' then 
# MAGIC CONCAT(LEFT(D_METE_GROU_EFFE,4),'-',SUBSTRING(D_METE_GROU_EFFE,5,2),'-',RIGHT(D_METE_GROU_EFFE,2))
# MAGIC else D_METE_GROU_EFFE end as meterGroupEffectiveDate,
# MAGIC case when D_METE_GROU_CANC <> 'null' then 
# MAGIC CONCAT(LEFT(D_METE_GROU_CANC,4),'-',SUBSTRING(D_METE_GROU_CANC,5,2),'-',RIGHT(D_METE_GROU_CANC,2))
# MAGIC else D_METE_GROU_CANC end as meterGroupCancelledDate
# MAGIC from Source
