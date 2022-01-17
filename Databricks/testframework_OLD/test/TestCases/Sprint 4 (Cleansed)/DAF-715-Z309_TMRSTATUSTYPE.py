# Databricks notebook source
# DBTITLE 1,[Config] Connection Setup
storage_account_name = "sadaftest01"
storage_account_access_key = dbutils.secrets.get(scope="Test-Access",key="test-datalake-key")
container_name = "raw"
file_location = "wasbs://raw@sadaftest01.blob.core.windows.net/landing/accessarchive/Z309_TMRSTATUSTYPE.csv"
file_type = "csv"
print(storage_account_name)

# COMMAND ----------

spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
  storage_account_access_key)


# COMMAND ----------

# DBTITLE 1,[Source] loading to a dataframe
 df = spark.read.format("csv").option('delimiter','|').option('header','true').load("wasbs://raw@sadaftest01.blob.core.windows.net/landing/accessarchive/Z309_TMRSTATUSTYPE.csv")

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
# MAGIC C_METE_READ_STAT as meterReadingStatusCode
# MAGIC ,T_READ_STAT_ABBR as readingStatusAbbreviation
# MAGIC ,T_METE_READ_STAT as meterReadingStatus,
# MAGIC case when D_READ_STAT_EFFE <> 'null' then 
# MAGIC CONCAT(LEFT(D_READ_STAT_EFFE,4),'-',SUBSTRING(D_READ_STAT_EFFE,5,2),'-',RIGHT(D_READ_STAT_EFFE,2))
# MAGIC else D_READ_STAT_EFFE end as readingStatusEffectiveDate,
# MAGIC case when D_READ_STAT_CANC <> 'null' then 
# MAGIC CONCAT(LEFT(D_READ_STAT_CANC,4),'-',SUBSTRING(D_READ_STAT_CANC,5,2),'-',RIGHT(D_READ_STAT_CANC,2))
# MAGIC else D_READ_STAT_CANC end as readingStatusCancelledDate
# MAGIC 
# MAGIC from Source

# COMMAND ----------

# DBTITLE 1,[Target] displaying records
# MAGIC %sql
# MAGIC select * from cleansed.t_access_z309_tmrstatustype

# COMMAND ----------

# DBTITLE 0,[Result] Load Count Result into DataFrame
cleansedf = spark.sql("select * from cleansed.t_access_z309_tmrstatustype")

# COMMAND ----------

# DBTITLE 1,[Target] Schema Check
cleansedf.printSchema()

# COMMAND ----------

cleansedf.createOrReplaceTempView("Target")

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate checks
# MAGIC %sql
# MAGIC SELECT meterReadingStatusCode, COUNT (*) as count
# MAGIC FROM cleansed.t_access_z309_tmrstatustype
# MAGIC GROUP BY meterReadingStatusCode
# MAGIC HAVING COUNT (*) > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Records count check
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from cleansed.t_access_z309_tmrstatustype
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from Source

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Source and Target Data
# MAGIC %sql
# MAGIC select 
# MAGIC C_METE_READ_STAT as meterReadingStatusCode
# MAGIC ,T_READ_STAT_ABBR as readingStatusAbbreviation
# MAGIC ,T_METE_READ_STAT as meterReadingStatus,
# MAGIC case when D_READ_STAT_EFFE <> 'null' then 
# MAGIC CONCAT(LEFT(D_READ_STAT_EFFE,4),'-',SUBSTRING(D_READ_STAT_EFFE,5,2),'-',RIGHT(D_READ_STAT_EFFE,2))
# MAGIC else D_READ_STAT_EFFE end as readingStatusEffectiveDate,
# MAGIC case when D_READ_STAT_CANC <> 'null' then 
# MAGIC CONCAT(LEFT(D_READ_STAT_CANC,4),'-',SUBSTRING(D_READ_STAT_CANC,5,2),'-',RIGHT(D_READ_STAT_CANC,2))
# MAGIC else D_READ_STAT_CANC end as readingStatusCancelledDate
# MAGIC from Source
# MAGIC except
# MAGIC select 
# MAGIC meterReadingStatusCode
# MAGIC ,UPPER(readingStatusAbbreviation)
# MAGIC ,UPPER(meterReadingStatus)
# MAGIC ,readingStatusEffectiveDate
# MAGIC ,readingStatusCancelledDate
# MAGIC from cleansed.t_access_z309_tmrstatustype

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Target and Source Data
# MAGIC %sql
# MAGIC select 
# MAGIC meterReadingStatusCode
# MAGIC ,UPPER(readingStatusAbbreviation)
# MAGIC ,UPPER(meterReadingStatus)
# MAGIC ,readingStatusEffectiveDate
# MAGIC ,readingStatusCancelledDate
# MAGIC from cleansed.t_access_z309_tmrstatustype
# MAGIC except
# MAGIC select 
# MAGIC C_METE_READ_STAT as meterReadingStatusCode
# MAGIC ,T_READ_STAT_ABBR as readingStatusAbbreviation
# MAGIC ,T_METE_READ_STAT as meterReadingStatus,
# MAGIC case when D_READ_STAT_EFFE <> 'null' then 
# MAGIC CONCAT(LEFT(D_READ_STAT_EFFE,4),'-',SUBSTRING(D_READ_STAT_EFFE,5,2),'-',RIGHT(D_READ_STAT_EFFE,2))
# MAGIC else D_READ_STAT_EFFE end as readingStatusEffectiveDate,
# MAGIC case when D_READ_STAT_CANC <> 'null' then 
# MAGIC CONCAT(LEFT(D_READ_STAT_CANC,4),'-',SUBSTRING(D_READ_STAT_CANC,5,2),'-',RIGHT(D_READ_STAT_CANC,2))
# MAGIC else D_READ_STAT_CANC end as readingStatusCancelledDate
# MAGIC from Source
