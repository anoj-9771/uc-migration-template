# Databricks notebook source
# DBTITLE 1,[Config] Connection Setup
storage_account_name = "sadaftest01"
storage_account_access_key = dbutils.secrets.get(scope="Test-Access",key="test-datalake-key")
container_name = "raw"
file_location = "wasbs://raw@sadaftest01.blob.core.windows.net/landing/accessarchive/Z309_TDEBIT.csv"
file_type = "csv"
print(storage_account_name)

# COMMAND ----------

spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
  storage_account_access_key)


# COMMAND ----------

# DBTITLE 1,[Source] loading to a dataframe
 df = spark.read.format("csv").option('delimiter','|').option('header','true').load("wasbs://raw@sadaftest01.blob.core.windows.net/landing/accessarchive/Z309_TDEBIT.csv")

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
# MAGIC C_LGA as LGACode,
# MAGIC 
# MAGIC N_PROP as propertyNumber,
# MAGIC N_DEBI_REFE as debitReferenceNumber ,
# MAGIC C_DEBI_TYPE as debitTypeCode
# MAGIC 
# MAGIC 
# MAGIC from Source

# COMMAND ----------

# DBTITLE 1,[Target] displaying records
# MAGIC %sql
# MAGIC select * from cleansed.t_access_z309_tdebit

# COMMAND ----------

# DBTITLE 0,[Result] Load Count Result into DataFrame
cleansedf = spark.sql("select * from cleansed.t_access_z309_tmetercantread")

# COMMAND ----------

# DBTITLE 1,[Target] Schema Check
cleansedf.printSchema()

# COMMAND ----------

cleansedf.createOrReplaceTempView("Target")

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate checks
# MAGIC %sql
# MAGIC SELECT 
# MAGIC cannotReadReason, COUNT (*) as count
# MAGIC FROM cleansed.t_access_z309_tmetercantread
# MAGIC GROUP BY cannotReadReason
# MAGIC HAVING COUNT (*) > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Records count check
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from cleansed.t_access_z309_tmetercantread
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from Source

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Source and Target Data
# MAGIC %sql
# MAGIC select 
# MAGIC C_METE_CANT_READ as cannotReadCode
# MAGIC ,T_METE_CANT_READ as cannotReadReason,
# MAGIC case when D_CANT_READ_EFFE <> 'null' then 
# MAGIC CONCAT(LEFT(D_CANT_READ_EFFE,4),'-',SUBSTRING(D_CANT_READ_EFFE,5,2),'-',RIGHT(D_CANT_READ_EFFE,2))
# MAGIC else D_CANT_READ_EFFE end as cannotReadEffectiveDate,
# MAGIC case when D_CANT_READ_CANC <> 'null' then 
# MAGIC CONCAT(LEFT(D_CANT_READ_CANC,4),'-',SUBSTRING(D_CANT_READ_CANC,5,2),'-',RIGHT(D_CANT_READ_CANC,2))
# MAGIC else D_CANT_READ_CANC end as cannotReadCancelledDate,
# MAGIC T_CANT_READ_ABBR as cannotReadAbbreviation
# MAGIC from Source
# MAGIC except
# MAGIC select 
# MAGIC cannotReadCode,
# MAGIC UPPER(cannotReadReason),
# MAGIC cannotReadEffectiveDate,
# MAGIC cannotReadCancelledDate,
# MAGIC cannotReadAbbreviation
# MAGIC from cleansed.t_access_z309_tmetercantread

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Target and Source Data
# MAGIC %sql
# MAGIC select 
# MAGIC cannotReadCode,
# MAGIC UPPER(cannotReadReason),
# MAGIC cannotReadEffectiveDate,
# MAGIC cannotReadCancelledDate,
# MAGIC cannotReadAbbreviation
# MAGIC from cleansed.t_access_z309_tmetercantread
# MAGIC except
# MAGIC select 
# MAGIC C_METE_CANT_READ as cannotReadCode
# MAGIC ,T_METE_CANT_READ as cannotReadReason,
# MAGIC case when D_CANT_READ_EFFE <> 'null' then 
# MAGIC CONCAT(LEFT(D_CANT_READ_EFFE,4),'-',SUBSTRING(D_CANT_READ_EFFE,5,2),'-',RIGHT(D_CANT_READ_EFFE,2))
# MAGIC else D_CANT_READ_EFFE end as cannotReadEffectiveDate,
# MAGIC case when D_CANT_READ_CANC <> 'null' then 
# MAGIC CONCAT(LEFT(D_CANT_READ_CANC,4),'-',SUBSTRING(D_CANT_READ_CANC,5,2),'-',RIGHT(D_CANT_READ_CANC,2))
# MAGIC else D_CANT_READ_CANC end as cannotReadCancelledDate,
# MAGIC T_CANT_READ_ABBR as cannotReadAbbreviation
# MAGIC 
# MAGIC from Source
