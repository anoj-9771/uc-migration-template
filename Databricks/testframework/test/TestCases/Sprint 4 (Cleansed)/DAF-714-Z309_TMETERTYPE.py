# Databricks notebook source
# DBTITLE 1,[Config] Connection Setup
storage_account_name = "sadaftest01"
storage_account_access_key = dbutils.secrets.get(scope="Test-Access",key="test-datalake-key")
container_name = "raw"
file_location = "wasbs://raw@sadaftest01.blob.core.windows.net/landing/accessarchive/Z309_TMETERTYPE.csv"
file_type = "csv"
print(storage_account_name)

# COMMAND ----------

spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
  storage_account_access_key)


# COMMAND ----------

# DBTITLE 1,[Source] loading to a dataframe
 df = spark.read.format("csv").option('delimiter','|').option('header','true').load("wasbs://raw@sadaftest01.blob.core.windows.net/landing/accessarchive/Z309_TMETERTYPE.csv")

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
# MAGIC C_METE_TYPE as meterTypeCode
# MAGIC ,CAST(Q_METE_SIZE AS DECIMAL(5,2)) AS meterSize
# MAGIC ,T_METE_TYPE_ABBR as meterSizeUnit,
# MAGIC T_METE_TYPE as meterType,
# MAGIC case when D_METE_TYPE_EFFE <> 'null' then 
# MAGIC CONCAT(LEFT(D_METE_TYPE_EFFE,4),'-',SUBSTRING(D_METE_TYPE_EFFE,5,2),'-',RIGHT(D_METE_TYPE_EFFE,2))
# MAGIC else D_METE_TYPE_EFFE end as meterTypeEffectiveDate,
# MAGIC case when D_METE_TYPE_CANC <> 'null' then 
# MAGIC CONCAT(LEFT(D_METE_TYPE_CANC,4),'-',SUBSTRING(D_METE_TYPE_CANC,5,2),'-',RIGHT(D_METE_TYPE_CANC,2))
# MAGIC else D_METE_TYPE_CANC end as meterTypeCancelledDate,
# MAGIC C_METR_METE_TYPE as equivMetricMeterTypeCode,
# MAGIC case when F_SMAL_METE = 'Y' then 'true' else 'false' end as isSmallMeter
# MAGIC 
# MAGIC from Source

# COMMAND ----------

# DBTITLE 1,[Target] displaying records
# MAGIC %sql
# MAGIC select * from cleansed.t_access_z309_tmetertype

# COMMAND ----------

# DBTITLE 0,[Result] Load Count Result into DataFrame
cleansedf = spark.sql("select * from cleansed.t_access_z309_tmetertype")

# COMMAND ----------

# DBTITLE 1,[Target] Schema Check
cleansedf.printSchema()

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate checks
# MAGIC %sql
# MAGIC SELECT meterTypeCode, COUNT (*) as count
# MAGIC FROM cleansed.t_access_z309_tmetertype
# MAGIC GROUP BY meterTypeCode
# MAGIC HAVING COUNT (*) > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Records count check
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from cleansed.t_access_z309_tmetertype
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from Source

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Source and Target Data
# MAGIC %sql
# MAGIC select 
# MAGIC C_METE_TYPE as meterTypeCode
# MAGIC ,CAST(Q_METE_SIZE AS DECIMAL(5,2)) AS meterSize
# MAGIC ,T_METE_TYPE_ABBR as meterSizeUnit,
# MAGIC T_METE_TYPE as meterType,
# MAGIC case when D_METE_TYPE_EFFE <> 'null' then 
# MAGIC CONCAT(LEFT(D_METE_TYPE_EFFE,4),'-',SUBSTRING(D_METE_TYPE_EFFE,5,2),'-',RIGHT(D_METE_TYPE_EFFE,2))
# MAGIC else D_METE_TYPE_EFFE end as meterTypeEffectiveDate,
# MAGIC case when D_METE_TYPE_CANC <> 'null' then 
# MAGIC CONCAT(LEFT(D_METE_TYPE_CANC,4),'-',SUBSTRING(D_METE_TYPE_CANC,5,2),'-',RIGHT(D_METE_TYPE_CANC,2))
# MAGIC else D_METE_TYPE_CANC end as meterTypeCancelledDate,
# MAGIC C_METR_METE_TYPE as equivMetricMeterTypeCode,
# MAGIC case when F_SMAL_METE = 'Y' then true else false end as isSmallMeter
# MAGIC from Source
# MAGIC except
# MAGIC select 
# MAGIC meterTypeCode,
# MAGIC meterSize,
# MAGIC UPPER(meterSizeUnit),
# MAGIC UPPER(meterType),
# MAGIC meterTypeEffectiveDate,
# MAGIC meterTypeCancelledDate,
# MAGIC equivMetricMeterTypeCode,
# MAGIC isSmallMeter
# MAGIC from cleansed.t_access_z309_tmetertype

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Target and Source Data
# MAGIC %sql
# MAGIC select 
# MAGIC meterTypeCode,
# MAGIC meterSize,
# MAGIC UPPER(meterSizeUnit),
# MAGIC UPPER(meterType),
# MAGIC meterTypeEffectiveDate,
# MAGIC meterTypeCancelledDate,
# MAGIC equivMetricMeterTypeCode,
# MAGIC isSmallMeter
# MAGIC from cleansed.t_access_z309_tmetertype
# MAGIC EXCEPT
# MAGIC select 
# MAGIC C_METE_TYPE as meterTypeCode
# MAGIC ,CAST(Q_METE_SIZE AS DECIMAL(5,2)) AS meterSize
# MAGIC ,T_METE_TYPE_ABBR as meterSizeUnit,
# MAGIC T_METE_TYPE as meterType,
# MAGIC case when D_METE_TYPE_EFFE <> 'null' then 
# MAGIC CONCAT(LEFT(D_METE_TYPE_EFFE,4),'-',SUBSTRING(D_METE_TYPE_EFFE,5,2),'-',RIGHT(D_METE_TYPE_EFFE,2))
# MAGIC else D_METE_TYPE_EFFE end as meterTypeEffectiveDate,
# MAGIC case when D_METE_TYPE_CANC <> 'null' then 
# MAGIC CONCAT(LEFT(D_METE_TYPE_CANC,4),'-',SUBSTRING(D_METE_TYPE_CANC,5,2),'-',RIGHT(D_METE_TYPE_CANC,2))
# MAGIC else D_METE_TYPE_CANC end as meterTypeCancelledDate,
# MAGIC C_METR_METE_TYPE as equivMetricMeterTypeCode,
# MAGIC case when F_SMAL_METE = 'Y' then true else false end as isSmallMeter
# MAGIC 
# MAGIC from Source
