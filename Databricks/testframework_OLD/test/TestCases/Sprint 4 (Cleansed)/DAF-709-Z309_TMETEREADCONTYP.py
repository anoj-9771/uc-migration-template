# Databricks notebook source
# DBTITLE 1,[Config] Connection Setup
storage_account_name = "sadaftest01"
storage_account_access_key = dbutils.secrets.get(scope="Test-Access",key="test-datalake-key")
container_name = "raw"
file_location = "wasbs://raw@sadaftest01.blob.core.windows.net/landing/accessarchive/Z309_TMETEREADCONTYP.csv"
file_type = "csv"
print(storage_account_name)

# COMMAND ----------

spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
  storage_account_access_key)


# COMMAND ----------

# DBTITLE 1,[Source] loading to a dataframe
 df = spark.read.format("csv").option('delimiter','|').option('header','true').load("wasbs://raw@sadaftest01.blob.core.windows.net/landing/accessarchive/Z309_TMETEREADCONTYP.csv")

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
# MAGIC C_METE_READ_CONS as consumptionTypeCode,
# MAGIC T_METE_READ_CONS as consumptionType,
# MAGIC case when D_READ_CONS_EFFE <> 'null' then 
# MAGIC CONCAT(LEFT(D_READ_CONS_EFFE,4),'-',SUBSTRING(D_READ_CONS_EFFE,5,2),'-',RIGHT(D_READ_CONS_EFFE,2))
# MAGIC else D_READ_CONS_EFFE end as consumptionTypeEffectiveDate,
# MAGIC case when D_READ_CONS_CANC <> 'null' then 
# MAGIC CONCAT(LEFT(D_READ_CONS_CANC,4),'-',SUBSTRING(D_READ_CONS_CANC,5,2),'-',RIGHT(D_READ_CONS_CANC,2))
# MAGIC else D_READ_CONS_CANC end as consumptionTypeCancelledDate
# MAGIC from Source

# COMMAND ----------

# DBTITLE 1,[Target] displaying records
# MAGIC %sql
# MAGIC select * from cleansed.t_access_z309_tmetereadcontyp

# COMMAND ----------

# DBTITLE 0,[Result] Load Count Result into DataFrame
cleansedf = spark.sql("select * from cleansed.t_access_z309_tmetereadcontyp")

# COMMAND ----------

# DBTITLE 1,[Target] Schema Check
cleansedf.printSchema()

# COMMAND ----------

cleansedf.createOrReplaceTempView("Target")

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate checks
# MAGIC %sql
# MAGIC SELECT consumptionTypeCode, COUNT (*) as count
# MAGIC FROM cleansed.t_access_z309_tmetereadcontyp
# MAGIC GROUP BY consumptionTypeCode
# MAGIC HAVING COUNT (*) > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Records count check
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from cleansed.t_access_z309_tmetereadcontyp
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from Source

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Source and Target Data
# MAGIC %sql
# MAGIC select 
# MAGIC C_METE_READ_CONS as consumptionTypeCode,
# MAGIC T_METE_READ_CONS as consumptionType,
# MAGIC case when D_READ_CONS_EFFE <> 'null' then 
# MAGIC CONCAT(LEFT(D_READ_CONS_EFFE,4),'-',SUBSTRING(D_READ_CONS_EFFE,5,2),'-',RIGHT(D_READ_CONS_EFFE,2))
# MAGIC else D_READ_CONS_EFFE end as consumptionTypeEffectiveDate,
# MAGIC case when D_READ_CONS_CANC <> 'null' then 
# MAGIC CONCAT(LEFT(D_READ_CONS_CANC,4),'-',SUBSTRING(D_READ_CONS_CANC,5,2),'-',RIGHT(D_READ_CONS_CANC,2))
# MAGIC else D_READ_CONS_CANC end as consumptionTypeCancelledDate
# MAGIC from Source
# MAGIC except
# MAGIC 
# MAGIC select 
# MAGIC consumptionTypeCode,
# MAGIC UPPER(consumptionType),
# MAGIC consumptionTypeEffectiveDate,
# MAGIC consumptionTypeCancelledDate
# MAGIC from cleansed.t_access_z309_tmetereadcontyp

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Target and Source Data
# MAGIC %sql
# MAGIC select 
# MAGIC consumptionTypeCode,
# MAGIC UPPER(consumptionType),
# MAGIC consumptionTypeEffectiveDate,
# MAGIC consumptionTypeCancelledDate
# MAGIC from cleansed.t_access_z309_tmetereadcontyp
# MAGIC except
# MAGIC select 
# MAGIC C_METE_READ_CONS as consumptionTypeCode,
# MAGIC T_METE_READ_CONS as consumptionType,
# MAGIC case when D_READ_CONS_EFFE <> 'null' then 
# MAGIC CONCAT(LEFT(D_READ_CONS_EFFE,4),'-',SUBSTRING(D_READ_CONS_EFFE,5,2),'-',RIGHT(D_READ_CONS_EFFE,2))
# MAGIC else D_READ_CONS_EFFE end as consumptionTypeEffectiveDate,
# MAGIC case when D_READ_CONS_CANC <> 'null' then 
# MAGIC CONCAT(LEFT(D_READ_CONS_CANC,4),'-',SUBSTRING(D_READ_CONS_CANC,5,2),'-',RIGHT(D_READ_CONS_CANC,2))
# MAGIC else D_READ_CONS_CANC end as consumptionTypeCancelledDate
# MAGIC from Source
