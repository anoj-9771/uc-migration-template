# Databricks notebook source
# DBTITLE 1,[Config] Connection Setup
storage_account_name = "sadaftest01"
storage_account_access_key = dbutils.secrets.get(scope="TestScope",key="test-sadaf-key")
container_name = "raw"
file_location = "wasbs://raw@sadaftest01.blob.core.windows.net/landing/accessarchive/Z309_TMETERCLASS.csv"
file_type = "csv"
print(storage_account_name)

# COMMAND ----------

spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
  storage_account_access_key)


# COMMAND ----------

# DBTITLE 1,[Source] loading to a dataframe
 df = spark.read.format("csv").option('delimiter','|').option('header','true').load("wasbs://raw@sadaftest01.blob.core.windows.net/landing/accessarchive/Z309_TMETERCLASS.csv")

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
# MAGIC C_METE_CLAS as meterClassCode
# MAGIC ,T_METE_CLAS as meterClass
# MAGIC ,T_METE_CLAS_ABBR as meterClassAbbreviation,
# MAGIC case when D_METE_CLAS_EFFE <> 'null' then 
# MAGIC CONCAT(LEFT(D_METE_CLAS_EFFE,4),'-',SUBSTRING(D_METE_CLAS_EFFE,5,2),'-',RIGHT(D_METE_CLAS_EFFE,2))
# MAGIC else D_METE_CLAS_EFFE end as meterClassEffectiveDate,
# MAGIC case when D_METE_CLAS_CANC <> 'null' then 
# MAGIC CONCAT(LEFT(D_METE_CLAS_CANC,4),'-',SUBSTRING(D_METE_CLAS_CANC,5,2),'-',RIGHT(D_METE_CLAS_CANC,2))
# MAGIC else D_METE_CLAS_CANC end as meterClassCancelledDate,
# MAGIC --C_WATE_METE_TYPE as waterMeterType
# MAGIC 
# MAGIC --C_WATE_METE_TYPE as waterMeterType
# MAGIC case when C_WATE_METE_TYPE = 'P' then 'Potable'
# MAGIC when C_WATE_METE_TYPE = 'R' then 'Recycled'
# MAGIC when C_WATE_METE_TYPE = 'O' then 'Other'
# MAGIC else
# MAGIC C_WATE_METE_TYPE end as waterMeterType
# MAGIC from Source

# COMMAND ----------

# DBTITLE 1,[Target] displaying records
# MAGIC %sql
# MAGIC select * from cleansed.t_access_z309_tmeterclass

# COMMAND ----------

# DBTITLE 0,[Result] Load Count Result into DataFrame
cleansedf = spark.sql("select * from cleansed.t_access_z309_tmeterclass")

# COMMAND ----------

# DBTITLE 1,[Target] Schema Check
cleansedf.printSchema()

# COMMAND ----------

cleansedf.createOrReplaceTempView("Target")

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate checks
# MAGIC %sql
# MAGIC SELECT meterClassCode, COUNT (*) as count
# MAGIC FROM cleansed.t_access_z309_tmeterclass
# MAGIC GROUP BY meterClassCode
# MAGIC HAVING COUNT (*) > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Records count check
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from cleansed.t_access_z309_tmeterclass
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from Source

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Source and Target Data
# MAGIC %sql
# MAGIC select 
# MAGIC C_METE_CLAS as meterClassCode
# MAGIC ,T_METE_CLAS as meterClass
# MAGIC ,T_METE_CLAS_ABBR as meterClassAbbreviation,
# MAGIC case when D_METE_CLAS_EFFE <> 'null' then 
# MAGIC CONCAT(LEFT(D_METE_CLAS_EFFE,4),'-',SUBSTRING(D_METE_CLAS_EFFE,5,2),'-',RIGHT(D_METE_CLAS_EFFE,2))
# MAGIC else D_METE_CLAS_EFFE end as meterClassEffectiveDate,
# MAGIC case when D_METE_CLAS_CANC <> 'null' then 
# MAGIC CONCAT(LEFT(D_METE_CLAS_CANC,4),'-',SUBSTRING(D_METE_CLAS_CANC,5,2),'-',RIGHT(D_METE_CLAS_CANC,2))
# MAGIC else D_METE_CLAS_CANC end as meterClassCancelledDate,
# MAGIC --C_WATE_METE_TYPE as waterMeterType
# MAGIC case when C_WATE_METE_TYPE = 'P' then 'Potable'
# MAGIC when C_WATE_METE_TYPE = 'R' then 'Recycled'
# MAGIC when C_WATE_METE_TYPE = 'O' then 'Other'
# MAGIC else C_WATE_METE_TYPE end as waterMeterType
# MAGIC from Source
# MAGIC except
# MAGIC select 
# MAGIC meterClassCode
# MAGIC ,UPPER(meterClass)
# MAGIC ,meterClassAbbreviation
# MAGIC ,meterClassEffectiveDate
# MAGIC ,meterClassCancelledDate
# MAGIC ,waterMeterType
# MAGIC from cleansed.t_access_z309_tmeterclass

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Target and Source Data
# MAGIC %sql
# MAGIC select 
# MAGIC meterClassCode
# MAGIC ,UPPER(meterClass)
# MAGIC ,meterClassAbbreviation
# MAGIC ,meterClassEffectiveDate
# MAGIC ,meterClassCancelledDate
# MAGIC ,waterMeterType
# MAGIC from cleansed.t_access_z309_tmeterclass
# MAGIC except
# MAGIC select 
# MAGIC C_METE_CLAS as meterClassCode
# MAGIC ,T_METE_CLAS as meterClass
# MAGIC ,T_METE_CLAS_ABBR as meterClassAbbreviation,
# MAGIC case when D_METE_CLAS_EFFE <> 'null' then 
# MAGIC CONCAT(LEFT(D_METE_CLAS_EFFE,4),'-',SUBSTRING(D_METE_CLAS_EFFE,5,2),'-',RIGHT(D_METE_CLAS_EFFE,2))
# MAGIC else D_METE_CLAS_EFFE end as meterClassEffectiveDate,
# MAGIC case when D_METE_CLAS_CANC <> 'null' then 
# MAGIC CONCAT(LEFT(D_METE_CLAS_CANC,4),'-',SUBSTRING(D_METE_CLAS_CANC,5,2),'-',RIGHT(D_METE_CLAS_CANC,2))
# MAGIC else D_METE_CLAS_CANC end as meterClassCancelledDate,
# MAGIC --C_WATE_METE_TYPE as waterMeterType
# MAGIC case when C_WATE_METE_TYPE = 'P' then 'Potable'
# MAGIC when C_WATE_METE_TYPE = 'R' then 'Recycled'
# MAGIC when C_WATE_METE_TYPE = 'O' then 'Other'
# MAGIC else C_WATE_METE_TYPE end as waterMeterType
# MAGIC from Source
