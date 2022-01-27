# Databricks notebook source
# DBTITLE 1,[Config] Connection Setup
storage_account_name = "sadaftest01"
storage_account_access_key = dbutils.secrets.get(scope="Test-Access",key="test-datalake-key")
container_name = "raw"
file_location = "wasbs://raw@sadaftest01.blob.core.windows.net/landing/accessarchive/Z309_TSTREETGUIDE.csv"
file_type = "csv"
print(storage_account_name)

# COMMAND ----------

spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
  storage_account_access_key)


# COMMAND ----------

# DBTITLE 1,[Source] loading to a dataframe
 df = spark.read.format("csv").option('delimiter','|').option('header','true').load("wasbs://raw@sadaftest01.blob.core.windows.net/landing/accessarchive/Z309_TSTREETGUIDE.csv")

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
# MAGIC C_STRE_GUID as streetGuideCode
# MAGIC ,C_LGA as LGACode
# MAGIC ,C_VALI_ADDI_STRE as  streetTypesuffix,
# MAGIC M_STRE as streetName ,
# MAGIC C_VALI_STRE_TYPE as streetType,
# MAGIC C_POST as postCode,
# MAGIC M_SUBU as suburb ,
# MAGIC 
# MAGIC case when D_STRE_GUID_EFFE <> 'null' then 
# MAGIC CONCAT(LEFT(D_STRE_GUID_EFFE,4),'-',SUBSTRING(D_STRE_GUID_EFFE,5,2),'-',RIGHT(D_STRE_GUID_EFFE,2))
# MAGIC else D_STRE_GUID_EFFE end as streetGuideEffectiveDate,
# MAGIC case when D_STRE_GUID_CANC <> 'null' then 
# MAGIC CONCAT(LEFT(D_STRE_GUID_CANC,4),'-',SUBSTRING(D_STRE_GUID_CANC,5,2),'-',RIGHT(D_STRE_GUID_CANC,2))
# MAGIC else D_STRE_GUID_CANC end as streetGuideCancelledDate
# MAGIC 
# MAGIC from Source

# COMMAND ----------

# DBTITLE 1,[Target] displaying records
# MAGIC %sql
# MAGIC select * from cleansed.t_access_Z309_TSTREETGUIDE

# COMMAND ----------

# DBTITLE 0,[Result] Load Count Result into DataFrame
cleansedf = spark.sql("select * from cleansed.t_access_Z309_TSTREETGUIDE")

# COMMAND ----------

# DBTITLE 1,[Target] Schema Check
cleansedf.printSchema()

# COMMAND ----------

cleansedf.createOrReplaceTempView("Target")

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate checks
# MAGIC %sql
# MAGIC SELECT streetGuideCode, COUNT (*) as count
# MAGIC FROM cleansed.t_access_Z309_TSTREETGUIDE
# MAGIC GROUP BY streetGuideCode
# MAGIC HAVING COUNT (*) > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Records count check
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from cleansed.t_access_Z309_TSTREETGUIDE
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from Source

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Source and Target Data
# MAGIC %sql
# MAGIC select 
# MAGIC C_STRE_GUID as streetGuideCode,
# MAGIC C_LGA as LGACode,
# MAGIC C_VALI_ADDI_STRE as streetSuffix  ,
# MAGIC M_STRE as streetName ,
# MAGIC C_VALI_STRE_TYPE as streetType,
# MAGIC C_POST as postCode,
# MAGIC M_SUBU as suburb ,case when D_STRE_GUID_EFFE <> 'null' then 
# MAGIC CONCAT(LEFT(D_STRE_GUID_EFFE,4),'-',SUBSTRING(D_STRE_GUID_EFFE,5,2),'-',RIGHT(D_STRE_GUID_EFFE,2))
# MAGIC else D_STRE_GUID_EFFE end as streetGuideEffectiveDate,
# MAGIC case when D_STRE_GUID_CANC <> 'null' then 
# MAGIC CONCAT(LEFT(D_STRE_GUID_CANC,4),'-',SUBSTRING(D_STRE_GUID_CANC,5,2),'-',RIGHT(D_STRE_GUID_CANC,2))
# MAGIC else D_STRE_GUID_CANC end as streetGuideCancelledDate
# MAGIC from Source
# MAGIC except
# MAGIC select 
# MAGIC streetGuideCode,
# MAGIC LGACode,
# MAGIC streetSuffix,
# MAGIC streetName,
# MAGIC streetType,
# MAGIC postCode,
# MAGIC suburb,
# MAGIC streetGuideEffectiveDate,
# MAGIC streetGuideCancelledDate
# MAGIC from cleansed.t_access_Z309_TSTREETGUIDE

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Target and Source Data
# MAGIC %sql
# MAGIC select 
# MAGIC streetGuideCode,
# MAGIC LGACode,
# MAGIC streetSuffix,
# MAGIC streetName,
# MAGIC streetType,
# MAGIC postCode,
# MAGIC suburb,
# MAGIC streetGuideEffectiveDate,
# MAGIC streetGuideCancelledDate
# MAGIC from cleansed.t_access_Z309_TSTREETGUIDE
# MAGIC except
# MAGIC select 
# MAGIC C_STRE_GUID as streetGuideCode,
# MAGIC C_LGA as LGACode,
# MAGIC C_VALI_ADDI_STRE as streetSuffix  ,
# MAGIC M_STRE as streetName ,
# MAGIC C_VALI_STRE_TYPE as streetType,
# MAGIC C_POST as postCode,
# MAGIC M_SUBU as suburb ,case when D_STRE_GUID_EFFE <> 'null' then 
# MAGIC CONCAT(LEFT(D_STRE_GUID_EFFE,4),'-',SUBSTRING(D_STRE_GUID_EFFE,5,2),'-',RIGHT(D_STRE_GUID_EFFE,2))
# MAGIC else D_STRE_GUID_EFFE end as streetGuideEffectiveDate,
# MAGIC case when D_STRE_GUID_CANC <> 'null' then 
# MAGIC CONCAT(LEFT(D_STRE_GUID_CANC,4),'-',SUBSTRING(D_STRE_GUID_CANC,5,2),'-',RIGHT(D_STRE_GUID_CANC,2))
# MAGIC else D_STRE_GUID_CANC end as streetGuideCancelledDate
# MAGIC from Source
