# Databricks notebook source
# DBTITLE 1,[Config] Connection Setup
storage_account_name = "sadaftest01"
storage_account_access_key = dbutils.secrets.get(scope="Test-Access",key="test-datalake-key")
container_name = "raw"
file_location = "wasbs://raw@sadaftest01.blob.core.windows.net/landing/accessarchive/Z309_TPROPMETER.csv"
file_type = "csv"
print(storage_account_name)

# COMMAND ----------

spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
  storage_account_access_key)


# COMMAND ----------

# DBTITLE 1,[Source] loading to a dataframe
 df = spark.read.format("csv").option('delimiter','|').option('header','true').load("wasbs://raw@sadaftest01.blob.core.windows.net/landing/accessarchive/Z309_TPROPTYPE.csv")

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.createOrReplaceTempView("Source")

# COMMAND ----------

# DBTITLE 1,[Source] displaying records
# MAGIC %sql
# MAGIC select * from Source

# COMMAND ----------

# DBTITLE 1,[Source] after applying mapping
# MAGIC %sql
# MAGIC select C_PROP_TYPE as propertyTypeCode,
# MAGIC C_SUPE_PROP_TYPE as superiorPropertyTypeCode,
# MAGIC T_PROP_TYPE_ABBR as propertyTypeAbbreviation,
# MAGIC case when D_SUPE_PROP_CANC <> 'null' then 
# MAGIC CONCAT(LEFT(D_SUPE_PROP_CANC,4),'-',SUBSTRING(D_SUPE_PROP_CANC,5,2),'-',RIGHT(D_SUPE_PROP_CANC,2))
# MAGIC else D_SUPE_PROP_CANC end as propertyTypeCancelledDate,
# MAGIC case when D_SUPE_PROP_EFFE <> 'null' then 
# MAGIC CONCAT(LEFT(D_SUPE_PROP_EFFE,4),'-',SUBSTRING(D_SUPE_PROP_EFFE,5,2),'-',RIGHT(D_SUPE_PROP_EFFE,2))
# MAGIC else D_SUPE_PROP_EFFE end as propertyTypeEffectiveDate,
# MAGIC case when F_FLAT_VALI = 'Y' then 'true' else 'false' end as isFlatValid,
# MAGIC case when F_SLIC_VALI = 'Y' then 'true' else 'false' end as isSLICValid,
# MAGIC case when F_ACCO_PROP_VALI = 'Y' then 'true' else 'false' end as isAccountPropertyValid,
# MAGIC case when F_RHLC_LIAB = 'Y' then 'true' else 'false' end as isRouseHillLandChargeLiable,
# MAGIC T_SUPE_OR_PROP as propertyType
# MAGIC 
# MAGIC from Source

# COMMAND ----------

# DBTITLE 1,[Target] displaying records
# MAGIC %sql
# MAGIC select * from cleansed.t_access_z309_tproperty

# COMMAND ----------

cleansedf = spark.sql("select * from cleansed.t_access_z309_tproptype")

# COMMAND ----------

# DBTITLE 1,[Target] Schema Check
cleansedf.printSchema()

# COMMAND ----------

cleansedf.createOrReplaceTempView("Target")

# COMMAND ----------

# DBTITLE 1,[Verification]Duplicate checks
# MAGIC %sql
# MAGIC SELECT propertyTypeCode, COUNT (*) as count
# MAGIC FROM cleansed.t_access_z309_tproptype
# MAGIC GROUP BY propertyTypeCode
# MAGIC HAVING COUNT (*) > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Records count check
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from cleansed.t_access_z309_tproptype
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from Source

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Source and Target Data
# MAGIC %sql
# MAGIC select 
# MAGIC C_PROP_TYPE as propertyTypeCode,
# MAGIC C_SUPE_PROP_TYPE as superiorPropertyTypeCode,
# MAGIC T_PROP_TYPE_ABBR as propertyTypeAbbreviation,
# MAGIC case when D_SUPE_PROP_CANC <> 'null' then 
# MAGIC CONCAT(LEFT(D_SUPE_PROP_CANC,4),'-',SUBSTRING(D_SUPE_PROP_CANC,5,2),'-',RIGHT(D_SUPE_PROP_CANC,2))
# MAGIC else D_SUPE_PROP_CANC end as propertyTypeCancelledDate,
# MAGIC case when D_SUPE_PROP_EFFE <> 'null' then 
# MAGIC CONCAT(LEFT(D_SUPE_PROP_EFFE,4),'-',SUBSTRING(D_SUPE_PROP_EFFE,5,2),'-',RIGHT(D_SUPE_PROP_EFFE,2))
# MAGIC else D_SUPE_PROP_EFFE end as propertyTypeEffectiveDate,
# MAGIC case when F_FLAT_VALI = 'Y' then true else false end as isFlatValid,
# MAGIC case when F_SLIC_VALI = 'Y' then true else false end as isSLICValid,
# MAGIC case when F_ACCO_PROP_VALI = 'Y' then true else false end as isAccountPropertyValid,
# MAGIC case when F_RHLC_LIAB = 'Y' then true else false end as isRouseHillLandChargeLiable,
# MAGIC T_SUPE_OR_PROP as propertyType
# MAGIC from Source
# MAGIC except
# MAGIC select propertyTypeCode,
# MAGIC superiorPropertyTypeCode,
# MAGIC propertyTypeAbbreviation,
# MAGIC propertyTypeCancelledDate,
# MAGIC propertyTypeEffectiveDate,
# MAGIC isFlatValid,
# MAGIC isSLICValid,
# MAGIC isAccountPropertyValid,
# MAGIC isRouseHillLandChargeLiable,
# MAGIC UPPER(propertyType)
# MAGIC from cleansed.t_access_z309_tproptype

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Target and Source Data
# MAGIC %sql
# MAGIC select propertyTypeCode,
# MAGIC superiorPropertyTypeCode,
# MAGIC propertyTypeAbbreviation,
# MAGIC propertyTypeCancelledDate,
# MAGIC propertyTypeEffectiveDate,
# MAGIC isFlatValid,
# MAGIC isSLICValid,
# MAGIC isAccountPropertyValid,
# MAGIC isRouseHillLandChargeLiable,
# MAGIC UPPER(propertyType)
# MAGIC 
# MAGIC from cleansed.t_access_z309_tproptype
# MAGIC except
# MAGIC select 
# MAGIC C_PROP_TYPE as propertyTypeCode,
# MAGIC C_SUPE_PROP_TYPE as superiorPropertyTypeCode,
# MAGIC T_PROP_TYPE_ABBR as propertyTypeAbbreviation,
# MAGIC case when D_SUPE_PROP_CANC <> 'null' then 
# MAGIC CONCAT(LEFT(D_SUPE_PROP_CANC,4),'-',SUBSTRING(D_SUPE_PROP_CANC,5,2),'-',RIGHT(D_SUPE_PROP_CANC,2))
# MAGIC else D_SUPE_PROP_CANC end as propertyTypeCancelledDate,
# MAGIC case when D_SUPE_PROP_EFFE <> 'null' then 
# MAGIC CONCAT(LEFT(D_SUPE_PROP_EFFE,4),'-',SUBSTRING(D_SUPE_PROP_EFFE,5,2),'-',RIGHT(D_SUPE_PROP_EFFE,2))
# MAGIC else D_SUPE_PROP_EFFE end as propertyTypeEffectiveDate,
# MAGIC case when F_FLAT_VALI = 'Y' then true else false end as isFlatValid,
# MAGIC case when F_SLIC_VALI = 'Y' then true else false end as isSLICValid,
# MAGIC case when F_ACCO_PROP_VALI = 'Y' then true else false end as isAccountPropertyValid,
# MAGIC case when F_RHLC_LIAB = 'Y' then true else false end as isRouseHillLandChargeLiable,
# MAGIC T_SUPE_OR_PROP as propertyType
# MAGIC from Source

# COMMAND ----------

# DBTITLE 1,[Additional Checks]
# MAGIC %sql
# MAGIC select *, CONCAT(LEFT(D_SUPE_PROP_CANC,4),'-',SUBSTRING(D_SUPE_PROP_CANC,5,2),'-',RIGHT(D_SUPE_PROP_CANC,2)) as propertyTypeCancelledDate from source where C_PROP_TYPE = '075'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cleansed.t_access_z309_tproptype
# MAGIC where propertytypecode = '075'
