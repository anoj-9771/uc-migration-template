# Databricks notebook source
# DBTITLE 1,[Config] Connection Setup
storage_account_name = "sadaftest01"
storage_account_access_key = dbutils.secrets.get(scope="Test-Access",key="test-datalake-key")
container_name = "raw"
file_location = "wasbs://raw@sadaftest01.blob.core.windows.net/landing/accessarchive/Z309_TRATATYPE.csv"
file_type = "csv"
print(storage_account_name)

# COMMAND ----------

spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
  storage_account_access_key)


# COMMAND ----------

# DBTITLE 1,[Source] loading to a dataframe
 df = spark.read.format("csv").option('delimiter','|').option('header','true').load("wasbs://raw@sadaftest01.blob.core.windows.net/landing/accessarchive/Z309_TRATATYPE.csv")

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.createOrReplaceTempView("Source")

# COMMAND ----------

cleansedf = spark.sql("select * from cleansed.t_access_z309_tratatype")

# COMMAND ----------

# DBTITLE 1,[Target] Schema Check
cleansedf.printSchema()

# COMMAND ----------

cleansedf.createOrReplaceTempView("Target")

# COMMAND ----------

# DBTITLE 1,[Source] displaying records
# MAGIC %sql
# MAGIC select * from Source

# COMMAND ----------

# DBTITLE 1,[Source] after applying mapping
# MAGIC %sql
# MAGIC select
# MAGIC C_RATA_TYPE as rateabilityTypeCode,
# MAGIC T_RATA_TYPE as rateabilityType,
# MAGIC case when D_RATA_TYPE_EFFE <> 'null' then CONCAT(LEFT(D_RATA_TYPE_EFFE,4),'-',SUBSTRING(D_RATA_TYPE_EFFE,5,2),'-',RIGHT(D_RATA_TYPE_EFFE,2)) else D_RATA_TYPE_EFFE end as rateabilityTypeEffectiveDate,
# MAGIC case when D_RATA_TYPE_CANC <> 'null' then CONCAT(LEFT(D_RATA_TYPE_CANC,4),'-',SUBSTRING(D_RATA_TYPE_CANC,5,2),'-',RIGHT(D_RATA_TYPE_CANC,2)) else D_RATA_TYPE_CANC end as rateabilityTypeCancelledDate
# MAGIC from source

# COMMAND ----------

# DBTITLE 1,[Target] displaying records
# MAGIC %sql
# MAGIC select 
# MAGIC rateabilityTypeCode,
# MAGIC rateabilityType,
# MAGIC rateabilityTypeEffectiveDate,
# MAGIC rateabilityTypeCancelledDate
# MAGIC from cleansed.t_access_z309_tratatype

# COMMAND ----------

# DBTITLE 1,[Verification]Duplicate checks
# MAGIC %sql
# MAGIC SELECT rateabilityTypeCode, COUNT (*) as count
# MAGIC FROM cleansed.t_access_z309_tratatype
# MAGIC GROUP BY rateabilityTypeCode
# MAGIC HAVING COUNT (*) > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Records count check
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from cleansed.t_access_z309_tratatype
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from Source

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Source and Target Data
# MAGIC %sql
# MAGIC select
# MAGIC C_RATA_TYPE as rateabilityTypeCode,
# MAGIC T_RATA_TYPE as rateabilityType,
# MAGIC case when D_RATA_TYPE_EFFE <> 'null' then CONCAT(LEFT(D_RATA_TYPE_EFFE,4),'-',SUBSTRING(D_RATA_TYPE_EFFE,5,2),'-',RIGHT(D_RATA_TYPE_EFFE,2)) else D_RATA_TYPE_EFFE end as rateabilityTypeEffectiveDate,
# MAGIC case when D_RATA_TYPE_CANC <> 'null' then CONCAT(LEFT(D_RATA_TYPE_CANC,4),'-',SUBSTRING(D_RATA_TYPE_CANC,5,2),'-',RIGHT(D_RATA_TYPE_CANC,2)) else D_RATA_TYPE_CANC end as rateabilityTypeCancelledDate
# MAGIC from source
# MAGIC except
# MAGIC select 
# MAGIC rateabilityTypeCode,
# MAGIC upper(rateabilityType),
# MAGIC rateabilityTypeEffectiveDate,
# MAGIC rateabilityTypeCancelledDate
# MAGIC from cleansed.t_access_z309_tratatype

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Target and Source Data
# MAGIC %sql
# MAGIC select 
# MAGIC rateabilityTypeCode,
# MAGIC upper(rateabilityType),
# MAGIC rateabilityTypeEffectiveDate,
# MAGIC rateabilityTypeCancelledDate
# MAGIC from cleansed.t_access_z309_tratatype
# MAGIC except
# MAGIC select
# MAGIC C_RATA_TYPE as rateabilityTypeCode,
# MAGIC T_RATA_TYPE as rateabilityType,
# MAGIC case when D_RATA_TYPE_EFFE <> 'null' then CONCAT(LEFT(D_RATA_TYPE_EFFE,4),'-',SUBSTRING(D_RATA_TYPE_EFFE,5,2),'-',RIGHT(D_RATA_TYPE_EFFE,2)) else D_RATA_TYPE_EFFE end as rateabilityTypeEffectiveDate,
# MAGIC case when D_RATA_TYPE_CANC <> 'null' then CONCAT(LEFT(D_RATA_TYPE_CANC,4),'-',SUBSTRING(D_RATA_TYPE_CANC,5,2),'-',RIGHT(D_RATA_TYPE_CANC,2)) else D_RATA_TYPE_CANC end as rateabilityTypeCancelledDate
# MAGIC from source
