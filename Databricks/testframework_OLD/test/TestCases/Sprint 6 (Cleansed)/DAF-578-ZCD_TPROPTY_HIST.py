# Databricks notebook source
# DBTITLE 1,[Config] Connection Setup
storage_account_name = "saswcnonprod01landingtst"
storage_account_access_key = dbutils.secrets.get(scope="Test-Access",key="test-blob-key")
container_name = "archive"
file_location = "wasbs://archive@saswcnonprod01landingtst.blob.core.windows.net/sapisu/20210920/20210920_13:57:00/ZCD_TPROPTY_HIST_20210920113924.json"
file_type = "json"
print(storage_account_name)

# COMMAND ----------

spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
  storage_account_access_key)


# COMMAND ----------

# DBTITLE 1,[Source] Loading data into Dataframe
df = spark.read.format(file_type).option("inferSchema", "true").load(file_location)

# COMMAND ----------

# DBTITLE 1,[Source] Schema Check - Refer to Raw2Cleansed Mapping
df.printSchema()

# COMMAND ----------

# DBTITLE 0,[Result] Load Count Result into DataFrame
lakedf = spark.sql("select * from cleansed.t_sapisu_zcd_tpropty_hist")

# COMMAND ----------

# DBTITLE 1,[Target] Schema Check
lakedf.printSchema()

# COMMAND ----------

# DBTITLE 1,[Source] Creating Temporary Table
df.createOrReplaceTempView("Source")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from Source

# COMMAND ----------

# DBTITLE 1,[Source with mapping]
# MAGIC %sql
# MAGIC SELECT
# MAGIC PROPERTY_NO	as	propertyNumber,
# MAGIC SUP_PROP_TYPE	as	superiorPropertyTypeCode,
# MAGIC  b.superiorPropertyType as superiorPropertyType,
# MAGIC INF_PROP_TYPE	as	inferiorPropertyTypeCode,
# MAGIC c.inferiorPropertyType AS inferiorPropertyType,
# MAGIC DATE_FROM	as	validFromDate,
# MAGIC DATE_TO	as	validToDate
# MAGIC from
# MAGIC Source a
# MAGIC  left join cleansed.t_sapisu_zcd_tsupprtyp_tx b
# MAGIC  on b.superiorPropertyTypeCode = a.SUP_PROP_TYPE
# MAGIC  left join cleansed.t_sapisu_zcd_tinfprty_tx c
# MAGIC  on c.inferiorPropertyTypeCode = a.INF_PROP_TYPE

# COMMAND ----------

# DBTITLE 1,[Additional check]
# MAGIC %sql
# MAGIC SELECT
# MAGIC PROPERTY_NO	as	propertyNumber,
# MAGIC SUP_PROP_TYPE	as	superiorPropertyTypeCode,
# MAGIC  b.superiorPropertyType as superiorPropertyType,
# MAGIC INF_PROP_TYPE	as	inferiorPropertyTypeCode,
# MAGIC c.inferiorPropertyType AS inferiorPropertyType,
# MAGIC DATE_FROM	as	validFromDate,
# MAGIC DATE_TO	as	validToDate
# MAGIC from
# MAGIC Source a
# MAGIC  left join cleansed.t_sapisu_zcd_tsupprtyp_tx b
# MAGIC  on b.superiorPropertyTypeCode = a.SUP_PROP_TYPE
# MAGIC  left join cleansed.t_sapisu_zcd_tinfprty_tx c
# MAGIC  on c.inferiorPropertyTypeCode = a.INF_PROP_TYPE
# MAGIC where PROPERTY_NO = 3101523

# COMMAND ----------

# DBTITLE 1,[Additional check]
# MAGIC %sql
# MAGIC select * from 
# MAGIC cleansed.t_sapisu_zcd_tpropty_hist where propertyNumber=3101523

# COMMAND ----------

lakedf.createOrReplaceTempView("Target")

# COMMAND ----------

# DBTITLE 1,[Verification] Count Checks
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from cleansed.t_sapisu_zcd_tpropty_hist
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from Source

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC * 
# MAGIC from cleansed.t_sapisu_zcd_tpropty_hist

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT propertyNumber,superiorPropertyTypeCode,inferiorPropertyTypeCode,validFromDate, COUNT (*) as count
# MAGIC FROM cleansed.t_sapisu_zcd_tpropty_hist
# MAGIC GROUP BY propertyNumber,superiorPropertyTypeCode,inferiorPropertyTypeCode,validFromDate
# MAGIC HAVING COUNT (*) > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT * FROM (
# MAGIC SELECT
# MAGIC *,
# MAGIC row_number() OVER(PARTITION BY propertyNumber,superiorPropertyTypeCode,inferiorPropertyTypeCode,validFromDate order by validFromDate) as rn
# MAGIC FROM  cleansed.t_sapisu_zcd_tpropty_hist
# MAGIC )a where a.rn > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Source and Target Data
# MAGIC %sql
# MAGIC SELECT
# MAGIC PROPERTY_NO	as	propertyNumber,
# MAGIC SUP_PROP_TYPE	as	superiorPropertyTypeCode,
# MAGIC  b.superiorPropertyType as superiorPropertyType,
# MAGIC INF_PROP_TYPE	as	inferiorPropertyTypeCode,
# MAGIC c.inferiorPropertyType AS inferiorPropertyType,
# MAGIC DATE_FROM	as	validFromDate,
# MAGIC DATE_TO	as	validToDate
# MAGIC from
# MAGIC Source a
# MAGIC  left join cleansed.t_sapisu_zcd_tsupprtyp_tx b
# MAGIC  on b.superiorPropertyTypeCode = a.SUP_PROP_TYPE
# MAGIC  left join cleansed.t_sapisu_zcd_tinfprty_tx c
# MAGIC  on c.inferiorPropertyTypeCode = a.INF_PROP_TYPE
# MAGIC except
# MAGIC select
# MAGIC propertyNumber,
# MAGIC superiorPropertyTypeCode,
# MAGIC superiorPropertyType,
# MAGIC inferiorPropertyTypeCode,
# MAGIC inferiorPropertyType,
# MAGIC validFromDate,
# MAGIC validToDate
# MAGIC from
# MAGIC cleansed.t_sapisu_zcd_tpropty_hist

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Target and Source Data
# MAGIC %sql
# MAGIC select
# MAGIC propertyNumber,
# MAGIC superiorPropertyTypeCode,
# MAGIC superiorPropertyType,
# MAGIC inferiorPropertyTypeCode,
# MAGIC inferiorPropertyType,
# MAGIC validFromDate,
# MAGIC validToDate
# MAGIC from
# MAGIC cleansed.t_sapisu_zcd_tpropty_hist
# MAGIC EXCEPT
# MAGIC SELECT
# MAGIC PROPERTY_NO	as	propertyNumber,
# MAGIC SUP_PROP_TYPE	as	superiorPropertyTypeCode,
# MAGIC  b.superiorPropertyType as superiorPropertyType,
# MAGIC INF_PROP_TYPE	as	inferiorPropertyTypeCode,
# MAGIC c.inferiorPropertyType AS inferiorPropertyType,
# MAGIC DATE_FROM	as	validFromDate,
# MAGIC DATE_TO	as	validToDate
# MAGIC from
# MAGIC Source a
# MAGIC  left join cleansed.t_sapisu_zcd_tsupprtyp_tx b
# MAGIC  on b.superiorPropertyTypeCode = a.SUP_PROP_TYPE
# MAGIC  left join cleansed.t_sapisu_zcd_tinfprty_tx c
# MAGIC  on c.inferiorPropertyTypeCode = a.INF_PROP_TYPE
