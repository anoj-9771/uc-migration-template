# Databricks notebook source
#config parameters
source = 'ISU' #either CRM or ISU
table = 'ZCD_TPROPTY_HIST'

environment = 'test'
storage_account_name = "sablobdaftest01"
storage_account_access_key = dbutils.secrets.get(scope="TestScope",key="test-sablob-key")
containerName = "archive"


# COMMAND ----------

# MAGIC %run ../../includes/tableEvaluation

# COMMAND ----------

# DBTITLE 1,[Source] with mapping
# MAGIC %sql
# MAGIC SELECT
# MAGIC propertyNumber,
# MAGIC superiorPropertyTypeCode,
# MAGIC superiorPropertyType,
# MAGIC inferiorPropertyTypeCode,
# MAGIC inferiorPropertyType,
# MAGIC validFromDate,
# MAGIC validToDate from(
# MAGIC SELECT
# MAGIC PROPERTY_NO	as	propertyNumber,
# MAGIC SUP_PROP_TYPE	as	superiorPropertyTypeCode,
# MAGIC  b.superiorPropertyType as superiorPropertyType,
# MAGIC INF_PROP_TYPE	as	inferiorPropertyTypeCode,
# MAGIC c.inferiorPropertyType AS inferiorPropertyType,
# MAGIC DATE_FROM	as	validFromDate,
# MAGIC DATE_TO	as	validToDate,
# MAGIC row_number() over (partition by PROPERTY_NO,SUP_PROP_TYPE,INF_PROP_TYPE,DATE_FROM order by EXTRACT_DATETIME desc) rn
# MAGIC from
# MAGIC test.${vars.table} a
# MAGIC  left join cleansed.isu_zcd_tsupprtyp_tx b
# MAGIC  on b.superiorPropertyTypeCode = a.SUP_PROP_TYPE
# MAGIC  left join cleansed.isu_zcd_tinfprty_tx c
# MAGIC  on c.inferiorPropertyTypeCode = a.INF_PROP_TYPE) where rn = 1 and validFromDate < validToDate

# COMMAND ----------

# DBTITLE 1,[Verification] Count Check
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from cleansed.${vars.table}
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from (SELECT
# MAGIC propertyNumber,
# MAGIC superiorPropertyTypeCode,
# MAGIC superiorPropertyType,
# MAGIC inferiorPropertyTypeCode,
# MAGIC inferiorPropertyType,
# MAGIC validFromDate,
# MAGIC validToDate from(
# MAGIC SELECT
# MAGIC PROPERTY_NO	as	propertyNumber,
# MAGIC SUP_PROP_TYPE	as	superiorPropertyTypeCode,
# MAGIC  b.superiorPropertyType as superiorPropertyType,
# MAGIC INF_PROP_TYPE	as	inferiorPropertyTypeCode,
# MAGIC c.inferiorPropertyType AS inferiorPropertyType,
# MAGIC DATE_FROM	as	validFromDate,
# MAGIC DATE_TO	as	validToDate,
# MAGIC row_number() over (partition by PROPERTY_NO,SUP_PROP_TYPE,INF_PROP_TYPE,DATE_FROM order by EXTRACT_DATETIME desc) rn
# MAGIC from
# MAGIC test.${vars.table} a
# MAGIC  left join cleansed.isu_zcd_tsupprtyp_tx b
# MAGIC  on b.superiorPropertyTypeCode = a.SUP_PROP_TYPE
# MAGIC  left join cleansed.isu_zcd_tinfprty_tx c
# MAGIC  on c.inferiorPropertyTypeCode = a.INF_PROP_TYPE) where rn = 1 and validFromDate < validToDate)

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT propertyNumber,superiorPropertyTypeCode, inferiorPropertyTypeCode, validFromDate, COUNT (*) as count
# MAGIC FROM cleansed.${vars.table}
# MAGIC GROUP BY propertyNumber,superiorPropertyTypeCode, inferiorPropertyTypeCode, validFromDate
# MAGIC HAVING COUNT (*) > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT * FROM (
# MAGIC SELECT
# MAGIC *,
# MAGIC row_number() OVER(PARTITION BY propertyNumber,superiorPropertyTypeCode, inferiorPropertyTypeCode, validFromDate order by propertyNumber,superiorPropertyTypeCode, inferiorPropertyTypeCode, validFromDate) as rn
# MAGIC FROM  cleansed.${vars.table}
# MAGIC )a where a.rn > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Source and Target Data
# MAGIC %sql
# MAGIC SELECT
# MAGIC propertyNumber,
# MAGIC superiorPropertyTypeCode,
# MAGIC superiorPropertyType,
# MAGIC inferiorPropertyTypeCode,
# MAGIC inferiorPropertyType,
# MAGIC validFromDate,
# MAGIC validToDate from(
# MAGIC SELECT
# MAGIC PROPERTY_NO	as	propertyNumber,
# MAGIC SUP_PROP_TYPE	as	superiorPropertyTypeCode,
# MAGIC  b.superiorPropertyType as superiorPropertyType,
# MAGIC INF_PROP_TYPE	as	inferiorPropertyTypeCode,
# MAGIC c.inferiorPropertyType AS inferiorPropertyType,
# MAGIC DATE_FROM	as	validFromDate,
# MAGIC DATE_TO	as	validToDate,
# MAGIC row_number() over (partition by PROPERTY_NO,SUP_PROP_TYPE,INF_PROP_TYPE,DATE_FROM order by EXTRACT_DATETIME desc) rn
# MAGIC from
# MAGIC test.${vars.table} a
# MAGIC  left join cleansed.isu_zcd_tsupprtyp_tx b
# MAGIC  on b.superiorPropertyTypeCode = a.SUP_PROP_TYPE
# MAGIC  left join cleansed.isu_zcd_tinfprty_tx c
# MAGIC  on c.inferiorPropertyTypeCode = a.INF_PROP_TYPE) where rn = 1 and validFromDate < validToDate
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
# MAGIC cleansed.${vars.table}

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Target and Source Data
# MAGIC %sql
# MAGIC 
# MAGIC select
# MAGIC propertyNumber,
# MAGIC superiorPropertyTypeCode,
# MAGIC superiorPropertyType,
# MAGIC inferiorPropertyTypeCode,
# MAGIC inferiorPropertyType,
# MAGIC validFromDate,
# MAGIC validToDate
# MAGIC from
# MAGIC cleansed.${vars.table}
# MAGIC except
# MAGIC SELECT
# MAGIC propertyNumber,
# MAGIC superiorPropertyTypeCode,
# MAGIC superiorPropertyType,
# MAGIC inferiorPropertyTypeCode,
# MAGIC inferiorPropertyType,
# MAGIC validFromDate,
# MAGIC validToDate from(
# MAGIC SELECT
# MAGIC PROPERTY_NO	as	propertyNumber,
# MAGIC SUP_PROP_TYPE	as	superiorPropertyTypeCode,
# MAGIC  b.superiorPropertyType as superiorPropertyType,
# MAGIC INF_PROP_TYPE	as	inferiorPropertyTypeCode,
# MAGIC c.inferiorPropertyType AS inferiorPropertyType,
# MAGIC DATE_FROM	as	validFromDate,
# MAGIC DATE_TO	as	validToDate,
# MAGIC row_number() over (partition by PROPERTY_NO,SUP_PROP_TYPE,INF_PROP_TYPE,DATE_FROM order by EXTRACT_DATETIME desc) rn
# MAGIC from
# MAGIC test.${vars.table} a
# MAGIC  left join cleansed.isu_zcd_tsupprtyp_tx b
# MAGIC  on b.superiorPropertyTypeCode = a.SUP_PROP_TYPE
# MAGIC  left join cleansed.isu_zcd_tinfprty_tx c
# MAGIC  on c.inferiorPropertyTypeCode = a.INF_PROP_TYPE) where rn = 1
