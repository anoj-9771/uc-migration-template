# Databricks notebook source
#config parameters
source = 'ISU' #either CRM or ISU
table = 'DD07T'

environment = 'test'
storage_account_name = "sablobdaftest01"
storage_account_access_key = dbutils.secrets.get(scope="TestScope",key="test-sablob-key")
containerName = "archive"


# COMMAND ----------

# MAGIC %run ../../includes/tableEvaluation

# COMMAND ----------

# DBTITLE 1,[Source] with mapping
# MAGIC %sql
# MAGIC select
# MAGIC domainName
# MAGIC ,domainValueSingleUpperLimit
# MAGIC ,domainValueText
# MAGIC ,domainValueKey
# MAGIC from
# MAGIC (select
# MAGIC DOMNAME as domainName
# MAGIC ,DOMVALUE_L as domainValueSingleUpperLimit
# MAGIC ,DDTEXT as domainValueText
# MAGIC ,VALPOS as domainValueKey
# MAGIC ,row_number() over (partition by DOMNAME,VALPOS order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table} -- WHERE LANGU = 'E'
# MAGIC )a where  a.rn = 1

# COMMAND ----------

# DBTITLE 1,[Verification] Count Check
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from cleansed.${vars.table}
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from (select
# MAGIC domainName
# MAGIC ,domainValueSingleUpperLimit
# MAGIC ,domainValueText
# MAGIC ,domainValueKey
# MAGIC from
# MAGIC (select
# MAGIC DOMNAME as domainName
# MAGIC ,DOMVALUE_L as domainValueSingleUpperLimit
# MAGIC ,DDTEXT as domainValueText
# MAGIC ,VALPOS as domainValueKey
# MAGIC ,row_number() over (partition by DOMNAME,VALPOS order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table} -- WHERE LANGU = 'E'
# MAGIC )a where  a.rn = 1)

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT domainName
# MAGIC ,domainValueSingleUpperLimit
# MAGIC ,domainValueText
# MAGIC ,domainValueKey, COUNT (*) as count
# MAGIC FROM cleansed.${vars.table}
# MAGIC GROUP BY domainName,domainValueSingleUpperLimit,domainValueText,domainValueKey
# MAGIC HAVING COUNT (*) > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT * FROM (
# MAGIC SELECT
# MAGIC *,
# MAGIC row_number() OVER(PARTITION BY domainName,domainValueKey  order by domainName,domainValueKey) as rn
# MAGIC FROM  cleansed.${vars.table}
# MAGIC )a where a.rn > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Source and Target Data
# MAGIC %sql
# MAGIC select
# MAGIC domainName
# MAGIC ,domainValueSingleUpperLimit
# MAGIC ,domainValueText
# MAGIC ,domainValueKey
# MAGIC from
# MAGIC (select
# MAGIC DOMNAME as domainName
# MAGIC ,DOMVALUE_L as domainValueSingleUpperLimit
# MAGIC ,DDTEXT as domainValueText
# MAGIC ,VALPOS as domainValueKey
# MAGIC ,row_number() over (partition by DOMNAME,VALPOS order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table} -- WHERE LANGU = 'E'
# MAGIC )a where  a.rn = 1
# MAGIC except
# MAGIC select
# MAGIC domainName
# MAGIC ,domainValueSingleUpperLimit
# MAGIC ,domainValueText
# MAGIC ,domainValueKey
# MAGIC from
# MAGIC cleansed.${vars.table}

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Target and Source Data
# MAGIC %sql
# MAGIC select
# MAGIC domainName
# MAGIC ,domainValueSingleUpperLimit
# MAGIC ,domainValueText
# MAGIC ,domainValueKey
# MAGIC from
# MAGIC cleansed.${vars.table}
# MAGIC except
# MAGIC select
# MAGIC domainName
# MAGIC ,domainValueSingleUpperLimit
# MAGIC ,domainValueText
# MAGIC ,domainValueKey
# MAGIC from
# MAGIC (select
# MAGIC DOMNAME as domainName
# MAGIC ,DOMVALUE_L as domainValueSingleUpperLimit
# MAGIC ,DDTEXT as domainValueText
# MAGIC ,VALPOS as domainValueKey
# MAGIC ,row_number() over (partition by DOMNAME,VALPOS order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table} -- WHERE LANGU = 'E'
# MAGIC )a where  a.rn = 1
