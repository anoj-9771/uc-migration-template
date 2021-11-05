# Databricks notebook source
#config parameters
source = 'ISU' #either CRM or ISU
table = '0UC_MRTYPE_TEXT'

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
# MAGIC meterReadingTypeCode
# MAGIC ,meterReadingType
# MAGIC from
# MAGIC (select
# MAGIC ISTABLART as meterReadingTypeCode
# MAGIC ,TEXT40 as meterReadingType
# MAGIC ,row_number() over (partition by ISTABLART order by ISTABLART desc) as rn
# MAGIC from test.${vars.table}
# MAGIC )a where  a.rn = 1

# COMMAND ----------

# DBTITLE 1,[Verification] Count Check
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from cleansed.${vars.table}
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from (select
# MAGIC meterReadingTypeCode
# MAGIC ,meterReadingType
# MAGIC from
# MAGIC (select
# MAGIC ISTABLART as meterReadingTypeCode
# MAGIC ,TEXT40 as meterReadingType
# MAGIC ,row_number() over (partition by ISTABLART order by ISTABLART desc) as rn
# MAGIC from test.${vars.table}
# MAGIC )a where  a.rn = 1)

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT meterReadingTypeCode, COUNT (*) as count
# MAGIC FROM cleansed.${vars.table}
# MAGIC GROUP BY meterReadingTypeCode
# MAGIC HAVING COUNT (*) > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT * FROM (
# MAGIC SELECT
# MAGIC *,
# MAGIC row_number() OVER(PARTITION BY meterReadingTypeCode order by meterReadingTypeCode) as rn
# MAGIC FROM  cleansed.${vars.table}
# MAGIC )a where a.rn > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Source and Target Data
# MAGIC %sql
# MAGIC select
# MAGIC meterReadingTypeCode
# MAGIC ,meterReadingType
# MAGIC from
# MAGIC (select
# MAGIC ISTABLART as meterReadingTypeCode
# MAGIC ,TEXT40 as meterReadingType
# MAGIC ,row_number() over (partition by ISTABLART order by ISTABLART desc) as rn
# MAGIC from test.${vars.table}
# MAGIC )a where  a.rn = 1
# MAGIC except
# MAGIC select
# MAGIC meterReadingTypeCode
# MAGIC ,meterReadingType
# MAGIC from
# MAGIC cleansed.${vars.table}

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Target and Source Data
# MAGIC %sql
# MAGIC select
# MAGIC meterReadingTypeCode
# MAGIC ,meterReadingType
# MAGIC from
# MAGIC cleansed.${vars.table}
# MAGIC except
# MAGIC select
# MAGIC meterReadingTypeCode
# MAGIC ,meterReadingType
# MAGIC from
# MAGIC (select
# MAGIC ISTABLART as meterReadingTypeCode
# MAGIC ,TEXT40 as meterReadingType
# MAGIC ,row_number() over (partition by ISTABLART order by ISTABLART desc) as rn
# MAGIC from test.${vars.table}
# MAGIC )a where  a.rn = 1
