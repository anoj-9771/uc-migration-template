# Databricks notebook source
#config parameters
source = 'CRM' #either CRM or ISU
table = '0BP_ID_TYPE_TEXT'

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
# MAGIC identificationTypeCode,
# MAGIC identificationType
# MAGIC from
# MAGIC (select
# MAGIC TYPE as identificationTypeCode,
# MAGIC TEXT as identificationType,
# MAGIC row_number() over (partition by TYPE order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table} 
# MAGIC )a  
# MAGIC  where  a.rn = 1

# COMMAND ----------

# DBTITLE 1,[Verification] Count Check
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from cleansed.${vars.table}
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from (select
# MAGIC identificationTypeCode,
# MAGIC identificationType
# MAGIC from
# MAGIC (select
# MAGIC TYPE as identificationTypeCode,
# MAGIC TEXT as identificationType,
# MAGIC row_number() over (partition by TYPE order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table} 
# MAGIC )a  
# MAGIC  where  a.rn = 1)

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT 
# MAGIC identificationTypeCode,
# MAGIC identificationType
# MAGIC , COUNT (*) as count
# MAGIC FROM cleansed.${vars.table}
# MAGIC GROUP BY identificationTypeCode,
# MAGIC identificationType
# MAGIC HAVING COUNT (*) > 1

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT * FROM (
# MAGIC SELECT
# MAGIC *,
# MAGIC row_number() OVER(PARTITION BY identificationTypeCode  order by identificationTypeCode) as rn
# MAGIC FROM  cleansed.${vars.table}
# MAGIC )a where a.rn > 1 

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Source and Target Data
# MAGIC %sql
# MAGIC select
# MAGIC identificationTypeCode,
# MAGIC identificationType
# MAGIC from
# MAGIC (select
# MAGIC TYPE as identificationTypeCode,
# MAGIC TEXT as identificationType,
# MAGIC row_number() over (partition by TYPE order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table} 
# MAGIC )a  
# MAGIC  where  a.rn = 1
# MAGIC except
# MAGIC select
# MAGIC identificationTypeCode,
# MAGIC identificationType
# MAGIC from
# MAGIC cleansed.${vars.table}

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Target and Source Data
# MAGIC %sql
# MAGIC select
# MAGIC identificationTypeCode,
# MAGIC identificationType
# MAGIC from
# MAGIC cleansed.${vars.table}
# MAGIC except
# MAGIC select
# MAGIC identificationTypeCode,
# MAGIC identificationType
# MAGIC from
# MAGIC (select
# MAGIC TYPE as identificationTypeCode,
# MAGIC TEXT as identificationType,
# MAGIC row_number() over (partition by TYPE order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table} 
# MAGIC )a  
# MAGIC  where  a.rn = 1