# Databricks notebook source
#config parameters
source = 'ISU' #either CRM or ISU
table = 'TFK047XT'

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
# MAGIC collectionStrategyCode,
# MAGIC collectionStrategyName
# MAGIC from
# MAGIC (select
# MAGIC STRAT as collectionStrategyCode
# MAGIC ,STRATTEXT as collectionStrategyName
# MAGIC ,row_number() over (partition by STRAT order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table}
# MAGIC )a where  a.rn = 1

# COMMAND ----------

# DBTITLE 1,[Verification] Count Check
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from cleansed.${vars.table}
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from (select
# MAGIC collectionStrategyCode
# MAGIC ,collectionStrategyName
# MAGIC from
# MAGIC (select
# MAGIC STRAT as collectionStrategyCode
# MAGIC ,STRATTEXT as collectionStrategyName
# MAGIC ,row_number() over (partition by STRAT order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table} 
# MAGIC )a where  a.rn = 1)

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT collectionStrategyCode,collectionStrategyName, COUNT (*) as count
# MAGIC FROM cleansed.${vars.table}
# MAGIC GROUP BY collectionStrategyCode,collectionStrategyName
# MAGIC HAVING COUNT (*) > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT * FROM (
# MAGIC SELECT
# MAGIC *,
# MAGIC row_number() OVER(PARTITION BY collectionStrategyCode order by collectionStrategyCode) as rn
# MAGIC FROM  cleansed.${vars.table}
# MAGIC )a where a.rn > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Source and Target Data
# MAGIC %sql
# MAGIC select
# MAGIC collectionStrategyCode
# MAGIC ,collectionStrategyName
# MAGIC from
# MAGIC (select
# MAGIC STRAT as collectionStrategyCode
# MAGIC ,STRATTEXT as collectionStrategyName
# MAGIC ,row_number() over (partition by STRAT order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table} -- WHERE LANGU = 'E'
# MAGIC )a where  a.rn = 1
# MAGIC except
# MAGIC select
# MAGIC collectionStrategyCode
# MAGIC ,collectionStrategyName
# MAGIC from
# MAGIC cleansed.${vars.table}

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Target and Source Data
# MAGIC %sql
# MAGIC select
# MAGIC registerTypeCode
# MAGIC ,registerType
# MAGIC from
# MAGIC cleansed.${vars.table}
# MAGIC except
# MAGIC select
# MAGIC registerTypeCode
# MAGIC ,registerType
# MAGIC from
# MAGIC (select
# MAGIC ZWART as registerTypeCode
# MAGIC ,ZWARTTXT as registerType
# MAGIC ,row_number() over (partition by ZWART order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table} -- WHERE LANGU = 'E'
# MAGIC )a where  a.rn = 1
