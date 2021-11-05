# Databricks notebook source
#config parameters
source = 'ISU' #either CRM or ISU
table = 'ZCD_TPROP_REL'

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
# MAGIC property1Number
# MAGIC ,property2Number
# MAGIC ,relationshipTypeCode1
# MAGIC ,validFromDate
# MAGIC ,relationshipTypeCode2
# MAGIC ,validToDate
# MAGIC from
# MAGIC (select
# MAGIC PROPERTY1 as property1Number
# MAGIC ,PROPERTY2 as property2Number
# MAGIC ,REL_TYPE1 as relationshipTypeCode1
# MAGIC ,DATE_FROM as validFromDate
# MAGIC ,REL_TYPE2 as relationshipTypeCode2
# MAGIC ,DATE_TO as validToDate
# MAGIC ,row_number() over (partition by PROPERTY1,PROPERTY2,REL_TYPE1,REL_TYPE2 order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table}
# MAGIC )a where  a.rn = 1

# COMMAND ----------

# DBTITLE 1,[Verification] Count Check
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from cleansed.${vars.table}
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from (select
# MAGIC property1Number
# MAGIC ,property2Number
# MAGIC ,relationshipTypeCode1
# MAGIC ,validFromDate
# MAGIC ,relationshipTypeCode2
# MAGIC ,validToDate
# MAGIC from
# MAGIC (select
# MAGIC PROPERTY1 as property1Number
# MAGIC ,PROPERTY2 as property2Number
# MAGIC ,REL_TYPE1 as relationshipTypeCode1
# MAGIC ,DATE_FROM as validFromDate
# MAGIC ,REL_TYPE2 as relationshipTypeCode2
# MAGIC ,DATE_TO as validToDate
# MAGIC ,row_number() over (partition by PROPERTY1,PROPERTY2,REL_TYPE1,REL_TYPE2 order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table}
# MAGIC )a where  a.rn = 1)

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT property1Number,property2Number,relationshipTypeCode1,relationshipTypeCode2, COUNT (*) as count
# MAGIC FROM cleansed.${vars.table}
# MAGIC GROUP BY property1Number,property2Number,relationshipTypeCode1,relationshipTypeCode2
# MAGIC HAVING COUNT (*) > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT * FROM (
# MAGIC SELECT
# MAGIC *,
# MAGIC row_number() OVER(PARTITION BY property1Number,property2Number,relationshipTypeCode1,relationshipTypeCode2 order by property1Number,property2Number,relationshipTypeCode1,relationshipTypeCode2) as rn
# MAGIC FROM  cleansed.${vars.table}
# MAGIC )a where a.rn > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Source and Target Data
# MAGIC %sql
# MAGIC select
# MAGIC property1Number
# MAGIC ,property2Number
# MAGIC ,relationshipTypeCode1
# MAGIC ,validFromDate
# MAGIC ,relationshipTypeCode2
# MAGIC ,validToDate
# MAGIC from
# MAGIC (select
# MAGIC PROPERTY1 as property1Number
# MAGIC ,PROPERTY2 as property2Number
# MAGIC ,REL_TYPE1 as relationshipTypeCode1
# MAGIC ,DATE_FROM as validFromDate
# MAGIC ,REL_TYPE2 as relationshipTypeCode2
# MAGIC ,DATE_TO as validToDate
# MAGIC ,row_number() over (partition by PROPERTY1,PROPERTY2,REL_TYPE1,REL_TYPE2 order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table}
# MAGIC )a where  a.rn = 1
# MAGIC except
# MAGIC select
# MAGIC property1Number
# MAGIC ,property2Number
# MAGIC ,relationshipTypeCode1
# MAGIC ,validFromDate
# MAGIC ,relationshipTypeCode2
# MAGIC ,validToDate
# MAGIC from
# MAGIC cleansed.${vars.table}

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Target and Source Data
# MAGIC %sql
# MAGIC select
# MAGIC property1Number
# MAGIC ,property2Number
# MAGIC ,relationshipTypeCode1
# MAGIC ,validFromDate
# MAGIC ,relationshipTypeCode2
# MAGIC ,validToDate
# MAGIC from
# MAGIC cleansed.${vars.table}
# MAGIC except
# MAGIC select
# MAGIC property1Number
# MAGIC ,property2Number
# MAGIC ,relationshipTypeCode1
# MAGIC ,validFromDate
# MAGIC ,relationshipTypeCode2
# MAGIC ,validToDate
# MAGIC from
# MAGIC (select
# MAGIC PROPERTY1 as property1Number
# MAGIC ,PROPERTY2 as property2Number
# MAGIC ,REL_TYPE1 as relationshipTypeCode1
# MAGIC ,DATE_FROM as validFromDate
# MAGIC ,REL_TYPE2 as relationshipTypeCode2
# MAGIC ,DATE_TO as validToDate
# MAGIC ,row_number() over (partition by PROPERTY1,PROPERTY2,REL_TYPE1,REL_TYPE2 order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table}
# MAGIC )a where  a.rn = 1
