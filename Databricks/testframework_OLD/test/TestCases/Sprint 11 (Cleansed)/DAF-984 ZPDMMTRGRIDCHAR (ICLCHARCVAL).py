# Databricks notebook source
#config parameters
source = 'ISU' #either CRM or ISU
table = 'ICLCHARCVAL'

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
# MAGIC classificationObjectInternalId
# MAGIC ,characteristicInternalId
# MAGIC ,characteristicvalueInternalId
# MAGIC ,classType
# MAGIC ,archivingObjectsInternalId
# MAGIC ,characteristicValueCode
# MAGIC ,validFromDate
# MAGIC ,validToDate
# MAGIC from
# MAGIC (select 
# MAGIC CLASSIFICATIONOBJECTINTERNALID as classificationObjectInternalId,
# MAGIC CHARACTERISTICINTERNALID as characteristicInternalId,
# MAGIC CHARACTERISTICVALUEINTERNALID as characteristicvalueInternalId,
# MAGIC CLASSTYPE as classType,
# MAGIC CHARCARCHIVINGOBJECTINTERNALID as archivingObjectsInternalId,
# MAGIC CHARACTERISTICVALUE as characteristicValueCode,
# MAGIC CHARCVALIDITYSTARTDATE as validFromDate,
# MAGIC CHARCVALIDITYENDDATE as validToDate,
# MAGIC row_number() over (partition by CLASSIFICATIONOBJECTINTERNALID order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table})a where  a.rn = 1

# COMMAND ----------

# DBTITLE 1,[Verification] Count Check
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from cleansed.${vars.table}
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from (select
# MAGIC classificationObjectInternalId
# MAGIC ,characteristicInternalId
# MAGIC ,characteristicvalueInternalId
# MAGIC ,classType
# MAGIC ,archivingObjectsInternalId
# MAGIC ,characteristicValueCode
# MAGIC ,validFromDate
# MAGIC ,validToDate
# MAGIC from
# MAGIC (select 
# MAGIC CLASSIFICATIONOBJECTINTERNALID as classificationObjectInternalId,
# MAGIC CHARACTERISTICINTERNALID as characteristicInternalId,
# MAGIC CHARACTERISTICVALUEINTERNALID as characteristicvalueInternalId,
# MAGIC CLASSTYPE as classType,
# MAGIC CHARCARCHIVINGOBJECTINTERNALID as archivingObjectsInternalId,
# MAGIC CHARACTERISTICVALUE as characteristicValueCode,
# MAGIC CHARCVALIDITYSTARTDATE as validFromDate,
# MAGIC CHARCVALIDITYENDDATE as validToDate,
# MAGIC row_number() over (partition by CLASSIFICATIONOBJECTINTERNALID,CHARACTERISTICINTERNALID,
# MAGIC CHARACTERISTICVALUEINTERNALID,CLASSTYPE,CHARCARCHIVINGOBJECTINTERNALID order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table})a where  a.rn = 1)

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT classificationObjectInternalId,characteristicInternalId,characteristicvalueInternalId
# MAGIC ,classType,archivingObjectsInternalId,characteristicValueCode,validFromDate,validToDate,
# MAGIC COUNT (*) as count
# MAGIC FROM cleansed.${vars.table}
# MAGIC GROUP BY classificationObjectInternalId,characteristicInternalId,characteristicvalueInternalId
# MAGIC ,classType,archivingObjectsInternalId,characteristicValueCode,validFromDate,validToDate
# MAGIC HAVING COUNT (*) > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT * FROM (
# MAGIC SELECT
# MAGIC *,
# MAGIC row_number() OVER(PARTITION BY classificationObjectInternalId,characteristicInternalId,characteristicvalueInternalId
# MAGIC ,classType,archivingObjectsInternalId 
# MAGIC order by classificationObjectInternalId,characteristicInternalId,characteristicvalueInternalId
# MAGIC ,classType,archivingObjectsInternalId) as rn
# MAGIC FROM  cleansed.${vars.table}
# MAGIC )a where a.rn > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Source and Target Data
# MAGIC %sql
# MAGIC (select
# MAGIC classificationObjectInternalId
# MAGIC ,characteristicInternalId
# MAGIC ,characteristicvalueInternalId
# MAGIC ,classType
# MAGIC ,archivingObjectsInternalId
# MAGIC ,characteristicValueCode
# MAGIC ,validFromDate
# MAGIC ,validToDate
# MAGIC from
# MAGIC (select 
# MAGIC case
# MAGIC when CLASSIFICATIONOBJECTINTERNALID is null then '' 
# MAGIC else CLASSIFICATIONOBJECTINTERNALID end as classificationObjectInternalId,
# MAGIC case
# MAGIC when CHARACTERISTICINTERNALID is null then ''
# MAGIC else CHARACTERISTICINTERNALID end as characteristicInternalId,
# MAGIC case
# MAGIC when CHARACTERISTICVALUEINTERNALID is null then ''
# MAGIC else CHARACTERISTICVALUEINTERNALID end as characteristicvalueInternalId,
# MAGIC case
# MAGIC when CLASSTYPE is null then ''
# MAGIC else CLASSTYPE end as classType,
# MAGIC case 
# MAGIC when CHARCARCHIVINGOBJECTINTERNALID is null then ''
# MAGIC else CHARCARCHIVINGOBJECTINTERNALID end as archivingObjectsInternalId,
# MAGIC CHARACTERISTICVALUE  as characteristicValueCode,
# MAGIC cast(CHARCVALIDITYSTARTDATE as DATE) as validFromDate,
# MAGIC cast(CHARCVALIDITYENDDATE as DATE) as validToDate,
# MAGIC row_number() over (partition by CLASSIFICATIONOBJECTINTERNALID,CHARACTERISTICINTERNALID,
# MAGIC CHARACTERISTICVALUEINTERNALID,CLASSTYPE,CHARCARCHIVINGOBJECTINTERNALID order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table})a where  a.rn = 1)
# MAGIC except
# MAGIC select
# MAGIC classificationObjectInternalId
# MAGIC ,characteristicInternalId
# MAGIC ,characteristicvalueInternalId
# MAGIC ,classType
# MAGIC ,archivingObjectsInternalId
# MAGIC ,characteristicValueCode
# MAGIC ,validFromDate
# MAGIC ,validToDate
# MAGIC from
# MAGIC cleansed.${vars.table}

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Target and Source Data
# MAGIC %sql
# MAGIC (select
# MAGIC classificationObjectInternalId
# MAGIC ,characteristicInternalId
# MAGIC ,characteristicvalueInternalId
# MAGIC ,classType
# MAGIC ,archivingObjectsInternalId
# MAGIC ,characteristicValueCode
# MAGIC ,validFromDate
# MAGIC ,validToDate
# MAGIC from cleansed.${vars.table})
# MAGIC except
# MAGIC (select
# MAGIC classificationObjectInternalId
# MAGIC ,characteristicInternalId
# MAGIC ,characteristicvalueInternalId
# MAGIC ,classType
# MAGIC ,archivingObjectsInternalId
# MAGIC ,characteristicValueCode
# MAGIC ,validFromDate
# MAGIC ,validToDate
# MAGIC from
# MAGIC (select 
# MAGIC case
# MAGIC when CLASSIFICATIONOBJECTINTERNALID is null then '' 
# MAGIC else CLASSIFICATIONOBJECTINTERNALID end as classificationObjectInternalId,
# MAGIC case
# MAGIC when CHARACTERISTICINTERNALID is null then ''
# MAGIC else CHARACTERISTICINTERNALID end as characteristicInternalId,
# MAGIC case
# MAGIC when CHARACTERISTICVALUEINTERNALID is null then ''
# MAGIC else CHARACTERISTICVALUEINTERNALID end as characteristicvalueInternalId,
# MAGIC case
# MAGIC when CLASSTYPE is null then ''
# MAGIC else CLASSTYPE end as classType,
# MAGIC case 
# MAGIC when CHARCARCHIVINGOBJECTINTERNALID is null then ''
# MAGIC else CHARCARCHIVINGOBJECTINTERNALID end as archivingObjectsInternalId,
# MAGIC CHARACTERISTICVALUE  as characteristicValueCode,
# MAGIC --cast(CHARCVALIDITYSTARTDATE as DATE) as validFromDate,
# MAGIC CHARCVALIDITYSTARTDATE as validFromDate,
# MAGIC cast(CHARCVALIDITYENDDATE as DATE) as validToDate,
# MAGIC row_number() over (partition by CLASSIFICATIONOBJECTINTERNALID,CHARACTERISTICINTERNALID,
# MAGIC CHARACTERISTICVALUEINTERNALID,CLASSTYPE,CHARCARCHIVINGOBJECTINTERNALID order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table})a where  a.rn = 1)

# COMMAND ----------

# MAGIC %sql 
# MAGIC select count(*) from test.ISU_ICLCHARCVAL where CHARCVALIDITYSTARTDATE < '1900-01-01'
# MAGIC except
# MAGIC select count(*) from cleansed.ISU_ICLCHARCVAL 

# COMMAND ----------

# MAGIC %sql 
# MAGIC select count(*) from cleansed.ISU_ICLCHARCVAL 
# MAGIC except
# MAGIC select count(*) from test.ISU_ICLCHARCVAL where CHARCVALIDITYSTARTDATE < '1900-01-01'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from test.ISU_ICLCHARCVAL
