# Databricks notebook source
#config parameters
source = 'ISU' #either CRM or ISU
table = '0UC_INSTFACTS'

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
# MAGIC seasonNumber
# MAGIC ,installationId
# MAGIC ,operandCode
# MAGIC ,validFromDate
# MAGIC ,validToDate
# MAGIC ,entryValue
# MAGIC ,valueToBeBilled
# MAGIC ,operandValue
# MAGIC ,amount
# MAGIC ,bwDeltaProcess
# MAGIC ,measurementUnit
# MAGIC from
# MAGIC (select 
# MAGIC SAISON as  seasonNumber,
# MAGIC ANLAGE as  installationId,
# MAGIC OPERAND as operandCode,
# MAGIC AB as      validFromDate,
# MAGIC BIS as     validToDate,
# MAGIC WERT1 as   entryValue,
# MAGIC WERT2 as   valueToBeBilled,
# MAGIC STRING3 as operandValue,
# MAGIC BETRAG as  amount,
# MAGIC UPDMOD as  bwDeltaProcess,
# MAGIC MASS as  measurementUnit
# MAGIC ,row_number() over (partition by SAISON,ANLAGE,OPERAND,AB order by SAISON,ANLAGE,OPERAND,AB desc) as rn
# MAGIC from test.${vars.table}
# MAGIC )a where  a.rn = 1

# COMMAND ----------

# DBTITLE 1,[Verification] Count Check
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from cleansed.${vars.table}
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from (select
# MAGIC seasonNumber
# MAGIC ,installationId
# MAGIC ,operandCode
# MAGIC ,validFromDate
# MAGIC ,validToDate
# MAGIC ,entryValue
# MAGIC ,valueToBeBilled
# MAGIC ,operandValue
# MAGIC ,amount
# MAGIC ,bwDeltaProcess
# MAGIC ,measurementUnit
# MAGIC from
# MAGIC (select 
# MAGIC SAISON as  seasonNumber,
# MAGIC ANLAGE as  installationId,
# MAGIC OPERAND as operandCode,
# MAGIC AB as      validFromDate,
# MAGIC BIS as     validToDate,
# MAGIC WERT1 as   entryValue,
# MAGIC WERT2 as   valueToBeBilled,
# MAGIC STRING3 as operandValue,
# MAGIC BETRAG as  amount,
# MAGIC UPDMOD as  bwDeltaProcess,
# MAGIC MASS as  measurementUnit
# MAGIC ,row_number() over (partition by SAISON,ANLAGE,OPERAND,AB order by SAISON,ANLAGE,OPERAND,AB desc) as rn
# MAGIC from test.${vars.table}
# MAGIC )a where  a.rn = 1)

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT seasonNumber,installationId,operandCode,validFromDate
# MAGIC ,validToDate,entryValue,valueToBeBilled,operandValue
# MAGIC ,amount,bwDeltaProcess,measurementUnit, COUNT (*) as count
# MAGIC FROM cleansed.${vars.table}
# MAGIC GROUP BY seasonNumber,installationId,operandCode,validFromDate
# MAGIC ,validToDate,entryValue,valueToBeBilled,operandValue
# MAGIC ,amount,bwDeltaProcess,measurementUnit
# MAGIC HAVING COUNT (*) > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT * FROM (
# MAGIC SELECT
# MAGIC *,
# MAGIC row_number() OVER(PARTITION BY seasonNumber,installationId,operandCode,validFromDate 
# MAGIC order by seasonNumber,installationId,operandCode,validFromDate) as rn
# MAGIC FROM  cleansed.${vars.table}
# MAGIC )a where a.rn > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Source and Target Data
# MAGIC %sql
# MAGIC select
# MAGIC seasonNumber
# MAGIC ,installationId
# MAGIC ,operandCode
# MAGIC ,validFromDate
# MAGIC ,validToDate
# MAGIC ,entryValue
# MAGIC ,valueToBeBilled
# MAGIC ,operandValue
# MAGIC ,amount
# MAGIC ,bwDeltaProcess
# MAGIC ,measurementUnit
# MAGIC from
# MAGIC (select case
# MAGIC when SAISON IS NULL then ''
# MAGIC else SAISON end as seasonNumber,
# MAGIC ANLAGE as  installationId,
# MAGIC OPERAND as operandCode,
# MAGIC AB as      validFromDate,
# MAGIC BIS as     validToDate,
# MAGIC cast(WERT1 as decimal(16,7))as entryValue,
# MAGIC cast(WERT2 as decimal(16,7))as valueToBeBilled,
# MAGIC STRING3 as operandValue,
# MAGIC cast(BETRAG as decimal(13,2))as amount,
# MAGIC UPDMOD as  bwDeltaProcess,
# MAGIC MASS as  measurementUnit
# MAGIC ,row_number() over (partition by SAISON,ANLAGE,OPERAND,AB order by SAISON,ANLAGE,OPERAND,AB desc) as rn
# MAGIC from test.${vars.table}
# MAGIC )a where  a.rn = 1 
# MAGIC except
# MAGIC select
# MAGIC seasonNumber
# MAGIC ,installationId
# MAGIC ,operandCode
# MAGIC ,validFromDate
# MAGIC ,validToDate
# MAGIC ,entryValue
# MAGIC ,valueToBeBilled
# MAGIC ,operandValue
# MAGIC ,amount
# MAGIC ,bwDeltaProcess
# MAGIC ,measurementUnit
# MAGIC from
# MAGIC cleansed.${vars.table}

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Target and Source Data
# MAGIC %sql
# MAGIC select
# MAGIC seasonNumber
# MAGIC ,installationId
# MAGIC ,operandCode
# MAGIC ,validFromDate
# MAGIC ,validToDate
# MAGIC ,entryValue
# MAGIC ,valueToBeBilled
# MAGIC ,operandValue
# MAGIC ,amount
# MAGIC ,bwDeltaProcess
# MAGIC ,measurementUnit
# MAGIC from
# MAGIC cleansed.${vars.table}
# MAGIC except
# MAGIC select
# MAGIC seasonNumber
# MAGIC ,installationId
# MAGIC ,operandCode
# MAGIC ,validFromDate
# MAGIC ,validToDate
# MAGIC ,entryValue
# MAGIC ,valueToBeBilled
# MAGIC ,operandValue
# MAGIC ,amount
# MAGIC ,bwDeltaProcess
# MAGIC ,measurementUnit
# MAGIC from
# MAGIC (select case
# MAGIC when SAISON IS NULL then ''
# MAGIC else SAISON end as seasonNumber,
# MAGIC ANLAGE as  installationId,
# MAGIC OPERAND as operandCode,
# MAGIC AB as      validFromDate,
# MAGIC BIS as     validToDate,
# MAGIC cast(WERT1 as decimal(16,7))as entryValue,
# MAGIC cast(WERT2 as decimal(16,7))as valueToBeBilled,
# MAGIC STRING3 as operandValue,
# MAGIC cast(BETRAG as decimal(13,2))as amount,
# MAGIC UPDMOD as  bwDeltaProcess,
# MAGIC MASS as  measurementUnit
# MAGIC ,row_number() over (partition by SAISON,ANLAGE,OPERAND,AB order by SAISON,ANLAGE,OPERAND,AB desc) as rn
# MAGIC from test.${vars.table}
# MAGIC )a where  a.rn = 1 
