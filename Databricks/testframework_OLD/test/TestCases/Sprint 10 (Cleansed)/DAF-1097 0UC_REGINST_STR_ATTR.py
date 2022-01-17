# Databricks notebook source
#config parameters
source = 'ISU' #either CRM or ISU
table = '0UC_REGINST_STR_ATTR'

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
# MAGIC logicalRegisterNumber,
# MAGIC registerNotRelevantToBilling,
# MAGIC installationId,
# MAGIC validToDate,
# MAGIC validFromDate,
# MAGIC payRentalPrice,
# MAGIC rateTypeCode,
# MAGIC rateFactGroupCode,
# MAGIC priceClassCode,
# MAGIC deletedIndicator,
# MAGIC bwDeltaProcess,
# MAGIC operationCode
# MAGIC from
# MAGIC (SELECT
# MAGIC LOGIKZW as  logicalRegisterNumber,
# MAGIC ZWNABR as   registerNotRelevantToBilling,
# MAGIC ANLAGE as   installationId,
# MAGIC BIS as      validToDate,
# MAGIC AB as       validFromDate,
# MAGIC GVERRECH as payRentalPrice,
# MAGIC TARIFART as rateTypeCode,
# MAGIC KONDIGR as  rateFactGroupCode,
# MAGIC PREISKLA as priceClassCode,
# MAGIC LOEVM as    deletedIndicator,
# MAGIC UPDMOD as   bwDeltaProcess,
# MAGIC ZOPCODE as  operationCode
# MAGIC ,row_number() over (partition by LOGIKZW, ANLAGE,BIS  order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table}
# MAGIC )a where  a.rn = 1

# COMMAND ----------

# DBTITLE 1,[Verification] Count Check
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from cleansed.${vars.table}
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from (select
# MAGIC logicalRegisterNumber,
# MAGIC registerNotRelevantToBilling,
# MAGIC installationId,
# MAGIC validToDate,
# MAGIC validFromDate,
# MAGIC payRentalPrice,
# MAGIC rateTypeCode,
# MAGIC rateFactGroupCode,
# MAGIC priceClassCode,
# MAGIC deletedIndicator,
# MAGIC bwDeltaProcess,
# MAGIC operationCode
# MAGIC from
# MAGIC (SELECT
# MAGIC LOGIKZW as  logicalRegisterNumber,
# MAGIC ZWNABR as   registerNotRelevantToBilling,
# MAGIC ANLAGE as   installationId,
# MAGIC BIS as      validToDate,
# MAGIC AB as       validFromDate,
# MAGIC GVERRECH as payRentalPrice,
# MAGIC TARIFART as rateTypeCode,
# MAGIC KONDIGR as  rateFactGroupCode,
# MAGIC PREISKLA as priceClassCode,
# MAGIC LOEVM as    deletedIndicator,
# MAGIC UPDMOD as   bwDeltaProcess,
# MAGIC ZOPCODE as  operationCode
# MAGIC ,row_number() over (partition by LOGIKZW, ANLAGE,BIS  order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table}
# MAGIC )a where  a.rn = 1)

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT logicalRegisterNumber,
# MAGIC registerNotRelevantToBilling,
# MAGIC installationId,
# MAGIC validToDate,
# MAGIC validFromDate,
# MAGIC payRentalPrice,
# MAGIC rateTypeCode,
# MAGIC rateFactGroupCode,
# MAGIC priceClassCode,
# MAGIC deletedIndicator,
# MAGIC bwDeltaProcess,
# MAGIC operationCode, COUNT (*) as count
# MAGIC FROM cleansed.${vars.table}
# MAGIC GROUP BY logicalRegisterNumber,
# MAGIC registerNotRelevantToBilling,
# MAGIC installationId,
# MAGIC validToDate,
# MAGIC validFromDate,
# MAGIC payRentalPrice,
# MAGIC rateTypeCode,
# MAGIC rateFactGroupCode,
# MAGIC priceClassCode,
# MAGIC deletedIndicator,
# MAGIC bwDeltaProcess,
# MAGIC operationCode
# MAGIC HAVING COUNT (*) > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT * FROM (
# MAGIC SELECT
# MAGIC *,
# MAGIC row_number() OVER(PARTITION BY logicalRegisterNumber,installationId,validToDate  
# MAGIC order by logicalRegisterNumber,installationId,validToDate) as rn
# MAGIC FROM  cleansed.${vars.table}
# MAGIC )a where a.rn > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Source and Target Data
# MAGIC %sql
# MAGIC select
# MAGIC logicalRegisterNumber,
# MAGIC registerNotRelevantToBilling,
# MAGIC installationId,
# MAGIC validToDate,
# MAGIC validFromDate,
# MAGIC payRentalPrice,
# MAGIC rateTypeCode,
# MAGIC rateFactGroupCode,
# MAGIC priceClassCode,
# MAGIC deletedIndicator,
# MAGIC bwDeltaProcess,
# MAGIC operationCode
# MAGIC from
# MAGIC (SELECT
# MAGIC LOGIKZW as  logicalRegisterNumber,
# MAGIC ZWNABR as   registerNotRelevantToBilling,
# MAGIC ANLAGE as   installationId,
# MAGIC BIS as      validToDate,
# MAGIC AB as       validFromDate,
# MAGIC GVERRECH as payRentalPrice,
# MAGIC TARIFART as rateTypeCode,
# MAGIC KONDIGR as  rateFactGroupCode,
# MAGIC PREISKLA as priceClassCode,
# MAGIC LOEVM as    deletedIndicator,
# MAGIC UPDMOD as   bwDeltaProcess,
# MAGIC ZOPCODE as  operationCode
# MAGIC ,row_number() over (partition by LOGIKZW, ANLAGE,BIS  order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table}
# MAGIC )a where  a.rn = 1
# MAGIC except
# MAGIC select
# MAGIC logicalRegisterNumber,
# MAGIC registerNotRelevantToBilling,
# MAGIC installationId,
# MAGIC validToDate,
# MAGIC validFromDate,
# MAGIC payRentalPrice,
# MAGIC rateTypeCode,
# MAGIC rateFactGroupCode,
# MAGIC priceClassCode,
# MAGIC deletedIndicator,
# MAGIC bwDeltaProcess,
# MAGIC operationCode
# MAGIC from
# MAGIC cleansed.${vars.table}

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Target and Source Data
# MAGIC %sql
# MAGIC select
# MAGIC logicalRegisterNumber,
# MAGIC registerNotRelevantToBilling,
# MAGIC installationId,
# MAGIC validToDate,
# MAGIC validFromDate,
# MAGIC payRentalPrice,
# MAGIC rateTypeCode,
# MAGIC rateFactGroupCode,
# MAGIC priceClassCode,
# MAGIC deletedIndicator,
# MAGIC bwDeltaProcess,
# MAGIC operationCode
# MAGIC from
# MAGIC cleansed.${vars.table}
# MAGIC except
# MAGIC select
# MAGIC logicalRegisterNumber,
# MAGIC registerNotRelevantToBilling,
# MAGIC installationId,
# MAGIC validToDate,
# MAGIC validFromDate,
# MAGIC payRentalPrice,
# MAGIC rateTypeCode,
# MAGIC rateFactGroupCode,
# MAGIC priceClassCode,
# MAGIC deletedIndicator,
# MAGIC bwDeltaProcess,
# MAGIC operationCode
# MAGIC from
# MAGIC (SELECT
# MAGIC LOGIKZW as  logicalRegisterNumber,
# MAGIC ZWNABR as   registerNotRelevantToBilling,
# MAGIC ANLAGE as   installationId,
# MAGIC BIS as      validToDate,
# MAGIC AB as       validFromDate,
# MAGIC GVERRECH as payRentalPrice,
# MAGIC TARIFART as rateTypeCode,
# MAGIC KONDIGR as  rateFactGroupCode,
# MAGIC PREISKLA as priceClassCode,
# MAGIC LOEVM as    deletedIndicator,
# MAGIC UPDMOD as   bwDeltaProcess,
# MAGIC ZOPCODE as  operationCode
# MAGIC ,row_number() over (partition by LOGIKZW, ANLAGE,BIS  order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table}
# MAGIC )a where  a.rn = 1
