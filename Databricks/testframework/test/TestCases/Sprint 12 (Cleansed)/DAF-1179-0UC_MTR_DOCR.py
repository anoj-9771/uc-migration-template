# Databricks notebook source
#config parameters
source = 'ISU' #either CRM or ISU
table = '0UC_MTR_DOCR'

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
# MAGIC suppressedMeterReadingDocumentID
# MAGIC ,meterReadingUnit
# MAGIC ,meterReadingReasonCode
# MAGIC ,meterReadingScheduleDate
# MAGIC ,installationId
# MAGIC ,deletedIndicator
# MAGIC ,bwDeltaProcess
# MAGIC from
# MAGIC (select
# MAGIC ABLBELNR as suppressedMeterReadingDocumentID
# MAGIC ,ABLEINH as meterReadingUnit
# MAGIC ,ABLESGR as meterReadingReasonCode
# MAGIC ,ADATSOLL as meterReadingScheduleDate
# MAGIC ,ANLAGE as installationId
# MAGIC ,LOEVM as deletedIndicator
# MAGIC ,UPDMOD as bwDeltaProcess
# MAGIC ,row_number() over (partition by ABLBELNR,ABLESGR,ANLAGE order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table}
# MAGIC )a where  a.rn = 1

# COMMAND ----------

# DBTITLE 1,[Verification] Count Check
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from cleansed.${vars.table}
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from (select
# MAGIC suppressedMeterReadingDocumentID
# MAGIC ,meterReadingUnit
# MAGIC ,meterReadingReasonCode
# MAGIC ,meterReadingScheduleDate
# MAGIC ,installationId
# MAGIC ,deletedIndicator
# MAGIC ,bwDeltaProcess
# MAGIC from
# MAGIC (select
# MAGIC ABLBELNR as suppressedMeterReadingDocumentID
# MAGIC ,ABLEINH as meterReadingUnit
# MAGIC ,ABLESGR as meterReadingReasonCode
# MAGIC ,ADATSOLL as meterReadingScheduleDate
# MAGIC ,ANLAGE as installationId
# MAGIC ,LOEVM as deletedIndicator
# MAGIC ,UPDMOD as bwDeltaProcess
# MAGIC ,row_number() over (partition by ABLBELNR,ABLESGR,ANLAGE order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table})a where  a.rn = 1)

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT suppressedMeterReadingDocumentID
# MAGIC ,meterReadingUnit
# MAGIC ,meterReadingReasonCode
# MAGIC ,meterReadingScheduleDate
# MAGIC ,installationId
# MAGIC ,deletedIndicator
# MAGIC ,bwDeltaProcess
# MAGIC , COUNT (*) as count
# MAGIC FROM cleansed.${vars.table}
# MAGIC GROUP BY suppressedMeterReadingDocumentID
# MAGIC ,meterReadingUnit
# MAGIC ,meterReadingReasonCode
# MAGIC ,meterReadingScheduleDate
# MAGIC ,installationId
# MAGIC ,deletedIndicator
# MAGIC ,bwDeltaProcess
# MAGIC HAVING COUNT (*) > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT * FROM (
# MAGIC SELECT
# MAGIC *,
# MAGIC row_number() OVER(PARTITION BY suppressedMeterReadingDocumentID, meterReadingReasonCode,installationId 
# MAGIC order by suppressedMeterReadingDocumentID, meterReadingReasonCode,installationId) as rn
# MAGIC FROM  cleansed.${vars.table}
# MAGIC )a where a.rn > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Source and Target Data
# MAGIC %sql
# MAGIC (select
# MAGIC suppressedMeterReadingDocumentID
# MAGIC ,meterReadingUnit
# MAGIC ,meterReadingReasonCode
# MAGIC ,meterReadingScheduleDate
# MAGIC ,installationId
# MAGIC ,deletedIndicator
# MAGIC ,bwDeltaProcess
# MAGIC from
# MAGIC (select
# MAGIC ABLBELNR as suppressedMeterReadingDocumentID
# MAGIC ,ABLEINH as meterReadingUnit
# MAGIC ,ABLESGR as meterReadingReasonCode
# MAGIC ,ADATSOLL as meterReadingScheduleDate
# MAGIC ,case
# MAGIC when ANLAGE is null then '' 
# MAGIC else ANLAGE end as installationId
# MAGIC ,LOEVM as deletedIndicator
# MAGIC ,UPDMOD as bwDeltaProcess
# MAGIC ,row_number() over (partition by ABLBELNR,ABLESGR,ANLAGE order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table})a where  a.rn = 1)
# MAGIC except
# MAGIC select
# MAGIC suppressedMeterReadingDocumentID
# MAGIC ,meterReadingUnit
# MAGIC ,meterReadingReasonCode
# MAGIC ,meterReadingScheduleDate
# MAGIC ,installationId
# MAGIC ,deletedIndicator
# MAGIC ,bwDeltaProcess
# MAGIC from
# MAGIC cleansed.${vars.table}

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Target and Source Data
# MAGIC %sql
# MAGIC select
# MAGIC suppressedMeterReadingDocumentID
# MAGIC ,meterReadingUnit
# MAGIC ,meterReadingReasonCode
# MAGIC ,meterReadingScheduleDate
# MAGIC ,installationId
# MAGIC ,deletedIndicator
# MAGIC ,bwDeltaProcess
# MAGIC from
# MAGIC cleansed.${vars.table}
# MAGIC except
# MAGIC (select
# MAGIC suppressedMeterReadingDocumentID
# MAGIC ,meterReadingUnit
# MAGIC ,meterReadingReasonCode
# MAGIC ,meterReadingScheduleDate
# MAGIC ,installationId
# MAGIC ,deletedIndicator
# MAGIC ,bwDeltaProcess
# MAGIC from
# MAGIC (select
# MAGIC ABLBELNR as suppressedMeterReadingDocumentID
# MAGIC ,ABLEINH as meterReadingUnit
# MAGIC ,ABLESGR as meterReadingReasonCode
# MAGIC ,ADATSOLL as meterReadingScheduleDate
# MAGIC ,case
# MAGIC when ANLAGE is null then '' 
# MAGIC else ANLAGE end as installationId
# MAGIC ,LOEVM as deletedIndicator
# MAGIC ,UPDMOD as bwDeltaProcess
# MAGIC ,row_number() over (partition by ABLBELNR,ABLESGR,ANLAGE order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table})a where  a.rn = 1)
