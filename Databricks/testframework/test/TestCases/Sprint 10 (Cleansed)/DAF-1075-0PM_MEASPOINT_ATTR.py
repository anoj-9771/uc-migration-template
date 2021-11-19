# Databricks notebook source
#config parameters
source = 'ISU' #either CRM or ISU
table = '0PM_MEASPOINT_ATTR'

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
# MAGIC measuringPointId
# MAGIC ,measuringPointObjectNumber
# MAGIC ,positionNumber
# MAGIC ,measuringPointCategory
# MAGIC ,createdDate
# MAGIC ,inactiveIndicator
# MAGIC ,internalCharacteristic
# MAGIC ,measurementRangeUnit
# MAGIC ,measurementReadingCatalogCode
# MAGIC ,measurementReadingGroupCode
# MAGIC ,lastChangedDate
# MAGIC ,deltaDate
# MAGIC from
# MAGIC (select
# MAGIC POINT as measuringPointId
# MAGIC ,MPOBJ as measuringPointObjectNumber
# MAGIC ,PSORT as positionNumber
# MAGIC ,MPTYP as measuringPointCategory
# MAGIC ,ERDAT as createdDate
# MAGIC ,INACT as inactiveIndicator
# MAGIC ,ATINN as internalCharacteristic
# MAGIC ,MRNGU as measurementRangeUnit
# MAGIC ,CODCT as measurementReadingCatalogCode
# MAGIC ,CODGR as measurementReadingGroupCode
# MAGIC ,AEDAT as lastChangedDate
# MAGIC ,DELTADATE as deltaDate
# MAGIC ,row_number() over (partition by POINT order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table}
# MAGIC )a where  a.rn = 1

# COMMAND ----------

# DBTITLE 1,[Verification] Count Check
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from cleansed.${vars.table}
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from (select
# MAGIC measuringPointId
# MAGIC ,measuringPointObjectNumber
# MAGIC ,positionNumber
# MAGIC ,measuringPointCategory
# MAGIC ,createdDate
# MAGIC ,inactiveIndicator
# MAGIC ,internalCharacteristic
# MAGIC ,measurementRangeUnit
# MAGIC ,measurementReadingCatalogCode
# MAGIC ,measurementReadingGroupCode
# MAGIC ,lastChangedDate
# MAGIC ,deltaDate
# MAGIC from
# MAGIC (select
# MAGIC POINT as measuringPointId
# MAGIC ,MPOBJ as measuringPointObjectNumber
# MAGIC ,PSORT as positionNumber
# MAGIC ,MPTYP as measuringPointCategory
# MAGIC ,ERDAT as createdDate
# MAGIC ,INACT as inactiveIndicator
# MAGIC ,ATINN as internalCharacteristic
# MAGIC ,MRNGU as measurementRangeUnit
# MAGIC ,CODCT as measurementReadingCatalogCode
# MAGIC ,CODGR as measurementReadingGroupCode
# MAGIC ,AEDAT as lastChangedDate
# MAGIC ,DELTADATE as deltaDate
# MAGIC ,row_number() over (partition by POINT order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table}
# MAGIC )a where  a.rn = 1)

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT measuringPointId
# MAGIC , COUNT (*) as count
# MAGIC FROM cleansed.${vars.table}
# MAGIC GROUP BY measuringPointId
# MAGIC HAVING COUNT (*) > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT * FROM (
# MAGIC SELECT
# MAGIC *,
# MAGIC row_number() OVER(PARTITION BY measuringPointId  order by measuringPointId) as rn
# MAGIC FROM  cleansed.${vars.table}
# MAGIC )a where a.rn > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Source and Target Data
# MAGIC %sql
# MAGIC select
# MAGIC measuringPointId
# MAGIC ,measuringPointObjectNumber
# MAGIC ,positionNumber
# MAGIC ,measuringPointCategory
# MAGIC ,createdDate
# MAGIC ,inactiveIndicator
# MAGIC ,internalCharacteristic
# MAGIC ,measurementRangeUnit
# MAGIC ,measurementReadingCatalogCode
# MAGIC ,measurementReadingGroupCode
# MAGIC ,lastChangedDate
# MAGIC ,deltaDate
# MAGIC from
# MAGIC (select
# MAGIC POINT as measuringPointId
# MAGIC ,MPOBJ as measuringPointObjectNumber
# MAGIC ,PSORT as positionNumber
# MAGIC ,MPTYP as measuringPointCategory
# MAGIC ,ERDAT as createdDate
# MAGIC ,INACT as inactiveIndicator
# MAGIC ,ATINN as internalCharacteristic
# MAGIC ,MRNGU as measurementRangeUnit
# MAGIC ,CODCT as measurementReadingCatalogCode
# MAGIC ,CODGR as measurementReadingGroupCode
# MAGIC ,AEDAT as lastChangedDate
# MAGIC ,DELTADATE as deltaDate
# MAGIC ,row_number() over (partition by POINT order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table}
# MAGIC )a where  a.rn = 1
# MAGIC except
# MAGIC select
# MAGIC measuringPointId
# MAGIC ,measuringPointObjectNumber
# MAGIC ,positionNumber
# MAGIC ,measuringPointCategory
# MAGIC ,createdDate
# MAGIC ,inactiveIndicator
# MAGIC ,internalCharacteristic
# MAGIC ,measurementRangeUnit
# MAGIC ,measurementReadingCatalogCode
# MAGIC ,measurementReadingGroupCode
# MAGIC ,lastChangedDate
# MAGIC ,deltaDate
# MAGIC from
# MAGIC cleansed.${vars.table}

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Target and Source Data
# MAGIC %sql
# MAGIC select
# MAGIC measuringPointId
# MAGIC ,measuringPointObjectNumber
# MAGIC ,positionNumber
# MAGIC ,measuringPointCategory
# MAGIC ,createdDate
# MAGIC ,inactiveIndicator
# MAGIC ,internalCharacteristic
# MAGIC ,measurementRangeUnit
# MAGIC ,measurementReadingCatalogCode
# MAGIC ,measurementReadingGroupCode
# MAGIC ,lastChangedDate
# MAGIC ,deltaDate
# MAGIC from
# MAGIC cleansed.${vars.table}
# MAGIC except
# MAGIC select
# MAGIC measuringPointId
# MAGIC ,measuringPointObjectNumber
# MAGIC ,positionNumber
# MAGIC ,measuringPointCategory
# MAGIC ,createdDate
# MAGIC ,inactiveIndicator
# MAGIC ,internalCharacteristic
# MAGIC ,measurementRangeUnit
# MAGIC ,measurementReadingCatalogCode
# MAGIC ,measurementReadingGroupCode
# MAGIC ,lastChangedDate
# MAGIC ,deltaDate
# MAGIC from
# MAGIC (select
# MAGIC POINT as measuringPointId
# MAGIC ,MPOBJ as measuringPointObjectNumber
# MAGIC ,PSORT as positionNumber
# MAGIC ,MPTYP as measuringPointCategory
# MAGIC ,ERDAT as createdDate
# MAGIC ,INACT as inactiveIndicator
# MAGIC ,ATINN as internalCharacteristic
# MAGIC ,MRNGU as measurementRangeUnit
# MAGIC ,CODCT as measurementReadingCatalogCode
# MAGIC ,CODGR as measurementReadingGroupCode
# MAGIC ,AEDAT as lastChangedDate
# MAGIC ,DELTADATE as deltaDate
# MAGIC ,row_number() over (partition by POINT order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table}
# MAGIC )a where  a.rn = 1
