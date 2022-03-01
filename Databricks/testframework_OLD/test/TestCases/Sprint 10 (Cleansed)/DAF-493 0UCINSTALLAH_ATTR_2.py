# Databricks notebook source
#config parameters
source = 'ISU' #either CRM or ISU
table = '0UCINSTALLAH_ATTR_2'

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
# MAGIC --clientId,
# MAGIC installationId,
# MAGIC validToDate,
# MAGIC validFromDate,
# MAGIC rateCategoryCode,
# MAGIC rateCategory,
# MAGIC industryCode,
# MAGIC industry,
# MAGIC billingClassCode,
# MAGIC billingClass,
# MAGIC meterReadingUnit,
# MAGIC industrySystemCode,
# MAGIC industrySystem,
# MAGIC deltaProcessRecordMode,
# MAGIC logicalDeviceNumber
# MAGIC from
# MAGIC (select
# MAGIC MANDT as clientId,
# MAGIC ANLAGE as installationId,
# MAGIC case
# MAGIC when  BIS = '9999-12-31' then '2099-12-31'
# MAGIC else BIS end as validToDate,
# MAGIC AB as validFromDate,
# MAGIC a.TARIFTYP as rateCategoryCode,
# MAGIC b.TTYPBEZ as rateCategory,
# MAGIC BRANCHE as industryCode,
# MAGIC st.industry as industry,
# MAGIC a.AKLASSE as billingClassCode,
# MAGIC c.billingClass as billingClass,
# MAGIC ABLEINH as meterReadingUnit,
# MAGIC ISTYPE as industrySystemCode,
# MAGIC nt.industry as industrySystem,
# MAGIC UPDMOD as deltaProcessRecordMode,
# MAGIC ZLOGIKNR as logicalDeviceNumber,
# MAGIC row_number() over (partition by MANDT,ANLAGE,BIS order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table} a
# MAGIC left join cleansed.ISU_0ind_sector_text st on st.industrySystem = a.ISTYPE and st.industryCode = a.BRANCHE --and st.SPRAS = 'E'
# MAGIC left join cleansed.ISU_0UC_TARIFTYP_TEXT b on b.TARIFTYP = a.TARIFTYP
# MAGIC left join cleansed.ISU_0UC_AKLASSE_TEXT c on c.billingClass = a.AKLASSE
# MAGIC left join cleansed.ISU_0IND_NUMSYS_TEXT nt on nt.industrySystem = a.ISTYPE --nt.LANGU = 'E' 
# MAGIC )a where  a.rn = 1

# COMMAND ----------

# DBTITLE 1,[Verification] Count Check
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from cleansed.${vars.table}
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from (select 
# MAGIC --clientId,
# MAGIC installationId,
# MAGIC validToDate,
# MAGIC validFromDate,
# MAGIC rateCategoryCode,
# MAGIC rateCategory,
# MAGIC industryCode,
# MAGIC industry,
# MAGIC billingClassCode,
# MAGIC billingClass,
# MAGIC meterReadingUnit,
# MAGIC industrySystemCode,
# MAGIC industrySystem,
# MAGIC deltaProcessRecordMode,
# MAGIC logicalDeviceNumber
# MAGIC from
# MAGIC (select
# MAGIC MANDT as clientId,
# MAGIC ANLAGE as installationId,
# MAGIC case
# MAGIC when  BIS = '9999-12-31' then '2099-12-31'
# MAGIC else BIS end as validToDate,
# MAGIC AB as validFromDate,
# MAGIC a.TARIFTYP as rateCategoryCode,
# MAGIC b.TTYPBEZ as rateCategory,
# MAGIC BRANCHE as industryCode,
# MAGIC st.industry as industry,
# MAGIC a.AKLASSE as billingClassCode,
# MAGIC c.billingClass as billingClass,
# MAGIC ABLEINH as meterReadingUnit,
# MAGIC ISTYPE as industrySystemCode,
# MAGIC nt.industry as industrySystem,
# MAGIC UPDMOD as deltaProcessRecordMode,
# MAGIC ZLOGIKNR as logicalDeviceNumber,
# MAGIC row_number() over (partition by MANDT,ANLAGE,BIS order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table} a
# MAGIC left join cleansed.ISU_0ind_sector_text st on st.industrySystem = a.ISTYPE and st.industryCode = a.BRANCHE --and st.SPRAS = 'E'
# MAGIC left join cleansed.ISU_0UC_TARIFTYP_TEXT b on b.TARIFTYP = a.TARIFTYP
# MAGIC left join cleansed.ISU_0UC_AKLASSE_TEXT c on c.billingClass = a.AKLASSE
# MAGIC left join cleansed.ISU_0IND_NUMSYS_TEXT nt on nt.industrySystem = a.ISTYPE --nt.LANGU = 'E' 
# MAGIC )a where  a.rn = 1)

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT --clientId,
# MAGIC installationId,
# MAGIC validToDate,
# MAGIC validFromDate,
# MAGIC rateCategoryCode,
# MAGIC rateCategory,
# MAGIC industryCode,
# MAGIC industry,
# MAGIC billingClassCode,
# MAGIC billingClass,
# MAGIC meterReadingUnit,
# MAGIC industrySystemCode,
# MAGIC industrySystem,
# MAGIC deltaProcessRecordMode,
# MAGIC logicalDeviceNumber, COUNT (*) as count
# MAGIC FROM cleansed.${vars.table}
# MAGIC GROUP BY --clientId,
# MAGIC installationId,
# MAGIC validToDate,
# MAGIC validFromDate,
# MAGIC rateCategoryCode,
# MAGIC rateCategory,
# MAGIC industryCode,
# MAGIC industry,
# MAGIC billingClassCode,
# MAGIC billingClass,
# MAGIC meterReadingUnit,
# MAGIC industrySystemCode,
# MAGIC industrySystem,
# MAGIC deltaProcessRecordMode,
# MAGIC logicalDeviceNumber
# MAGIC HAVING COUNT (*) > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT * FROM (
# MAGIC SELECT
# MAGIC *,
# MAGIC row_number() OVER(PARTITION BY --clientId,
# MAGIC installationId,validToDate 
# MAGIC order by --clientId,
# MAGIC installationId,validToDate) as rn
# MAGIC FROM  cleansed.${vars.table}
# MAGIC )a where a.rn > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Source and Target Data
# MAGIC %sql
# MAGIC select 
# MAGIC --clientId,
# MAGIC installationId,
# MAGIC validToDate,
# MAGIC validFromDate,
# MAGIC rateCategoryCode,
# MAGIC rateCategory,
# MAGIC industryCode,
# MAGIC industry,
# MAGIC billingClassCode,
# MAGIC billingClass,
# MAGIC meterReadingUnit,
# MAGIC industrySystemCode,
# MAGIC industrySystem,
# MAGIC deltaProcessRecordMode,
# MAGIC logicalDeviceNumber
# MAGIC from
# MAGIC (select
# MAGIC MANDT as clientId,
# MAGIC ANLAGE as installationId,
# MAGIC case
# MAGIC when  BIS = '9999-12-31' then '2099-12-31'
# MAGIC else BIS end as validToDate,
# MAGIC AB as validFromDate,
# MAGIC a.TARIFTYP as rateCategoryCode,
# MAGIC b.TTYPBEZ as rateCategory,
# MAGIC BRANCHE as industryCode,
# MAGIC st.industry as industry,
# MAGIC a.AKLASSE as billingClassCode,
# MAGIC c.billingClass as billingClass,
# MAGIC ABLEINH as meterReadingUnit,
# MAGIC ISTYPE as industrySystemCode,
# MAGIC nt.industry as industrySystem,
# MAGIC UPDMOD as deltaProcessRecordMode,
# MAGIC ZLOGIKNR as logicalDeviceNumber,
# MAGIC row_number() over (partition by MANDT,ANLAGE,BIS order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table} a
# MAGIC left join cleansed.ISU_0ind_sector_text st on st.industrySystem = a.ISTYPE and st.industryCode = a.BRANCHE --and st.SPRAS = 'E'
# MAGIC left join cleansed.ISU_0UC_TARIFTYP_TEXT b on b.TARIFTYP = a.TARIFTYP
# MAGIC left join cleansed.ISU_0UC_AKLASSE_TEXT c on c.billingClass = a.AKLASSE
# MAGIC left join cleansed.ISU_0IND_NUMSYS_TEXT nt on nt.industrySystem = a.ISTYPE --nt.LANGU = 'E' 
# MAGIC )a where  a.rn = 1
# MAGIC except
# MAGIC select
# MAGIC --clientId,
# MAGIC installationId,
# MAGIC validToDate,
# MAGIC validFromDate,
# MAGIC rateCategoryCode,
# MAGIC rateCategory,
# MAGIC industryCode,
# MAGIC industry,
# MAGIC billingClassCode,
# MAGIC billingClass,
# MAGIC meterReadingUnit,
# MAGIC industrySystemCode,
# MAGIC industrySystem,
# MAGIC deltaProcessRecordMode,
# MAGIC logicalDeviceNumber
# MAGIC from
# MAGIC cleansed.${vars.table}

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Target and Source Data
# MAGIC %sql
# MAGIC select
# MAGIC --clientId,
# MAGIC installationId,
# MAGIC validToDate,
# MAGIC validFromDate,
# MAGIC rateCategoryCode,
# MAGIC rateCategory,
# MAGIC industryCode,
# MAGIC industry,
# MAGIC billingClassCode,
# MAGIC billingClass,
# MAGIC meterReadingUnit,
# MAGIC industrySystemCode,
# MAGIC industrySystem,
# MAGIC deltaProcessRecordMode,
# MAGIC logicalDeviceNumber
# MAGIC from
# MAGIC cleansed.${vars.table}
# MAGIC except
# MAGIC select 
# MAGIC --clientId,
# MAGIC installationId,
# MAGIC validToDate,
# MAGIC validFromDate,
# MAGIC rateCategoryCode,
# MAGIC rateCategory,
# MAGIC industryCode,
# MAGIC industry,
# MAGIC billingClassCode,
# MAGIC billingClass,
# MAGIC meterReadingUnit,
# MAGIC industrySystemCode,
# MAGIC industrySystem,
# MAGIC deltaProcessRecordMode,
# MAGIC logicalDeviceNumber
# MAGIC from
# MAGIC (select
# MAGIC MANDT as clientId,
# MAGIC ANLAGE as installationId,
# MAGIC case
# MAGIC when  BIS = '9999-12-31' then '2099-12-31'
# MAGIC else BIS end as validToDate,
# MAGIC AB as validFromDate,
# MAGIC a.TARIFTYP as rateCategoryCode,
# MAGIC b.TTYPBEZ as rateCategory,
# MAGIC BRANCHE as industryCode,
# MAGIC st.industry as industry,
# MAGIC a.AKLASSE as billingClassCode,
# MAGIC c.billingClass as billingClass,
# MAGIC ABLEINH as meterReadingUnit,
# MAGIC ISTYPE as industrySystemCode,
# MAGIC nt.industry as industrySystem,
# MAGIC UPDMOD as deltaProcessRecordMode,
# MAGIC ZLOGIKNR as logicalDeviceNumber,
# MAGIC row_number() over (partition by MANDT,ANLAGE,BIS order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table} a
# MAGIC left join cleansed.ISU_0ind_sector_text st on st.industrySystem = a.ISTYPE and st.industryCode = a.BRANCHE --and st.SPRAS = 'E'
# MAGIC left join cleansed.ISU_0UC_TARIFTYP_TEXT b on b.TARIFTYP = a.TARIFTYP
# MAGIC left join cleansed.ISU_0UC_AKLASSE_TEXT c on c.billingClass = a.AKLASSE
# MAGIC left join cleansed.ISU_0IND_NUMSYS_TEXT nt on nt.industrySystem = a.ISTYPE --nt.LANGU = 'E' 
# MAGIC )a where  a.rn = 1
