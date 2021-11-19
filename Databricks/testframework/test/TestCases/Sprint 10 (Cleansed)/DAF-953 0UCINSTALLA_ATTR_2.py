# Databricks notebook source
#config parameters
source = 'ISU' #either CRM or ISU
table = '0UCINSTALLA_ATTR_2'

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
# MAGIC   installationId,
# MAGIC   divisionCode,
# MAGIC   division,
# MAGIC   premise,
# MAGIC   reference,
# MAGIC   meterReadingControlCode,
# MAGIC   meterReadingControl,
# MAGIC   serviceTypeCode,
# MAGIC   serviceType,
# MAGIC   timeZone,
# MAGIC   createdDate,
# MAGIC   createdBy,
# MAGIC   lastChangedDate,
# MAGIC   lastChangedBy,
# MAGIC   authorizationGroupCode,
# MAGIC   deletedIndicator,
# MAGIC   propertyNumber,
# MAGIC   industry,
# MAGIC   addressNumber,
# MAGIC   objectNumber
# MAGIC from
# MAGIC   (
# MAGIC     select
# MAGIC       ANLAGE as installationId,
# MAGIC       SPARTE as divisionCode,
# MAGIC       b.divisionCode as division,
# MAGIC       VSTELLE as premise,
# MAGIC       BEZUG as reference,
# MAGIC       ABLESARTST as meterReadingControlCode,
# MAGIC       derived as meterReadingControl,
# MAGIC       SERVICE as serviceTypeCode,
# MAGIC       c.serviceTypeCode  as serviceType,
# MAGIC       ETIMEZONE as timeZone,
# MAGIC       ERDAT as createdDate,
# MAGIC       ERNAM as createdBy,
# MAGIC       AEDAT as lastChangedDate,
# MAGIC       AENAM as lastChangedBy,
# MAGIC       BEGRU as authorizationGroupCode,
# MAGIC       LOEVM as deletedIndicator,
# MAGIC       ZZ_HAUS as propertyNumber,
# MAGIC       ZZ_PROPTYPE as industry,
# MAGIC       ZZ_ADRNR as addressNumber,
# MAGIC       ZZ_OBJNR as objectNumber, 
# MAGIC       row_number() over (partition by ANLAGE
# MAGIC         order by EXTRACT_DATETIME desc) as rn
# MAGIC     from test.${vars.table} a
# MAGIC       left join cleansed.ISU_0DIVISION_TEXT b on b.divisionCode = a.SPARTE
# MAGIC       left join cleansed.ISU_0UC_SERTYPE_TEXT c on c.serviceTypeCode = a.SERVICE
# MAGIC   ) a where a.rn = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cleansed.ISU_0DIVISION_TEXT

# COMMAND ----------

# DBTITLE 1,[Verification] Count Check
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from cleansed.${vars.table}
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from (select 
# MAGIC clientId,
# MAGIC installationId,
# MAGIC validToDate,
# MAGIC validFromDate,
# MAGIC rateCategoryCode,
# MAGIC rateCategory,
# MAGIC industry,
# MAGIC billingClassCode,
# MAGIC billingClass,
# MAGIC meterReadingUnit,
# MAGIC deltaProcessRecordMode,
# MAGIC logicalDeviceNumber
# MAGIC from
# MAGIC (select
# MAGIC MANDT as clientId,
# MAGIC ANLAGE as installationId,
# MAGIC case
# MAGIC when BIS = '9999-12-31' then '2099-12-31'
# MAGIC else BIS end as validToDate,
# MAGIC AB as validFromDate,
# MAGIC a.TARIFTYP as rateCategoryCode,
# MAGIC b.TTYPBEZ as rateCategory,
# MAGIC BRANCHE as industry,
# MAGIC a.AKLASSE as billingClassCode,
# MAGIC c.billingClass as billingClass,
# MAGIC ABLEINH as meterReadingUnit,
# MAGIC UPDMOD as deltaProcessRecordMode,
# MAGIC ZLOGIKNR as logicalDeviceNumber,
# MAGIC row_number() over (partition by MANDT,ANLAGE,BIS order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table} a
# MAGIC left join cleansed.ISU_0UC_TARIFTYP_TEXT b on b.TARIFTYP = a.TARIFTYP
# MAGIC left join cleansed.ISU_0UC_AKLASSE_TEXT c on c.billingClass = a.AKLASSE
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
# MAGIC industry,
# MAGIC billingClassCode,
# MAGIC billingClass,
# MAGIC meterReadingUnit,
# MAGIC deltaProcessRecordMode,
# MAGIC logicalDeviceNumber, COUNT (*) as count
# MAGIC FROM cleansed.${vars.table}
# MAGIC GROUP BY --clientId,
# MAGIC installationId,
# MAGIC validToDate,
# MAGIC validFromDate,
# MAGIC rateCategoryCode,
# MAGIC rateCategory,
# MAGIC industry,
# MAGIC billingClassCode,
# MAGIC billingClass,
# MAGIC meterReadingUnit,
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
# MAGIC industry,
# MAGIC billingClassCode,
# MAGIC billingClass,
# MAGIC meterReadingUnit,
# MAGIC deltaProcessRecordMode,
# MAGIC logicalDeviceNumber
# MAGIC from
# MAGIC (select
# MAGIC MANDT as clientId,
# MAGIC ANLAGE as installationId,
# MAGIC case
# MAGIC when BIS = '9999-12-31' then '2099-12-31'
# MAGIC else BIS end as validToDate,
# MAGIC AB as validFromDate,
# MAGIC a.TARIFTYP as rateCategoryCode,
# MAGIC b.TTYPBEZ as rateCategory,
# MAGIC BRANCHE as industry,
# MAGIC a.AKLASSE as billingClassCode,
# MAGIC c.billingClass as billingClass,
# MAGIC ABLEINH as meterReadingUnit,
# MAGIC UPDMOD as deltaProcessRecordMode,
# MAGIC ZLOGIKNR as logicalDeviceNumber,
# MAGIC row_number() over (partition by MANDT,ANLAGE,BIS order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table} a
# MAGIC left join cleansed.ISU_0UC_TARIFTYP_TEXT b on b.TARIFTYP = a.TARIFTYP
# MAGIC left join cleansed.ISU_0UC_AKLASSE_TEXT c on c.billingClass = a.AKLASSE
# MAGIC )a where  a.rn = 1
# MAGIC except
# MAGIC select
# MAGIC --clientId,
# MAGIC installationId,
# MAGIC validToDate,
# MAGIC validFromDate,
# MAGIC rateCategoryCode,
# MAGIC rateCategory,
# MAGIC industry,
# MAGIC billingClassCode,
# MAGIC billingClass,
# MAGIC meterReadingUnit,
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
# MAGIC industry,
# MAGIC billingClassCode,
# MAGIC billingClass,
# MAGIC meterReadingUnit,
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
# MAGIC industry,
# MAGIC billingClassCode,
# MAGIC billingClass,
# MAGIC meterReadingUnit,
# MAGIC deltaProcessRecordMode,
# MAGIC logicalDeviceNumber
# MAGIC from
# MAGIC (select
# MAGIC MANDT as clientId,
# MAGIC ANLAGE as installationId,
# MAGIC case
# MAGIC when BIS = '9999-12-31' then '2099-12-31'
# MAGIC else BIS end as validToDate,
# MAGIC AB as validFromDate,
# MAGIC a.TARIFTYP as rateCategoryCode,
# MAGIC b.TTYPBEZ as rateCategory,
# MAGIC BRANCHE as industry,
# MAGIC a.AKLASSE as billingClassCode,
# MAGIC c.billingClass as billingClass,
# MAGIC ABLEINH as meterReadingUnit,
# MAGIC UPDMOD as deltaProcessRecordMode,
# MAGIC ZLOGIKNR as logicalDeviceNumber,
# MAGIC row_number() over (partition by MANDT,ANLAGE,BIS order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table} a
# MAGIC left join cleansed.ISU_0UC_TARIFTYP_TEXT b on b.TARIFTYP = a.TARIFTYP
# MAGIC left join cleansed.ISU_0UC_AKLASSE_TEXT c on c.billingClass = a.AKLASSE
# MAGIC )a where  a.rn = 1
