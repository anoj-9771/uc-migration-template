# Databricks notebook source
#config parameters
source = 'ISU' #either CRM or ISU
table = '0UC_DEVICEH_ATTR'

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
# MAGIC equipmentNumber,
# MAGIC validToDate,
# MAGIC validFromDate,
# MAGIC deviceCategoryCombination,
# MAGIC logicalDeviceNumber,
# MAGIC registerGroupCode,
# MAGIC installationDate,
# MAGIC deviceRemovalDate,
# MAGIC activityReasonCode,
# MAGIC deviceLocation,
# MAGIC windingGroup,
# MAGIC deletedIndicator,
# MAGIC bwDeltaProcess,
# MAGIC advancedMeterCapabilityGroup,
# MAGIC messageAttributeId,
# MAGIC materialNumber,
# MAGIC installationId,
# MAGIC addressNumber,
# MAGIC cityName,
# MAGIC houseNumber,
# MAGIC streetName,
# MAGIC postalCode,
# MAGIC superiorFunctionalLocationNumber,
# MAGIC policeEventNumber,
# MAGIC orderNumber,
# MAGIC createdBy
# MAGIC from
# MAGIC (select
# MAGIC EQUNR as equipmentNumber,
# MAGIC BIS as validToDate,
# MAGIC AB as validFromDate,
# MAGIC KOMBINAT as deviceCategoryCombination,
# MAGIC LOGIKNR as logicalDeviceNumber,
# MAGIC ZWGRUPPE as registerGroupCode,
# MAGIC EINBDAT as installationDate,
# MAGIC AUSBDAT as deviceRemovalDate,
# MAGIC GERWECHS as activityReasonCode,
# MAGIC DEVLOC as deviceLocation,
# MAGIC WGRUPPE as windingGroup,
# MAGIC LOEVM as deletedIndicator,
# MAGIC UPDMOD as bwDeltaProcess,
# MAGIC AMCG_CAP_GRP as advancedMeterCapabilityGroup,
# MAGIC MSG_ATTR_ID as messageAttributeId,
# MAGIC ZZMATNR as materialNumber,
# MAGIC ZANLAGE as installationId,
# MAGIC ZADDRNUMBER as addressNumber,
# MAGIC ZCITY1 as cityName,
# MAGIC ZHOUSE_NUM1 as houseNumber,
# MAGIC ZSTREET as streetName,
# MAGIC ZPOST_CODE1 as postalCode,
# MAGIC ZTPLMA as superiorFunctionalLocationNumber,
# MAGIC ZZ_POLICE_EVENT as policeEventNumber,
# MAGIC ZAUFNR as orderNumber,
# MAGIC ZERNAM as createdBy
# MAGIC row_number() over (partition by EQUNR,BIS order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table}
# MAGIC )a where  a.rn = 1

# COMMAND ----------

# DBTITLE 1,[Verification] Count Check
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from cleansed.${vars.table}
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from (select 
# MAGIC equipmentNumber,
# MAGIC validToDate,
# MAGIC validFromDate,
# MAGIC deviceCategoryCombination,
# MAGIC logicalDeviceNumber,
# MAGIC registerGroupCode,
# MAGIC installationDate,
# MAGIC deviceRemovalDate,
# MAGIC activityReasonCode,
# MAGIC deviceLocation,
# MAGIC windingGroup,
# MAGIC deletedIndicator,
# MAGIC bwDeltaProcess,
# MAGIC advancedMeterCapabilityGroup,
# MAGIC messageAttributeId,
# MAGIC materialNumber,
# MAGIC installationId,
# MAGIC addressNumber,
# MAGIC cityName,
# MAGIC houseNumber,
# MAGIC streetName,
# MAGIC postalCode,
# MAGIC superiorFunctionalLocationNumber,
# MAGIC policeEventNumber,
# MAGIC orderNumber,
# MAGIC createdBy
# MAGIC from
# MAGIC (select
# MAGIC EQUNR as equipmentNumber,
# MAGIC BIS as validToDate,
# MAGIC AB as validFromDate,
# MAGIC KOMBINAT as deviceCategoryCombination,
# MAGIC LOGIKNR as logicalDeviceNumber,
# MAGIC ZWGRUPPE as registerGroupCode,
# MAGIC EINBDAT as installationDate,
# MAGIC AUSBDAT as deviceRemovalDate,
# MAGIC GERWECHS as activityReasonCode,
# MAGIC DEVLOC as deviceLocation,
# MAGIC WGRUPPE as windingGroup,
# MAGIC LOEVM as deletedIndicator,
# MAGIC UPDMOD as bwDeltaProcess,
# MAGIC AMCG_CAP_GRP as advancedMeterCapabilityGroup,
# MAGIC MSG_ATTR_ID as messageAttributeId,
# MAGIC ZZMATNR as materialNumber,
# MAGIC ZANLAGE as installationId,
# MAGIC ZADDRNUMBER as addressNumber,
# MAGIC ZCITY1 as cityName,
# MAGIC ZHOUSE_NUM1 as houseNumber,
# MAGIC ZSTREET as streetName,
# MAGIC ZPOST_CODE1 as postalCode,
# MAGIC ZTPLMA as superiorFunctionalLocationNumber,
# MAGIC ZZ_POLICE_EVENT as policeEventNumber,
# MAGIC ZAUFNR as orderNumber,
# MAGIC ZERNAM as createdBy
# MAGIC row_number() over (partition by EQUNR,BIS order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table}
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
# MAGIC BIS as validToDate,
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
# MAGIC BIS as validToDate,
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
