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
# MAGIC       b.division as division,
# MAGIC       VSTELLE as premise,
# MAGIC       BEZUG as reference,
# MAGIC       ABLESARTST as meterReadingControlCode,
# MAGIC       d.meterReadingControl as meterReadingControl,
# MAGIC       SERVICE as serviceTypeCode,
# MAGIC       c.serviceType  as serviceType,
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
# MAGIC       left join cleansed.ISU_TE438T d on d.meterReadingControlCode =a.ABLESARTST
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
# MAGIC       b.division as division,
# MAGIC       VSTELLE as premise,
# MAGIC       BEZUG as reference,
# MAGIC       ABLESARTST as meterReadingControlCode,
# MAGIC       d.meterReadingControl as meterReadingControl,
# MAGIC       SERVICE as serviceTypeCode,
# MAGIC       c.serviceType  as serviceType,
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
# MAGIC       left join cleansed.ISU_TE438T d on d.meterReadingControlCode =a.ABLESARTST
# MAGIC       left join cleansed.ISU_0UC_SERTYPE_TEXT c on c.serviceTypeCode = a.SERVICE
# MAGIC   ) a where a.rn = 1)

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT installationId,  divisionCode,  division,  premise,
# MAGIC   reference,  meterReadingControlCode,  meterReadingControl,
# MAGIC   serviceTypeCode,  serviceType,  timeZone,  createdDate,
# MAGIC   createdBy,  lastChangedDate,  lastChangedBy,  authorizationGroupCode,
# MAGIC   deletedIndicator,  propertyNumber,  industry,  addressNumber,
# MAGIC   objectNumber, COUNT (*) as count
# MAGIC FROM cleansed.${vars.table}
# MAGIC GROUP BY installationId,  divisionCode,  division,  premise,
# MAGIC   reference,  meterReadingControlCode,  meterReadingControl,
# MAGIC   serviceTypeCode,  serviceType,  timeZone,  createdDate,
# MAGIC   createdBy,  lastChangedDate,  lastChangedBy,  authorizationGroupCode,
# MAGIC   deletedIndicator,  propertyNumber,  industry,  addressNumber,
# MAGIC   objectNumber
# MAGIC HAVING COUNT (*) > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT * FROM (
# MAGIC SELECT
# MAGIC *,
# MAGIC row_number() OVER(PARTITION BY installationId
# MAGIC order by installationId) as rn
# MAGIC FROM  cleansed.${vars.table}
# MAGIC )a where a.rn > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Source and Target Data
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
# MAGIC       b.division as division,
# MAGIC       VSTELLE as premise,
# MAGIC       BEZUG as reference,
# MAGIC       ABLESARTST as meterReadingControlCode,
# MAGIC       d.meterReadingControl as meterReadingControl,
# MAGIC       SERVICE as serviceTypeCode,
# MAGIC       c.serviceType  as serviceType,
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
# MAGIC       left join cleansed.ISU_TE438T d on d.meterReadingControlCode =a.ABLESARTST
# MAGIC       left join cleansed.ISU_0UC_SERTYPE_TEXT c on c.serviceTypeCode = a.SERVICE
# MAGIC   ) a where a.rn = 1
# MAGIC except
# MAGIC select
# MAGIC installationId,
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
# MAGIC cleansed.${vars.table}

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Target and Source Data
# MAGIC %sql
# MAGIC select
# MAGIC installationId,
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
# MAGIC cleansed.${vars.table}
# MAGIC except
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
# MAGIC       b.division as division,
# MAGIC       VSTELLE as premise,
# MAGIC       BEZUG as reference,
# MAGIC       ABLESARTST as meterReadingControlCode,
# MAGIC       d.meterReadingControl as meterReadingControl,
# MAGIC       SERVICE as serviceTypeCode,
# MAGIC       c.serviceType  as serviceType,
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
# MAGIC       left join cleansed.ISU_TE438T d on d.meterReadingControlCode =a.ABLESARTST
# MAGIC       left join cleansed.ISU_0UC_SERTYPE_TEXT c on c.serviceTypeCode = a.SERVICE
# MAGIC   ) a where a.rn = 1
