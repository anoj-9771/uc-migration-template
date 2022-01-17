# Databricks notebook source
# DBTITLE 1,Target
# MAGIC %sql
# MAGIC select * from curated.dimInstallation --where paymentAssistSchemeFlag = 'Y'

# COMMAND ----------

# DBTITLE 1,[Target] Schema Check
lakedftarget = spark.sql("select * from curated.dimInstallation")
lakedftarget.printSchema()

# COMMAND ----------

# DBTITLE 1,[Source] Applying Transformation
# MAGIC %sql
# MAGIC select * from (
# MAGIC select
# MAGIC a.installationId as installationId
# MAGIC ,'ISU' as sourceSystemCode
# MAGIC ,b.validFromDate as validFromDate
# MAGIC ,b.validToDate as validToDate
# MAGIC ,a.divisionCode as divisionCode
# MAGIC ,a.division as division
# MAGIC ,b.rateCategoryCode as rateCategoryCode
# MAGIC ,b.rateCategory as rateCategory
# MAGIC ,b.industryCode as industryCode
# MAGIC ,b.industry as industry
# MAGIC ,b.billingClassCode as billingClassCode
# MAGIC ,b.billingClass as billingClass
# MAGIC ,b.industrySystemCode as industrySystemCode
# MAGIC ,b.industrySystem as industrySystem
# MAGIC ,a.meterReadingControlCode as meterReadingControlCode
# MAGIC ,a.meterReadingControl as meterReadingControl
# MAGIC ,a.authorizationGroupCode as authorizationGroupCode
# MAGIC ,a.serviceTypeCode as serviceTypeCode
# MAGIC ,a.serviceType as serviceType
# MAGIC ,case
# MAGIC  when c.disconnectionDocumentNumber is null then ''
# MAGIC  else  c.disconnectionDocumentNumber end as disconnectionDocumentNumber
# MAGIC  ,case
# MAGIC  when c.disconnectionActivityPeriod is null then ''
# MAGIC  else  c.disconnectionActivityPeriod end as disconnectionActivityPeriod
# MAGIC ,case
# MAGIC  when c.disconnectionObjectNumber is null then ''
# MAGIC  else  c.disconnectionObjectNumber end as disconnectionObjectNumber
# MAGIC ,c.disconnectionDate as disconnectionDate
# MAGIC ,c.disconnectionActivityTypeCode as disconnectionActivityTypeCode
# MAGIC ,c.disconnectionActivityType as disconnectionActivityType
# MAGIC ,c.disconnectionObjectTypeCode as disconnectionObjectTypeCode
# MAGIC ,c.disconnectionReasonCode as disconnectionReasonCode
# MAGIC ,c.disconnectionReason as disconnectionReason
# MAGIC ,c.disconnectionReconnectionStatusCode as disconnectionReconnectionStatusCode
# MAGIC ,c.disconnectionReconnectionStatus as disconnectionReconnectionStatus
# MAGIC ,c.disconnectionDocumentStatusCode as disconnectionDocumentStatusCode
# MAGIC ,c.disconnectionDocumentStatus as disconnectionDocumentStatus
# MAGIC ,c.ProcessingVariantCode as disconnectionProcessingVariantCode
# MAGIC ,c.ProcessingVariant as disconnectionProcessingVariant
# MAGIC ,a.createdDate as createdDate
# MAGIC ,a.createdBy as createdBy
# MAGIC ,a.lastchangedDate as changedDate
# MAGIC ,a.lastchangedBy as changedDate
# MAGIC from
# MAGIC cleansed.ISU_0UCINSTALLA_ATTR_2  a
# MAGIC left join cleansed.ISU_0UCINSTALLAH_ATTR_2 b
# MAGIC ON
# MAGIC a.installationId = b.installationId
# MAGIC 
# MAGIC left join cleansed.isu_0UC_ISU_32 c
# MAGIC ON b.installationId = c.installationId
# MAGIC AND b.validToDate in (to_date('9999-12-31'),to_date('2099-12-31'))
# MAGIC and c.referenceObjectTypeCode = 'INSTLN'
# MAGIC AND c.validToDate in (to_date('9999-12-31'),to_date('2099-12-31'))
# MAGIC union all 
# MAGIC select * from (
# MAGIC select 
# MAGIC '-1' as installationId 
# MAGIC ,'ISU' as sourceSystemCode 
# MAGIC ,null as validFromDate 
# MAGIC ,'2099-12-31' as validToDate 
# MAGIC ,null as divisionCode 
# MAGIC ,null as division 
# MAGIC ,null as rateCategoryCode 
# MAGIC ,null as rateCategory 
# MAGIC ,null as industryCode 
# MAGIC ,null as industry 
# MAGIC ,null as billingClassCode 
# MAGIC ,null as billingClass 
# MAGIC ,null as industrySystemCode 
# MAGIC ,null as industrySystem 
# MAGIC ,null as meterReadingControlCode 
# MAGIC ,null as meterReadingControl 
# MAGIC ,null as authorizationGroupCode 
# MAGIC ,null as serviceTypeCode 
# MAGIC ,null as serviceType 
# MAGIC ,'' as disconnectionDocumentNumber 
# MAGIC ,'' as disconnectionActivityPeriod 
# MAGIC ,'' as disconnectionObjectNumber 
# MAGIC ,null as disconnectionDate 
# MAGIC ,null as disconnectionActivityTypeCode 
# MAGIC ,null as disconnectionActivityType 
# MAGIC ,null as disconnectionObjectTypeCode 
# MAGIC ,null as disconnectionReasonCode 
# MAGIC ,null as disconnectionReason 
# MAGIC ,null as disconnectionReconnectionStatusCode 
# MAGIC ,null as disconnectionReconnectionStatus 
# MAGIC ,null as disconnectionDocumentStatusCode 
# MAGIC ,null as disconnectionDocumentStatus 
# MAGIC ,null as disconnectionProcessingVariantCode 
# MAGIC ,null as disconnectionProcessingVariant 
# MAGIC ,null as createdDate 
# MAGIC ,null as createdBy 
# MAGIC ,null as changedDate 
# MAGIC ,null as changedBy 
# MAGIC  from  cleansed.ISU_0UCINSTALLA_ATTR_2  limit 1)d)e

# COMMAND ----------

# MAGIC %sql
# MAGIC select dimpropertySK from curated.dimInstallation

# COMMAND ----------

# DBTITLE 1,[Verification] Auto Generate field check
# MAGIC %sql
# MAGIC select * from curated.dimInstallation where dimpropertySK is null or dimpropertySK = '' or dimpropertySK = ' '

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from curated.dimInstallation where diminstallationSK is null or diminstallationSK = '' or diminstallationSK = ' '

# COMMAND ----------

# MAGIC %sql
# MAGIC --duplicate check of Surrogate key
# MAGIC SELECT diminstallationSK,COUNT (*) as count
# MAGIC FROM curated.dimInstallation
# MAGIC GROUP BY diminstallationSK
# MAGIC HAVING COUNT (*) > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Check
# MAGIC %sql
# MAGIC --duplicate check of Surrogate key
# MAGIC SELECT dimpropertySK,COUNT (*) as count
# MAGIC FROM curated.dimInstallation
# MAGIC GROUP BY dimPropertySK
# MAGIC HAVING COUNT (*) > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Check
# MAGIC %sql
# MAGIC --duplicate check of unique key
# MAGIC SELECT installationId,validToDate,disconnectionDocumentNumber,disconnectionActivityPeriod,disconnectionObjectNumber,COUNT (*) as count
# MAGIC FROM curated.dimInstallation
# MAGIC GROUP BY installationId,validToDate,disconnectionDocumentNumber,disconnectionActivityPeriod,disconnectionObjectNumber
# MAGIC HAVING COUNT (*) > 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select * FROM curated.dimInstallation WHERE installationId = -1

# COMMAND ----------

# DBTITLE 1,[Verification] Count Check
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from curated.dimInstallation
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from (
# MAGIC select * from (
# MAGIC select
# MAGIC a.installationId as installationId
# MAGIC ,'ISU' as sourceSystemCode
# MAGIC ,b.validFromDate as validFromDate
# MAGIC ,b.validToDate as validToDate
# MAGIC ,a.divisionCode as divisionCode
# MAGIC ,a.division as division
# MAGIC ,b.rateCategoryCode as rateCategoryCode
# MAGIC ,b.rateCategory as rateCategory
# MAGIC ,b.industryCode as industryCode
# MAGIC ,b.industry as industry
# MAGIC ,b.billingClassCode as billingClassCode
# MAGIC ,b.billingClass as billingClass
# MAGIC ,b.industrySystemCode as industrySystemCode
# MAGIC ,b.industrySystem as industrySystem
# MAGIC ,a.meterReadingControlCode as meterReadingControlCode
# MAGIC ,a.meterReadingControl as meterReadingControl
# MAGIC ,a.authorizationGroupCode as authorizationGroupCode
# MAGIC ,a.serviceTypeCode as serviceTypeCode
# MAGIC ,a.serviceType as serviceType
# MAGIC ,case
# MAGIC  when c.disconnectionDocumentNumber is null then ''
# MAGIC  else  c.disconnectionDocumentNumber end as disconnectionDocumentNumber
# MAGIC  ,case
# MAGIC  when c.disconnectionActivityPeriod is null then ''
# MAGIC  else  c.disconnectionActivityPeriod end as disconnectionActivityPeriod
# MAGIC ,case
# MAGIC  when c.disconnectionObjectNumber is null then ''
# MAGIC  else  c.disconnectionObjectNumber end as disconnectionObjectNumber
# MAGIC ,c.disconnectionDate as disconnectionDate
# MAGIC ,c.disconnectionActivityTypeCode as disconnectionActivityTypeCode
# MAGIC ,c.disconnectionActivityType as disconnectionActivityType
# MAGIC ,c.disconnectionObjectTypeCode as disconnectionObjectTypeCode
# MAGIC ,c.disconnectionReasonCode as disconnectionReasonCode
# MAGIC ,c.disconnectionReason as disconnectionReason
# MAGIC ,c.disconnectionReconnectionStatusCode as disconnectionReconnectionStatusCode
# MAGIC ,c.disconnectionReconnectionStatus as disconnectionReconnectionStatus
# MAGIC ,c.disconnectionDocumentStatusCode as disconnectionDocumentStatusCode
# MAGIC ,c.disconnectionDocumentStatus as disconnectionDocumentStatus
# MAGIC ,c.ProcessingVariantCode as disconnectionProcessingVariantCode
# MAGIC ,c.ProcessingVariant as disconnectionProcessingVariant
# MAGIC ,a.createdDate as createdDate
# MAGIC ,a.createdBy as createdBy
# MAGIC ,a.lastchangedDate as changedDate
# MAGIC ,a.lastchangedBy as changedDate
# MAGIC from
# MAGIC cleansed.ISU_0UCINSTALLA_ATTR_2  a
# MAGIC left join cleansed.ISU_0UCINSTALLAH_ATTR_2 b
# MAGIC ON
# MAGIC a.installationId = b.installationId
# MAGIC 
# MAGIC left join cleansed.isu_0UC_ISU_32 c
# MAGIC ON b.installationId = c.installationId
# MAGIC AND b.validToDate in (to_date('9999-12-31'),to_date('2099-12-31'))
# MAGIC and c.referenceObjectTypeCode = 'INSTLN'
# MAGIC AND c.validToDate in (to_date('9999-12-31'),to_date('2099-12-31'))
# MAGIC union all 
# MAGIC select * from (
# MAGIC select 
# MAGIC '-1' as installationId 
# MAGIC ,'ISU' as sourceSystemCode 
# MAGIC ,null as validFromDate 
# MAGIC ,'2099-12-31' as validToDate 
# MAGIC ,null as divisionCode 
# MAGIC ,null as division 
# MAGIC ,null as rateCategoryCode 
# MAGIC ,null as rateCategory 
# MAGIC ,null as industryCode 
# MAGIC ,null as industry 
# MAGIC ,null as billingClassCode 
# MAGIC ,null as billingClass 
# MAGIC ,null as industrySystemCode 
# MAGIC ,null as industrySystem 
# MAGIC ,null as meterReadingControlCode 
# MAGIC ,null as meterReadingControl 
# MAGIC ,null as authorizationGroupCode 
# MAGIC ,null as serviceTypeCode 
# MAGIC ,null as serviceType 
# MAGIC ,'' as disconnectionDocumentNumber 
# MAGIC ,'' as disconnectionActivityPeriod 
# MAGIC ,'' as disconnectionObjectNumber 
# MAGIC ,null as disconnectionDate 
# MAGIC ,null as disconnectionActivityTypeCode 
# MAGIC ,null as disconnectionActivityType 
# MAGIC ,null as disconnectionObjectTypeCode 
# MAGIC ,null as disconnectionReasonCode 
# MAGIC ,null as disconnectionReason 
# MAGIC ,null as disconnectionReconnectionStatusCode 
# MAGIC ,null as disconnectionReconnectionStatus 
# MAGIC ,null as disconnectionDocumentStatusCode 
# MAGIC ,null as disconnectionDocumentStatus 
# MAGIC ,null as disconnectionProcessingVariantCode 
# MAGIC ,null as disconnectionProcessingVariant 
# MAGIC ,null as createdDate 
# MAGIC ,null as createdBy 
# MAGIC ,null as changedDate 
# MAGIC ,null as changedBy 
# MAGIC  from  cleansed.ISU_0UCINSTALLA_ATTR_2  limit 1)d)e)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from curated.dimInstallation where installationId = -1

# COMMAND ----------

# DBTITLE 1,[Verification] Source vs Target
# MAGIC %sql
# MAGIC select * from (
# MAGIC select
# MAGIC a.installationId as installationId
# MAGIC ,'ISU' as sourceSystemCode
# MAGIC ,b.validFromDate as validFromDate
# MAGIC ,b.validToDate as validToDate
# MAGIC ,a.divisionCode as divisionCode
# MAGIC ,a.division as division
# MAGIC ,b.rateCategoryCode as rateCategoryCode
# MAGIC ,b.rateCategory as rateCategory
# MAGIC ,b.industryCode as industryCode
# MAGIC ,b.industry as industry
# MAGIC ,b.billingClassCode as billingClassCode
# MAGIC ,b.billingClass as billingClass
# MAGIC ,b.industrySystemCode as industrySystemCode
# MAGIC ,b.industrySystem as industrySystem
# MAGIC ,a.meterReadingControlCode as meterReadingControlCode
# MAGIC ,a.meterReadingControl as meterReadingControl
# MAGIC ,a.authorizationGroupCode as authorizationGroupCode
# MAGIC ,a.serviceTypeCode as serviceTypeCode
# MAGIC ,a.serviceType as serviceType
# MAGIC ,case
# MAGIC  when c.disconnectionDocumentNumber is null then ''
# MAGIC  else  c.disconnectionDocumentNumber end as disconnectionDocumentNumber
# MAGIC  ,case
# MAGIC  when c.disconnectionActivityPeriod is null then ''
# MAGIC  else  c.disconnectionActivityPeriod end as disconnectionActivityPeriod
# MAGIC ,case
# MAGIC  when c.disconnectionObjectNumber is null then ''
# MAGIC  else  c.disconnectionObjectNumber end as disconnectionObjectNumber
# MAGIC ,c.disconnectionDate as disconnectionDate
# MAGIC ,c.disconnectionActivityTypeCode as disconnectionActivityTypeCode
# MAGIC ,c.disconnectionActivityType as disconnectionActivityType
# MAGIC ,c.disconnectionObjectTypeCode as disconnectionObjectTypeCode
# MAGIC ,c.disconnectionReasonCode as disconnectionReasonCode
# MAGIC ,c.disconnectionReason as disconnectionReason
# MAGIC ,c.disconnectionReconnectionStatusCode as disconnectionReconnectionStatusCode
# MAGIC ,c.disconnectionReconnectionStatus as disconnectionReconnectionStatus
# MAGIC ,c.disconnectionDocumentStatusCode as disconnectionDocumentStatusCode
# MAGIC ,c.disconnectionDocumentStatus as disconnectionDocumentStatus
# MAGIC ,c.ProcessingVariantCode as disconnectionProcessingVariantCode
# MAGIC ,c.ProcessingVariant as disconnectionProcessingVariant
# MAGIC ,a.createdDate as createdDate
# MAGIC ,a.createdBy as createdBy
# MAGIC ,a.lastchangedDate as changedDate
# MAGIC ,a.lastchangedBy as changedDate
# MAGIC from
# MAGIC cleansed.ISU_0UCINSTALLA_ATTR_2  a
# MAGIC left join cleansed.ISU_0UCINSTALLAH_ATTR_2 b
# MAGIC ON
# MAGIC a.installationId = b.installationId
# MAGIC 
# MAGIC left join cleansed.isu_0UC_ISU_32 c
# MAGIC ON b.installationId = c.installationId
# MAGIC AND b.validToDate in (to_date('9999-12-31'),to_date('2099-12-31'))
# MAGIC and c.referenceObjectTypeCode = 'INSTLN'
# MAGIC AND c.validToDate in (to_date('9999-12-31'),to_date('2099-12-31'))
# MAGIC union all 
# MAGIC select * from (
# MAGIC select 
# MAGIC '-1' as installationId 
# MAGIC ,'ISU' as sourceSystemCode 
# MAGIC ,null as validFromDate 
# MAGIC ,'2099-12-31' as validToDate 
# MAGIC ,null as divisionCode 
# MAGIC ,null as division 
# MAGIC ,null as rateCategoryCode 
# MAGIC ,null as rateCategory 
# MAGIC ,null as industryCode 
# MAGIC ,null as industry 
# MAGIC ,null as billingClassCode 
# MAGIC ,null as billingClass 
# MAGIC ,null as industrySystemCode 
# MAGIC ,null as industrySystem 
# MAGIC ,null as meterReadingControlCode 
# MAGIC ,null as meterReadingControl 
# MAGIC ,null as authorizationGroupCode 
# MAGIC ,null as serviceTypeCode 
# MAGIC ,null as serviceType 
# MAGIC ,'' as disconnectionDocumentNumber 
# MAGIC ,'' as disconnectionActivityPeriod 
# MAGIC ,'' as disconnectionObjectNumber 
# MAGIC ,null as disconnectionDate 
# MAGIC ,null as disconnectionActivityTypeCode 
# MAGIC ,null as disconnectionActivityType 
# MAGIC ,null as disconnectionObjectTypeCode 
# MAGIC ,null as disconnectionReasonCode 
# MAGIC ,null as disconnectionReason 
# MAGIC ,null as disconnectionReconnectionStatusCode 
# MAGIC ,null as disconnectionReconnectionStatus 
# MAGIC ,null as disconnectionDocumentStatusCode 
# MAGIC ,null as disconnectionDocumentStatus 
# MAGIC ,null as disconnectionProcessingVariantCode 
# MAGIC ,null as disconnectionProcessingVariant 
# MAGIC ,null as createdDate 
# MAGIC ,null as createdBy 
# MAGIC ,null as changedDate 
# MAGIC ,null as changedBy 
# MAGIC  from  cleansed.ISU_0UCINSTALLA_ATTR_2  limit 1)d)e
# MAGIC except
# MAGIC select
# MAGIC installationId
# MAGIC ,sourceSystemCode
# MAGIC ,validFromDate
# MAGIC ,validToDate
# MAGIC ,divisionCode
# MAGIC ,division
# MAGIC ,rateCategoryCode
# MAGIC ,rateCategory
# MAGIC ,industryCode
# MAGIC ,industry
# MAGIC ,billingClassCode
# MAGIC ,billingClass
# MAGIC ,industrySystemCode
# MAGIC ,industrySystem
# MAGIC ,meterReadingControlCode
# MAGIC ,meterReadingControl
# MAGIC ,authorizationGroupCode
# MAGIC ,serviceTypeCode
# MAGIC ,serviceType
# MAGIC ,disconnectionDocumentNumber
# MAGIC ,disconnectionActivityPeriod
# MAGIC ,disconnectionObjectNumber
# MAGIC ,disconnectionDate
# MAGIC ,disconnectionActivityTypeCode
# MAGIC ,disconnectionActivityType
# MAGIC ,disconnectionObjectTypeCode
# MAGIC ,disconnectionReasonCode
# MAGIC ,disconnectionReason
# MAGIC ,disconnectionReconnectionStatusCode
# MAGIC ,disconnectionReconnectionStatus
# MAGIC ,disconnectionDocumentStatusCode
# MAGIC ,disconnectionDocumentStatus
# MAGIC ,disconnectionProcessingVariantCode
# MAGIC ,disconnectionProcessingVariant
# MAGIC ,createdDate
# MAGIC ,createdBy
# MAGIC ,changedDate
# MAGIC ,changedBy
# MAGIC from curated.dimInstallation

# COMMAND ----------

# DBTITLE 1,[Verification] Target  vs  Source
# MAGIC %sql
# MAGIC select
# MAGIC installationId
# MAGIC ,sourceSystemCode
# MAGIC ,validFromDate
# MAGIC ,validToDate
# MAGIC ,divisionCode
# MAGIC ,division
# MAGIC ,rateCategoryCode
# MAGIC ,rateCategory
# MAGIC ,industryCode
# MAGIC ,industry
# MAGIC ,billingClassCode
# MAGIC ,billingClass
# MAGIC ,industrySystemCode
# MAGIC ,industrySystem
# MAGIC ,meterReadingControlCode
# MAGIC ,meterReadingControl
# MAGIC ,authorizationGroupCode
# MAGIC ,serviceTypeCode
# MAGIC ,serviceType
# MAGIC ,disconnectionDocumentNumber
# MAGIC ,disconnectionActivityPeriod
# MAGIC ,disconnectionObjectNumber
# MAGIC ,disconnectionDate
# MAGIC ,disconnectionActivityTypeCode
# MAGIC ,disconnectionActivityType
# MAGIC ,disconnectionObjectTypeCode
# MAGIC ,disconnectionReasonCode
# MAGIC ,disconnectionReason
# MAGIC ,disconnectionReconnectionStatusCode
# MAGIC ,disconnectionReconnectionStatus
# MAGIC ,disconnectionDocumentStatusCode
# MAGIC ,disconnectionDocumentStatus
# MAGIC ,disconnectionProcessingVariantCode
# MAGIC ,disconnectionProcessingVariant
# MAGIC ,createdDate
# MAGIC ,createdBy
# MAGIC ,changedDate
# MAGIC ,changedBy
# MAGIC from curated.dimInstallation
# MAGIC except
# MAGIC select * from (
# MAGIC select
# MAGIC a.installationId as installationId
# MAGIC ,'ISU' as sourceSystemCode
# MAGIC ,b.validFromDate as validFromDate
# MAGIC ,b.validToDate as validToDate
# MAGIC ,a.divisionCode as divisionCode
# MAGIC ,a.division as division
# MAGIC ,b.rateCategoryCode as rateCategoryCode
# MAGIC ,b.rateCategory as rateCategory
# MAGIC ,b.industryCode as industryCode
# MAGIC ,b.industry as industry
# MAGIC ,b.billingClassCode as billingClassCode
# MAGIC ,b.billingClass as billingClass
# MAGIC ,b.industrySystemCode as industrySystemCode
# MAGIC ,b.industrySystem as industrySystem
# MAGIC ,a.meterReadingControlCode as meterReadingControlCode
# MAGIC ,a.meterReadingControl as meterReadingControl
# MAGIC ,a.authorizationGroupCode as authorizationGroupCode
# MAGIC ,a.serviceTypeCode as serviceTypeCode
# MAGIC ,a.serviceType as serviceType
# MAGIC ,case
# MAGIC  when c.disconnectionDocumentNumber is null then ''
# MAGIC  else  c.disconnectionDocumentNumber end as disconnectionDocumentNumber
# MAGIC  ,case
# MAGIC  when c.disconnectionActivityPeriod is null then ''
# MAGIC  else  c.disconnectionActivityPeriod end as disconnectionActivityPeriod
# MAGIC ,case
# MAGIC  when c.disconnectionObjectNumber is null then ''
# MAGIC  else  c.disconnectionObjectNumber end as disconnectionObjectNumber
# MAGIC ,c.disconnectionDate as disconnectionDate
# MAGIC ,c.disconnectionActivityTypeCode as disconnectionActivityTypeCode
# MAGIC ,c.disconnectionActivityType as disconnectionActivityType
# MAGIC ,c.disconnectionObjectTypeCode as disconnectionObjectTypeCode
# MAGIC ,c.disconnectionReasonCode as disconnectionReasonCode
# MAGIC ,c.disconnectionReason as disconnectionReason
# MAGIC ,c.disconnectionReconnectionStatusCode as disconnectionReconnectionStatusCode
# MAGIC ,c.disconnectionReconnectionStatus as disconnectionReconnectionStatus
# MAGIC ,c.disconnectionDocumentStatusCode as disconnectionDocumentStatusCode
# MAGIC ,c.disconnectionDocumentStatus as disconnectionDocumentStatus
# MAGIC ,c.ProcessingVariantCode as disconnectionProcessingVariantCode
# MAGIC ,c.ProcessingVariant as disconnectionProcessingVariant
# MAGIC ,a.createdDate as createdDate
# MAGIC ,a.createdBy as createdBy
# MAGIC ,a.lastchangedDate as changedDate
# MAGIC ,a.lastchangedBy as changedDate
# MAGIC from
# MAGIC cleansed.ISU_0UCINSTALLA_ATTR_2  a
# MAGIC left join cleansed.ISU_0UCINSTALLAH_ATTR_2 b
# MAGIC ON
# MAGIC a.installationId = b.installationId
# MAGIC 
# MAGIC left join cleansed.isu_0UC_ISU_32 c
# MAGIC ON b.installationId = c.installationId
# MAGIC AND b.validToDate in (to_date('9999-12-31'),to_date('2099-12-31'))
# MAGIC and c.referenceObjectTypeCode = 'INSTLN'
# MAGIC AND c.validToDate in (to_date('9999-12-31'),to_date('2099-12-31'))
# MAGIC union all 
# MAGIC select * from (
# MAGIC select 
# MAGIC '-1' as installationId 
# MAGIC ,'ISU' as sourceSystemCode 
# MAGIC ,null as validFromDate 
# MAGIC ,'2099-12-31' as validToDate 
# MAGIC ,null as divisionCode 
# MAGIC ,null as division 
# MAGIC ,null as rateCategoryCode 
# MAGIC ,null as rateCategory 
# MAGIC ,null as industryCode 
# MAGIC ,null as industry 
# MAGIC ,null as billingClassCode 
# MAGIC ,null as billingClass 
# MAGIC ,null as industrySystemCode 
# MAGIC ,null as industrySystem 
# MAGIC ,null as meterReadingControlCode 
# MAGIC ,null as meterReadingControl 
# MAGIC ,null as authorizationGroupCode 
# MAGIC ,null as serviceTypeCode 
# MAGIC ,null as serviceType 
# MAGIC ,'' as disconnectionDocumentNumber 
# MAGIC ,'' as disconnectionActivityPeriod 
# MAGIC ,'' as disconnectionObjectNumber 
# MAGIC ,null as disconnectionDate 
# MAGIC ,null as disconnectionActivityTypeCode 
# MAGIC ,null as disconnectionActivityType 
# MAGIC ,null as disconnectionObjectTypeCode 
# MAGIC ,null as disconnectionReasonCode 
# MAGIC ,null as disconnectionReason 
# MAGIC ,null as disconnectionReconnectionStatusCode 
# MAGIC ,null as disconnectionReconnectionStatus 
# MAGIC ,null as disconnectionDocumentStatusCode 
# MAGIC ,null as disconnectionDocumentStatus 
# MAGIC ,null as disconnectionProcessingVariantCode 
# MAGIC ,null as disconnectionProcessingVariant 
# MAGIC ,null as createdDate 
# MAGIC ,null as createdBy 
# MAGIC ,null as changedDate 
# MAGIC ,null as changedBy 
# MAGIC  from  cleansed.ISU_0UCINSTALLA_ATTR_2  limit 1)d)e

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from curated.dimInstallation where installationId='4103439005'
