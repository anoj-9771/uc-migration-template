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
# MAGIC ,c.disconnectionDocumentNumber as disconnectionDocumentNumber
# MAGIC ,c.disconnectionActivityPeriod as disconnectionActivityPeriod
# MAGIC ,c.disconnectionObjectNumber as disconnectionObjectNumber
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
# MAGIC LEFT outer join cleansed.ISU_0UCINSTALLAH_ATTR_2 b
# MAGIC ON
# MAGIC a.installationId = b.installationId
# MAGIC 
# MAGIC left outer join cleansed.isu_0UC_ISU_32 c
# MAGIC ON 
# MAGIC b.installationId = c.installationId and 
# MAGIC b.validToDate = to_date('9999.12.31', 'yyyy.mm.dd') and
# MAGIC c.referenceObjectTypeCode ='INSTLN' and
# MAGIC c.validToDate = to_date('9999.12.31', 'yyyy.mm.dd')

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

# DBTITLE 1,[Verification] Count Check
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from curated.dimInstallation
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from (
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
# MAGIC ,c.disconnectionDocumentNumber as disconnectionDocumentNumber
# MAGIC ,c.disconnectionActivityPeriod as disconnectionActivityPeriod
# MAGIC ,c.disconnectionObjectNumber as disconnectionObjectNumber
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
# MAGIC LEFT outer join cleansed.ISU_0UCINSTALLAH_ATTR_2 b
# MAGIC ON
# MAGIC a.installationId = b.installationId
# MAGIC 
# MAGIC left outer join cleansed.isu_0UC_ISU_32 c
# MAGIC ON 
# MAGIC b.installationId = c.installationId and 
# MAGIC b.validToDate = to_date('9999.12.31', 'yyyy.mm.dd') and
# MAGIC c.referenceObjectTypeCode ='INSTLN' and
# MAGIC c.validToDate = to_date('9999.12.31', 'yyyy.mm.dd') )--c where c.rn =1)

# COMMAND ----------

# DBTITLE 1,[Verification] Source vs Target
# MAGIC %sql
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
# MAGIC ,c.disconnectionDocumentNumber as disconnectionDocumentNumber
# MAGIC ,c.disconnectionActivityPeriod as disconnectionActivityPeriod
# MAGIC ,c.disconnectionObjectNumber as disconnectionObjectNumber
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
# MAGIC LEFT outer join cleansed.ISU_0UCINSTALLAH_ATTR_2 b
# MAGIC ON
# MAGIC a.installationId = b.installationId
# MAGIC 
# MAGIC left outer join cleansed.isu_0UC_ISU_32 c
# MAGIC ON 
# MAGIC b.installationId = c.installationId and 
# MAGIC b.validToDate = to_date('9999.12.31', 'yyyy.mm.dd') and
# MAGIC c.referenceObjectTypeCode ='INSTLN' and
# MAGIC c.validToDate = to_date('9999.12.31', 'yyyy.mm.dd')
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

# DBTITLE 1,[Verification] Target vs  Source 
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
# MAGIC from curated.dimInstallation --where businessPartnerGroupNumber = '0003100669'
# MAGIC except
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
# MAGIC ,c.disconnectionDocumentNumber as disconnectionDocumentNumber
# MAGIC ,c.disconnectionActivityPeriod as disconnectionActivityPeriod
# MAGIC ,c.disconnectionObjectNumber as disconnectionObjectNumber
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
# MAGIC LEFT outer join cleansed.ISU_0UCINSTALLAH_ATTR_2 b
# MAGIC ON
# MAGIC a.installationId = b.installationId
# MAGIC 
# MAGIC left outer join cleansed.isu_0UC_ISU_32 c
# MAGIC ON 
# MAGIC b.installationId = c.installationId and 
# MAGIC b.validToDate = to_date('9999.12.31', 'yyyy.mm.dd') and
# MAGIC c.referenceObjectTypeCode ='INSTLN' and
# MAGIC c.validToDate = to_date('9999.12.31', 'yyyy.mm.dd')

# COMMAND ----------

# DBTITLE 1,Source
# MAGIC %sql
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
# MAGIC ,c.disconnectionDocumentNumber as disconnectionDocumentNumber
# MAGIC ,c.disconnectionActivityPeriod as disconnectionActivityPeriod
# MAGIC ,c.disconnectionObjectNumber as disconnectionObjectNumber
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
# MAGIC LEFT outer join cleansed.ISU_0UCINSTALLAH_ATTR_2 b
# MAGIC ON
# MAGIC a.installationId = b.installationId
# MAGIC 
# MAGIC left outer join cleansed.isu_0UC_ISU_32 c
# MAGIC ON 
# MAGIC b.installationId = c.installationId and 
# MAGIC b.validToDate = to_date('9999.12.31', 'yyyy.mm.dd') and
# MAGIC c.referenceObjectTypeCode ='INSTLN' and
# MAGIC c.validToDate = to_date('9999.12.31', 'yyyy.mm.dd') where a.installationId = '4101006842'

# COMMAND ----------

# DBTITLE 1,Target
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
# MAGIC from curated.dimInstallation where installationId = '4101006842'
