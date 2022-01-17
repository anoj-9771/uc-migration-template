# Databricks notebook source
#config parameters
source = 'ISU' #either CRM or ISU
table = '0UC_ISU_32'

environment = 'test'
storage_account_name = "sablobdaftest01"
storage_account_access_key = dbutils.secrets.get(scope="TestScope",key="test-sablob-key")
containerName = "archive"


# COMMAND ----------

# MAGIC %run ../../includes/tableEvaluation

# COMMAND ----------

# DBTITLE 1,Source with mapping
# MAGIC %sql
# MAGIC select
# MAGIC validFromDate
# MAGIC ,disconnectiondate
# MAGIC ,installationId
# MAGIC ,validToDate
# MAGIC ,concessionCardTypeCode
# MAGIC ,propertyNumber
# MAGIC ,currentInstallationContract
# MAGIC ,billedValueCounter
# MAGIC ,disconnectionActivityPeriod
# MAGIC ,disconnectionActivityTypeCode
# MAGIC ,disconnectionActivityType
# MAGIC ,disconnectionDocumentNumber
# MAGIC ,disconnectionObjectNumber
# MAGIC ,disconnectionObjectTypeCode
# MAGIC ,processingVariantCode
# MAGIC ,processingVariant
# MAGIC ,disconnectionReasonCode
# MAGIC ,disconnectionReason
# MAGIC ,equipmentNumber
# MAGIC ,documentItemNumber
# MAGIC ,documentPostingDate
# MAGIC ,loadMode
# MAGIC ,activityText
# MAGIC ,activityCode
# MAGIC ,meterReadingAfterDecimalPoint
# MAGIC ,disconnectionReconnectionStatusCode
# MAGIC ,disconnectionReconnectionStatus
# MAGIC ,businessPartnerNumber
# MAGIC ,premise
# MAGIC ,recordMode
# MAGIC ,referenceObjectKey
# MAGIC ,referenceObjectTypeCode
# MAGIC ,meterReadingBeforeDecimalPoint
# MAGIC ,contractAccountNumber
# MAGIC ,zDisconnectionDate
# MAGIC ,maintenanceTypeCode
# MAGIC ,disconnectionDocumentStatusCode
# MAGIC ,disconnectionDocumentStatus
# MAGIC ,orderNumber
# MAGIC from(
# MAGIC select
# MAGIC AB as validFromDate
# MAGIC ,ACTDATE as disconnectiondate
# MAGIC ,ANLAGE as installationId
# MAGIC ,BIS as validToDate
# MAGIC ,CON_CARDTYP as concessionCardTypeCode
# MAGIC ,CONNOBJ as propertyNumber
# MAGIC ,CONTRACT as currentInstallationContract
# MAGIC ,COUNTER as billedValueCounter
# MAGIC ,DISCACT_BEGIN as disconnectionActivityPeriod
# MAGIC ,DISCACTTYP as disconnectionActivityTypeCode
# MAGIC ,b.domainValueText as disconnectionActivityType
# MAGIC ,DISCNO as disconnectionDocumentNumber
# MAGIC ,DISCOBJ as disconnectionObjectNumber
# MAGIC ,DISCOBJTYP as disconnectionObjectTypeCode
# MAGIC ,DISCPROCV as processingVariantCode
# MAGIC ,c.applicationFormDescription as processingVariant
# MAGIC ,DISCREASON as disconnectionReasonCode
# MAGIC ,d.disconnecionReason as disconnectionReason
# MAGIC ,EQUINR as equipmentNumber
# MAGIC ,FAILED_ATTEMPTS as documentItemNumber
# MAGIC ,FPD as documentPostingDate
# MAGIC ,LADEMODUS as loadMode
# MAGIC ,MATXT as activityText
# MAGIC ,MNCOD as activityCode
# MAGIC ,N_ZWSTAND as meterReadingAfterDecimalPoint
# MAGIC ,ORDSTATE as disconnectionReconnectionStatusCode
# MAGIC ,e.confirmationStatus as disconnectionReconnectionStatus
# MAGIC ,PARTNER as businessPartnerNumber
# MAGIC ,PREMISE as premise
# MAGIC ,RECORDMODE as recordMode
# MAGIC ,REFOBJKEY as referenceObjectKey
# MAGIC ,REFOBJTYPE as referenceObjectTypeCode
# MAGIC ,V_ZWSTAND as meterReadingBeforeDecimalPoint
# MAGIC ,VKONT as contractAccountNumber
# MAGIC ,ZACTDATE as zDisconnectionDate
# MAGIC ,ZILART as maintenanceTypeCode
# MAGIC ,ZSTATUS as disconnectionDocumentStatusCode
# MAGIC ,g.domainValueText as disconnectionDocumentStatus
# MAGIC ,ZZORDERNUM as orderNumber
# MAGIC ,row_number() over (partition by BIS,DISCACT_BEGIN,DISCNO,DISCOBJ order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table} 
# MAGIC left join cleansed.isu_dd07t b
# MAGIC on  b.domainName = 'DISCACTTYP'  and  b.domainValueSingleUpperLimit =DISCACTTYP
# MAGIC left join cleansed.isu_0UC_DISCPRV_TEXT c
# MAGIC on DISCPROCV = c.processingVariantCode
# MAGIC left join cleansed.isu_0UC_DISCREAS_TEXT d
# MAGIC on DISCREASON = d.disconnecionReasonCode
# MAGIC left join cleansed.isu_EDISCORDSTATET e
# MAGIC on ORDSTATE = e.confirmationStatusCode
# MAGIC left join cleansed.isu_dd07t g
# MAGIC on g.domainName ='EDCDOCSTAT' and g.domainValueSingleUpperLimit = ZSTATUS
# MAGIC )a  
# MAGIC  where  a.rn = 1

# COMMAND ----------

# DBTITLE 1,[Verification] Count Check
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from cleansed.${vars.table}
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from (select
# MAGIC validFromDate
# MAGIC ,disconnectiondate
# MAGIC ,installationId
# MAGIC ,validToDate
# MAGIC ,concessionCardTypeCode
# MAGIC ,propertyNumber
# MAGIC ,currentInstallationContract
# MAGIC ,billedValueCounter
# MAGIC ,disconnectionActivityPeriod
# MAGIC ,disconnectionActivityTypeCode
# MAGIC ,disconnectionActivityType
# MAGIC ,disconnectionDocumentNumber
# MAGIC ,disconnectionObjectNumber
# MAGIC ,disconnectionObjectTypeCode
# MAGIC ,processingVariantCode
# MAGIC ,processingVariant
# MAGIC ,disconnectionReasonCode
# MAGIC ,disconnectionReason
# MAGIC ,equipmentNumber
# MAGIC ,documentItemNumber
# MAGIC ,documentPostingDate
# MAGIC ,loadMode
# MAGIC ,activityText
# MAGIC ,activityCode
# MAGIC ,meterReadingAfterDecimalPoint
# MAGIC ,disconnectionReconnectionStatusCode
# MAGIC ,disconnectionReconnectionStatus
# MAGIC ,businessPartnerNumber
# MAGIC ,premise
# MAGIC ,recordMode
# MAGIC ,referenceObjectKey
# MAGIC ,referenceObjectTypeCode
# MAGIC ,meterReadingBeforeDecimalPoint
# MAGIC ,contractAccountNumber
# MAGIC ,zDisconnectionDate
# MAGIC ,maintenanceTypeCode
# MAGIC ,disconnectionDocumentStatusCode
# MAGIC ,disconnectionDocumentStatus
# MAGIC ,orderNumber
# MAGIC from(
# MAGIC select
# MAGIC AB as validFromDate
# MAGIC ,ACTDATE as disconnectiondate
# MAGIC ,ANLAGE as installationId
# MAGIC ,BIS as validToDate
# MAGIC ,CON_CARDTYP as concessionCardTypeCode
# MAGIC ,CONNOBJ as propertyNumber
# MAGIC ,CONTRACT as currentInstallationContract
# MAGIC ,COUNTER as billedValueCounter
# MAGIC ,DISCACT_BEGIN as disconnectionActivityPeriod
# MAGIC ,DISCACTTYP as disconnectionActivityTypeCode
# MAGIC ,b.domainValueText as disconnectionActivityType
# MAGIC ,DISCNO as disconnectionDocumentNumber
# MAGIC ,DISCOBJ as disconnectionObjectNumber
# MAGIC ,DISCOBJTYP as disconnectionObjectTypeCode
# MAGIC ,DISCPROCV as processingVariantCode
# MAGIC ,c.applicationFormDescription as processingVariant
# MAGIC ,DISCREASON as disconnectionReasonCode
# MAGIC ,d.disconnecionReason as disconnectionReason
# MAGIC ,EQUINR as equipmentNumber
# MAGIC ,FAILED_ATTEMPTS as documentItemNumber
# MAGIC ,FPD as documentPostingDate
# MAGIC ,LADEMODUS as loadMode
# MAGIC ,MATXT as activityText
# MAGIC ,MNCOD as activityCode
# MAGIC ,N_ZWSTAND as meterReadingAfterDecimalPoint
# MAGIC ,ORDSTATE as disconnectionReconnectionStatusCode
# MAGIC ,e.confirmationStatus as disconnectionReconnectionStatus
# MAGIC ,PARTNER as businessPartnerNumber
# MAGIC ,PREMISE as premise
# MAGIC ,RECORDMODE as recordMode
# MAGIC ,REFOBJKEY as referenceObjectKey
# MAGIC ,REFOBJTYPE as referenceObjectTypeCode
# MAGIC ,V_ZWSTAND as meterReadingBeforeDecimalPoint
# MAGIC ,VKONT as contractAccountNumber
# MAGIC ,ZACTDATE as zDisconnectionDate
# MAGIC ,ZILART as maintenanceTypeCode
# MAGIC ,ZSTATUS as disconnectionDocumentStatusCode
# MAGIC ,g.domainValueText as disconnectionDocumentStatus
# MAGIC ,ZZORDERNUM as orderNumber
# MAGIC ,row_number() over (partition by BIS,DISCACT_BEGIN,DISCNO,DISCOBJ order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table} 
# MAGIC left join cleansed.isu_dd07t b
# MAGIC on  b.domainName = 'DISCACTTYP'  and  b.domainValueSingleUpperLimit =DISCACTTYP
# MAGIC left join cleansed.isu_0UC_DISCPRV_TEXT c
# MAGIC on DISCPROCV = c.processingVariantCode
# MAGIC left join cleansed.isu_0UC_DISCREAS_TEXT d
# MAGIC on DISCREASON = d.disconnecionReasonCode
# MAGIC left join cleansed.isu_EDISCORDSTATET e
# MAGIC on ORDSTATE = e.confirmationStatusCode
# MAGIC left join cleansed.isu_dd07t g
# MAGIC on g.domainName ='EDCDOCSTAT' and g.domainValueSingleUpperLimit = ZSTATUS
# MAGIC )a  
# MAGIC  where  a.rn = 1)

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT 
# MAGIC validToDate,disconnectionActivityPeriod,disconnectionDocumentNumber,disconnectionObjectNumber
# MAGIC , COUNT (*) as count
# MAGIC FROM cleansed.${vars.table}
# MAGIC GROUP BY validToDate,disconnectionActivityPeriod,disconnectionDocumentNumber,disconnectionObjectNumber
# MAGIC HAVING COUNT (*) > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT * FROM (
# MAGIC SELECT
# MAGIC *,
# MAGIC row_number() OVER(PARTITION BY  validToDate,disconnectionActivityPeriod,disconnectionDocumentNumber,disconnectionObjectNumber order by validToDate,disconnectionActivityPeriod,disconnectionDocumentNumber,disconnectionObjectNumber ) as rn
# MAGIC FROM  cleansed.${vars.table}
# MAGIC )a where a.rn > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Source and Target Data
# MAGIC %sql
# MAGIC select
# MAGIC validFromDate
# MAGIC ,disconnectiondate
# MAGIC ,installationId
# MAGIC ,validToDate
# MAGIC ,concessionCardTypeCode
# MAGIC ,propertyNumber
# MAGIC ,currentInstallationContract
# MAGIC ,billedValueCounter
# MAGIC ,disconnectionActivityPeriod
# MAGIC ,disconnectionActivityTypeCode
# MAGIC ,disconnectionActivityType
# MAGIC ,disconnectionDocumentNumber
# MAGIC ,disconnectionObjectNumber
# MAGIC ,disconnectionObjectTypeCode
# MAGIC ,processingVariantCode
# MAGIC ,processingVariant
# MAGIC ,disconnectionReasonCode
# MAGIC ,disconnectionReason
# MAGIC ,equipmentNumber
# MAGIC ,documentItemNumber
# MAGIC ,documentPostingDate
# MAGIC ,loadMode
# MAGIC ,activityText
# MAGIC ,activityCode
# MAGIC ,meterReadingAfterDecimalPoint
# MAGIC ,disconnectionReconnectionStatusCode
# MAGIC ,disconnectionReconnectionStatus
# MAGIC ,businessPartnerNumber
# MAGIC ,premise
# MAGIC ,recordMode
# MAGIC ,referenceObjectKey
# MAGIC ,referenceObjectTypeCode
# MAGIC ,meterReadingBeforeDecimalPoint
# MAGIC ,contractAccountNumber
# MAGIC ,zDisconnectionDate
# MAGIC ,maintenanceTypeCode
# MAGIC ,disconnectionDocumentStatusCode
# MAGIC ,disconnectionDocumentStatus
# MAGIC ,orderNumber
# MAGIC from(
# MAGIC select
# MAGIC AB as validFromDate
# MAGIC ,ACTDATE as disconnectiondate
# MAGIC ,ANLAGE as installationId
# MAGIC ,BIS as validToDate
# MAGIC ,CON_CARDTYP as concessionCardTypeCode
# MAGIC ,CONNOBJ as propertyNumber
# MAGIC ,CONTRACT as currentInstallationContract
# MAGIC ,COUNTER as billedValueCounter
# MAGIC ,DISCACT_BEGIN as disconnectionActivityPeriod
# MAGIC ,DISCACTTYP as disconnectionActivityTypeCode
# MAGIC ,b.domainValueText as disconnectionActivityType
# MAGIC ,DISCNO as disconnectionDocumentNumber
# MAGIC ,DISCOBJ as disconnectionObjectNumber
# MAGIC ,DISCOBJTYP as disconnectionObjectTypeCode
# MAGIC ,DISCPROCV as processingVariantCode
# MAGIC ,c.applicationFormDescription as processingVariant
# MAGIC ,DISCREASON as disconnectionReasonCode
# MAGIC ,d.disconnecionReason as disconnectionReason
# MAGIC ,EQUINR as equipmentNumber
# MAGIC ,FAILED_ATTEMPTS as documentItemNumber
# MAGIC ,FPD as documentPostingDate
# MAGIC ,LADEMODUS as loadMode
# MAGIC ,MATXT as activityText
# MAGIC ,MNCOD as activityCode
# MAGIC ,N_ZWSTAND as meterReadingAfterDecimalPoint
# MAGIC ,ORDSTATE as disconnectionReconnectionStatusCode
# MAGIC ,e.confirmationStatus as disconnectionReconnectionStatus
# MAGIC ,PARTNER as businessPartnerNumber
# MAGIC ,PREMISE as premise
# MAGIC ,RECORDMODE as recordMode
# MAGIC ,REFOBJKEY as referenceObjectKey
# MAGIC ,REFOBJTYPE as referenceObjectTypeCode
# MAGIC ,V_ZWSTAND as meterReadingBeforeDecimalPoint
# MAGIC ,VKONT as contractAccountNumber
# MAGIC ,ZACTDATE as zDisconnectionDate
# MAGIC ,ZILART as maintenanceTypeCode
# MAGIC ,ZSTATUS as disconnectionDocumentStatusCode
# MAGIC ,g.domainValueText as disconnectionDocumentStatus
# MAGIC ,ZZORDERNUM as orderNumber
# MAGIC ,row_number() over (partition by BIS,DISCACT_BEGIN,DISCNO,DISCOBJ order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table} 
# MAGIC left join cleansed.isu_dd07t b
# MAGIC on  b.domainName = 'DISCACTTYP'  and  b.domainValueSingleUpperLimit =DISCACTTYP
# MAGIC left join cleansed.isu_0UC_DISCPRV_TEXT c
# MAGIC on  c.processingVariantCode = DISCPROCV
# MAGIC left join cleansed.isu_0UC_DISCREAS_TEXT d
# MAGIC on  d.disconnecionReasonCode = DISCREASON
# MAGIC left join cleansed.isu_EDISCORDSTATET e
# MAGIC on  e.confirmationStatusCode = ORDSTATE
# MAGIC left join cleansed.isu_dd07t g
# MAGIC on g.domainName ='EDCDOCSTAT' and g.domainValueSingleUpperLimit = ZSTATUS
# MAGIC )a  
# MAGIC  where  a.rn = 1
# MAGIC except
# MAGIC select
# MAGIC validFromDate
# MAGIC ,disconnectiondate
# MAGIC ,installationId
# MAGIC ,validToDate
# MAGIC ,concessionCardTypeCode
# MAGIC ,propertyNumber
# MAGIC ,currentInstallationContract
# MAGIC ,billedValueCounter
# MAGIC ,disconnectionActivityPeriod
# MAGIC ,disconnectionActivityTypeCode
# MAGIC ,disconnectionActivityType
# MAGIC ,disconnectionDocumentNumber
# MAGIC ,disconnectionObjectNumber
# MAGIC ,disconnectionObjectTypeCode
# MAGIC ,processingVariantCode
# MAGIC ,processingVariant
# MAGIC ,disconnectionReasonCode
# MAGIC ,disconnectionReason
# MAGIC ,equipmentNumber
# MAGIC ,documentItemNumber
# MAGIC ,documentPostingDate
# MAGIC ,loadMode
# MAGIC ,activityText
# MAGIC ,activityCode
# MAGIC ,meterReadingAfterDecimalPoint
# MAGIC ,disconnectionReconnectionStatusCode
# MAGIC ,disconnectionReconnectionStatus
# MAGIC ,businessPartnerNumber
# MAGIC ,premise
# MAGIC ,recordMode
# MAGIC ,referenceObjectKey
# MAGIC ,referenceObjectTypeCode
# MAGIC ,meterReadingBeforeDecimalPoint
# MAGIC ,contractAccountNumber
# MAGIC ,zDisconnectionDate
# MAGIC ,maintenanceTypeCode
# MAGIC ,disconnectionDocumentStatusCode
# MAGIC ,disconnectionDocumentStatus
# MAGIC ,orderNumber
# MAGIC from
# MAGIC cleansed.${vars.table}

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Target vs Source
# MAGIC %sql
# MAGIC select
# MAGIC validFromDate
# MAGIC ,disconnectiondate
# MAGIC ,installationId
# MAGIC ,validToDate
# MAGIC ,concessionCardTypeCode
# MAGIC ,propertyNumber
# MAGIC ,currentInstallationContract
# MAGIC ,billedValueCounter
# MAGIC ,disconnectionActivityPeriod
# MAGIC ,disconnectionActivityTypeCode
# MAGIC ,disconnectionActivityType
# MAGIC ,disconnectionDocumentNumber
# MAGIC ,disconnectionObjectNumber
# MAGIC ,disconnectionObjectTypeCode
# MAGIC ,processingVariantCode
# MAGIC ,processingVariant
# MAGIC ,disconnectionReasonCode
# MAGIC ,disconnectionReason
# MAGIC ,equipmentNumber
# MAGIC ,documentItemNumber
# MAGIC ,documentPostingDate
# MAGIC ,loadMode
# MAGIC ,activityText
# MAGIC ,activityCode
# MAGIC ,meterReadingAfterDecimalPoint
# MAGIC ,disconnectionReconnectionStatusCode
# MAGIC ,disconnectionReconnectionStatus
# MAGIC ,businessPartnerNumber
# MAGIC ,premise
# MAGIC ,recordMode
# MAGIC ,referenceObjectKey
# MAGIC ,referenceObjectTypeCode
# MAGIC ,meterReadingBeforeDecimalPoint
# MAGIC ,contractAccountNumber
# MAGIC ,zDisconnectionDate
# MAGIC ,maintenanceTypeCode
# MAGIC ,disconnectionDocumentStatusCode
# MAGIC ,disconnectionDocumentStatus
# MAGIC ,orderNumber
# MAGIC from
# MAGIC cleansed.${vars.table}
# MAGIC except
# MAGIC select
# MAGIC validFromDate
# MAGIC ,disconnectiondate
# MAGIC ,installationId
# MAGIC ,validToDate
# MAGIC ,concessionCardTypeCode
# MAGIC ,propertyNumber
# MAGIC ,currentInstallationContract
# MAGIC ,billedValueCounter
# MAGIC ,disconnectionActivityPeriod
# MAGIC ,disconnectionActivityTypeCode
# MAGIC ,disconnectionActivityType
# MAGIC ,disconnectionDocumentNumber
# MAGIC ,disconnectionObjectNumber
# MAGIC ,disconnectionObjectTypeCode
# MAGIC ,processingVariantCode
# MAGIC ,processingVariant
# MAGIC ,disconnectionReasonCode
# MAGIC ,disconnectionReason
# MAGIC ,equipmentNumber
# MAGIC ,documentItemNumber
# MAGIC ,documentPostingDate
# MAGIC ,loadMode
# MAGIC ,activityText
# MAGIC ,activityCode
# MAGIC ,meterReadingAfterDecimalPoint
# MAGIC ,disconnectionReconnectionStatusCode
# MAGIC ,disconnectionReconnectionStatus
# MAGIC ,businessPartnerNumber
# MAGIC ,premise
# MAGIC ,recordMode
# MAGIC ,referenceObjectKey
# MAGIC ,referenceObjectTypeCode
# MAGIC ,meterReadingBeforeDecimalPoint
# MAGIC ,contractAccountNumber
# MAGIC ,zDisconnectionDate
# MAGIC ,maintenanceTypeCode
# MAGIC ,disconnectionDocumentStatusCode
# MAGIC ,disconnectionDocumentStatus
# MAGIC ,orderNumber
# MAGIC from(
# MAGIC select
# MAGIC AB as validFromDate
# MAGIC ,ACTDATE as disconnectiondate
# MAGIC ,ANLAGE as installationId
# MAGIC ,BIS as validToDate
# MAGIC ,CON_CARDTYP as concessionCardTypeCode
# MAGIC ,CONNOBJ as propertyNumber
# MAGIC ,CONTRACT as currentInstallationContract
# MAGIC ,COUNTER as billedValueCounter
# MAGIC ,DISCACT_BEGIN as disconnectionActivityPeriod
# MAGIC ,DISCACTTYP as disconnectionActivityTypeCode
# MAGIC ,b.domainValueText as disconnectionActivityType
# MAGIC ,DISCNO as disconnectionDocumentNumber
# MAGIC ,DISCOBJ as disconnectionObjectNumber
# MAGIC ,DISCOBJTYP as disconnectionObjectTypeCode
# MAGIC ,DISCPROCV as processingVariantCode
# MAGIC ,c.applicationFormDescription as processingVariant
# MAGIC ,DISCREASON as disconnectionReasonCode
# MAGIC ,d.disconnecionReason as disconnectionReason
# MAGIC ,EQUINR as equipmentNumber
# MAGIC ,FAILED_ATTEMPTS as documentItemNumber
# MAGIC ,FPD as documentPostingDate
# MAGIC ,LADEMODUS as loadMode
# MAGIC ,MATXT as activityText
# MAGIC ,MNCOD as activityCode
# MAGIC ,N_ZWSTAND as meterReadingAfterDecimalPoint
# MAGIC ,ORDSTATE as disconnectionReconnectionStatusCode
# MAGIC ,e.confirmationStatus as disconnectionReconnectionStatus
# MAGIC ,PARTNER as businessPartnerNumber
# MAGIC ,PREMISE as premise
# MAGIC ,RECORDMODE as recordMode
# MAGIC ,REFOBJKEY as referenceObjectKey
# MAGIC ,REFOBJTYPE as referenceObjectTypeCode
# MAGIC ,V_ZWSTAND as meterReadingBeforeDecimalPoint
# MAGIC ,VKONT as contractAccountNumber
# MAGIC ,ZACTDATE as zDisconnectionDate
# MAGIC ,ZILART as maintenanceTypeCode
# MAGIC ,ZSTATUS as disconnectionDocumentStatusCode
# MAGIC ,g.domainValueText as disconnectionDocumentStatus
# MAGIC ,ZZORDERNUM as orderNumber
# MAGIC ,row_number() over (partition by BIS,DISCACT_BEGIN,DISCNO,DISCOBJ order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table} 
# MAGIC left join cleansed.isu_dd07t b
# MAGIC on  b.domainName = 'DISCACTTYP'  and  b.domainValueSingleUpperLimit =DISCACTTYP
# MAGIC left join cleansed.isu_0UC_DISCPRV_TEXT c
# MAGIC on  c.processingVariantCode = DISCPROCV
# MAGIC left join cleansed.isu_0UC_DISCREAS_TEXT d
# MAGIC on  d.disconnecionReasonCode = DISCREASON
# MAGIC left join cleansed.isu_EDISCORDSTATET e
# MAGIC on  e.confirmationStatusCode = ORDSTATE
# MAGIC left join cleansed.isu_dd07t g
# MAGIC on g.domainName ='EDCDOCSTAT' and g.domainValueSingleUpperLimit = ZSTATUS
# MAGIC )a  
# MAGIC  where  a.rn = 1
