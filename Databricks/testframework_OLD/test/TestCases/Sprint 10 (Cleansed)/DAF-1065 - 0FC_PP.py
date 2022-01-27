# Databricks notebook source
#config parameters
source = 'ISU' #either CRM or ISU
table = '0FC_PP'

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
# MAGIC promiseToPayId,
# MAGIC businessPartnerGroupNumber,
# MAGIC contractAccountNumber,
# MAGIC companyCode,
# MAGIC promiseToPayReasonCode,
# MAGIC withdrawalReasonCode,
# MAGIC promiseToPayCategoryCode,
# MAGIC numberOfChecks,
# MAGIC currency,
# MAGIC paymentAmountPromised,
# MAGIC promiseToPayCharges,
# MAGIC promiseToPayInterest,
# MAGIC amountCleared,
# MAGIC createdBy,
# MAGIC createdDateTime,
# MAGIC changedDate,
# MAGIC promiseToPayStatus,
# MAGIC statusChangedIndicator,
# MAGIC replacementPromiseToPayId,
# MAGIC installmentsAgreed,
# MAGIC firstDueDate,
# MAGIC finalDueDate,
# MAGIC numberOfPayments,
# MAGIC paymentPromised,
# MAGIC amountPaidByToday,
# MAGIC currentLevelOfFulfillment
# MAGIC from
# MAGIC (select
# MAGIC PPKEY as promiseToPayId,
# MAGIC GPART as businessPartnerGroupNumber,
# MAGIC VKONT as contractAccountNumber,
# MAGIC BUKRS as companyCode,
# MAGIC PPRSC as promiseToPayReasonCode,
# MAGIC PPRSW as withdrawalReasonCode,
# MAGIC PPCAT as promiseToPayCategoryCode,
# MAGIC C4LEV as numberOfChecks,
# MAGIC PRCUR as currency,
# MAGIC PRAMT as paymentAmountPromised,
# MAGIC PRAMT_CHR as promiseToPayCharges,
# MAGIC PRAMT_INT as promiseToPayInterest,
# MAGIC RDAMT as amountCleared,
# MAGIC ERNAM as createdBy,
# MAGIC cast(to_unix_timestamp(concat(ERDAT,' ',ERTIM), 'yyyy-MM-dd HH:mm:ss') as timestamp) as createdDateTime,
# MAGIC CHDAT as changedDate,
# MAGIC PPSTA as promiseToPayStatus,
# MAGIC XSTCH as statusChangedIndicator,
# MAGIC PPKEY_NEW as replacementPromiseToPayId,
# MAGIC XINDR as installmentsAgreed,
# MAGIC FTDAT as firstDueDate,
# MAGIC LTDAT as finalDueDate,
# MAGIC NRRTS as numberOfPayments,
# MAGIC PPDUE as paymentPromised,
# MAGIC PPPAY as amountPaidByToday,
# MAGIC DEGFA as currentLevelOfFulfillment,
# MAGIC row_number() over (partition by PPKEY order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table} 
# MAGIC )a  
# MAGIC  where  a.rn = 1

# COMMAND ----------

# DBTITLE 1,[Verification] Count Check
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from cleansed.${vars.table}
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from (select
# MAGIC promiseToPayId,
# MAGIC businessPartnerGroupNumber,
# MAGIC contractAccountNumber,
# MAGIC companyCode,
# MAGIC promiseToPayReasonCode,
# MAGIC withdrawalReasonCode,
# MAGIC promiseToPayCategoryCode,
# MAGIC numberOfChecks,
# MAGIC currency,
# MAGIC paymentAmountPromised,
# MAGIC promiseToPayCharges,
# MAGIC promiseToPayInterest,
# MAGIC amountCleared,
# MAGIC createdBy,
# MAGIC createdDateTime,
# MAGIC changedDate,
# MAGIC promiseToPayStatus,
# MAGIC statusChangedIndicator,
# MAGIC replacementPromiseToPayId,
# MAGIC installmentsAgreed,
# MAGIC firstDueDate,
# MAGIC finalDueDate,
# MAGIC numberOfPayments,
# MAGIC paymentPromised,
# MAGIC amountPaidByToday,
# MAGIC currentLevelOfFulfillment
# MAGIC from
# MAGIC (select
# MAGIC PPKEY as promiseToPayId,
# MAGIC GPART as businessPartnerGroupNumber,
# MAGIC VKONT as contractAccountNumber,
# MAGIC BUKRS as companyCode,
# MAGIC PPRSC as promiseToPayReasonCode,
# MAGIC PPRSW as withdrawalReasonCode,
# MAGIC PPCAT as promiseToPayCategoryCode,
# MAGIC C4LEV as numberOfChecks,
# MAGIC PRCUR as currency,
# MAGIC PRAMT as paymentAmountPromised,
# MAGIC PRAMT_CHR as promiseToPayCharges,
# MAGIC PRAMT_INT as promiseToPayInterest,
# MAGIC RDAMT as amountCleared,
# MAGIC ERNAM as createdBy,
# MAGIC concat(ERDAT,ERTIM) as createdDateTime,
# MAGIC CHDAT as changedDate,
# MAGIC PPSTA as promiseToPayStatus,
# MAGIC XSTCH as statusChangedIndicator,
# MAGIC PPKEY_NEW as replacementPromiseToPayId,
# MAGIC XINDR as installmentsAgreed,
# MAGIC FTDAT as firstDueDate,
# MAGIC LTDAT as finalDueDate,
# MAGIC NRRTS as numberOfPayments,
# MAGIC PPDUE as paymentPromised,
# MAGIC PPPAY as amountPaidByToday,
# MAGIC DEGFA as currentLevelOfFulfillment,
# MAGIC row_number() over (partition by PPKEY order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table} 
# MAGIC )a  
# MAGIC  where  a.rn = 1)

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT 
# MAGIC promiseToPayId,businessPartnerGroupNumber,contractAccountNumber,
# MAGIC companyCode,promiseToPayReasonCode,withdrawalReasonCode,
# MAGIC promiseToPayCategoryCode,numberOfChecks,currency,paymentAmountPromised,
# MAGIC promiseToPayCharges,promiseToPayInterest,amountCleared,createdBy,
# MAGIC createdDateTime,changedDate,promiseToPayStatus,statusChangedIndicator,
# MAGIC replacementPromiseToPayId,installmentsAgreed,firstDueDate,finalDueDate,
# MAGIC numberOfPayments,paymentPromised,amountPaidByToday,currentLevelOfFulfillment
# MAGIC , COUNT (*) as count
# MAGIC FROM cleansed.${vars.table}
# MAGIC GROUP BY promiseToPayId,businessPartnerGroupNumber,contractAccountNumber,
# MAGIC companyCode,promiseToPayReasonCode,withdrawalReasonCode,
# MAGIC promiseToPayCategoryCode,numberOfChecks,currency,paymentAmountPromised,
# MAGIC promiseToPayCharges,promiseToPayInterest,amountCleared,createdBy,
# MAGIC createdDateTime,changedDate,promiseToPayStatus,statusChangedIndicator,
# MAGIC replacementPromiseToPayId,installmentsAgreed,firstDueDate,finalDueDate,
# MAGIC numberOfPayments,paymentPromised,amountPaidByToday,currentLevelOfFulfillment
# MAGIC HAVING COUNT (*) > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT * FROM (
# MAGIC SELECT
# MAGIC *,
# MAGIC row_number() OVER(PARTITION BY promiseToPayId  order by promiseToPayId) as rn
# MAGIC FROM  cleansed.${vars.table}
# MAGIC )a where a.rn > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Source and Target Data
# MAGIC %sql
# MAGIC select
# MAGIC promiseToPayId,
# MAGIC businessPartnerGroupNumber,
# MAGIC contractAccountNumber,
# MAGIC companyCode,
# MAGIC promiseToPayReasonCode,
# MAGIC withdrawalReasonCode,
# MAGIC promiseToPayCategoryCode,
# MAGIC numberOfChecks,
# MAGIC currency,
# MAGIC paymentAmountPromised,
# MAGIC promiseToPayCharges,
# MAGIC promiseToPayInterest,
# MAGIC amountCleared,
# MAGIC createdBy,
# MAGIC createdDateTime,
# MAGIC changedDate,
# MAGIC promiseToPayStatus,
# MAGIC statusChangedIndicator,
# MAGIC replacementPromiseToPayId,
# MAGIC installmentsAgreed,
# MAGIC firstDueDate,
# MAGIC finalDueDate,
# MAGIC numberOfPayments,
# MAGIC paymentPromised,
# MAGIC amountPaidByToday,
# MAGIC currentLevelOfFulfillment
# MAGIC from
# MAGIC (select
# MAGIC PPKEY as promiseToPayId,
# MAGIC GPART as businessPartnerGroupNumber,
# MAGIC VKONT as contractAccountNumber,
# MAGIC BUKRS as companyCode,
# MAGIC PPRSC as promiseToPayReasonCode,
# MAGIC PPRSW as withdrawalReasonCode,
# MAGIC PPCAT as promiseToPayCategoryCode,
# MAGIC C4LEV as numberOfChecks,
# MAGIC PRCUR as currency,
# MAGIC cast(PRAMT as decimal(13,2))  as paymentAmountPromised,
# MAGIC cast(PRAMT_CHR as decimal(13,2)) as promiseToPayCharges,
# MAGIC cast(PRAMT_INT as decimal(13,2)) as promiseToPayInterest,
# MAGIC cast(RDAMT as decimal(13,2)) as amountCleared,
# MAGIC ERNAM as createdBy,
# MAGIC cast(to_unix_timestamp(concat(ERDAT,' ',ERTIM), 'yyyy-MM-dd HH:mm:ss') as timestamp) as createdDateTime,
# MAGIC CHDAT as changedDate,
# MAGIC PPSTA as promiseToPayStatus,
# MAGIC XSTCH as statusChangedIndicator,
# MAGIC PPKEY_NEW as replacementPromiseToPayId,
# MAGIC XINDR as installmentsAgreed,
# MAGIC FTDAT as firstDueDate,
# MAGIC LTDAT as finalDueDate,
# MAGIC NRRTS as numberOfPayments,
# MAGIC cast(PPDUE as decimal(13,2))as paymentPromised,
# MAGIC cast(PPPAY as decimal(13,2))as amountPaidByToday,
# MAGIC cast(DEGFA as decimal(5,0))as currentLevelOfFulfillment,
# MAGIC row_number() over (partition by PPKEY order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table} 
# MAGIC )a  
# MAGIC  where  a.rn = 1 
# MAGIC except
# MAGIC select
# MAGIC promiseToPayId,
# MAGIC businessPartnerGroupNumber,
# MAGIC contractAccountNumber,
# MAGIC companyCode,
# MAGIC promiseToPayReasonCode,
# MAGIC withdrawalReasonCode,
# MAGIC promiseToPayCategoryCode,
# MAGIC numberOfChecks,
# MAGIC currency,
# MAGIC paymentAmountPromised,
# MAGIC promiseToPayCharges,
# MAGIC promiseToPayInterest,
# MAGIC amountCleared,
# MAGIC createdBy,
# MAGIC createdDateTime,
# MAGIC changedDate,
# MAGIC promiseToPayStatus,
# MAGIC statusChangedIndicator,
# MAGIC replacementPromiseToPayId,
# MAGIC installmentsAgreed,
# MAGIC firstDueDate,
# MAGIC finalDueDate,
# MAGIC numberOfPayments,
# MAGIC paymentPromised,
# MAGIC amountPaidByToday,
# MAGIC currentLevelOfFulfillment
# MAGIC from
# MAGIC cleansed.${vars.table}

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Target and Source Data
# MAGIC %sql
# MAGIC select
# MAGIC promiseToPayId,
# MAGIC businessPartnerGroupNumber,
# MAGIC contractAccountNumber,
# MAGIC companyCode,
# MAGIC promiseToPayReasonCode,
# MAGIC withdrawalReasonCode,
# MAGIC promiseToPayCategoryCode,
# MAGIC numberOfChecks,
# MAGIC currency,
# MAGIC paymentAmountPromised,
# MAGIC promiseToPayCharges,
# MAGIC promiseToPayInterest,
# MAGIC amountCleared,
# MAGIC createdBy,
# MAGIC createdDateTime,
# MAGIC changedDate,
# MAGIC promiseToPayStatus,
# MAGIC statusChangedIndicator,
# MAGIC replacementPromiseToPayId,
# MAGIC installmentsAgreed,
# MAGIC firstDueDate,
# MAGIC finalDueDate,
# MAGIC numberOfPayments,
# MAGIC paymentPromised,
# MAGIC amountPaidByToday,
# MAGIC currentLevelOfFulfillment
# MAGIC from
# MAGIC cleansed.${vars.table}
# MAGIC except
# MAGIC select
# MAGIC promiseToPayId,
# MAGIC businessPartnerGroupNumber,
# MAGIC contractAccountNumber,
# MAGIC companyCode,
# MAGIC promiseToPayReasonCode,
# MAGIC withdrawalReasonCode,
# MAGIC promiseToPayCategoryCode,
# MAGIC numberOfChecks,
# MAGIC currency,
# MAGIC paymentAmountPromised,
# MAGIC promiseToPayCharges,
# MAGIC promiseToPayInterest,
# MAGIC amountCleared,
# MAGIC createdBy,
# MAGIC createdDateTime,
# MAGIC changedDate,
# MAGIC promiseToPayStatus,
# MAGIC statusChangedIndicator,
# MAGIC replacementPromiseToPayId,
# MAGIC installmentsAgreed,
# MAGIC firstDueDate,
# MAGIC finalDueDate,
# MAGIC numberOfPayments,
# MAGIC paymentPromised,
# MAGIC amountPaidByToday,
# MAGIC currentLevelOfFulfillment
# MAGIC from
# MAGIC (select
# MAGIC PPKEY as promiseToPayId,
# MAGIC GPART as businessPartnerGroupNumber,
# MAGIC VKONT as contractAccountNumber,
# MAGIC BUKRS as companyCode,
# MAGIC PPRSC as promiseToPayReasonCode,
# MAGIC PPRSW as withdrawalReasonCode,
# MAGIC PPCAT as promiseToPayCategoryCode,
# MAGIC C4LEV as numberOfChecks,
# MAGIC PRCUR as currency,
# MAGIC cast(PRAMT as decimal(13,2))  as paymentAmountPromised,
# MAGIC cast(PRAMT_CHR as decimal(13,2)) as promiseToPayCharges,
# MAGIC cast(PRAMT_INT as decimal(13,2)) as promiseToPayInterest,
# MAGIC cast(RDAMT as decimal(13,2)) as amountCleared,
# MAGIC ERNAM as createdBy,
# MAGIC cast(to_unix_timestamp(concat(ERDAT,' ',ERTIM), 'yyyy-MM-dd HH:mm:ss') as timestamp) as createdDateTime,
# MAGIC CHDAT as changedDate,
# MAGIC PPSTA as promiseToPayStatus,
# MAGIC XSTCH as statusChangedIndicator,
# MAGIC PPKEY_NEW as replacementPromiseToPayId,
# MAGIC XINDR as installmentsAgreed,
# MAGIC FTDAT as firstDueDate,
# MAGIC LTDAT as finalDueDate,
# MAGIC NRRTS as numberOfPayments,
# MAGIC cast(PPDUE as decimal(13,2))as paymentPromised,
# MAGIC cast(PPPAY as decimal(13,2))as amountPaidByToday,
# MAGIC cast(DEGFA as decimal(5,0))as currentLevelOfFulfillment,
# MAGIC row_number() over (partition by PPKEY order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table} 
# MAGIC )a  
# MAGIC  where  a.rn = 1
