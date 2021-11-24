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
# MAGIC propmiseToPayId,
# MAGIC businessPartnerGroupNumber,
# MAGIC contractAccountNumber,
# MAGIC companyCode,
# MAGIC promiseToPayReasonCode,
# MAGIC withdrawalReasonCode,
# MAGIC propmiseToPayCategoryCode,
# MAGIC numberOfChecks,
# MAGIC currency,
# MAGIC paymentAmountPromised,
# MAGIC promiseToPayCharges,
# MAGIC promiseToPayInterest,
# MAGIC amountCleared,
# MAGIC createdBy,
# MAGIC createdDateTime,
# MAGIC changedDate,
# MAGIC propmiseToPayStatus,
# MAGIC statusChangedIndicator,
# MAGIC replacementPropmiseToPayId,
# MAGIC installmentsAgreed,
# MAGIC firstDueDate,
# MAGIC finalDueDate,
# MAGIC numberOfPayments,
# MAGIC paymentPromised,
# MAGIC amountPaidByToday,
# MAGIC currentLevelOfFulfillment
# MAGIC from
# MAGIC (select
# MAGIC PPKEY as propmiseToPayId,
# MAGIC GPART as businessPartnerGroupNumber,
# MAGIC VKONT as contractAccountNumber,
# MAGIC BUKRS as companyCode,
# MAGIC PPRSC as promiseToPayReasonCode,
# MAGIC PPRSW as withdrawalReasonCode,
# MAGIC PPCAT as propmiseToPayCategoryCode,
# MAGIC C4LEV as numberOfChecks,
# MAGIC PRCUR as currency,
# MAGIC PRAMT as paymentAmountPromised,
# MAGIC PRAMT_CHR as promiseToPayCharges,
# MAGIC PRAMT_INT as promiseToPayInterest,
# MAGIC RDAMT as amountCleared,
# MAGIC ERNAM as createdBy,
# MAGIC cast(to_unix_timestamp(concat(ERDAT,' ',ERTIM), 'yyyy-MM-dd HH:mm:ss') as timestamp) as createdDateTime,
# MAGIC CHDAT as changedDate,
# MAGIC PPSTA as propmiseToPayStatus,
# MAGIC XSTCH as statusChangedIndicator,
# MAGIC PPKEY_NEW as replacementPropmiseToPayId,
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
# MAGIC propmiseToPayId,
# MAGIC businessPartnerGroupNumber,
# MAGIC contractAccountNumber,
# MAGIC companyCode,
# MAGIC promiseToPayReasonCode,
# MAGIC withdrawalReasonCode,
# MAGIC propmiseToPayCategoryCode,
# MAGIC numberOfChecks,
# MAGIC currency,
# MAGIC paymentAmountPromised,
# MAGIC promiseToPayCharges,
# MAGIC promiseToPayInterest,
# MAGIC amountCleared,
# MAGIC createdBy,
# MAGIC createdDateTime,
# MAGIC changedDate,
# MAGIC propmiseToPayStatus,
# MAGIC statusChangedIndicator,
# MAGIC replacementPropmiseToPayId,
# MAGIC installmentsAgreed,
# MAGIC firstDueDate,
# MAGIC finalDueDate,
# MAGIC numberOfPayments,
# MAGIC paymentPromised,
# MAGIC amountPaidByToday,
# MAGIC currentLevelOfFulfillment
# MAGIC from
# MAGIC (select
# MAGIC PPKEY as propmiseToPayId,
# MAGIC GPART as businessPartnerGroupNumber,
# MAGIC VKONT as contractAccountNumber,
# MAGIC BUKRS as companyCode,
# MAGIC PPRSC as promiseToPayReasonCode,
# MAGIC PPRSW as withdrawalReasonCode,
# MAGIC PPCAT as propmiseToPayCategoryCode,
# MAGIC C4LEV as numberOfChecks,
# MAGIC PRCUR as currency,
# MAGIC PRAMT as paymentAmountPromised,
# MAGIC PRAMT_CHR as promiseToPayCharges,
# MAGIC PRAMT_INT as promiseToPayInterest,
# MAGIC RDAMT as amountCleared,
# MAGIC ERNAM as createdBy,
# MAGIC concat(ERDAT,ERTIM) as createdDateTime,
# MAGIC CHDAT as changedDate,
# MAGIC PPSTA as propmiseToPayStatus,
# MAGIC XSTCH as statusChangedIndicator,
# MAGIC PPKEY_NEW as replacementPropmiseToPayId,
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
# MAGIC propmiseToPayId,businessPartnerGroupNumber,contractAccountNumber,
# MAGIC companyCode,promiseToPayReasonCode,withdrawalReasonCode,
# MAGIC propmiseToPayCategoryCode,numberOfChecks,currency,paymentAmountPromised,
# MAGIC promiseToPayCharges,promiseToPayInterest,amountCleared,createdBy,
# MAGIC createdDateTime,changedDate,propmiseToPayStatus,statusChangedIndicator,
# MAGIC replacementPropmiseToPayId,installmentsAgreed,firstDueDate,finalDueDate,
# MAGIC numberOfPayments,paymentPromised,amountPaidByToday,currentLevelOfFulfillment
# MAGIC , COUNT (*) as count
# MAGIC FROM cleansed.${vars.table}
# MAGIC GROUP BY propmiseToPayId,businessPartnerGroupNumber,contractAccountNumber,
# MAGIC companyCode,promiseToPayReasonCode,withdrawalReasonCode,propmiseToPayCategoryCode,
# MAGIC numberOfChecks,currency,paymentAmountPromised,promiseToPayCharges,promiseToPayInterest,
# MAGIC amountCleared,createdBy,createdDateTime,changedDate,propmiseToPayStatus,statusChangedIndicator,
# MAGIC replacementPropmiseToPayId,installmentsAgreed,firstDueDate,finalDueDate,numberOfPayments,
# MAGIC paymentPromised,amountPaidByToday,currentLevelOfFulfillment
# MAGIC HAVING COUNT (*) > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT * FROM (
# MAGIC SELECT
# MAGIC *,
# MAGIC row_number() OVER(PARTITION BY propmiseToPayId  order by propmiseToPayId) as rn
# MAGIC FROM  cleansed.${vars.table}
# MAGIC )a where a.rn > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Source and Target Data
# MAGIC %sql
# MAGIC select
# MAGIC propmiseToPayId,
# MAGIC businessPartnerGroupNumber,
# MAGIC contractAccountNumber,
# MAGIC companyCode,
# MAGIC promiseToPayReasonCode,
# MAGIC withdrawalReasonCode,
# MAGIC propmiseToPayCategoryCode,
# MAGIC numberOfChecks,
# MAGIC currency,
# MAGIC paymentAmountPromised,
# MAGIC promiseToPayCharges,
# MAGIC promiseToPayInterest,
# MAGIC amountCleared,
# MAGIC createdBy,
# MAGIC createdDateTime,
# MAGIC changedDate,
# MAGIC propmiseToPayStatus,
# MAGIC statusChangedIndicator,
# MAGIC replacementPropmiseToPayId,
# MAGIC installmentsAgreed,
# MAGIC firstDueDate,
# MAGIC finalDueDate,
# MAGIC numberOfPayments,
# MAGIC paymentPromised,
# MAGIC amountPaidByToday,
# MAGIC currentLevelOfFulfillment
# MAGIC from
# MAGIC (select
# MAGIC PPKEY as propmiseToPayId,
# MAGIC GPART as businessPartnerGroupNumber,
# MAGIC VKONT as contractAccountNumber,
# MAGIC BUKRS as companyCode,
# MAGIC PPRSC as promiseToPayReasonCode,
# MAGIC PPRSW as withdrawalReasonCode,
# MAGIC PPCAT as propmiseToPayCategoryCode,
# MAGIC C4LEV as numberOfChecks,
# MAGIC PRCUR as currency,
# MAGIC cast(PRAMT as decimal(13,2))  as paymentAmountPromised,
# MAGIC cast(PRAMT_CHR as decimal(13,2)) as promiseToPayCharges,
# MAGIC cast(PRAMT_INT as decimal(13,2)) as promiseToPayInterest,
# MAGIC cast(RDAMT as decimal(13,2)) as amountCleared,
# MAGIC ERNAM as createdBy,
# MAGIC cast(to_unix_timestamp(concat(ERDAT,' ',ERTIM), 'yyyy-MM-dd HH:mm:ss') as timestamp) as createdDateTime,
# MAGIC CHDAT as changedDate,
# MAGIC PPSTA as propmiseToPayStatus,
# MAGIC XSTCH as statusChangedIndicator,
# MAGIC PPKEY_NEW as replacementPropmiseToPayId,
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
# MAGIC propmiseToPayId,
# MAGIC businessPartnerGroupNumber,
# MAGIC contractAccountNumber,
# MAGIC companyCode,
# MAGIC promiseToPayReasonCode,
# MAGIC withdrawalReasonCode,
# MAGIC propmiseToPayCategoryCode,
# MAGIC numberOfChecks,
# MAGIC currency,
# MAGIC paymentAmountPromised,
# MAGIC promiseToPayCharges,
# MAGIC promiseToPayInterest,
# MAGIC amountCleared,
# MAGIC createdBy,
# MAGIC createdDateTime,
# MAGIC changedDate,
# MAGIC propmiseToPayStatus,
# MAGIC statusChangedIndicator,
# MAGIC replacementPropmiseToPayId,
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
# MAGIC propmiseToPayId,
# MAGIC businessPartnerGroupNumber,
# MAGIC contractAccountNumber,
# MAGIC companyCode,
# MAGIC promiseToPayReasonCode,
# MAGIC withdrawalReasonCode,
# MAGIC propmiseToPayCategoryCode,
# MAGIC numberOfChecks,
# MAGIC currency,
# MAGIC paymentAmountPromised,
# MAGIC promiseToPayCharges,
# MAGIC promiseToPayInterest,
# MAGIC amountCleared,
# MAGIC createdBy,
# MAGIC createdDateTime,
# MAGIC changedDate,
# MAGIC propmiseToPayStatus,
# MAGIC statusChangedIndicator,
# MAGIC replacementPropmiseToPayId,
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
# MAGIC propmiseToPayId,
# MAGIC businessPartnerGroupNumber,
# MAGIC contractAccountNumber,
# MAGIC companyCode,
# MAGIC promiseToPayReasonCode,
# MAGIC withdrawalReasonCode,
# MAGIC propmiseToPayCategoryCode,
# MAGIC numberOfChecks,
# MAGIC currency,
# MAGIC paymentAmountPromised,
# MAGIC promiseToPayCharges,
# MAGIC promiseToPayInterest,
# MAGIC amountCleared,
# MAGIC createdBy,
# MAGIC createdDateTime,
# MAGIC changedDate,
# MAGIC propmiseToPayStatus,
# MAGIC statusChangedIndicator,
# MAGIC replacementPropmiseToPayId,
# MAGIC installmentsAgreed,
# MAGIC firstDueDate,
# MAGIC finalDueDate,
# MAGIC numberOfPayments,
# MAGIC paymentPromised,
# MAGIC amountPaidByToday,
# MAGIC currentLevelOfFulfillment
# MAGIC from
# MAGIC (select
# MAGIC PPKEY as propmiseToPayId,
# MAGIC GPART as businessPartnerGroupNumber,
# MAGIC VKONT as contractAccountNumber,
# MAGIC BUKRS as companyCode,
# MAGIC PPRSC as promiseToPayReasonCode,
# MAGIC PPRSW as withdrawalReasonCode,
# MAGIC PPCAT as propmiseToPayCategoryCode,
# MAGIC C4LEV as numberOfChecks,
# MAGIC PRCUR as currency,
# MAGIC cast(PRAMT as decimal(13,2))  as paymentAmountPromised,
# MAGIC cast(PRAMT_CHR as decimal(13,2)) as promiseToPayCharges,
# MAGIC cast(PRAMT_INT as decimal(13,2)) as promiseToPayInterest,
# MAGIC cast(RDAMT as decimal(13,2)) as amountCleared,
# MAGIC ERNAM as createdBy,
# MAGIC cast(to_unix_timestamp(concat(ERDAT,' ',ERTIM), 'yyyy-MM-dd HH:mm:ss') as timestamp) as createdDateTime,
# MAGIC CHDAT as changedDate,
# MAGIC PPSTA as propmiseToPayStatus,
# MAGIC XSTCH as statusChangedIndicator,
# MAGIC PPKEY_NEW as replacementPropmiseToPayId,
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
