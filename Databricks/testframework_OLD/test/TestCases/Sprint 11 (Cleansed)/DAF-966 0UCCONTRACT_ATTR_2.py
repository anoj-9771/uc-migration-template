# Databricks notebook source
#config parameters
source = 'ISU' #either CRM or ISU
table = '0UCCONTRACT_ATTR_2'

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
# MAGIC companyCode
# MAGIC ,divisionCode
# MAGIC ,accountDeterminationCode
# MAGIC ,allowableBudgetBillingCycles
# MAGIC ,invoiceContractsJointly
# MAGIC ,billBlockingReasonCode
# MAGIC ,billReleasingReasonCode
# MAGIC ,contractText
# MAGIC ,legacyMoveInDate
# MAGIC ,numberOfCancellations
# MAGIC ,numberOfRenewals
# MAGIC ,personnelNumber
# MAGIC ,contractNumberLegacy
# MAGIC ,createdDate
# MAGIC ,createdBy
# MAGIC ,lastChangedDate
# MAGIC ,lastChangedBy
# MAGIC ,deletedIndicator
# MAGIC ,isContractInvoiced
# MAGIC ,wbsElement
# MAGIC ,outsortingCheckGroupForBilling
# MAGIC ,manualOutsortingCount
# MAGIC ,paymentPlanStartMonth
# MAGIC ,serviceProvider
# MAGIC ,alternativePaymentStartMonth
# MAGIC ,contractTerminatedForBilling
# MAGIC ,salesEmployee
# MAGIC ,invoicingParty
# MAGIC ,cancellationReasonCRM
# MAGIC ,installationId
# MAGIC ,contractAccountNumber
# MAGIC ,specialMoveOutCase
# MAGIC ,moveInDate
# MAGIC ,moveOutDate
# MAGIC ,budgetBillingStopDate
# MAGIC ,isContractTransferred
# MAGIC ,businessPartnerGroupNumber
# MAGIC ,validFromDate
# MAGIC ,agreementNumber
# MAGIC ,premise
# MAGIC ,propertyNumber
# MAGIC ,alternativeAddressNumber
# MAGIC ,identificationNumber
# MAGIC ,addressNumber
# MAGIC ,objectReferenceId
# MAGIC ,objectNumber
# MAGIC ,contractId
# MAGIC from
# MAGIC (select 
# MAGIC BUKRS as companyCode,
# MAGIC SPARTE as divisionCode,
# MAGIC KOFIZ as accountDeterminationCode,
# MAGIC ABSZYK as allowableBudgetBillingCycles,
# MAGIC GEMFAKT as invoiceContractsJointly,
# MAGIC ABRSPERR as billBlockingReasonCode,
# MAGIC ABRFREIG as billReleasingReasonCode,
# MAGIC VBEZ as contractText,
# MAGIC EINZDAT_ALT as legacyMoveInDate,
# MAGIC KFRIST as numberOfCancellations,
# MAGIC VERLAENG as numberOfRenewals,
# MAGIC PERSNR as personnelNumber,
# MAGIC VREFER as contractNumberLegacy,
# MAGIC ERDAT as createdDate,
# MAGIC ERNAM as createdBy,
# MAGIC AEDAT as lastChangedDate,
# MAGIC AENAM as lastChangedBy,
# MAGIC LOEVM as deletedIndicator,
# MAGIC FAKTURIERT as isContractInvoiced,
# MAGIC PS_PSP_PNR as wbsElement,
# MAGIC AUSGRUP as outsortingCheckGroupForBilling,
# MAGIC OUTCOUNT as manualOutsortingCount,
# MAGIC PYPLS as paymentPlanStartMonth,
# MAGIC SERVICEID as serviceProvider,
# MAGIC PYPLA as alternativePaymentStartMonth,
# MAGIC BILLFINIT as contractTerminatedForBilling,
# MAGIC SALESEMPLOYEE as salesEmployee,
# MAGIC INVOICING_PARTY as invoicingParty,
# MAGIC CANCREASON_NEW as cancellationReasonCRM,
# MAGIC ANLAGE as installationId,
# MAGIC VKONTO as contractAccountNumber,
# MAGIC KZSONDAUSZ as specialMoveOutCase,
# MAGIC EINZDAT as moveInDate,
# MAGIC AUSZDAT as moveOutDate,
# MAGIC ABSSTOPDAT as budgetBillingStopDate,
# MAGIC XVERA as isContractTransferred,
# MAGIC ZGPART as businessPartnerGroupNumber,
# MAGIC case
# MAGIC when cast(ZDATE_FROM as DATE) < '1900-01-01' then '1900-01-01'
# MAGIC else ZDATE_FROM end as validFromDate,
# MAGIC ZZAGREEMENT_NUM as agreementNumber,
# MAGIC VSTELLE as premise,
# MAGIC HAUS as propertyNumber,
# MAGIC ZZZ_ADRMA as alternativeAddressNumber,
# MAGIC ZZZ_IDNUMBER as identificationNumber,
# MAGIC ZZ_ADRNR as addressNumber,
# MAGIC ZZ_OWNER as objectReferenceId,
# MAGIC ZZ_OBJNR as objectNumber,
# MAGIC VERTRAG as contractId,
# MAGIC row_number() over (partition by VERTRAG order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table})a where  a.rn = 1

# COMMAND ----------

# DBTITLE 1,[Verification] Count Check
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from cleansed.${vars.table}
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from (select
# MAGIC companyCode
# MAGIC ,divisionCode
# MAGIC ,accountDeterminationCode
# MAGIC ,allowableBudgetBillingCycles
# MAGIC ,invoiceContractsJointly
# MAGIC ,billBlockingReasonCode
# MAGIC ,billReleasingReasonCode
# MAGIC ,contractText
# MAGIC ,legacyMoveInDate
# MAGIC ,numberOfCancellations
# MAGIC ,numberOfRenewals
# MAGIC ,personnelNumber
# MAGIC ,contractNumberLegacy
# MAGIC ,createdDate
# MAGIC ,createdBy
# MAGIC ,lastChangedDate
# MAGIC ,lastChangedBy
# MAGIC ,deletedIndicator
# MAGIC ,isContractInvoiced
# MAGIC ,wbsElement
# MAGIC ,outsortingCheckGroupForBilling
# MAGIC ,manualOutsortingCount
# MAGIC ,paymentPlanStartMonth
# MAGIC ,serviceProvider
# MAGIC ,alternativePaymentStartMonth
# MAGIC ,contractTerminatedForBilling
# MAGIC ,salesEmployee
# MAGIC ,invoicingParty
# MAGIC ,cancellationReasonCRM
# MAGIC ,installationId
# MAGIC ,contractAccountNumber
# MAGIC ,specialMoveOutCase
# MAGIC ,moveInDate
# MAGIC ,moveOutDate
# MAGIC ,budgetBillingStopDate
# MAGIC ,isContractTransferred
# MAGIC ,businessPartnerGroupNumber
# MAGIC ,validFromDate
# MAGIC ,agreementNumber
# MAGIC ,premise
# MAGIC ,propertyNumber
# MAGIC ,alternativeAddressNumber
# MAGIC ,identificationNumber
# MAGIC ,addressNumber
# MAGIC ,objectReferenceId
# MAGIC ,objectNumber
# MAGIC ,contractId
# MAGIC from
# MAGIC (select 
# MAGIC BUKRS as companyCode,
# MAGIC SPARTE as divisionCode,
# MAGIC KOFIZ as accountDeterminationCode,
# MAGIC ABSZYK as allowableBudgetBillingCycles,
# MAGIC GEMFAKT as invoiceContractsJointly,
# MAGIC ABRSPERR as billBlockingReasonCode,
# MAGIC ABRFREIG as billReleasingReasonCode,
# MAGIC VBEZ as contractText,
# MAGIC EINZDAT_ALT as legacyMoveInDate,
# MAGIC KFRIST as numberOfCancellations,
# MAGIC VERLAENG as numberOfRenewals,
# MAGIC PERSNR as personnelNumber,
# MAGIC VREFER as contractNumberLegacy,
# MAGIC ERDAT as createdDate,
# MAGIC ERNAM as createdBy,
# MAGIC AEDAT as lastChangedDate,
# MAGIC AENAM as lastChangedBy,
# MAGIC LOEVM as deletedIndicator,
# MAGIC FAKTURIERT as isContractInvoiced,
# MAGIC PS_PSP_PNR as wbsElement,
# MAGIC AUSGRUP as outsortingCheckGroupForBilling,
# MAGIC OUTCOUNT as manualOutsortingCount,
# MAGIC PYPLS as paymentPlanStartMonth,
# MAGIC SERVICEID as serviceProvider,
# MAGIC PYPLA as alternativePaymentStartMonth,
# MAGIC BILLFINIT as contractTerminatedForBilling,
# MAGIC SALESEMPLOYEE as salesEmployee,
# MAGIC INVOICING_PARTY as invoicingParty,
# MAGIC CANCREASON_NEW as cancellationReasonCRM,
# MAGIC ANLAGE as installationId,
# MAGIC VKONTO as contractAccountNumber,
# MAGIC KZSONDAUSZ as specialMoveOutCase,
# MAGIC EINZDAT as moveInDate,
# MAGIC AUSZDAT as moveOutDate,
# MAGIC ABSSTOPDAT as budgetBillingStopDate,
# MAGIC XVERA as isContractTransferred,
# MAGIC ZGPART as businessPartnerGroupNumber,
# MAGIC case
# MAGIC when cast(ZDATE_FROM as DATE) < '1900-01-01' then '1900-01-01'
# MAGIC else ZDATE_FROM end as validFromDate,
# MAGIC ZZAGREEMENT_NUM as agreementNumber,
# MAGIC VSTELLE as premise,
# MAGIC HAUS as propertyNumber,
# MAGIC ZZZ_ADRMA as alternativeAddressNumber,
# MAGIC ZZZ_IDNUMBER as identificationNumber,
# MAGIC ZZ_ADRNR as addressNumber,
# MAGIC ZZ_OWNER as objectReferenceId,
# MAGIC ZZ_OBJNR as objectNumber,
# MAGIC VERTRAG as contractId,
# MAGIC row_number() over (partition by VERTRAG order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table})a where  a.rn = 1)

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT companyCode ,divisionCode,accountDeterminationCode,allowableBudgetBillingCycles
# MAGIC ,invoiceContractsJointly ,billBlockingReasonCode,billReleasingReasonCode,contractText
# MAGIC ,legacyMoveInDate,numberOfCancellations,numberOfRenewals,personnelNumber,contractNumberLegacy
# MAGIC ,createdDate,createdBy,lastChangedDate,lastChangedBy,deletedIndicator,isContractInvoiced
# MAGIC ,wbsElement,outsortingCheckGroupForBilling,manualOutsortingCount,paymentPlanStartMonth,serviceProvider
# MAGIC ,alternativePaymentStartMonth,contractTerminatedForBilling,salesEmployee,invoicingParty,cancellationReasonCRM
# MAGIC ,installationId,contractAccountNumber,specialMoveOutCase,moveInDate,moveOutDate,budgetBillingStopDate,isContractTransferred
# MAGIC ,businessPartnerGroupNumber,validFromDate,agreementNumber,premise,propertyNumber,alternativeAddressNumber,identificationNumber
# MAGIC ,addressNumber,objectReferenceId,objectNumber,contractId, COUNT (*) as count
# MAGIC FROM cleansed.${vars.table}
# MAGIC GROUP BY companyCode ,divisionCode,accountDeterminationCode,allowableBudgetBillingCycles
# MAGIC ,invoiceContractsJointly ,billBlockingReasonCode,billReleasingReasonCode,contractText
# MAGIC ,legacyMoveInDate,numberOfCancellations,numberOfRenewals,personnelNumber,contractNumberLegacy
# MAGIC ,createdDate,createdBy,lastChangedDate,lastChangedBy,deletedIndicator,isContractInvoiced
# MAGIC ,wbsElement,outsortingCheckGroupForBilling,manualOutsortingCount,paymentPlanStartMonth,serviceProvider
# MAGIC ,alternativePaymentStartMonth,contractTerminatedForBilling,salesEmployee,invoicingParty,cancellationReasonCRM
# MAGIC ,installationId,contractAccountNumber,specialMoveOutCase,moveInDate,moveOutDate,budgetBillingStopDate,isContractTransferred
# MAGIC ,businessPartnerGroupNumber,validFromDate,agreementNumber,premise,propertyNumber,alternativeAddressNumber,identificationNumber
# MAGIC ,addressNumber,objectReferenceId,objectNumber,contractId
# MAGIC HAVING COUNT (*) > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT * FROM (
# MAGIC SELECT
# MAGIC *,
# MAGIC row_number() OVER(PARTITION BY contractId 
# MAGIC order by contractId) as rn
# MAGIC FROM  cleansed.${vars.table}
# MAGIC )a where a.rn > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Source and Target Data
# MAGIC %sql
# MAGIC select
# MAGIC companyCode
# MAGIC ,divisionCode
# MAGIC ,accountDeterminationCode
# MAGIC ,allowableBudgetBillingCycles
# MAGIC ,invoiceContractsJointly
# MAGIC ,billBlockingReasonCode
# MAGIC ,billReleasingReasonCode
# MAGIC ,contractText
# MAGIC ,legacyMoveInDate
# MAGIC ,numberOfCancellations
# MAGIC ,numberOfRenewals
# MAGIC ,personnelNumber
# MAGIC ,contractNumberLegacy
# MAGIC ,createdDate
# MAGIC ,createdBy
# MAGIC ,lastChangedDate
# MAGIC ,lastChangedBy
# MAGIC ,deletedIndicator
# MAGIC ,isContractInvoiced
# MAGIC ,wbsElement
# MAGIC ,outsortingCheckGroupForBilling
# MAGIC ,manualOutsortingCount
# MAGIC ,paymentPlanStartMonth
# MAGIC ,serviceProvider
# MAGIC ,alternativePaymentStartMonth
# MAGIC ,contractTerminatedForBilling
# MAGIC ,salesEmployee
# MAGIC ,invoicingParty
# MAGIC ,cancellationReasonCRM
# MAGIC ,installationId
# MAGIC ,contractAccountNumber
# MAGIC ,specialMoveOutCase
# MAGIC ,moveInDate
# MAGIC ,moveOutDate
# MAGIC ,budgetBillingStopDate
# MAGIC ,isContractTransferred
# MAGIC ,businessPartnerGroupNumber
# MAGIC ,validFromDate
# MAGIC ,agreementNumber
# MAGIC ,premise
# MAGIC ,propertyNumber
# MAGIC ,alternativeAddressNumber
# MAGIC ,identificationNumber
# MAGIC ,addressNumber
# MAGIC ,objectReferenceId
# MAGIC ,objectNumber
# MAGIC ,contractId
# MAGIC from
# MAGIC (select 
# MAGIC BUKRS as companyCode,
# MAGIC SPARTE as divisionCode,
# MAGIC KOFIZ as accountDeterminationCode,
# MAGIC ABSZYK as allowableBudgetBillingCycles,
# MAGIC GEMFAKT as invoiceContractsJointly,
# MAGIC ABRSPERR as billBlockingReasonCode,
# MAGIC ABRFREIG as billReleasingReasonCode,
# MAGIC VBEZ as contractText,
# MAGIC EINZDAT_ALT as legacyMoveInDate,
# MAGIC KFRIST as numberOfCancellations,
# MAGIC VERLAENG as numberOfRenewals,
# MAGIC PERSNR as personnelNumber,
# MAGIC VREFER as contractNumberLegacy,
# MAGIC ERDAT as createdDate,
# MAGIC ERNAM as createdBy,
# MAGIC AEDAT as lastChangedDate,
# MAGIC AENAM as lastChangedBy,
# MAGIC LOEVM as deletedIndicator,
# MAGIC FAKTURIERT as isContractInvoiced,
# MAGIC PS_PSP_PNR as wbsElement,
# MAGIC AUSGRUP as outsortingCheckGroupForBilling,
# MAGIC OUTCOUNT as manualOutsortingCount,
# MAGIC PYPLS as paymentPlanStartMonth,
# MAGIC SERVICEID as serviceProvider,
# MAGIC PYPLA as alternativePaymentStartMonth,
# MAGIC BILLFINIT as contractTerminatedForBilling,
# MAGIC SALESEMPLOYEE as salesEmployee,
# MAGIC INVOICING_PARTY as invoicingParty,
# MAGIC CANCREASON_NEW as cancellationReasonCRM,
# MAGIC ANLAGE as installationId,
# MAGIC VKONTO as contractAccountNumber,
# MAGIC KZSONDAUSZ as specialMoveOutCase,
# MAGIC EINZDAT as moveInDate,
# MAGIC AUSZDAT as moveOutDate,
# MAGIC ABSSTOPDAT as budgetBillingStopDate,
# MAGIC XVERA as isContractTransferred,
# MAGIC ZGPART as businessPartnerGroupNumber,
# MAGIC case
# MAGIC when cast(ZDATE_FROM as DATE) < '1900-01-01' then '1900-01-01'
# MAGIC else ZDATE_FROM end as validFromDate,
# MAGIC ZZAGREEMENT_NUM as agreementNumber,
# MAGIC VSTELLE as premise,
# MAGIC HAUS as propertyNumber,
# MAGIC ZZZ_ADRMA as alternativeAddressNumber,
# MAGIC ZZZ_IDNUMBER as identificationNumber,
# MAGIC ZZ_ADRNR as addressNumber,
# MAGIC ZZ_OWNER as objectReferenceId,
# MAGIC ZZ_OBJNR as objectNumber,
# MAGIC VERTRAG as contractId,
# MAGIC row_number() over (partition by VERTRAG order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table})a where  a.rn = 1
# MAGIC except
# MAGIC select
# MAGIC companyCode
# MAGIC ,divisionCode
# MAGIC ,accountDeterminationCode
# MAGIC ,allowableBudgetBillingCycles
# MAGIC ,invoiceContractsJointly
# MAGIC ,billBlockingReasonCode
# MAGIC ,billReleasingReasonCode
# MAGIC ,contractText
# MAGIC ,legacyMoveInDate
# MAGIC ,numberOfCancellations
# MAGIC ,numberOfRenewals
# MAGIC ,personnelNumber
# MAGIC ,contractNumberLegacy
# MAGIC ,createdDate
# MAGIC ,createdBy
# MAGIC ,lastChangedDate
# MAGIC ,lastChangedBy
# MAGIC ,deletedIndicator
# MAGIC ,isContractInvoiced
# MAGIC ,wbsElement
# MAGIC ,outsortingCheckGroupForBilling
# MAGIC ,manualOutsortingCount
# MAGIC ,paymentPlanStartMonth
# MAGIC ,serviceProvider
# MAGIC ,alternativePaymentStartMonth
# MAGIC ,contractTerminatedForBilling
# MAGIC ,salesEmployee
# MAGIC ,invoicingParty
# MAGIC ,cancellationReasonCRM
# MAGIC ,installationId
# MAGIC ,contractAccountNumber
# MAGIC ,specialMoveOutCase
# MAGIC ,moveInDate
# MAGIC ,moveOutDate
# MAGIC ,budgetBillingStopDate
# MAGIC ,isContractTransferred
# MAGIC ,businessPartnerGroupNumber
# MAGIC ,validFromDate
# MAGIC ,agreementNumber
# MAGIC ,premise
# MAGIC ,propertyNumber
# MAGIC ,alternativeAddressNumber
# MAGIC ,identificationNumber
# MAGIC ,addressNumber
# MAGIC ,objectReferenceId
# MAGIC ,objectNumber
# MAGIC ,contractId
# MAGIC from
# MAGIC cleansed.${vars.table}

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Target and Source Data
# MAGIC %sql
# MAGIC select
# MAGIC companyCode
# MAGIC ,divisionCode
# MAGIC ,accountDeterminationCode
# MAGIC ,allowableBudgetBillingCycles
# MAGIC ,invoiceContractsJointly
# MAGIC ,billBlockingReasonCode
# MAGIC ,billReleasingReasonCode
# MAGIC ,contractText
# MAGIC ,legacyMoveInDate
# MAGIC ,numberOfCancellations
# MAGIC ,numberOfRenewals
# MAGIC ,personnelNumber
# MAGIC ,contractNumberLegacy
# MAGIC ,createdDate
# MAGIC ,createdBy
# MAGIC ,lastChangedDate
# MAGIC ,lastChangedBy
# MAGIC ,deletedIndicator
# MAGIC ,isContractInvoiced
# MAGIC ,wbsElement
# MAGIC ,outsortingCheckGroupForBilling
# MAGIC ,manualOutsortingCount
# MAGIC ,paymentPlanStartMonth
# MAGIC ,serviceProvider
# MAGIC ,alternativePaymentStartMonth
# MAGIC ,contractTerminatedForBilling
# MAGIC ,salesEmployee
# MAGIC ,invoicingParty
# MAGIC ,cancellationReasonCRM
# MAGIC ,installationId
# MAGIC ,contractAccountNumber
# MAGIC ,specialMoveOutCase
# MAGIC ,moveInDate
# MAGIC ,moveOutDate
# MAGIC ,budgetBillingStopDate
# MAGIC ,isContractTransferred
# MAGIC ,businessPartnerGroupNumber
# MAGIC ,validFromDate
# MAGIC ,agreementNumber
# MAGIC ,premise
# MAGIC ,propertyNumber
# MAGIC ,alternativeAddressNumber
# MAGIC ,identificationNumber
# MAGIC ,addressNumber
# MAGIC ,objectReferenceId
# MAGIC ,objectNumber
# MAGIC ,contractId
# MAGIC from
# MAGIC cleansed.${vars.table}
# MAGIC except
# MAGIC select
# MAGIC companyCode
# MAGIC ,divisionCode
# MAGIC ,accountDeterminationCode
# MAGIC ,allowableBudgetBillingCycles
# MAGIC ,invoiceContractsJointly
# MAGIC ,billBlockingReasonCode
# MAGIC ,billReleasingReasonCode
# MAGIC ,contractText
# MAGIC ,legacyMoveInDate
# MAGIC ,numberOfCancellations
# MAGIC ,numberOfRenewals
# MAGIC ,personnelNumber
# MAGIC ,contractNumberLegacy
# MAGIC ,createdDate
# MAGIC ,createdBy
# MAGIC ,lastChangedDate
# MAGIC ,lastChangedBy
# MAGIC ,deletedIndicator
# MAGIC ,isContractInvoiced
# MAGIC ,wbsElement
# MAGIC ,outsortingCheckGroupForBilling
# MAGIC ,manualOutsortingCount
# MAGIC ,paymentPlanStartMonth
# MAGIC ,serviceProvider
# MAGIC ,alternativePaymentStartMonth
# MAGIC ,contractTerminatedForBilling
# MAGIC ,salesEmployee
# MAGIC ,invoicingParty
# MAGIC ,cancellationReasonCRM
# MAGIC ,installationId
# MAGIC ,contractAccountNumber
# MAGIC ,specialMoveOutCase
# MAGIC ,moveInDate
# MAGIC ,moveOutDate
# MAGIC ,budgetBillingStopDate
# MAGIC ,isContractTransferred
# MAGIC ,businessPartnerGroupNumber
# MAGIC ,validFromDate
# MAGIC ,agreementNumber
# MAGIC ,premise
# MAGIC ,propertyNumber
# MAGIC ,alternativeAddressNumber
# MAGIC ,identificationNumber
# MAGIC ,addressNumber
# MAGIC ,objectReferenceId
# MAGIC ,objectNumber
# MAGIC ,contractId
# MAGIC from
# MAGIC (select 
# MAGIC BUKRS as companyCode,
# MAGIC SPARTE as divisionCode,
# MAGIC KOFIZ as accountDeterminationCode,
# MAGIC ABSZYK as allowableBudgetBillingCycles,
# MAGIC GEMFAKT as invoiceContractsJointly,
# MAGIC ABRSPERR as billBlockingReasonCode,
# MAGIC ABRFREIG as billReleasingReasonCode,
# MAGIC VBEZ as contractText,
# MAGIC EINZDAT_ALT as legacyMoveInDate,
# MAGIC KFRIST as numberOfCancellations,
# MAGIC VERLAENG as numberOfRenewals,
# MAGIC PERSNR as personnelNumber,
# MAGIC VREFER as contractNumberLegacy,
# MAGIC ERDAT as createdDate,
# MAGIC ERNAM as createdBy,
# MAGIC AEDAT as lastChangedDate,
# MAGIC AENAM as lastChangedBy,
# MAGIC LOEVM as deletedIndicator,
# MAGIC FAKTURIERT as isContractInvoiced,
# MAGIC PS_PSP_PNR as wbsElement,
# MAGIC AUSGRUP as outsortingCheckGroupForBilling,
# MAGIC OUTCOUNT as manualOutsortingCount,
# MAGIC PYPLS as paymentPlanStartMonth,
# MAGIC SERVICEID as serviceProvider,
# MAGIC PYPLA as alternativePaymentStartMonth,
# MAGIC BILLFINIT as contractTerminatedForBilling,
# MAGIC SALESEMPLOYEE as salesEmployee,
# MAGIC INVOICING_PARTY as invoicingParty,
# MAGIC CANCREASON_NEW as cancellationReasonCRM,
# MAGIC ANLAGE as installationId,
# MAGIC VKONTO as contractAccountNumber,
# MAGIC KZSONDAUSZ as specialMoveOutCase,
# MAGIC EINZDAT as moveInDate,
# MAGIC AUSZDAT as moveOutDate,
# MAGIC ABSSTOPDAT as budgetBillingStopDate,
# MAGIC XVERA as isContractTransferred,
# MAGIC ZGPART as businessPartnerGroupNumber,
# MAGIC case
# MAGIC when cast(ZDATE_FROM as DATE) < '1900-01-01' then '1900-01-01'
# MAGIC else ZDATE_FROM end as validFromDate,
# MAGIC ZZAGREEMENT_NUM as agreementNumber,
# MAGIC VSTELLE as premise,
# MAGIC HAUS as propertyNumber,
# MAGIC ZZZ_ADRMA as alternativeAddressNumber,
# MAGIC ZZZ_IDNUMBER as identificationNumber,
# MAGIC ZZ_ADRNR as addressNumber,
# MAGIC ZZ_OWNER as objectReferenceId,
# MAGIC ZZ_OBJNR as objectNumber,
# MAGIC VERTRAG as contractId,
# MAGIC row_number() over (partition by VERTRAG order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table})a where  a.rn = 1
