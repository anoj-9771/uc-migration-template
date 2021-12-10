# Databricks notebook source
#config parameters
source = 'ISU' #either CRM or ISU
table = '0UC_ACCNTBP_ATTR_2'

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
# MAGIC businessPartnerGroupNumber
# MAGIC ,contractAccountNumber
# MAGIC ,budgetBillingRequestForDebtor
# MAGIC ,budgetBillingRequestForCashPayer
# MAGIC ,noPaymentFormIndicator
# MAGIC ,numberOfSuccessfulDirectDebits
# MAGIC ,numberOfDirectDebitReturns
# MAGIC ,sendAdditionalDunningNoticeIndicator
# MAGIC ,sendAdditionalBillIndicator
# MAGIC ,applicationForm
# MAGIC ,outsortingCheckGroupCode
# MAGIC ,manualOutsortingCount
# MAGIC ,manualOutsortingReasonCode
# MAGIC ,shippingControlForAlternativeDunningRecipient
# MAGIC ,dispatchControlForAlternativeBillRecipient
# MAGIC ,dispatchControl
# MAGIC ,billingProcedureActivationIndicator
# MAGIC ,participationInYearlyAdvancePayment
# MAGIC ,createdDate
# MAGIC ,createdBy
# MAGIC ,lastChangedDate
# MAGIC ,changedBy
# MAGIC ,additionalDaysForCashManagement
# MAGIC ,headerUUID
# MAGIC ,directDebitLimit
# MAGIC ,numberOfMonthsForDirectDebitLimit
# MAGIC ,businessPartnerReferenceNumber
# MAGIC ,companyCodeGroup
# MAGIC ,standardCompanyCode
# MAGIC ,alternativeDunningRecipient
# MAGIC ,bankDetailsId
# MAGIC ,incomingPaymentMethodCode
# MAGIC ,deletedIndicator
# MAGIC ,alternativeContractAccountForCollectiveBills
# MAGIC ,accountRelationshipCode
# MAGIC ,accountRelationship
# MAGIC ,addressNumber
# MAGIC ,addressNumberForAlternativeDunningRecipient
# MAGIC ,alternativeInvoiceRecipient
# MAGIC ,addressNumberForAlternativeBillRecipient
# MAGIC ,toleranceGroupCode
# MAGIC ,paymentCardId
# MAGIC ,clearingCategory
# MAGIC ,collectionManagementMasterDataGroup
# MAGIC ,collectionStrategyCode
# MAGIC ,paymentConditionCode
# MAGIC ,accountDeterminationCode
# MAGIC from
# MAGIC (select
# MAGIC GPART as businessPartnerGroupNumber
# MAGIC ,VKONT as contractAccountNumber
# MAGIC ,ABSANFAB as budgetBillingRequestForDebtor
# MAGIC ,ABSANFBZ as budgetBillingRequestForCashPayer
# MAGIC ,KEINZAHL as noPaymentFormIndicator
# MAGIC ,EINZUGSZ as numberOfSuccessfulDirectDebits
# MAGIC ,RUECKLZ as numberOfDirectDebitReturns
# MAGIC ,MAHNUNG_Z as sendAdditionalDunningNoticeIndicator
# MAGIC ,RECHNUNG_Z as sendAdditionalBillIndicator
# MAGIC ,FORMKEY as applicationForm
# MAGIC ,AUSGRUP_IN as outsortingCheckGroupCode
# MAGIC ,OUTCOUNT as manualOutsortingCount
# MAGIC ,MANOUTS_IN as manualOutsortingReasonCode
# MAGIC ,SENDCONTROL_MA as shippingControlForAlternativeDunningRecipient
# MAGIC ,SENDCONTROL_RH as dispatchControlForAlternativeBillRecipient
# MAGIC ,SENDCONTROL_GP as dispatchControl
# MAGIC ,KZABSVER as billingProcedureActivationIndicator
# MAGIC ,JVLTE as participationInYearlyAdvancePayment
# MAGIC ,ERDAT as createdDate
# MAGIC ,ERNAM as createdBy
# MAGIC ,AEDATP as lastChangedDate
# MAGIC ,AENAMP as changedBy
# MAGIC ,FDZTG as additionalDaysForCashManagement
# MAGIC ,GUID as headerUUID
# MAGIC ,DDLAM as directDebitLimit
# MAGIC ,DDLNM as numberOfMonthsForDirectDebitLimit
# MAGIC ,EXVKO as businessPartnerReferenceNumber
# MAGIC ,OPBUK as companyCodeGroup
# MAGIC ,STDBK as standardCompanyCode
# MAGIC ,ABWMA as alternativeDunningRecipient
# MAGIC ,EBVTY as bankDetailsId
# MAGIC ,EZAWE as incomingPaymentMethodCode
# MAGIC ,LOEVM as deletedIndicator
# MAGIC ,ABWVK as alternativeContractAccountForCollectiveBills
# MAGIC ,VKPBZ as accountRelationshipCode
# MAGIC ,b.accountRelationship as accountRelationship
# MAGIC ,ADRNB as addressNumber
# MAGIC ,ADRMA as addressNumberForAlternativeDunningRecipient
# MAGIC ,ABWRH as alternativeInvoiceRecipient
# MAGIC ,ADRRH as addressNumberForAlternativeBillRecipient
# MAGIC ,TOGRU as toleranceGroupCode
# MAGIC ,CCARD_ID as paymentCardId
# MAGIC ,VERTYP as clearingCategory
# MAGIC ,CMGRP as collectionManagementMasterDataGroup
# MAGIC ,STRAT as collectionStrategyCode
# MAGIC ,ZAHLKOND as paymentConditionCode
# MAGIC ,KOFIZ_SD as accountDeterminationCode
# MAGIC ,row_number() over (partition by GPART,VKONT order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table}
# MAGIC left join cleansed.isu_0fc_acctrel_text b
# MAGIC on b.accountRelationshipCode = VKPBZ)a where  a.rn = 1

# COMMAND ----------

# DBTITLE 1,[Verification] Count Check
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from cleansed.${vars.table}
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from (select
# MAGIC businessPartnerGroupNumber
# MAGIC ,contractAccountNumber
# MAGIC ,budgetBillingRequestForDebtor
# MAGIC ,budgetBillingRequestForCashPayer
# MAGIC ,noPaymentFormIndicator
# MAGIC ,numberOfSuccessfulDirectDebits
# MAGIC ,numberOfDirectDebitReturns
# MAGIC ,sendAdditionalDunningNoticeIndicator
# MAGIC ,sendAdditionalBillIndicator
# MAGIC ,applicationForm
# MAGIC ,outsortingCheckGroupCode
# MAGIC ,manualOutsortingCount
# MAGIC ,manualOutsortingReasonCode
# MAGIC ,shippingControlForAlternativeDunningRecipient
# MAGIC ,dispatchControlForAlternativeBillRecipient
# MAGIC ,dispatchControl
# MAGIC ,billingProcedureActivationIndicator
# MAGIC ,participationInYearlyAdvancePayment
# MAGIC ,createdDate
# MAGIC ,createdBy
# MAGIC ,lastChangedDate
# MAGIC ,changedBy
# MAGIC ,additionalDaysForCashManagement
# MAGIC ,headerUUID
# MAGIC ,directDebitLimit
# MAGIC ,numberOfMonthsForDirectDebitLimit
# MAGIC ,businessPartnerReferenceNumber
# MAGIC ,companyCodeGroup
# MAGIC ,standardCompanyCode
# MAGIC ,alternativeDunningRecipient
# MAGIC ,bankDetailsId
# MAGIC ,incomingPaymentMethodCode
# MAGIC ,deletedIndicator
# MAGIC ,alternativeContractAccountForCollectiveBills
# MAGIC ,accountRelationshipCode
# MAGIC ,accountRelationship
# MAGIC ,addressNumber
# MAGIC ,addressNumberForAlternativeDunningRecipient
# MAGIC ,alternativeInvoiceRecipient
# MAGIC ,addressNumberForAlternativeBillRecipient
# MAGIC ,toleranceGroupCode
# MAGIC ,paymentCardId
# MAGIC ,clearingCategory
# MAGIC ,collectionManagementMasterDataGroup
# MAGIC ,collectionStrategyCode
# MAGIC ,paymentConditionCode
# MAGIC ,accountDeterminationCode
# MAGIC from
# MAGIC (select
# MAGIC GPART as businessPartnerGroupNumber
# MAGIC ,VKONT as contractAccountNumber
# MAGIC ,ABSANFAB as budgetBillingRequestForDebtor
# MAGIC ,ABSANFBZ as budgetBillingRequestForCashPayer
# MAGIC ,KEINZAHL as noPaymentFormIndicator
# MAGIC ,EINZUGSZ as numberOfSuccessfulDirectDebits
# MAGIC ,RUECKLZ as numberOfDirectDebitReturns
# MAGIC ,MAHNUNG_Z as sendAdditionalDunningNoticeIndicator
# MAGIC ,RECHNUNG_Z as sendAdditionalBillIndicator
# MAGIC ,FORMKEY as applicationForm
# MAGIC ,AUSGRUP_IN as outsortingCheckGroupCode
# MAGIC ,OUTCOUNT as manualOutsortingCount
# MAGIC ,MANOUTS_IN as manualOutsortingReasonCode
# MAGIC ,SENDCONTROL_MA as shippingControlForAlternativeDunningRecipient
# MAGIC ,SENDCONTROL_RH as dispatchControlForAlternativeBillRecipient
# MAGIC ,SENDCONTROL_GP as dispatchControl
# MAGIC ,KZABSVER as billingProcedureActivationIndicator
# MAGIC ,JVLTE as participationInYearlyAdvancePayment
# MAGIC ,ERDAT as createdDate
# MAGIC ,ERNAM as createdBy
# MAGIC ,AEDATP as lastChangedDate
# MAGIC ,AENAMP as changedBy
# MAGIC ,FDZTG as additionalDaysForCashManagement
# MAGIC ,GUID as headerUUID
# MAGIC ,DDLAM as directDebitLimit
# MAGIC ,DDLNM as numberOfMonthsForDirectDebitLimit
# MAGIC ,EXVKO as businessPartnerReferenceNumber
# MAGIC ,OPBUK as companyCodeGroup
# MAGIC ,STDBK as standardCompanyCode
# MAGIC ,ABWMA as alternativeDunningRecipient
# MAGIC ,EBVTY as bankDetailsId
# MAGIC ,EZAWE as incomingPaymentMethodCode
# MAGIC ,LOEVM as deletedIndicator
# MAGIC ,ABWVK as alternativeContractAccountForCollectiveBills
# MAGIC ,VKPBZ as accountRelationshipCode
# MAGIC ,b.accountRelationship as accountRelationship
# MAGIC ,ADRNB as addressNumber
# MAGIC ,ADRMA as addressNumberForAlternativeDunningRecipient
# MAGIC ,ABWRH as alternativeInvoiceRecipient
# MAGIC ,ADRRH as addressNumberForAlternativeBillRecipient
# MAGIC ,TOGRU as toleranceGroupCode
# MAGIC ,CCARD_ID as paymentCardId
# MAGIC ,VERTYP as clearingCategory
# MAGIC ,CMGRP as collectionManagementMasterDataGroup
# MAGIC ,STRAT as collectionStrategyCode
# MAGIC ,ZAHLKOND as paymentConditionCode
# MAGIC ,KOFIZ_SD as accountDeterminationCode
# MAGIC ,row_number() over (partition by GPART,VKONT order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table}
# MAGIC left join cleansed.isu_0fc_acctrel_text b
# MAGIC on b.accountRelationshipCode = VKPBZ)a where  a.rn = 1
# MAGIC 
# MAGIC )

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT businessPartnerGroupNumber,contractAccountNumber
# MAGIC , COUNT (*) as count
# MAGIC FROM cleansed.${vars.table}
# MAGIC GROUP BY businessPartnerGroupNumber,contractAccountNumber
# MAGIC HAVING COUNT (*) > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT * FROM (
# MAGIC SELECT
# MAGIC *,
# MAGIC row_number() OVER(PARTITION BY businessPartnerGroupNumber,contractAccountNumber  order by businessPartnerGroupNumber,contractAccountNumber) as rn
# MAGIC FROM  cleansed.${vars.table}
# MAGIC )a where a.rn > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Source and Target Data
# MAGIC %sql
# MAGIC select
# MAGIC businessPartnerGroupNumber
# MAGIC ,contractAccountNumber
# MAGIC ,budgetBillingRequestForDebtor
# MAGIC ,budgetBillingRequestForCashPayer
# MAGIC ,noPaymentFormIndicator
# MAGIC ,numberOfSuccessfulDirectDebits
# MAGIC ,numberOfDirectDebitReturns
# MAGIC ,sendAdditionalDunningNoticeIndicator
# MAGIC ,sendAdditionalBillIndicator
# MAGIC ,applicationForm
# MAGIC ,outsortingCheckGroupCode
# MAGIC ,manualOutsortingCount
# MAGIC ,manualOutsortingReasonCode
# MAGIC ,shippingControlForAlternativeDunningRecipient
# MAGIC ,dispatchControlForAlternativeBillRecipient
# MAGIC ,dispatchControl
# MAGIC ,billingProcedureActivationIndicator
# MAGIC ,participationInYearlyAdvancePayment
# MAGIC ,createdDate
# MAGIC ,createdBy
# MAGIC ,lastChangedDate
# MAGIC ,changedBy
# MAGIC ,additionalDaysForCashManagement
# MAGIC ,headerUUID
# MAGIC ,directDebitLimit
# MAGIC ,numberOfMonthsForDirectDebitLimit
# MAGIC ,businessPartnerReferenceNumber
# MAGIC ,companyCodeGroup
# MAGIC ,standardCompanyCode
# MAGIC ,alternativeDunningRecipient
# MAGIC ,bankDetailsId
# MAGIC ,incomingPaymentMethodCode
# MAGIC ,deletedIndicator
# MAGIC ,alternativeContractAccountForCollectiveBills
# MAGIC ,accountRelationshipCode
# MAGIC ,accountRelationship
# MAGIC ,addressNumber
# MAGIC ,addressNumberForAlternativeDunningRecipient
# MAGIC ,alternativeInvoiceRecipient
# MAGIC ,addressNumberForAlternativeBillRecipient
# MAGIC ,toleranceGroupCode
# MAGIC ,paymentCardId
# MAGIC ,clearingCategory
# MAGIC ,collectionManagementMasterDataGroup
# MAGIC ,collectionStrategyCode
# MAGIC ,paymentConditionCode
# MAGIC ,accountDeterminationCode
# MAGIC from
# MAGIC (select
# MAGIC GPART as businessPartnerGroupNumber
# MAGIC ,VKONT as contractAccountNumber
# MAGIC ,ABSANFAB as budgetBillingRequestForDebtor
# MAGIC ,ABSANFBZ as budgetBillingRequestForCashPayer
# MAGIC ,KEINZAHL as noPaymentFormIndicator
# MAGIC ,EINZUGSZ as numberOfSuccessfulDirectDebits
# MAGIC ,RUECKLZ as numberOfDirectDebitReturns
# MAGIC ,MAHNUNG_Z as sendAdditionalDunningNoticeIndicator
# MAGIC ,RECHNUNG_Z as sendAdditionalBillIndicator
# MAGIC ,FORMKEY as applicationForm
# MAGIC ,AUSGRUP_IN as outsortingCheckGroupCode
# MAGIC ,OUTCOUNT as manualOutsortingCount
# MAGIC ,MANOUTS_IN as manualOutsortingReasonCode
# MAGIC ,SENDCONTROL_MA as shippingControlForAlternativeDunningRecipient
# MAGIC ,SENDCONTROL_RH as dispatchControlForAlternativeBillRecipient
# MAGIC ,SENDCONTROL_GP as dispatchControl
# MAGIC ,KZABSVER as billingProcedureActivationIndicator
# MAGIC ,JVLTE as participationInYearlyAdvancePayment
# MAGIC ,ERDAT as createdDate
# MAGIC ,ERNAM as createdBy
# MAGIC ,AEDATP as lastChangedDate
# MAGIC ,AENAMP as changedBy
# MAGIC ,FDZTG as additionalDaysForCashManagement
# MAGIC ,GUID as headerUUID
# MAGIC ,DDLAM as directDebitLimit
# MAGIC ,DDLNM as numberOfMonthsForDirectDebitLimit
# MAGIC ,EXVKO as businessPartnerReferenceNumber
# MAGIC ,OPBUK as companyCodeGroup
# MAGIC ,STDBK as standardCompanyCode
# MAGIC ,ABWMA as alternativeDunningRecipient
# MAGIC ,EBVTY as bankDetailsId
# MAGIC ,EZAWE as incomingPaymentMethodCode
# MAGIC ,LOEVM as deletedIndicator
# MAGIC ,ABWVK as alternativeContractAccountForCollectiveBills
# MAGIC ,VKPBZ as accountRelationshipCode
# MAGIC ,b.accountRelationship as accountRelationship
# MAGIC ,ADRNB as addressNumber
# MAGIC ,ADRMA as addressNumberForAlternativeDunningRecipient
# MAGIC ,ABWRH as alternativeInvoiceRecipient
# MAGIC ,ADRRH as addressNumberForAlternativeBillRecipient
# MAGIC ,TOGRU as toleranceGroupCode
# MAGIC ,CCARD_ID as paymentCardId
# MAGIC ,VERTYP as clearingCategory
# MAGIC ,CMGRP as collectionManagementMasterDataGroup
# MAGIC ,STRAT as collectionStrategyCode
# MAGIC ,ZAHLKOND as paymentConditionCode
# MAGIC ,KOFIZ_SD as accountDeterminationCode
# MAGIC ,row_number() over (partition by GPART,VKONT order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table}
# MAGIC left join cleansed.isu_0fc_acctrel_text b
# MAGIC on b.accountRelationshipCode = VKPBZ)a where  a.rn = 1
# MAGIC except
# MAGIC select
# MAGIC businessPartnerGroupNumber
# MAGIC ,contractAccountNumber
# MAGIC ,budgetBillingRequestForDebtor
# MAGIC ,budgetBillingRequestForCashPayer
# MAGIC ,noPaymentFormIndicator
# MAGIC ,numberOfSuccessfulDirectDebits
# MAGIC ,numberOfDirectDebitReturns
# MAGIC ,sendAdditionalDunningNoticeIndicator
# MAGIC ,sendAdditionalBillIndicator
# MAGIC ,applicationForm
# MAGIC ,outsortingCheckGroupCode
# MAGIC ,manualOutsortingCount
# MAGIC ,manualOutsortingReasonCode
# MAGIC ,shippingControlForAlternativeDunningRecipient
# MAGIC ,dispatchControlForAlternativeBillRecipient
# MAGIC ,dispatchControl
# MAGIC ,billingProcedureActivationIndicator
# MAGIC ,participationInYearlyAdvancePayment
# MAGIC ,createdDate
# MAGIC ,createdBy
# MAGIC ,lastChangedDate
# MAGIC ,changedBy
# MAGIC ,additionalDaysForCashManagement
# MAGIC ,headerUUID
# MAGIC ,directDebitLimit
# MAGIC ,numberOfMonthsForDirectDebitLimit
# MAGIC ,businessPartnerReferenceNumber
# MAGIC ,companyCodeGroup
# MAGIC ,standardCompanyCode
# MAGIC ,alternativeDunningRecipient
# MAGIC ,bankDetailsId
# MAGIC ,incomingPaymentMethodCode
# MAGIC ,deletedIndicator
# MAGIC ,alternativeContractAccountForCollectiveBills
# MAGIC ,accountRelationshipCode
# MAGIC ,accountRelationship
# MAGIC ,addressNumber
# MAGIC ,addressNumberForAlternativeDunningRecipient
# MAGIC ,alternativeInvoiceRecipient
# MAGIC ,addressNumberForAlternativeBillRecipient
# MAGIC ,toleranceGroupCode
# MAGIC ,paymentCardId
# MAGIC ,clearingCategory
# MAGIC ,collectionManagementMasterDataGroup
# MAGIC ,collectionStrategyCode
# MAGIC ,paymentConditionCode
# MAGIC ,accountDeterminationCode
# MAGIC from
# MAGIC cleansed.${vars.table}

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Target and Source Data
# MAGIC %sql
# MAGIC select
# MAGIC businessPartnerGroupNumber
# MAGIC ,contractAccountNumber
# MAGIC ,budgetBillingRequestForDebtor
# MAGIC ,budgetBillingRequestForCashPayer
# MAGIC ,noPaymentFormIndicator
# MAGIC ,numberOfSuccessfulDirectDebits
# MAGIC ,numberOfDirectDebitReturns
# MAGIC ,sendAdditionalDunningNoticeIndicator
# MAGIC ,sendAdditionalBillIndicator
# MAGIC ,applicationForm
# MAGIC ,outsortingCheckGroupCode
# MAGIC ,manualOutsortingCount
# MAGIC ,manualOutsortingReasonCode
# MAGIC ,shippingControlForAlternativeDunningRecipient
# MAGIC ,dispatchControlForAlternativeBillRecipient
# MAGIC ,dispatchControl
# MAGIC ,billingProcedureActivationIndicator
# MAGIC ,participationInYearlyAdvancePayment
# MAGIC ,createdDate
# MAGIC ,createdBy
# MAGIC ,lastChangedDate
# MAGIC ,changedBy
# MAGIC ,additionalDaysForCashManagement
# MAGIC ,headerUUID
# MAGIC ,directDebitLimit
# MAGIC ,numberOfMonthsForDirectDebitLimit
# MAGIC ,businessPartnerReferenceNumber
# MAGIC ,companyCodeGroup
# MAGIC ,standardCompanyCode
# MAGIC ,alternativeDunningRecipient
# MAGIC ,bankDetailsId
# MAGIC ,incomingPaymentMethodCode
# MAGIC ,deletedIndicator
# MAGIC ,alternativeContractAccountForCollectiveBills
# MAGIC ,accountRelationshipCode
# MAGIC ,accountRelationship
# MAGIC ,addressNumber
# MAGIC ,addressNumberForAlternativeDunningRecipient
# MAGIC ,alternativeInvoiceRecipient
# MAGIC ,addressNumberForAlternativeBillRecipient
# MAGIC ,toleranceGroupCode
# MAGIC ,paymentCardId
# MAGIC ,clearingCategory
# MAGIC ,collectionManagementMasterDataGroup
# MAGIC ,collectionStrategyCode
# MAGIC ,paymentConditionCode
# MAGIC ,accountDeterminationCode
# MAGIC from
# MAGIC cleansed.${vars.table}
# MAGIC except
# MAGIC select
# MAGIC businessPartnerGroupNumber
# MAGIC ,contractAccountNumber
# MAGIC ,budgetBillingRequestForDebtor
# MAGIC ,budgetBillingRequestForCashPayer
# MAGIC ,noPaymentFormIndicator
# MAGIC ,numberOfSuccessfulDirectDebits
# MAGIC ,numberOfDirectDebitReturns
# MAGIC ,sendAdditionalDunningNoticeIndicator
# MAGIC ,sendAdditionalBillIndicator
# MAGIC ,applicationForm
# MAGIC ,outsortingCheckGroupCode
# MAGIC ,manualOutsortingCount
# MAGIC ,manualOutsortingReasonCode
# MAGIC ,shippingControlForAlternativeDunningRecipient
# MAGIC ,dispatchControlForAlternativeBillRecipient
# MAGIC ,dispatchControl
# MAGIC ,billingProcedureActivationIndicator
# MAGIC ,participationInYearlyAdvancePayment
# MAGIC ,createdDate
# MAGIC ,createdBy
# MAGIC ,lastChangedDate
# MAGIC ,changedBy
# MAGIC ,additionalDaysForCashManagement
# MAGIC ,headerUUID
# MAGIC ,directDebitLimit
# MAGIC ,numberOfMonthsForDirectDebitLimit
# MAGIC ,businessPartnerReferenceNumber
# MAGIC ,companyCodeGroup
# MAGIC ,standardCompanyCode
# MAGIC ,alternativeDunningRecipient
# MAGIC ,bankDetailsId
# MAGIC ,incomingPaymentMethodCode
# MAGIC ,deletedIndicator
# MAGIC ,alternativeContractAccountForCollectiveBills
# MAGIC ,accountRelationshipCode
# MAGIC ,accountRelationship
# MAGIC ,addressNumber
# MAGIC ,addressNumberForAlternativeDunningRecipient
# MAGIC ,alternativeInvoiceRecipient
# MAGIC ,addressNumberForAlternativeBillRecipient
# MAGIC ,toleranceGroupCode
# MAGIC ,paymentCardId
# MAGIC ,clearingCategory
# MAGIC ,collectionManagementMasterDataGroup
# MAGIC ,collectionStrategyCode
# MAGIC ,paymentConditionCode
# MAGIC ,accountDeterminationCode
# MAGIC from
# MAGIC (select
# MAGIC GPART as businessPartnerGroupNumber
# MAGIC ,VKONT as contractAccountNumber
# MAGIC ,ABSANFAB as budgetBillingRequestForDebtor
# MAGIC ,ABSANFBZ as budgetBillingRequestForCashPayer
# MAGIC ,KEINZAHL as noPaymentFormIndicator
# MAGIC ,EINZUGSZ as numberOfSuccessfulDirectDebits
# MAGIC ,RUECKLZ as numberOfDirectDebitReturns
# MAGIC ,MAHNUNG_Z as sendAdditionalDunningNoticeIndicator
# MAGIC ,RECHNUNG_Z as sendAdditionalBillIndicator
# MAGIC ,FORMKEY as applicationForm
# MAGIC ,AUSGRUP_IN as outsortingCheckGroupCode
# MAGIC ,OUTCOUNT as manualOutsortingCount
# MAGIC ,MANOUTS_IN as manualOutsortingReasonCode
# MAGIC ,SENDCONTROL_MA as shippingControlForAlternativeDunningRecipient
# MAGIC ,SENDCONTROL_RH as dispatchControlForAlternativeBillRecipient
# MAGIC ,SENDCONTROL_GP as dispatchControl
# MAGIC ,KZABSVER as billingProcedureActivationIndicator
# MAGIC ,JVLTE as participationInYearlyAdvancePayment
# MAGIC ,ERDAT as createdDate
# MAGIC ,ERNAM as createdBy
# MAGIC ,AEDATP as lastChangedDate
# MAGIC ,AENAMP as changedBy
# MAGIC ,FDZTG as additionalDaysForCashManagement
# MAGIC ,GUID as headerUUID
# MAGIC ,DDLAM as directDebitLimit
# MAGIC ,DDLNM as numberOfMonthsForDirectDebitLimit
# MAGIC ,EXVKO as businessPartnerReferenceNumber
# MAGIC ,OPBUK as companyCodeGroup
# MAGIC ,STDBK as standardCompanyCode
# MAGIC ,ABWMA as alternativeDunningRecipient
# MAGIC ,EBVTY as bankDetailsId
# MAGIC ,EZAWE as incomingPaymentMethodCode
# MAGIC ,LOEVM as deletedIndicator
# MAGIC ,ABWVK as alternativeContractAccountForCollectiveBills
# MAGIC ,VKPBZ as accountRelationshipCode
# MAGIC ,b.accountRelationship as accountRelationship
# MAGIC ,ADRNB as addressNumber
# MAGIC ,ADRMA as addressNumberForAlternativeDunningRecipient
# MAGIC ,ABWRH as alternativeInvoiceRecipient
# MAGIC ,ADRRH as addressNumberForAlternativeBillRecipient
# MAGIC ,TOGRU as toleranceGroupCode
# MAGIC ,CCARD_ID as paymentCardId
# MAGIC ,VERTYP as clearingCategory
# MAGIC ,CMGRP as collectionManagementMasterDataGroup
# MAGIC ,STRAT as collectionStrategyCode
# MAGIC ,ZAHLKOND as paymentConditionCode
# MAGIC ,KOFIZ_SD as accountDeterminationCode
# MAGIC ,row_number() over (partition by GPART,VKONT order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table}
# MAGIC left join cleansed.isu_0fc_acctrel_text b
# MAGIC on b.accountRelationshipCode = VKPBZ)a where  a.rn = 1
