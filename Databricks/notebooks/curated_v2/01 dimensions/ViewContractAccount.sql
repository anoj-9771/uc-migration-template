-- Databricks notebook source
-- View: view_ContractAccount
-- Description: view_ContractAccount
CREATE OR REPLACE VIEW curated_v2.view_ContractAccount AS
WITH dateDriver AS (
         SELECT DISTINCT
             sourceSystemCode,
             contractAccountNumber,
             _recordStart AS _effectiveFrom
         FROM curated_v2.dimContractAccount WHERE _recordDeleted <> 1
     ),
 
     effectiveDateRanges AS (
         SELECT 
             sourceSystemCode,
             contractAccountNumber, 
             _effectiveFrom, 
             COALESCE(
                 TIMESTAMP(
                     DATE_ADD(
                         LEAD(_effectiveFrom,1) OVER (PARTITION BY sourceSystemCode, contractAccountNumber ORDER BY _effectiveFrom),-1)
                 ), 
             TIMESTAMP('9999-12-31')) AS _effectiveTo
         from dateDriver
     ) 
SELECT
      effectiveDateRanges._effectiveFrom
     ,effectiveDateRanges._effectiveTo
     ,effectiveDateRanges.contractAccountNumber
     ,dimContractAccount.contractAccountSK
     ,dimContractAccount.sourceSystemCode
     ,dimContractAccount.legacyContractAccountNumber
     ,dimContractAccount.applicationAreaCode
     ,dimContractAccount.applicationArea
     ,dimContractAccount.contractAccountCategoryCode
     ,dimContractAccount.contractAccountCategory
     ,dimContractAccount.createdBy as contractAccountCreatedBy
     ,dimContractAccount.createdDate as contractAccountCreatedDate
     ,dimContractAccount.lastChangedBy as contractAccountLastChangedBy
     ,dimContractAccount.lastChangedDate as contractAccountLastChangedDate
     ,dimAccountBusinessPartner.accountBusinessPartnerSK
--      ,dimAccountBusinessPartner.contractAccountNumber
     ,dimAccountBusinessPartner.businessPartnerGroupNumber
     ,dimAccountBusinessPartner.accountRelationshipCode
     ,dimAccountBusinessPartner.accountRelationship
     ,dimAccountBusinessPartner.businessPartnerReferenceNumber
     ,dimAccountBusinessPartner.toleranceGroupCode
     ,dimAccountBusinessPartner.toleranceGroup
     ,dimAccountBusinessPartner.manualOutsortingReasonCode
     ,dimAccountBusinessPartner.manualOutsortingReason
     ,dimAccountBusinessPartner.outsortingCheckGroupCode
     ,dimAccountBusinessPartner.outsortingCheckGroup
     ,dimAccountBusinessPartner.manualOutsortingCount
     ,dimAccountBusinessPartner.participationInYearlyAdvancePaymentCode
     ,dimAccountBusinessPartner.participationInYearlyAdvancePayment
     ,dimAccountBusinessPartner.activatebudgetbillingProcedureCode
     ,dimAccountBusinessPartner.activatebudgetbillingProcedure
     ,dimAccountBusinessPartner.paymentConditionCode
     ,dimAccountBusinessPartner.paymentCondition
     ,dimAccountBusinessPartner.accountDeterminationCode
     ,dimAccountBusinessPartner.accountDetermination
     ,dimAccountBusinessPartner.alternativeInvoiceRecipient
     ,dimAccountBusinessPartner.addressNumber
     ,dimAccountBusinessPartner.addressNumberForAlternativeBillRecipient
     ,dimAccountBusinessPartner.alternativeContractAccountForCollectiveBills
     ,dimAccountBusinessPartner.dispatchControlForAltBillRecipientCode
     ,dimAccountBusinessPartner.dispatchControlForAltBillRecipient
     ,dimAccountBusinessPartner.applicationFormCode
     ,dimAccountBusinessPartner.applicationForm
     ,dimAccountBusinessPartner.sendAdditionalBillFlag
     ,dimAccountBusinessPartner.headerUUID
     ,dimAccountBusinessPartner.companyGroupCode
     ,dimAccountBusinessPartner.companyGroupName
     ,dimAccountBusinessPartner.standardCompanyCode
     ,dimAccountBusinessPartner.standardCompanyName
     ,dimAccountBusinessPartner.incomingPaymentMethodCode
     ,dimAccountBusinessPartner.incomingPaymentMethod
     ,dimAccountBusinessPartner.bankDetailsId
     ,dimAccountBusinessPartner.paymentCardId
     ,dimAccountBusinessPartner.noPaymentFormFlag
     ,dimAccountBusinessPartner.alternativeDunningRecipient
     ,dimAccountBusinessPartner.collectionStrategyCode
     ,dimAccountBusinessPartner.collectionStrategyName
     ,dimAccountBusinessPartner.collectionManagementMasterDataGroupCode
     ,dimAccountBusinessPartner.collectionManagementMasterDataGroup
     ,dimAccountBusinessPartner.shippingControlForAltDunningRecipientCode
     ,dimAccountBusinessPartner.shippingControlForAltDunningRecipient
     ,dimAccountBusinessPartner.sendAdditionalDunningNoticeFlag
     ,dimAccountBusinessPartner.dispatchControlForOriginalCustomerCode
     ,dimAccountBusinessPartner.dispatchControlForOriginalCustomer
     ,dimAccountBusinessPartner.budgetBillingRequestForCashPayerCode
     ,dimAccountBusinessPartner.budgetBillingRequestForCashPayer
     ,dimAccountBusinessPartner.budgetBillingRequestForDebtorCode
     ,dimAccountBusinessPartner.budgetBillingRequestForDebtor
     ,dimAccountBusinessPartner.directDebitLimit
     ,dimAccountBusinessPartner.addressNumberForAlternativeDunningRecipient
     ,dimAccountBusinessPartner.numberOfSuccessfulDirectDebits
     ,dimAccountBusinessPartner.numberOfDirectDebitReturns
     ,dimAccountBusinessPartner.additionalDaysForCashManagement
     ,dimAccountBusinessPartner.numberOfMonthsForDirectDebitLimit
     ,dimAccountBusinessPartner.clearingCategoryCode
     ,dimAccountBusinessPartner.clearingCategory
     ,dimAccountBusinessPartner.createdBy as accountBpCreatedBy
     ,dimAccountBusinessPartner.createdDate as accountBpCreatedDate
     ,dimAccountBusinessPartner.changedBy as accountBpChangedBy
     ,dimAccountBusinessPartner.lastChangedDate as accountBplastChangedDate
FROM effectiveDateRanges
LEFT OUTER JOIN curated_v2.dimContractAccount
  ON effectiveDateRanges.contractAccountNumber = dimContractAccount.contractAccountNumber
        AND effectiveDateRanges.sourceSystemCode = dimContractAccount.sourceSystemCode
        AND effectiveDateRanges._effectiveFrom <= dimContractAccount._RecordEnd
        AND effectiveDateRanges._effectiveTo >= dimContractAccount._RecordStart 
LEFT OUTER JOIN curated_v2.dimAccountBusinessPartner
    ON effectiveDateRanges.contractAccountNumber = dimAccountBusinessPartner.contractAccountNumber
      AND effectiveDateRanges.sourceSystemCode = dimAccountBusinessPartner.sourceSystemCode
      AND effectiveDateRanges._effectiveFrom <= dimAccountBusinessPartner._RecordEnd
      AND effectiveDateRanges._effectiveTo >= dimAccountBusinessPartner._RecordStart
ORDER BY effectiveDateRanges._effectiveFrom

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.notebook.exit("1")
