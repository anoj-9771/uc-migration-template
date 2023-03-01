-- Databricks notebook source
-- View: viewContractAccount
-- Description: viewContractAccount
CREATE OR REPLACE VIEW curated_v2.viewContractAccount AS

With ContractAccountDateRanges AS
(
                SELECT
                contractAccountNumber, 
                _recordStart, 
                _recordEnd,
                COALESCE( LEAD( _recordStart, 1 ) OVER( PARTITION BY contractAccountNumber ORDER BY _recordStart ), 
                  CASE WHEN _recordEnd < cast('9999-12-31T23:59:59' as timestamp) then _recordEnd + INTERVAL 1 SECOND else _recordEnd end) AS _newRecordEnd
                FROM curated_v2.dimContractAccount
                --WHERE _recordDeleted = 0
),

AccountBusinessPartnerDateRanges AS
(
                SELECT
                contractAccountNumber, 
                _recordStart, 
                _recordEnd,
                COALESCE( LEAD( _recordStart, 1 ) OVER( PARTITION BY contractAccountNumber ORDER BY _recordStart ), 
                  CASE WHEN _recordEnd < cast('9999-12-31T23:59:59' as timestamp) then _recordEnd + INTERVAL 1 SECOND else _recordEnd end) AS _newRecordEnd
                FROM curated_v2.dimAccountBusinessPartner
                --WHERE _recordDeleted = 0
),

dateDriver AS
(
                SELECT contractAccountNumber, _recordStart from ContractAccountDateRanges
                UNION
                SELECT contractAccountNumber, _newRecordEnd as _recordStart from ContractAccountDateRanges
                UNION
                SELECT contractAccountNumber, _recordStart from AccountBusinessPartnerDateRanges
                UNION
                SELECT contractAccountNumber, _newRecordEnd as _recordStart from AccountBusinessPartnerDateRanges
),

effectiveDateRanges AS
(
                SELECT 
                contractAccountNumber, 
                _recordStart AS _effectiveFrom,
                COALESCE( LEAD( _recordStart, 1 ) OVER( PARTITION BY contractAccountNumber ORDER BY _recordStart ) - INTERVAL 1 SECOND, 
                  cast( '9999-12-31T23:59:59' as timestamp ) ) AS _effectiveTo                           
                FROM dateDriver where _recordStart < cast('9999-12-31T23:59:59' as timestamp)                            
)              
     
     
SELECT * FROM 
(
SELECT
      dimContractAccount.contractAccountSK
     ,dimAccountBusinessPartner.accountBusinessPartnerSK
     ,coalesce(dimContractAccount.sourceSystemCode, dimAccountBusinessPartner.sourceSystemCode) as sourceSystemCode
     ,coalesce(dimContractAccount.contractAccountNumber, dimAccountBusinessPartner.contractAccountNumber, -1) as contractAccountNumber     
     ,dimContractAccount.legacyContractAccountNumber
     ,dimContractAccount.applicationAreaCode
     ,dimContractAccount.applicationArea
     ,dimContractAccount.contractAccountCategoryCode
     ,dimContractAccount.contractAccountCategory
     ,dimContractAccount.createdBy as contractAccountCreatedBy
     ,dimContractAccount.createdDate as contractAccountCreatedDate
     ,dimContractAccount.lastChangedBy as contractAccountLastChangedBy
     ,dimContractAccount.lastChangedDate as contractAccountLastChangedDate
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
     ,dimAccountBusinessPartner.lastChangedBy as accountBpChangedBy
     ,dimAccountBusinessPartner.lastChangedDate as accountBplastChangedDate
     ,effectiveDateRanges._effectiveFrom
     ,effectiveDateRanges._effectiveTo
     ,CASE
      WHEN CURRENT_TIMESTAMP() BETWEEN effectiveDateRanges._effectiveFrom AND effectiveDateRanges._effectiveTo then 'Y'
      ELSE 'N'
      END AS currentFlag
      ,if(dimContractAccount._RecordDeleted = 0,'Y','N') AS currentRecordFlag
FROM effectiveDateRanges as effectiveDateRanges
LEFT OUTER JOIN curated_v2.dimContractAccount
  ON effectiveDateRanges.contractAccountNumber = dimContractAccount.contractAccountNumber
        AND effectiveDateRanges._effectiveFrom <= dimContractAccount._RecordEnd
        AND effectiveDateRanges._effectiveTo >= dimContractAccount._RecordStart 
        --AND dimContractAccount._recordDeleted = 0
LEFT OUTER JOIN curated_v2.dimAccountBusinessPartner
    ON effectiveDateRanges.contractAccountNumber = dimAccountBusinessPartner.contractAccountNumber
      AND effectiveDateRanges._effectiveFrom <= dimAccountBusinessPartner._RecordEnd
      AND effectiveDateRanges._effectiveTo >= dimAccountBusinessPartner._RecordStart
      --AND dimAccountBusinessPartner._recordDeleted = 0
)
ORDER BY _effectiveFrom

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.notebook.exit("1")
