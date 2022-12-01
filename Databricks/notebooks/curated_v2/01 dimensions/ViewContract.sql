-- Databricks notebook source
-- View: viewContract
-- Description: viewContract
CREATE OR REPLACE VIEW curated_v2.viewContract AS
WITH dateDriverNonDelete AS
(
    SELECT DISTINCT contractId, _recordStart AS _effectiveFrom FROM curated_v2.dimContract
    WHERE _recordDeleted = 0
    union
    SELECT DISTINCT contractId, _recordStart AS _effectiveFrom FROM curated_v2.dimContractHistory
    WHERE _recordDeleted = 0
),
effectiveDaterangesNonDelete AS 
(
    SELECT contractId, _effectiveFrom, COALESCE(TIMESTAMP(DATE_ADD(LEAD(_effectiveFrom,1) OVER(PARTITION BY contractId ORDER BY _effectiveFrom), -1)), TIMESTAMP('9999-12-31')) AS _effectiveTo
    FROM dateDriverNonDelete
),
dateDriverDelete AS
(
    SELECT DISTINCT contractId, _recordStart AS _effectiveFrom FROM curated_v2.dimContract
    WHERE _recordDeleted = 1
    union
    SELECT DISTINCT contractId, _recordStart AS _effectiveFrom FROM curated_v2.dimContractHistory
    WHERE _recordDeleted = 1
),
effectiveDaterangesDelete AS 
(
    SELECT contractId, _effectiveFrom, COALESCE(TIMESTAMP(DATE_ADD(LEAD(_effectiveFrom,1) OVER(PARTITION BY contractId ORDER BY _effectiveFrom), -1)), TIMESTAMP('9999-12-31')) AS _effectiveTo
    FROM dateDriverDelete
)
SELECT * FROM 
(
SELECT
     dimContract.contractSK
    ,dimContractHistory.contractHistorySK
    ,dimContract.sourceSystemCode
    ,coalesce(dimContract.contractId, dimContractHistory.contractId, -1) as contractId    
    ,dimContract.companyCode
    ,dimContract.companyName
    ,dimContract.divisionCode
    ,dimContract.division
    ,dimContract.installationNumber
    ,dimContract.contractAccountNumber
    ,dimContract.accountDeterminationCode
    ,dimContract.accountDetermination
    ,dimContract.allowableBudgetBillingCyclesCode
    ,dimContract.allowableBudgetBillingCycles
    ,dimContract.invoiceContractsJointlyCode
    ,dimContract.invoiceContractsJointly
    ,dimContract.manualBillContractflag
    ,dimContract.billBlockingReasonCode
    ,dimContract.billBlockingReason
    ,dimContract.specialMoveOutCaseCode
    ,dimContract.specialMoveOutCase
    ,dimContract.contractText
    ,dimContract.legacyMoveInDate
    ,dimContract.numberOfCancellations
    ,dimContract.numberOfRenewals
    ,dimContract.personnelNumber
    ,dimContract.contractNumberLegacy
    ,dimContract.isContractInvoicedFlag
    ,dimContract.isContractTransferredFlag
    ,dimContract.outsortingCheckGroupForBilling
    ,dimContract.manualOutsortingCount
    ,dimContract.serviceProvider
    ,dimContract.contractTerminatedForBillingFlag
    ,dimContract.invoicingParty
    ,dimContract.cancellationReasonCRM
    ,dimContract.moveInDate
    ,dimContract.moveOutDate
    ,dimContract.budgetBillingStopDate
    ,dimContract.premise
    ,dimContract.propertyNumber
    ,dimContract.validFromDate
    ,dimContract.agreementNumber
    ,dimContract.addressNumber
    ,dimContract.alternativeAddressNumber
    ,dimContract.identificationNumber
    ,dimContract.objectReferenceIndicator
    ,dimContract.objectNumber
    ,dimContract.createdDate
    ,dimContract.createdBy
    ,dimContract.lastChangedDate
    ,dimContract.lastChangedBy
    ,dimContractHistory.validFromDate as contractHistoryValidFromDate
    ,dimContractHistory.validToDate as contractHistoryValidToDate
    ,dimContractHistory.CRMProduct
    ,dimContractHistory.CRMObjectId
    ,dimContractHistory.CRMDocumentItemNumber
    ,dimContractHistory.marketingCampaign
    ,dimContractHistory.individualContractId
    ,dimContractHistory.productBeginFlag
    ,dimContractHistory.productChangeFlag
    ,dimContractHistory.replicationControlsCode
    ,dimContractHistory.replicationControls
    ,dimContractHistory.podUUID
    ,dimContractHistory.headerTypeCode
    ,dimContractHistory.headerType
    ,dimContractHistory.isCancelledFlag
    ,effectiveDateRanges._effectiveFrom
    ,effectiveDateRanges._effectiveTo
    ,dimContract._recordDeleted as _dimContractRecordDeleted
    ,dimContractHistory._recordDeleted as _dimContractHistoryRecordDeleted
    ,dimContract._recordCurrent as _dimContractRecordCurrent
    ,dimContractHistory._recordCurrent as _dimContractHistoryRecordCurrent
    ,dimContract._recordStart as _dimContractRecordStart
    ,dimContract._recordEnd as _dimContractRecordEnd
    ,dimContractHistory._recordStart as _dimContractHistoryRecordStart
    ,dimContractHistory._recordEnd as _dimContractHistoryRecordEnd
    , CASE
      WHEN CURRENT_DATE() BETWEEN effectiveDateRanges._effectiveFrom AND effectiveDateRanges._effectiveTo then 'Y'
      ELSE 'N'
      END AS currentRecordFlag
FROM effectiveDaterangesNonDelete as effectiveDateRanges
LEFT OUTER JOIN curated_v2.dimContract
    ON dimContract.contractId = effectiveDateRanges.contractId 
        AND dimContract._recordEnd >= effectiveDateRanges._effectiveFrom 
        AND dimContract._recordStart <= effectiveDateRanges._effectiveTo
        AND dimContract._recordDeleted = 0
LEFT OUTER JOIN curated_v2.dimContractHistory
    ON dimContractHistory.contractId = effectiveDateRanges.contractId 
        AND dimContractHistory._recordEnd >= effectiveDateRanges._effectiveFrom 
        AND dimContractHistory._recordStart <= effectiveDateRanges._effectiveTo
        AND dimContractHistory._recordDeleted = 0
UNION
SELECT
     dimContract.contractSK
    ,dimContractHistory.contractHistorySK
    ,dimContract.sourceSystemCode
    ,coalesce(dimContract.contractId, dimContractHistory.contractId, -1) as contractId   
    ,dimContract.companyCode
    ,dimContract.companyName
    ,dimContract.divisionCode
    ,dimContract.division
    ,dimContract.installationNumber
    ,dimContract.contractAccountNumber
    ,dimContract.accountDeterminationCode
    ,dimContract.accountDetermination
    ,dimContract.allowableBudgetBillingCyclesCode
    ,dimContract.allowableBudgetBillingCycles
    ,dimContract.invoiceContractsJointlyCode
    ,dimContract.invoiceContractsJointly
    ,dimContract.manualBillContractflag
    ,dimContract.billBlockingReasonCode
    ,dimContract.billBlockingReason
    ,dimContract.specialMoveOutCaseCode
    ,dimContract.specialMoveOutCase
    ,dimContract.contractText
    ,dimContract.legacyMoveInDate
    ,dimContract.numberOfCancellations
    ,dimContract.numberOfRenewals
    ,dimContract.personnelNumber
    ,dimContract.contractNumberLegacy
    ,dimContract.isContractInvoicedFlag
    ,dimContract.isContractTransferredFlag
    ,dimContract.outsortingCheckGroupForBilling
    ,dimContract.manualOutsortingCount
    ,dimContract.serviceProvider
    ,dimContract.contractTerminatedForBillingFlag
    ,dimContract.invoicingParty
    ,dimContract.cancellationReasonCRM
    ,dimContract.moveInDate
    ,dimContract.moveOutDate
    ,dimContract.budgetBillingStopDate
    ,dimContract.premise
    ,dimContract.propertyNumber
    ,dimContract.validFromDate
    ,dimContract.agreementNumber
    ,dimContract.addressNumber
    ,dimContract.alternativeAddressNumber
    ,dimContract.identificationNumber
    ,dimContract.objectReferenceIndicator
    ,dimContract.objectNumber
    ,dimContract.createdDate
    ,dimContract.createdBy
    ,dimContract.lastChangedDate
    ,dimContract.lastChangedBy
    ,dimContractHistory.validFromDate as contractHistoryValidFromDate
    ,dimContractHistory.validToDate as contractHistoryValidToDate
    ,dimContractHistory.CRMProduct
    ,dimContractHistory.CRMObjectId
    ,dimContractHistory.CRMDocumentItemNumber
    ,dimContractHistory.marketingCampaign
    ,dimContractHistory.individualContractId
    ,dimContractHistory.productBeginFlag
    ,dimContractHistory.productChangeFlag
    ,dimContractHistory.replicationControlsCode
    ,dimContractHistory.replicationControls
    ,dimContractHistory.podUUID
    ,dimContractHistory.headerTypeCode
    ,dimContractHistory.headerType
    ,dimContractHistory.isCancelledFlag
    ,effectiveDateRanges._effectiveFrom
    ,effectiveDateRanges._effectiveTo
    ,dimContract._recordDeleted as _dimContractRecordDeleted
    ,dimContractHistory._recordDeleted as _dimContractHistoryRecordDeleted
    ,dimContract._recordCurrent as _dimContractRecordCurrent
    ,dimContractHistory._recordCurrent as _dimContractHistoryRecordCurrent
    ,dimContract._recordStart as _dimContractRecordStart
    ,dimContract._recordEnd as _dimContractRecordEnd
    ,dimContractHistory._recordStart as _dimContractHistoryRecordStart
    ,dimContractHistory._recordEnd as _dimContractHistoryRecordEnd
    , CASE
      WHEN CURRENT_DATE() BETWEEN effectiveDateRanges._effectiveFrom AND effectiveDateRanges._effectiveTo then 'Y'
      ELSE 'N'
      END AS currentRecordFlag
FROM effectiveDaterangesDelete as effectiveDateRanges
LEFT OUTER JOIN curated_v2.dimContract
    ON dimContract.contractId = effectiveDateRanges.contractId 
        AND dimContract._recordEnd >= effectiveDateRanges._effectiveFrom 
        AND dimContract._recordStart <= effectiveDateRanges._effectiveTo
        AND dimContract._recordDeleted = 1
LEFT OUTER JOIN curated_v2.dimContractHistory
    ON dimContractHistory.contractId = effectiveDateRanges.contractId 
        AND dimContractHistory._recordEnd >= effectiveDateRanges._effectiveFrom 
        AND dimContractHistory._recordStart <= effectiveDateRanges._effectiveTo
        AND dimContractHistory._recordDeleted = 1
)
ORDER BY _effectiveFrom

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.notebook.exit("1")
