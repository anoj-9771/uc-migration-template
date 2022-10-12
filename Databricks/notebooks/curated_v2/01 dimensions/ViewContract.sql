-- Databricks notebook source
-- View: view_contract
-- Description: view_contract
CREATE OR REPLACE VIEW curated_v2.view_contract AS
WITH dateDriver AS
(
    SELECT DISTINCT contractId, _recordStart AS _effectiveFrom FROM curated_v2.dimContract
    WHERE _recordDeleted <> 1
    union
    SELECT DISTINCT contractId, _recordStart AS _effectiveFrom FROM curated_v2.dimContractHistory
    WHERE _recordDeleted <> 1
),
effectiveDateranges AS 
(
    SELECT contractId, _effectiveFrom, COALESCE(TIMESTAMP(DATE_ADD(LEAD(_effectiveFrom,1) OVER(PARTITION BY contractId ORDER BY _effectiveFrom), -1)), '9999-12-31') AS _effectiveTo
    FROM dateDriver
)
SELECT
     effectiveDateRanges._effectiveFrom
    ,effectiveDateRanges._effectiveTo
    ,effectiveDateRanges.contractId
    ,contractSK
    ,dimContract.sourceSystemCode
--     ,dimContract.contractId
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
    ,dimContractHistory.contractHistorySK
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
    ,CASE 
        WHEN (dimContractHistory.validFromDate <= CURRENT_DATE() AND dimContractHistory.validToDate >= CURRENT_DATE())
           THEN 'Y' 
           ELSE 'N' END AS currentIndicator
FROM effectiveDateRanges
LEFT OUTER JOIN curated_v2.dimContract
    ON dimContract.contractId = effectiveDateRanges.contractId 
        AND dimContract._recordEnd >= effectiveDateRanges._effectiveFrom 
        AND dimContract._recordStart <= effectiveDateRanges._effectiveTo
LEFT OUTER JOIN curated_v2.dimContractHistory
    ON dimContractHistory.contractId = effectiveDateRanges.contractId 
        AND dimContractHistory._recordEnd >= effectiveDateRanges._effectiveFrom 
        AND dimContractHistory._recordStart <= effectiveDateRanges._effectiveTo
ORDER BY effectiveDateRanges._effectiveFrom

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.notebook.exit("1")
