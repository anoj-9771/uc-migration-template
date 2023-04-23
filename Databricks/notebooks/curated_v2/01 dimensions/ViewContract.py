# Databricks notebook source
notebookPath = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().split("/")
view = notebookPath[-1:][0]
db = notebookPath[-3:][0]

spark.sql("""
-- View: viewContract
-- Description: viewContract
CREATE OR REPLACE VIEW curated_v2.viewContract AS

With ContractDateRanges AS
(
                SELECT
                contractId, 
                _recordStart, 
                _recordEnd,
                COALESCE( LEAD( _recordStart, 1 ) OVER( PARTITION BY contractId ORDER BY _recordStart ), 
                  CASE WHEN _recordEnd < cast('9999-12-31T23:59:59' as timestamp) then _recordEnd + INTERVAL 1 SECOND else _recordEnd end) AS _newRecordEnd
                FROM curated_v2.dimContract
                --WHERE _recordDeleted = 0
),

ContractHistoryDateRanges AS
(
                SELECT
                contractId, 
                _recordStart, 
                _recordEnd,
                COALESCE( LEAD( _recordStart, 1 ) OVER( PARTITION BY contractId ORDER BY _recordStart ), 
                  CASE WHEN _recordEnd < cast('9999-12-31T23:59:59' as timestamp) then _recordEnd + INTERVAL 1 SECOND else _recordEnd end) AS _newRecordEnd
                FROM curated_v2.dimContractHistory
                WHERE _recordDeleted = 0
),

dateDriver AS
(
                SELECT contractId, _recordStart from ContractDateRanges
                UNION
                SELECT contractId, _newRecordEnd as _recordStart from ContractDateRanges
                UNION
                SELECT contractId, _recordStart from ContractHistoryDateRanges
                UNION
                SELECT contractId, _newRecordEnd as _recordStart from ContractHistoryDateRanges
),

effectiveDateRanges AS
(
                SELECT 
                contractId, 
                _recordStart AS _effectiveFrom,
                COALESCE( LEAD( _recordStart, 1 ) OVER( PARTITION BY contractId ORDER BY _recordStart ) - INTERVAL 1 SECOND, 
                  cast( '9999-12-31T23:59:59' as timestamp ) ) AS _effectiveTo                           
                FROM dateDriver where _recordStart < cast('9999-12-31T23:59:59' as timestamp)                            
)              

SELECT * FROM 
(
SELECT
     --dimContract.contractSK
    --,dimContractHistory.contractHistorySK,
    coalesce(dimContract.sourceSystemCode, dimContractHistory.sourceSystemCode) as sourceSystemCode
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
    , CASE
      WHEN  coalesce(dimContractHistory.validToDate, '1900-01-01') = '9999-12-31' and _effectiveTo = '9999-12-31 23:59:59.000' then 'Y'
      ELSE 'N'
      END AS currentFlag
    , if(dimContract._RecordDeleted = 0,'Y','N') AS currentRecordFlag 
FROM effectiveDateRanges as effectiveDateRanges
LEFT OUTER JOIN curated_v2.dimContract
    ON dimContract.contractId = effectiveDateRanges.contractId 
        AND dimContract._recordEnd >= effectiveDateRanges._effectiveFrom 
        AND dimContract._recordStart <= effectiveDateRanges._effectiveTo
        --AND dimContract._recordDeleted = 0
LEFT OUTER JOIN curated_v2.dimContractHistory
    ON dimContractHistory.contractId = effectiveDateRanges.contractId 
        AND dimContractHistory._recordEnd >= effectiveDateRanges._effectiveFrom 
        AND dimContractHistory._recordStart <= effectiveDateRanges._effectiveTo
        AND dimContractHistory._recordDeleted = 0
)
ORDER BY _effectiveFrom
""".replace("CREATE OR REPLACE VIEW", "ALTER VIEW" if spark.sql(f"SHOW VIEWS FROM {db} LIKE '{view}'").count() == 1 else "CREATE OR REPLACE VIEW"))
