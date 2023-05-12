# Databricks notebook source
notebookPath = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().split("/")
view = notebookPath[-1:][0]
db = notebookPath[-3:][0]

spark.sql("""
-- View: viewInstallation
-- Description: viewInstallation

CREATE OR REPLACE VIEW curated.viewInstallation
as

With dimInstallationRanges AS
(
                SELECT
                installationNumber, 
                _recordStart, 
                _recordEnd,
                COALESCE( LEAD( _recordStart, 1 ) OVER( PARTITION BY installationNumber ORDER BY _recordStart ), 
                  CASE WHEN _recordEnd < cast('9999-12-31T23:59:59' as timestamp) then _recordEnd + INTERVAL 1 SECOND else _recordEnd end) AS _newRecordEnd
                FROM curated.dimInstallation
                --WHERE _recordDeleted = 0
),

dimInstallationHistoryRanges AS
(
                SELECT
                installationNumber, 
                _recordStart, 
                _recordEnd,
                COALESCE( LEAD( _recordStart, 1 ) OVER( PARTITION BY installationNumber ORDER BY _recordStart ), 
                  CASE WHEN _recordEnd < cast('9999-12-31T23:59:59' as timestamp) then _recordEnd + INTERVAL 1 SECOND else _recordEnd end) AS _newRecordEnd
                FROM curated.dimInstallationHistory
                WHERE _recordDeleted = 0
),

dateDriver AS
(
                SELECT installationNumber, _recordStart from dimInstallationRanges
                UNION
                SELECT installationNumber, _newRecordEnd as _recordStart from dimInstallationRanges
                UNION
                SELECT installationNumber, _recordStart from dimInstallationHistoryRanges
                UNION
                SELECT installationNumber, _newRecordEnd as _recordStart from dimInstallationHistoryRanges
),

effectiveDateRanges AS
(
                SELECT 
                installationNumber, 
                _recordStart AS _effectiveFrom,
                COALESCE( LEAD( _recordStart, 1 ) OVER( PARTITION BY installationNumber ORDER BY _recordStart ) - INTERVAL 1 SECOND, 
                  cast( '9999-12-31T23:59:59' as timestamp ) ) AS _effectiveTo                           
                FROM dateDriver where _recordStart < cast('9999-12-31T23:59:59' as timestamp)                            
)              

SELECT * FROM
(
    SELECT
        /* Installation */
        --dimInstallation.installationSK                                 AS installationSK,
        --dimInstallationHistory.installationHistorySK                   AS installationHistorySK,
        COALESCE(dimInstallation.sourceSystemCode, dimInstallationHistory.sourceSystemCode, dimDisconnectionDocument.sourceSystemCode) AS sourceSystemCode,
        COALESCE(
          dimInstallation.installationNumber,
          dimInstallationHistory.installationNumber,
          dimDisconnectionDocument.installationNumber
        )                                                              AS installationNumber,
        dimInstallation.divisionCode                                   AS divisionCode,
        dimInstallation.division                                       AS division,
        dimInstallation.propertyNumber                                 AS propertyNumber,
        dimInstallation.Premise                                        AS Premise,
        dimInstallation.meterReadingBlockedReason                      AS meterReadingBlockedReason,
        dimInstallation.basePeriodCategory                             AS basePeriodCategory,
        dimInstallation.installationType                               AS installationType,
        dimInstallation.meterReadingControlCode                        AS meterReadingControlCode,
        dimInstallation.meterReadingControl                            AS meterReadingControl,
        dimInstallation.reference                                      AS reference,
        dimInstallation.authorizationGroupCode                         AS authorizationGroupCode,
        dimInstallation.guaranteedSupplyReason                         AS guaranteedSupplyReason,
        dimInstallation.serviceTypeCode                                AS serviceTypeCode,
        dimInstallation.serviceType                                    AS serviceType,
        dimInstallation.deregulationStatus                             AS deregulationStatus,
        dimInstallation.createdDate                                    AS createdDate,
        dimInstallation.createdBy                                      AS createdBy,
        dimInstallation.lastChangedDate                                AS lastChangedDate,
        dimInstallation.lastChangedBy                                  AS lastChangedBy,
        /* Installation History */
        dimInstallationHistory.validFromDate                           AS validFromDate,
        dimInstallationHistory.validToDate                             AS validToDate,
        dimInstallationHistory.rateCategoryCode                        AS rateCategoryCode,
        dimInstallationHistory.rateCategory                            AS rateCategory,
        dimInstallationHistory.portionNumber                           AS portionNumber,
        dimInstallationHistory.portionText                             AS portionText,
        dimInstallationHistory.industrySystemCode                      AS industrySystemCode,
        dimInstallationHistory.IndustrySystem                          AS IndustrySystem,
        dimInstallationHistory.industryCode                            AS industryCode,
        dimInstallationHistory.industry                                AS industry,
        dimInstallationHistory.billingClassCode                        AS billingClassCode,
        dimInstallationHistory.billingClass                            AS billingClass,
        dimInstallationHistory.meterReadingUnit                        AS meterReadingUnit,
        /* Disconnection Document */
        --dimDisconnectionDocument.disconnectionDocumentSK               AS disconnectionDocumentSK,
        dimDisconnectionDocument.disconnectionDocumentNumber           AS disconnectionDocumentNumber,
        dimDisconnectionDocument.disconnectionActivityPeriod           AS disconnectionActivityPeriod,
        dimDisconnectionDocument.disconnectionObjectNumber             AS disconnectionObjectNumber,
        dimDisconnectionDocument.disconnectionDate                     AS disconnectionDate,
        dimDisconnectionDocument.validFromDate                         AS disconnectionValidFromDate,
        dimDisconnectionDocument.validToDate                           AS disconnectionValidToDate,        
        dimDisconnectionDocument.disconnectionActivityTypeCode         AS disconnectionActivityTypeCode,
        dimDisconnectionDocument.disconnectionActivityType             AS disconnectionActivityType,
        dimDisconnectionDocument.disconnectionObjectTypeCode           AS disconnectionObjectTypeCode,
        dimDisconnectionDocument.disconnectionReasonCode               AS disconnectionReasonCode,
        dimDisconnectionDocument.disconnectionReason                   AS disconnectionReason,
        dimDisconnectionDocument.processingVariantCode                 AS processingVariantCode,
        dimDisconnectionDocument.processingVariant                     AS processingVariant,
        dimDisconnectionDocument.disconnectionReconnectionStatusCode   AS disconnectionReconnectionStatusCode,
        dimDisconnectionDocument.disconnectionReconnectionStatus       AS disconnectionReconnectionStatus,
        dimDisconnectionDocument.disconnectionDocumentStatusCode       AS disconnectionDocumentStatusCode,
        dimDisconnectionDocument.disconnectionDocumentStatus           AS disconnectionDocumentStatus,
        effectiveDateRanges._effectiveFrom                             AS _effectiveFrom,
        effectiveDateRanges._effectiveTo                               AS _effectiveTo,
        CASE
        WHEN CURRENT_TIMESTAMP() BETWEEN effectiveDateRanges._effectiveFrom AND effectiveDateRanges._effectiveTo then 'Y'
        ELSE 'N'
        END AS currentFlag,
        if(dimInstallation._RecordDeleted = 0,'Y','N') AS currentRecordFlag 
    FROM effectiveDateRanges as effectiveDateRanges
    LEFT OUTER JOIN curated.dimInstallation dimInstallation
        ON dimInstallation.installationNumber = effectiveDateRanges.installationNumber
        AND dimInstallation._recordEnd >= effectiveDateRanges._effectiveFrom 
        AND dimInstallation._recordStart <= effectiveDateRanges._effectiveTo
        --AND dimInstallation._recordDeleted = 0
    LEFT OUTER JOIN curated.dimInstallationHistory dimInstallationHistory 
        ON dimInstallationHistory.installationNumber = effectiveDateRanges.installationNumber 
        AND dimInstallationHistory._recordEnd >= effectiveDateRanges._effectiveFrom 
        AND dimInstallationHistory._recordStart <= effectiveDateRanges._effectiveTo
        AND dimInstallationHistory._recordDeleted = 0
    LEFT OUTER JOIN curated.dimDisconnectionDocument dimDisconnectionDocument 
        ON dimDisconnectionDocument.installationNumber = effectiveDateRanges.installationNumber 
        AND dimDisconnectionDocument.referenceObjectTypeCode = 'INSTLN'
        AND dimDisconnectionDocument.disconnectionActivityTypeCode = '02'
        AND dimDisconnectionDocument.validtodate = '9999-12-31' 
        AND dimDisconnectionDocument._recordDeleted = 0 
        AND dimInstallationHistory.validtodate = '9999-12-31' 
   )
ORDER BY _effectiveFrom
""".replace("CREATE OR REPLACE VIEW", "ALTER VIEW" if spark.sql(f"SHOW VIEWS FROM {db} LIKE '{view}'").count() == 1 else "CREATE OR REPLACE VIEW"))
