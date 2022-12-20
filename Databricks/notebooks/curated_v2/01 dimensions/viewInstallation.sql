-- Databricks notebook source
-- MAGIC %md
-- MAGIC # viewInstallation

-- COMMAND ----------

-- View: viewInstallation
-- Description: viewInstallation

create or replace view curated_v2.viewInstallation
as

with dateDriverNonDelete as
(
	select distinct installationNumber, _recordStart as _effectiveFrom from curated_v2.dimInstallation where isnotnull(installationNumber) and _RecordDeleted = 0  
	union
	select distinct installationNumber, _recordStart as _effectiveFrom from curated_v2.dimInstallationHistory  where isnotnull(installationNumber) and _RecordDeleted = 0  
    union
    select distinct installationNumber, _recordStart as _effectiveFrom from curated_v2.dimDisconnectionDocument where isnotnull(installationNumber) and _RecordDeleted = 0 
),
effectiveDaterangesNonDelete as 
(
	select 
		installationNumber, 
		_effectiveFrom, 
		cast(coalesce(timestamp(date_add(lead(_effectiveFrom,1) over(partition by installationNumber order by _effectiveFrom), -1)), '9999-12-31 00:00:00') as timestamp) as _effectiveTo
	from dateDriverNonDelete 
),
dateDriverDelete as
(
	select distinct installationNumber, _recordStart as _effectiveFrom from curated_v2.dimInstallation where isnotnull(installationNumber) and _RecordDeleted = 1  
	union
	select distinct installationNumber, _recordStart as _effectiveFrom from curated_v2.dimInstallationHistory  where isnotnull(installationNumber) and _RecordDeleted = 1 
    union
    select distinct installationNumber, _recordStart as _effectiveFrom from curated_v2.dimDisconnectionDocument where isnotnull(installationNumber) and _RecordDeleted = 1 
),
effectiveDaterangesDelete as 
(
	select 
		installationNumber, 
		_effectiveFrom, 
		cast(coalesce(timestamp(date_add(lead(_effectiveFrom,1) over(partition by installationNumber order by _effectiveFrom), -1)), '9999-12-31 00:00:00') as timestamp) as _effectiveTo
	from dateDriverDelete 
)
SELECT * FROM
(
    SELECT
        /* Installation */
        dimInstallation.installationSK                                 AS installationSK,
        dimInstallationHistory.installationHistorySK                   AS installationHistorySK,
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
        dimDisconnectionDocument.disconnectionDocumentSK               AS disconnectionDocumentSK,
        dimDisconnectionDocument.disconnectionDocumentNumber           AS disconnectionDocumentNumber,
        dimDisconnectionDocument.disconnectionActivityPeriod           AS disconnectionActivityPeriod,
        dimDisconnectionDocument.disconnectionObjectNumber             AS disconnectionObjectNumber,
        dimDisconnectionDocument.disconnectionDate                     AS disconnectionDate,
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
        dimInstallation._recordDeleted as _dimInstallationRecordDeleted,
        dimInstallationHistory._recordDeleted as _dimInstallationHistoryRecordDeleted,
        dimDisconnectionDocument._recordDeleted as _dimDisconnectionDocumentRecordDeleted,
        dimInstallation._recordCurrent as _dimInstallationRecordCurrent,
        dimInstallationHistory._recordCurrent as _dimInstallationHistoryRecordCurrent,
        dimDisconnectionDocument._recordCurrent as _dimDisconnectionDocumentRecordCurrent
       ,CASE
        WHEN CURRENT_TIMESTAMP() BETWEEN effectiveDateRanges._effectiveFrom AND effectiveDateRanges._effectiveTo then 'Y'
        ELSE 'N'
        END AS currentRecordFlag
    FROM effectiveDaterangesNonDelete as effectiveDateRanges
    LEFT OUTER JOIN curated_v2.dimInstallation dimInstallation
        ON dimInstallation.installationNumber = effectiveDateRanges.installationNumber
        AND dimInstallation._recordEnd >= effectiveDateRanges._effectiveFrom 
        AND dimInstallation._recordStart <= effectiveDateRanges._effectiveTo
        AND dimInstallation._recordDeleted = 0
    LEFT OUTER JOIN curated_v2.dimInstallationHistory dimInstallationHistory 
        ON dimInstallationHistory.installationNumber = effectiveDateRanges.installationNumber 
        AND dimInstallationHistory._recordEnd >= effectiveDateRanges._effectiveFrom 
        AND dimInstallationHistory._recordStart <= effectiveDateRanges._effectiveTo
        AND dimInstallationHistory._recordDeleted = 0
    LEFT OUTER JOIN curated_v2.dimDisconnectionDocument dimDisconnectionDocument 
        ON dimDisconnectionDocument.installationNumber = effectiveDateRanges.installationNumber 
        AND dimDisconnectionDocument.referenceObjectTypeCode = 'INSTLN'
        AND dimDisconnectionDocument.validToDate >= effectiveDateRanges._effectiveFrom 
        AND dimDisconnectionDocument.validFromDate <= effectiveDateRanges._effectiveTo
        AND dimDisconnectionDocument._recordDeleted = 0
    UNION
    SELECT
        /* Installation */
        dimInstallation.installationSK                                 AS installationSK,
        dimInstallationHistory.installationHistorySK                   AS installationHistorySK,
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
        dimDisconnectionDocument.disconnectionDocumentSK               AS disconnectionDocumentSK,
        dimDisconnectionDocument.disconnectionDocumentNumber           AS disconnectionDocumentNumber,
        dimDisconnectionDocument.disconnectionActivityPeriod           AS disconnectionActivityPeriod,
        dimDisconnectionDocument.disconnectionObjectNumber             AS disconnectionObjectNumber,
        dimDisconnectionDocument.disconnectionDate                     AS disconnectionDate,
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
        dimInstallation._recordDeleted as _dimInstallationRecordDeleted,
        dimInstallationHistory._recordDeleted as _dimInstallationHistoryRecordDeleted,
        dimDisconnectionDocument._recordDeleted as _dimDisconnectionDocumentRecordDeleted,
        dimInstallation._recordCurrent as _dimInstallationRecordCurrent,
        dimInstallationHistory._recordCurrent as _dimInstallationHistoryRecordCurrent,
        dimDisconnectionDocument._recordCurrent as _dimDisconnectionDocumentRecordCurrent
       ,CASE
        WHEN CURRENT_TIMESTAMP() BETWEEN effectiveDateRanges._effectiveFrom AND effectiveDateRanges._effectiveTo then 'Y'
        ELSE 'N'
        END AS currentRecordFlag
    FROM effectiveDaterangesDelete as effectiveDateRanges
    LEFT OUTER JOIN curated_v2.dimInstallation dimInstallation
        ON dimInstallation.installationNumber = effectiveDateRanges.installationNumber
        AND dimInstallation._recordEnd >= effectiveDateRanges._effectiveFrom 
        AND dimInstallation._recordStart <= effectiveDateRanges._effectiveTo
        AND dimInstallation._recordDeleted = 1
    LEFT OUTER JOIN curated_v2.dimInstallationHistory dimInstallationHistory 
        ON dimInstallationHistory.installationNumber = effectiveDateRanges.installationNumber 
        AND dimInstallationHistory._recordEnd >= effectiveDateRanges._effectiveFrom 
        AND dimInstallationHistory._recordStart <= effectiveDateRanges._effectiveTo
        AND dimInstallationHistory._recordDeleted = 1
    LEFT OUTER JOIN curated_v2.dimDisconnectionDocument dimDisconnectionDocument 
        ON dimDisconnectionDocument.installationNumber = effectiveDateRanges.installationNumber 
        AND dimDisconnectionDocument.referenceObjectTypeCode = 'INSTLN'
        AND dimDisconnectionDocument.validToDate >= effectiveDateRanges._effectiveFrom 
        AND dimDisconnectionDocument.validFromDate <= effectiveDateRanges._effectiveTo
        AND dimDisconnectionDocument._recordDeleted = 1
)
ORDER BY _effectiveFrom
