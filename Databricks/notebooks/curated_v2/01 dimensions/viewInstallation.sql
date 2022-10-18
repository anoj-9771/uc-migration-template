-- Databricks notebook source
-- MAGIC %md
-- MAGIC # viewInstallation

-- COMMAND ----------

-- View: viewInstallation
-- Description: viewInstallation

create or replace view curated_v2.viewInstallation
as

with dateDriver as
(
	select distinct installationNumber, to_date(_recordStart) as _effectiveFrom from curated_v2.dimInstallation where isnotnull(installationNumber) AND _recordDeleted <> 1
	union
	select distinct installationNumber,to_date(_recordStart) as _effectiveFrom from curated_v2.dimInstallationHistory  where isnotnull(installationNumber) AND _recordDeleted <> 1
    union
    select distinct installationNumber, to_date(_recordStart) as _effectiveFrom from curated_v2.dimDisconnectionDocument where isnotnull(installationNumber) AND _recordDeleted <> 1
),
effectiveDateranges as 
(
	select 
		installationNumber, 
		_effectiveFrom, 
		coalesce(timestamp(date_add(lead(_effectiveFrom,1) over(partition by installationNumber order by _effectiveFrom), -1)), '9999-12-31 00:00:00') as _effectiveTo
	from dateDriver 
)

    SELECT
        /* Installation */
        dimInstallation.installationSK,
        dimInstallation.sourceSystemCode,
        dimInstallation.installationNumber,
        dimInstallation.divisionCode,
        dimInstallation.division,
        dimInstallation.propertyNumber,
        dimInstallation.Premise,
        dimInstallation.meterReadingBlockedReason,
        dimInstallation.basePeriodCategory,
        dimInstallation.installationType,
        dimInstallation.meterReadingControlCode,
        dimInstallation.meterReadingControl,
        dimInstallation.reference,
        dimInstallation.authorizationGroupCode,
        dimInstallation.guaranteedSupplyReason,
        dimInstallation.serviceTypeCode,
        dimInstallation.serviceType,
        dimInstallation.deregulationStatus,
        dimInstallation.createdDate,
        dimInstallation.createdBy,
        dimInstallation.changedDate,
        dimInstallation.changedBy,
        /* Installation History */
        dimInstallationHistory.installationHistorySK,
        dimInstallationHistory.validFromDate,
        dimInstallationHistory.validToDate,
        dimInstallationHistory.rateCategoryCode,
        dimInstallationHistory.rateCategory,
        dimInstallationHistory.portionNumber,
        dimInstallationHistory.portionText,
        dimInstallationHistory.industrySystemCode,
        dimInstallationHistory.Industry System,
        dimInstallationHistory.industryCode,
        dimInstallationHistory.industry,
        dimInstallationHistory.billingClassCode,
        dimInstallationHistory.billingClass,
        dimInstallationHistory.meterReadingUnit,
        /* Disconnection Document */
        dimDisconnectionDocument.disconnectionDocumentSK,
        dimDisconnectionDocument.disconnectionDocumentNumber,
        dimDisconnectionDocument.disconnectionActivityPeriod,
        dimDisconnectionDocument.disconnectionObjectNumber,
        dimDisconnectionDocument.disconnectionDate,
        dimDisconnectionDocument.disconnectionActivityTypeCode,
        dimDisconnectionDocument.disconnectionActivityType,
        dimDisconnectionDocument.disconnectionObjectTypeCode,
        dimDisconnectionDocument.disconnectionReasonCode,
        dimDisconnectionDocument.disconnectionReason,
        dimDisconnectionDocument.processingVariantCode,
        dimDisconnectionDocument.processingVariant,
        dimDisconnectionDocument.disconnectionReconnectionStatusCode,
        dimDisconnectionDocument.disconnectionReconnectionStatus,
        dimDisconnectionDocument.disconnectionDocumentStatusCode,
        dimDisconnectionDocument.disconnectionDocumentStatus,
        effectiveDateRanges._effectiveFrom,
        effectiveDateRanges._effectiveTo,
        case
          when current_date() between effectiveDateRanges._effectiveFrom and effectiveDateRanges._effectiveTo then 'Y'
          else 'N'
        end as currentIndicator 
    FROM effectiveDateRanges
    LEFT OUTER JOIN curated_v2.dimInstallation dimInstallation
        ON dimInstallation.installationNumber = effectiveDateRanges.installationNumber
        AND dimInstallation._recordEnd >= effectiveDateRanges._effectiveFrom 
        AND dimInstallation._recordStart <= effectiveDateRanges._effectiveTo
    LEFT OUTER JOIN curated_v2.dimInstallationHistory dimInstallationHistory 
        ON dimInstallationHistory.installationNumber = effectiveDateRanges.installationNumber 
        AND dimInstallationHistory.validToDate >= effectiveDateRanges._effectiveFrom 
        AND dimInstallationHistory.validFromDate <= effectiveDateRanges._effectiveTo
    LEFT OUTER JOIN curated_v2.dimDisconnectionDocument dimDisconnectionDocument 
        ON dimDisconnectionDocument.installationNumber = effectiveDateRanges.installationNumber 
        AND dimDisconnectionDocument.referenceObjectTypeCode = 'INSTLN'
        AND dimDisconnectionDocument.validToDate >= effectiveDateRanges._effectiveFrom 
        AND dimDisconnectionDocument.validFromDate <= effectiveDateRanges._effectiveTo

