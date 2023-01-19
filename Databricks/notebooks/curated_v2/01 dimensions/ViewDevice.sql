-- Databricks notebook source
-- View: viewDevice
-- Description: viewDevice
CREATE OR REPLACE VIEW curated_v2.viewDevice AS
WITH dateDriverNonDelete AS
(
    SELECT DISTINCT deviceNumber, _recordStart AS _effectiveFrom FROM curated_v2.dimDevice
    WHERE _recordDeleted = 0
    union
    SELECT DISTINCT deviceNumber, _recordStart AS _effectiveFrom FROM curated_v2.dimDeviceHistory
    WHERE _recordDeleted = 0
),
effectiveDaterangesNonDelete AS 
(
    SELECT deviceNumber, _effectiveFrom, cast(COALESCE(TIMESTAMP((LEAD(_effectiveFrom,1) OVER(PARTITION BY deviceNumber ORDER BY _effectiveFrom)) - INTERVAL 1 seconds), '9999-12-31') as timestamp) AS _effectiveTo
    FROM dateDriverNonDelete
),
dateDriverDelete AS
(
    SELECT DISTINCT deviceNumber, _recordStart AS _effectiveFrom FROM curated_v2.dimDevice
    WHERE _recordDeleted = 1
    union
    SELECT DISTINCT deviceNumber, _recordStart AS _effectiveFrom FROM curated_v2.dimDeviceHistory
    WHERE _recordDeleted = 1
),
effectiveDaterangesDelete AS 
(
    SELECT deviceNumber, _effectiveFrom, cast(COALESCE(TIMESTAMP((LEAD(_effectiveFrom,1) OVER(PARTITION BY deviceNumber ORDER BY _effectiveFrom)) - INTERVAL 1 seconds), '9999-12-31') as timestamp) AS _effectiveTo
    FROM dateDriverDelete
)
SELECT * FROM 
(
SELECT
     dimdevice.deviceSK
    ,dimdevicehistory.deviceHistorySK
    ,dimdevice.sourceSystemCode
    ,coalesce(dimdevice.deviceNumber, dimdevicehistory.deviceNumber, -1) as deviceNumber
    ,dimdevice.materialNumber
    ,dimdevice.deviceID
    ,dimdevice.inspectionRelevanceIndicator
    ,dimdevice.deviceSize
    ,dimdevice.assetManufacturerName
    ,dimdevice.manufacturerSerialNumber
    ,dimdevice.manufacturerModelNumber
    ,dimdevice.objectNumber
    ,dimdevice.functionClassCode
    ,dimdevice.functionClass
    ,dimdevice.constructionClassCode
    ,dimdevice.constructionClass
    ,dimdevice.deviceCategoryName
    ,dimdevice.deviceCategoryDescription
    ,dimdevice.ptiNumber
    ,dimdevice.ggwaNumber
    ,dimdevice.certificationRequirementType
    ,dimdevicehistory.validToDate AS deviceHistoryValidToDate
    ,dimdevicehistory.validFromDate AS deviceHistoryValidFromDate
    ,dimdevicehistory.logicalDeviceNumber
    ,dimdevicehistory.deviceLocation
    ,dimdevicehistory.deviceCategoryCombination
    ,dimdevicehistory.registerGroupCode
    ,dimdevicehistory.registerGroup
    ,dimdevicehistory.installationDate
    ,dimdevicehistory.deviceRemovalDate
    ,dimdevicehistory.activityReasonCode
    ,dimdevicehistory.activityReason
    ,dimdevicehistory.firstInstallationDate
    ,dimdevicehistory.lastDeviceRemovalDate
    ,dimdeviceinstallationhistory.deviceInstallationHistorySK
    ,dimdeviceinstallationhistory.installationNumber
    ,dimdeviceinstallationhistory.validToDate AS deviceInstallationHistoryValidToDate
    ,dimdeviceinstallationhistory.validFromDate AS deviceInstallationHistoryValidFromDate
    ,dimdeviceinstallationhistory.priceClassCode
    ,dimdeviceinstallationhistory.priceClass
    ,dimdeviceinstallationhistory.rateTypeCode AS deviceInstallationHistoryRateTypeCode
    ,dimdeviceinstallationhistory.rateType AS deviceInstallationHistoryRateType
    ,dimregisterhistory.registerHistorySK
    ,dimregisterhistory.registerNumber
    ,dimregisterhistory.validToDate AS registerHistoryValidToDate
    ,dimregisterhistory.validFromDate AS registerHistoryvalidFromDate
    ,dimregisterhistory.logicalRegisterNumber
    ,dimregisterhistory.divisionCategoryCode
    ,dimregisterhistory.divisionCategory
    ,dimregisterhistory.registerIdCode
    ,dimregisterhistory.registerId
    ,dimregisterhistory.registerTypeCode
    ,dimregisterhistory.registerType
    ,dimregisterhistory.registerCategoryCode
    ,dimregisterhistory.registerCategory
    ,dimregisterhistory.reactiveApparentOrActiveRegisterCode
    ,dimregisterhistory.reactiveApparentOrActiveRegister
    ,dimregisterhistory.unitOfMeasurementMeterReading
    ,dimregisterhistory.doNotReadFlag	
    ,dimregisterinstallationhistory.registerInstallationHistorySK
    ,dimregisterinstallationhistory.validToDate AS registerInstallationHistoryValidToDate
    ,dimregisterinstallationhistory.validFromDate AS registerInstallationHistoryValidFromDate
    ,dimregisterinstallationhistory.operationCode
    ,dimregisterinstallationhistory.operationDescription
    ,dimregisterinstallationhistory.rateTypeCode AS registerInstallationHistoryRateTypeCode
    ,dimregisterinstallationhistory.rateType AS registerInstallationHistoryRateType
    ,dimregisterinstallationhistory.registerNotRelevantToBilling
    ,dimregisterinstallationhistory.rateFactGroupCode
    ,installAttr.divisionCode
    ,installAttr.division
    ,effectiveDateRanges._effectiveFrom
    ,effectiveDateRanges._effectiveTo
    ,dimdevice._recordDeleted as _dimDeviceRecordDeleted
    ,dimdevicehistory._recordDeleted as _dimDeviceHistoryRecordDeleted
    ,dimdeviceinstallationhistory._recordDeleted as _dimDeviceInstallationHistoryRecordDeleted
    ,dimregisterhistory._recordDeleted as _dimRegisterHistoryRecordDeleted
    ,dimregisterinstallationhistory._recordDeleted as _dimRegisterInstallationHistoryRecordDeleted
    ,dimdevice._recordCurrent as _dimDeviceRecordCurrent
    ,dimdevicehistory._recordCurrent as _dimDeviceHistoryRecordCurrent
    ,dimdeviceinstallationhistory._recordCurrent as _dimDeviceInstallationHistoryRecordCurrent
    ,dimregisterhistory._recordCurrent as _dimRegisterHistoryRecordCurrent
    ,dimregisterinstallationhistory._recordCurrent as _dimRegisterInstallationHistoryRecordCurrent
    ,CASE
      WHEN CURRENT_TIMESTAMP() BETWEEN effectiveDateRanges._effectiveFrom AND effectiveDateRanges._effectiveTo then 'Y'
      ELSE 'N'
      END AS currentRecordFlag
FROM effectiveDaterangesNonDelete as effectiveDateRanges
LEFT OUTER JOIN curated_v2.dimDevice dimdevice
    ON dimdevice.deviceNumber = effectiveDateRanges.deviceNumber 
        AND dimdevice._recordEnd >= effectiveDateRanges._effectiveFrom 
        AND dimdevice._recordStart <= effectiveDateRanges._effectiveTo
        AND dimdevice._recordDeleted = 0
LEFT OUTER JOIN curated_v2.dimDeviceHistory dimdevicehistory
    ON dimdevicehistory.deviceNumber = effectiveDateRanges.deviceNumber 
        AND dimdevicehistory._recordEnd >= effectiveDateRanges._effectiveFrom 
        AND dimdevicehistory._recordStart <= effectiveDateRanges._effectiveTo
        AND dimdevicehistory._recordDeleted = 0
LEFT OUTER JOIN curated_v2.dimDeviceInstallationHistory dimdeviceinstallationhistory
    ON dimdeviceinstallationhistory.logicalDeviceNumber = dimdevicehistory.logicalDeviceNumber
        AND dimdevicehistory._recordStart >= dimdeviceinstallationhistory._recordStart
        AND dimdevicehistory._recordStart <= dimdeviceinstallationhistory._recordEnd
        AND dimdeviceinstallationhistory._recordDeleted = 0
LEFT OUTER JOIN curated_v2.dimRegisterHistory dimregisterhistory
    ON dimregisterhistory.deviceNumber = dimdevice.deviceNumber
      AND dimDeviceHistory._recordStart >= dimregisterhistory._recordStart
      AND dimDeviceHistory._recordStart <= dimregisterhistory._recordEnd
      AND dimregisterhistory._recordDeleted = 0
LEFT OUTER JOIN curated_v2.dimRegisterInstallationHistory dimregisterinstallationhistory
    ON dimregisterinstallationhistory.installationNumber = dimdeviceinstallationhistory.installationNumber
      AND dimregisterhistory.logicalRegisterNumber = dimregisterinstallationhistory.logicalRegisterNumber
      AND dimDeviceHistory._recordStart >= dimregisterinstallationhistory._recordStart
      AND dimDeviceHistory._recordStart <= dimregisterhistory._recordEnd
      AND dimregisterinstallationhistory._recordDeleted = 0
LEFT OUTER JOIN cleansed.isu_0UCINSTALLA_ATTR_2 installAttr
    ON installAttr.installationNumber = dimdeviceinstallationhistory.installationNumber
    AND installAttr._recordDeleted = 0
WHERE dimregisterinstallationhistory.registerNotRelevantToBilling = 'N'
UNION
SELECT
     dimdevice.deviceSK
    ,dimdevicehistory.deviceHistorySK
    ,dimdevice.sourceSystemCode
    ,coalesce(dimdevice.deviceNumber, dimdevicehistory.deviceNumber, -1) as deviceNumber
    ,dimdevice.materialNumber
    ,dimdevice.deviceID
    ,dimdevice.inspectionRelevanceIndicator
    ,dimdevice.deviceSize
    ,dimdevice.assetManufacturerName
    ,dimdevice.manufacturerSerialNumber
    ,dimdevice.manufacturerModelNumber
    ,dimdevice.objectNumber
    ,dimdevice.functionClassCode
    ,dimdevice.functionClass
    ,dimdevice.constructionClassCode
    ,dimdevice.constructionClass
    ,dimdevice.deviceCategoryName
    ,dimdevice.deviceCategoryDescription
    ,dimdevice.ptiNumber
    ,dimdevice.ggwaNumber
    ,dimdevice.certificationRequirementType
    ,dimdevicehistory.validToDate AS deviceHistoryValidToDate
    ,dimdevicehistory.validFromDate AS deviceHistoryValidFromDate
    ,dimdevicehistory.logicalDeviceNumber
    ,dimdevicehistory.deviceLocation
    ,dimdevicehistory.deviceCategoryCombination
    ,dimdevicehistory.registerGroupCode
    ,dimdevicehistory.registerGroup
    ,dimdevicehistory.installationDate
    ,dimdevicehistory.deviceRemovalDate
    ,dimdevicehistory.activityReasonCode
    ,dimdevicehistory.activityReason
    ,dimdevicehistory.firstInstallationDate
    ,dimdevicehistory.lastDeviceRemovalDate
    ,dimdeviceinstallationhistory.deviceInstallationHistorySK
    ,dimdeviceinstallationhistory.installationNumber
    ,dimdeviceinstallationhistory.validToDate AS deviceInstallationHistoryValidToDate
    ,dimdeviceinstallationhistory.validFromDate AS deviceInstallationHistoryValidFromDate
    ,dimdeviceinstallationhistory.priceClassCode
    ,dimdeviceinstallationhistory.priceClass
    ,dimdeviceinstallationhistory.rateTypeCode AS deviceInstallationHistoryRateTypeCode
    ,dimdeviceinstallationhistory.rateType AS deviceInstallationHistoryRateType
    ,dimregisterhistory.registerHistorySK
    ,dimregisterhistory.registerNumber
    ,dimregisterhistory.validToDate AS registerHistoryValidToDate
    ,dimregisterhistory.validFromDate AS registerHistoryvalidFromDate
    ,dimregisterhistory.logicalRegisterNumber
    ,dimregisterhistory.divisionCategoryCode
    ,dimregisterhistory.divisionCategory
    ,dimregisterhistory.registerIdCode
    ,dimregisterhistory.registerId
    ,dimregisterhistory.registerTypeCode
    ,dimregisterhistory.registerType
    ,dimregisterhistory.registerCategoryCode
    ,dimregisterhistory.registerCategory
    ,dimregisterhistory.reactiveApparentOrActiveRegisterCode
    ,dimregisterhistory.reactiveApparentOrActiveRegister
    ,dimregisterhistory.unitOfMeasurementMeterReading
    ,dimregisterhistory.doNotReadFlag	
    ,dimregisterinstallationhistory.registerInstallationHistorySK
    ,dimregisterinstallationhistory.validToDate AS registerInstallationHistoryValidToDate
    ,dimregisterinstallationhistory.validFromDate AS registerInstallationHistoryValidFromDate
    ,dimregisterinstallationhistory.operationCode
    ,dimregisterinstallationhistory.operationDescription
    ,dimregisterinstallationhistory.rateTypeCode AS registerInstallationHistoryRateTypeCode
    ,dimregisterinstallationhistory.rateType AS registerInstallationHistoryRateType
    ,dimregisterinstallationhistory.registerNotRelevantToBilling
    ,dimregisterinstallationhistory.rateFactGroupCode
    ,installAttr.divisionCode
    ,installAttr.division
    ,effectiveDateRanges._effectiveFrom
    ,effectiveDateRanges._effectiveTo
    ,dimdevice._recordDeleted as _dimDeviceRecordDeleted
    ,dimdevicehistory._recordDeleted as _dimDeviceHistoryRecordDeleted
    ,dimdeviceinstallationhistory._recordDeleted as _dimDeviceInstallationHistoryRecordDeleted
    ,dimregisterhistory._recordDeleted as _dimRegisterHistoryRecordDeleted
    ,dimregisterinstallationhistory._recordDeleted as _dimRegisterInstallationHistoryRecordDeleted
    ,dimdevice._recordCurrent as _dimDeviceRecordCurrent
    ,dimdevicehistory._recordCurrent as _dimDeviceHistoryRecordCurrent
    ,dimdeviceinstallationhistory._recordCurrent as _dimDeviceInstallationHistoryRecordCurrent
    ,dimregisterhistory._recordCurrent as _dimRegisterHistoryRecordCurrent
    ,dimregisterinstallationhistory._recordCurrent as _dimRegisterInstallationHistoryRecordCurrent
    ,CASE
      WHEN CURRENT_TIMESTAMP() BETWEEN effectiveDateRanges._effectiveFrom AND effectiveDateRanges._effectiveTo then 'Y'
      ELSE 'N'
      END AS currentRecordFlag
FROM effectiveDaterangesDelete as effectiveDateRanges
LEFT OUTER JOIN curated_v2.dimDevice dimdevice
    ON dimdevice.deviceNumber = effectiveDateRanges.deviceNumber 
        AND dimdevice._recordEnd >= effectiveDateRanges._effectiveFrom 
        AND dimdevice._recordStart <= effectiveDateRanges._effectiveTo
        AND dimdevice._recordDeleted = 1
LEFT OUTER JOIN curated_v2.dimDeviceHistory dimdevicehistory
    ON dimdevicehistory.deviceNumber = effectiveDateRanges.deviceNumber 
        AND dimdevicehistory._recordEnd >= effectiveDateRanges._effectiveFrom 
        AND dimdevicehistory._recordStart <= effectiveDateRanges._effectiveTo
        AND dimdevicehistory._recordDeleted = 1
LEFT OUTER JOIN curated_v2.dimDeviceInstallationHistory dimdeviceinstallationhistory
    ON dimdeviceinstallationhistory.logicalDeviceNumber = dimdevicehistory.logicalDeviceNumber
        AND dimdevicehistory._recordStart >= dimdeviceinstallationhistory._recordStart
        AND dimdevicehistory._recordStart <= dimdeviceinstallationhistory._recordEnd
        AND dimdeviceinstallationhistory._recordDeleted = 1
LEFT OUTER JOIN curated_v2.dimRegisterHistory dimregisterhistory
    ON dimregisterhistory.deviceNumber = dimdevice.deviceNumber
      AND dimDeviceHistory._recordStart >= dimregisterhistory._recordStart
      AND dimDeviceHistory._recordStart <= dimregisterhistory._recordEnd
      AND dimregisterhistory._recordDeleted = 1
LEFT OUTER JOIN curated_v2.dimRegisterInstallationHistory dimregisterinstallationhistory
    ON dimregisterinstallationhistory.installationNumber = dimdeviceinstallationhistory.installationNumber
      AND dimregisterhistory.logicalRegisterNumber = dimregisterinstallationhistory.logicalRegisterNumber
      AND dimDeviceHistory._recordStart >= dimregisterinstallationhistory._recordStart
      AND dimDeviceHistory._recordStart <= dimregisterhistory._recordEnd
      AND dimregisterinstallationhistory._recordDeleted = 1
LEFT OUTER JOIN cleansed.isu_0UCINSTALLA_ATTR_2 installAttr
    ON installAttr.installationNumber = dimdeviceinstallationhistory.installationNumber
    AND installAttr._recordDeleted = 1
WHERE dimregisterinstallationhistory.registerNotRelevantToBilling = 'N'
)
ORDER BY _effectiveFrom

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.notebook.exit("1")
