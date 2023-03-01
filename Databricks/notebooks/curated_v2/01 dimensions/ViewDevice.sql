-- Databricks notebook source
-- View: viewDevice
-- Description: viewDevice
CREATE OR REPLACE VIEW curated_v2.viewDevice AS

With dimDeviceRanges AS
(
                SELECT
                deviceNumber, 
                _recordStart, 
                _recordEnd,
                COALESCE( LEAD( _recordStart, 1 ) OVER( PARTITION BY deviceNumber ORDER BY _recordStart ), 
                  CASE WHEN _recordEnd < cast('9999-12-31T23:59:59' as timestamp) then _recordEnd + INTERVAL 1 SECOND else _recordEnd end) AS _newRecordEnd
                FROM curated_v2.dimDevice
                --WHERE _recordDeleted = 0
),

dimDeviceHistoryRanges AS
(
                SELECT
                deviceNumber, 
                _recordStart, 
                _recordEnd,
                COALESCE( LEAD( _recordStart, 1 ) OVER( PARTITION BY deviceNumber ORDER BY _recordStart ), 
                  CASE WHEN _recordEnd < cast('9999-12-31T23:59:59' as timestamp) then _recordEnd + INTERVAL 1 SECOND else _recordEnd end) AS _newRecordEnd
                FROM curated_v2.dimDeviceHistory
                WHERE _recordDeleted = 0
),

dateDriver AS
(
                SELECT deviceNumber, _recordStart from dimDeviceRanges
                UNION
                SELECT deviceNumber, _newRecordEnd as _recordStart from dimDeviceRanges
                UNION
                SELECT deviceNumber, _recordStart from dimDeviceHistoryRanges
                UNION
                SELECT deviceNumber, _newRecordEnd as _recordStart from dimDeviceHistoryRanges
),

effectiveDateRanges AS
(
                SELECT 
                deviceNumber, 
                _recordStart AS _effectiveFrom,
                COALESCE( LEAD( _recordStart, 1 ) OVER( PARTITION BY deviceNumber ORDER BY _recordStart ) - INTERVAL 1 SECOND, 
                  cast( '9999-12-31T23:59:59' as timestamp ) ) AS _effectiveTo                           
                FROM dateDriver where _recordStart < cast('9999-12-31T23:59:59' as timestamp)                            
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
    ,effectiveDateRanges._effectiveTo,
    CASE
      WHEN CURRENT_TIMESTAMP() BETWEEN effectiveDateRanges._effectiveFrom AND effectiveDateRanges._effectiveTo then 'Y'
      ELSE 'N'
      END AS currentFlag 
    ,if(dimdevice._RecordDeleted = 0,'Y','N') AS currentRecordFlag
FROM effectiveDateRanges as effectiveDateRanges
LEFT OUTER JOIN curated_v2.dimDevice dimdevice
    ON dimdevice.deviceNumber = effectiveDateRanges.deviceNumber 
        AND dimdevice._recordEnd >= effectiveDateRanges._effectiveFrom 
        AND dimdevice._recordStart <= effectiveDateRanges._effectiveTo
        --AND dimdevice._recordDeleted = 0
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
)
ORDER BY _effectiveFrom

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.notebook.exit("1")
