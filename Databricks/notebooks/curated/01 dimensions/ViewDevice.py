# Databricks notebook source
# MAGIC %run ../common/common-curated-includeMain

# COMMAND ----------

# notebookPath = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().split("/")
# view = notebookPath[-1:][0]
# db = notebookPath[-3:][0]
schema_name = 'water_consumption'
view_name = 'viewdevice'
view_fqn = f"{ADS_DATABASE_CURATED}.{schema_name}.{view_name}"

spark.sql(f"""
-- View: viewDevice
-- Description: viewDevice
CREATE OR REPLACE VIEW {view_fqn} AS

With dimDeviceRanges AS
(
                SELECT
                deviceNumber, 
                _recordStart, 
                _recordEnd,
                COALESCE( LEAD( _recordStart, 1 ) OVER( PARTITION BY deviceNumber ORDER BY _recordStart ), 
                  CASE WHEN _recordEnd < cast('9999-12-31T23:59:59' as timestamp) then _recordEnd + INTERVAL 1 SECOND else _recordEnd end) AS _newRecordEnd
                FROM {ADS_DATABASE_CURATED}.dim.device
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
                FROM {ADS_DATABASE_CURATED}.dim.deviceHistory
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
     --dimdevice.deviceSK
    --,dimdevicehistory.deviceHistorySK,
    dimdevice.sourceSystemCode
   ,coalesce(dimdevice.deviceNumber, dimdevicehistory.deviceNumber, -1) as deviceNumber
    ,dimdevice.materialNumber
    ,dimdevice.deviceID
    ,dimdevice.inspectionRelevanceIndicator
    ,dimdevice.deviceSize
    ,dimdevice.deviceSizeUnit
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
    --,dimdeviceinstallationhistory.deviceInstallationHistorySK
    ,dimdeviceinstallationhistory.installationNumber
    ,dimdeviceinstallationhistory.validToDate AS deviceInstallationHistoryValidToDate
    ,dimdeviceinstallationhistory.validFromDate AS deviceInstallationHistoryValidFromDate
    ,dimdeviceinstallationhistory.priceClassCode
    ,dimdeviceinstallationhistory.priceClass
    ,dimdeviceinstallationhistory.rateTypeCode AS deviceInstallationHistoryRateTypeCode
    ,dimdeviceinstallationhistory.rateType AS deviceInstallationHistoryRateType
    --,dimregisterhistory.registerHistorySK
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
    --,dimregisterinstallationhistory.registerInstallationHistorySK
    ,dimregisterinstallationhistory.validToDate AS registerInstallationHistoryValidToDate
    ,dimregisterinstallationhistory.validFromDate AS registerInstallationHistoryValidFromDate
    ,dimregisterinstallationhistory.operationCode
    ,dimregisterinstallationhistory.operationDescription
    ,dimregisterinstallationhistory.rateTypeCode AS registerInstallationHistoryRateTypeCode
    ,dimregisterinstallationhistory.rateType AS registerInstallationHistoryRateType
    ,CASE 
    WHEN (dimregisterinstallationhistory.registerNotRelevantToBilling = 'N' or dimregisterinstallationhistory.registerNotRelevantToBilling is null) Then 'Y' 
    Else 'N' 
    End AS registerRelevantToBilling  
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
LEFT OUTER JOIN {ADS_DATABASE_CURATED}.dim.device dimdevice
    ON dimdevice.deviceNumber = effectiveDateRanges.deviceNumber 
        AND dimdevice._recordEnd >= effectiveDateRanges._effectiveFrom 
        AND dimdevice._recordStart <= effectiveDateRanges._effectiveTo
        --AND dimdevice._recordDeleted = 0
LEFT OUTER JOIN {ADS_DATABASE_CURATED}.dim.deviceHistory dimdevicehistory
    ON dimdevicehistory.deviceNumber = effectiveDateRanges.deviceNumber 
        AND dimdevicehistory._recordEnd >= effectiveDateRanges._effectiveFrom 
        AND dimdevicehistory._recordStart <= effectiveDateRanges._effectiveTo
        AND dimdevicehistory._recordDeleted = 0
LEFT OUTER JOIN {ADS_DATABASE_CURATED}.dim.deviceInstallationHistory dimdeviceinstallationhistory
    ON dimdeviceinstallationhistory.logicalDeviceNumber = dimdevicehistory.logicalDeviceNumber
        AND dimdevicehistory._recordStart >= dimdeviceinstallationhistory._recordStart
        AND dimdevicehistory._recordEnd <= dimdeviceinstallationhistory._recordEnd
        AND dimdeviceinstallationhistory._recordDeleted = 0
LEFT OUTER JOIN {ADS_DATABASE_CURATED}.dim.registerHistory dimregisterhistory
    ON dimregisterhistory.deviceNumber = dimdevice.deviceNumber
      AND dimDeviceHistory._recordStart >= dimregisterhistory._recordStart
      AND dimDeviceHistory._recordStart <= dimregisterhistory._recordEnd
      AND dimregisterhistory._recordDeleted = 0
LEFT OUTER JOIN {ADS_DATABASE_CURATED}.dim.registerInstallationHistory dimregisterinstallationhistory
    ON dimregisterinstallationhistory.installationNumber = dimdeviceinstallationhistory.installationNumber
      AND dimregisterhistory.logicalRegisterNumber = dimregisterinstallationhistory.logicalRegisterNumber
      AND dimDeviceHistory._recordStart >= dimregisterinstallationhistory._recordStart
      AND dimDeviceHistory._recordStart <= dimregisterinstallationhistory._recordEnd
      AND dimregisterinstallationhistory._recordDeleted = 0
LEFT OUTER JOIN {ADS_DATABASE_CLEANSED}.isu.0UCINSTALLA_ATTR_2 installAttr
    ON installAttr.installationNumber = dimdeviceinstallationhistory.installationNumber
    AND installAttr._recordDeleted = 0
WHERE dimregisterinstallationhistory.registerNotRelevantToBilling = 'N' or dimdevicehistory.logicalDeviceNumber = 0 or dimdevicehistory.logicalDeviceNumber is null or dimregisterinstallationhistory.registerNotRelevantToBilling is null 

)
ORDER BY _effectiveFrom
""".replace("CREATE OR REPLACE VIEW", "ALTER VIEW" if viewExists(view_fqn) else "CREATE OR REPLACE VIEW"))

# COMMAND ----------

# MAGIC %run ./ViewDeviceCharacteristics