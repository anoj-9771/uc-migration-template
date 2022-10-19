-- Databricks notebook source
-- View: viewDeviceCharacteristics
-- Description: viewDeviceCharacteristics
CREATE OR REPLACE VIEW curated_v2.viewDeviceCharacteristics AS
WITH dateDriver AS (
         SELECT DISTINCT
             deviceNumber,
             characteristicInternalId,
             classifiedEntityType,
             classTypeCode,
             archivingObjectsInternalId,
             _recordStart AS _effectiveFrom
         FROM curated_v2.dimDeviceCharacteristics WHERE _recordDeleted <> 1
     ),
 
     effectiveDateRanges AS (
         SELECT 
             deviceNumber,
             characteristicInternalId,
             classifiedEntityType,
             classTypeCode,
             archivingObjectsInternalId, 
             _effectiveFrom, 
             COALESCE(
                 TIMESTAMP(
                     DATE_ADD(
                         LEAD(_effectiveFrom,1) OVER (PARTITION BY deviceNumber,characteristicInternalId,classifiedEntityType,classTypeCode,archivingObjectsInternalId
                                                       ORDER BY _effectiveFrom),-1)
                 ), 
             TIMESTAMP('9999-12-31')) AS _effectiveTo
         from dateDriver
     )
SELECT
      effectiveDateRanges._effectiveFrom
     ,effectiveDateRanges._effectiveTo
     deviceCharacteristicsSK
    ,dimDeviceCharacteristics.deviceNumber
    ,dimDeviceCharacteristics.classifiedEntityType
    ,dimDeviceCharacteristics.classTypeCode
    ,dimDeviceCharacteristics.classType
    ,dimDeviceCharacteristics.archivingObjectsInternalId
    , CASE WHEN characteristicName in ('MTR_GRID_LOCATION_CODE') THEN dimDeviceCharacteristics.characteristicInternalId ELSE NULL END AS meterGridLocationInternalId
    , CASE WHEN characteristicName in ('MTR_GRID_LOCATION_CODE') THEN characteristicValueCode ELSE NULL END AS meterGridLocationCode
    , CASE WHEN characteristicName in ('MTR_GRID_LOCATION_CODE') THEN characteristicValueDescription ELSE NULL END AS meterGridLocation
    , CASE WHEN characteristicName in ('MR_LOCATION') THEN dimDeviceCharacteristics.characteristicInternalId ELSE NULL END AS meterReadLocationInternalId
    , CASE WHEN characteristicName in ('MR_LOCATION') THEN characteristicValueCode ELSE NULL END AS meterReadLocationCode
    , CASE WHEN characteristicName in ('MR_LOCATION') THEN characteristicValueDescription ELSE NULL END AS meterReadLocation
    , CASE WHEN characteristicName in ('COMMON_AREA_METER') THEN dimDeviceCharacteristics.characteristicInternalId ELSE NULL END AS commonAreaMeterInternalId
    , CASE WHEN characteristicName in ('COMMON_AREA_METER') THEN characteristicValueCode ELSE NULL END AS commonAreaMeterCode
    , CASE WHEN characteristicName in ('COMMON_AREA_METER') THEN characteristicValueDescription ELSE NULL END AS commonAreaMeter
    , CASE WHEN characteristicName in ('MLOC_LEVEL') THEN dimDeviceCharacteristics.characteristicInternalId ELSE NULL END AS meterLocationLevelInternalId
    , CASE WHEN characteristicName in ('MLOC_LEVEL') THEN characteristicValueCode ELSE NULL END AS meterLocationLevelCode
    , CASE WHEN characteristicName in ('MLOC_LEVEL') THEN characteristicValueDescription ELSE NULL END AS meterLocationLevel
    , CASE WHEN characteristicName in ('MLOC_DESC') THEN dimDeviceCharacteristics.characteristicInternalId ELSE NULL END AS meterLocationInternalId
    , CASE WHEN characteristicName in ('MLOC_DESC') THEN characteristicValueCode ELSE NULL END AS meterLocationCode
    , CASE WHEN characteristicName in ('MLOC_DESC') THEN characteristicValueDescription ELSE NULL END AS meterLocation
    , CASE WHEN characteristicName in ('MTR_PROPERTY_POSITION_ID') THEN dimDeviceCharacteristics.characteristicInternalId ELSE NULL END AS meterPropertyPositionInternalId
    , CASE WHEN characteristicName in ('MTR_PROPERTY_POSITION_ID') THEN characteristicValueCode ELSE NULL END AS meterPropertyPositionId
    , CASE WHEN characteristicName in ('MTR_PROPERTY_POSITION_ID') THEN characteristicValueDescription ELSE NULL END AS meterPropertyPosition
    , CASE WHEN characteristicName in ('MTR_READ_GRID_LOCATION_CODE') THEN dimDeviceCharacteristics.characteristicInternalId ELSE NULL END AS meterReadGridLocationInternalId
    , CASE WHEN characteristicName in ('MTR_READ_GRID_LOCATION_CODE') THEN characteristicValueCode ELSE NULL END AS meterReadGridLocationCode
    , CASE WHEN characteristicName in ('MTR_READ_GRID_LOCATION_CODE') THEN characteristicValueDescription ELSE NULL END AS meterReadGridLocation
    , CASE WHEN characteristicName in ('RFDEVICEMODEL') THEN dimDeviceCharacteristics.characteristicInternalId ELSE NULL END AS RFDeviceModelInternalId
    , CASE WHEN characteristicName in ('RFDEVICEMODEL') THEN characteristicValueCode ELSE NULL END AS RFDeviceModelCode
    , CASE WHEN characteristicName in ('RFDEVICEMODEL') THEN characteristicValueDescription ELSE NULL END AS RFDeviceModel
    , CASE WHEN characteristicName in ('RFDEVICEMAKER') THEN dimDeviceCharacteristics.characteristicInternalId ELSE NULL END AS RFDeviceMakerInternalId
    , CASE WHEN characteristicName in ('RFDEVICEMAKER') THEN characteristicValueCode ELSE NULL END AS RFDeviceMakerCode
    , CASE WHEN characteristicName in ('RFDEVICEMAKER') THEN characteristicValueDescription ELSE NULL END AS RFDeviceMaker
    , CASE WHEN characteristicName in ('RF_ID') THEN dimDeviceCharacteristics.characteristicInternalId ELSE NULL END AS RFIDInternalId
    , CASE WHEN characteristicName in ('RF_ID') THEN characteristicValueCode ELSE NULL END AS RFID
    , CASE WHEN characteristicName in ('RF_ID') THEN characteristicValueDescription ELSE NULL END AS RFIDDescription
    , CASE WHEN characteristicName in ('WARNINGNOTES') THEN dimDeviceCharacteristics.characteristicInternalId ELSE NULL END AS warningNotesInternalId
    , CASE WHEN characteristicName in ('WARNINGNOTES') THEN characteristicValueCode ELSE NULL END AS warningNotes
    , CASE WHEN characteristicName in ('WARNINGNOTES') THEN characteristicValueDescription ELSE NULL END AS warningDescription
    , CASE WHEN characteristicName in ('MTR_ACCESS_NOTE') THEN dimDeviceCharacteristics.characteristicInternalId ELSE NULL END AS meterAccessNoteInternalId
    , CASE WHEN characteristicName in ('MTR_ACCESS_NOTE') THEN characteristicValueCode ELSE NULL END AS meterAccessNoteCode
    , CASE WHEN characteristicName in ('MTR_ACCESS_NOTE') THEN characteristicValueDescription ELSE NULL END AS meterAccessNote
    , CASE WHEN characteristicName in ('MTR_READING_NOTE') THEN dimDeviceCharacteristics.characteristicInternalId ELSE NULL END AS meterReadingInternalId
    , CASE WHEN characteristicName in ('MTR_READING_NOTE') THEN characteristicValueCode ELSE NULL END AS meterReadingNote
    , CASE WHEN characteristicName in ('MTR_READING_NOTE') THEN characteristicValueDescription ELSE NULL END AS meterReadingNoteDescription
    , CASE WHEN characteristicName in ('METER_SERVES') THEN dimDeviceCharacteristics.characteristicInternalId ELSE NULL END AS meterServesInternalId
    , CASE WHEN characteristicName in ('METER_SERVES') THEN characteristicValueCode ELSE NULL END AS meterServesCode
    , CASE WHEN characteristicName in ('METER_SERVES') THEN characteristicValueDescription ELSE NULL END AS meterServes
    , CASE WHEN characteristicName in ('SO_COMMENTS') THEN dimDeviceCharacteristics.characteristicInternalId ELSE NULL END AS SOCommentsInternalId
    , CASE WHEN characteristicName in ('SO_COMMENTS') THEN characteristicValueCode ELSE NULL END AS SOComments
    , CASE WHEN characteristicName in ('SO_COMMENTS') THEN characteristicValueDescription ELSE NULL END AS SODescription
    , CASE WHEN characteristicName in ('COMMENTS') THEN dimDeviceCharacteristics.characteristicInternalId ELSE NULL END AS commentsCodeInternalId
    , CASE WHEN characteristicName in ('COMMENTS') THEN characteristicValueCode ELSE NULL END AS commentsCode
    , CASE WHEN characteristicName in ('COMMENTS') THEN characteristicValueDescription ELSE NULL END AS comments
    , CASE WHEN characteristicName in ('METER_COMPLETION_NOTES') THEN dimDeviceCharacteristics.characteristicInternalId ELSE NULL END AS meterCompleteNotesInternalId
    , CASE WHEN characteristicName in ('METER_COMPLETION_NOTES') THEN characteristicValueCode ELSE NULL END AS meterCompleteNotes
    , CASE WHEN characteristicName in ('METER_COMPLETION_NOTES') THEN characteristicValueDescription ELSE NULL END AS meterCompleteNotesDescription
    , CASE WHEN characteristicName in ('SIM_ITEM_NO') THEN dimDeviceCharacteristics.characteristicInternalId ELSE NULL END AS simItemNumberInternalId
    , CASE WHEN characteristicName in ('SIM_ITEM_NO') THEN characteristicValueCode ELSE NULL END AS simItemNumber
    , CASE WHEN characteristicName in ('SIM_ITEM_NO') THEN characteristicValueDescription ELSE NULL END AS simItem
    , CASE WHEN characteristicName in ('PRJ_NUM') THEN dimDeviceCharacteristics.characteristicInternalId ELSE NULL END AS ProjectNumberInternalId
    , CASE WHEN characteristicName in ('PRJ_NUM') THEN characteristicValueCode ELSE NULL END AS ProjectNumber
    , CASE WHEN characteristicName in ('PRJ_NUM') THEN characteristicValueDescription ELSE NULL END AS Project
    , CASE WHEN characteristicName in ('IOTFIRMWARE') THEN dimDeviceCharacteristics.characteristicInternalId ELSE NULL END AS IOTFirmwareInternalId
    , CASE WHEN characteristicName in ('IOTFIRMWARE') THEN characteristicValueCode ELSE NULL END AS IOTFirmwareCode
    , CASE WHEN characteristicName in ('IOTFIRMWARE') THEN characteristicValueDescription ELSE NULL END AS IOTFirmware
    , CASE WHEN characteristicName in ('ADDITIONAL_INFO') THEN dimDeviceCharacteristics.characteristicInternalId ELSE NULL END AS additionalInformationInternalId
    , CASE WHEN characteristicName in ('ADDITIONAL_INFO') THEN characteristicValueCode ELSE NULL END AS additionalInformationCode
    , CASE WHEN characteristicName in ('ADDITIONAL_INFO') THEN characteristicValueDescription ELSE NULL END AS additionalInformationDescription
    , CASE WHEN characteristicName in ('CANT_DO_CODE') THEN dimDeviceCharacteristics.characteristicInternalId ELSE NULL END AS cantDoCodeInternalId
    , CASE WHEN characteristicName in ('CANT_DO_CODE') THEN characteristicValueCode ELSE NULL END AS cantDoCode
    , CASE WHEN characteristicName in ('CANT_DO_CODE') THEN characteristicValueDescription ELSE NULL END AS cantDoDescription
    , CASE WHEN characteristicName in ('BARCODE') THEN dimDeviceCharacteristics.characteristicInternalId ELSE NULL END AS barcodeInternalId
    , CASE WHEN characteristicName in ('BARCODE') THEN characteristicValueCode ELSE NULL END AS barcode
    , CASE WHEN characteristicName in ('BARCODE') THEN characteristicValueDescription ELSE NULL END AS barcodeDescription
    , CASE WHEN characteristicName in ('CENTRALISED_HOTWATER') THEN dimDeviceCharacteristics.characteristicInternalId ELSE NULL END AS centralisedHotwaterInternalId
    , CASE WHEN characteristicName in ('CENTRALISED_HOTWATER') THEN characteristicValueCode ELSE NULL END AS centralisedHotwaterCode
    , CASE WHEN characteristicName in ('CENTRALISED_HOTWATER') THEN characteristicValueDescription ELSE NULL END AS centralisedHotwater
    , CASE WHEN characteristicName in ('MLIM_SYSTEM') THEN dimDeviceCharacteristics.characteristicInternalId ELSE NULL END AS mlimSystemInternalId
    , CASE WHEN characteristicName in ('MLIM_SYSTEM') THEN characteristicValueCode ELSE NULL END AS mlimSystemCode
    , CASE WHEN characteristicName in ('MLIM_SYSTEM') THEN characteristicValueDescription ELSE NULL END AS mlimSystem
    , CASE WHEN characteristicName in ('SITE_ID') THEN dimDeviceCharacteristics.characteristicInternalId ELSE NULL END AS siteIdInternalId
    , CASE WHEN characteristicName in ('SITE_ID') THEN characteristicValueCode ELSE NULL END AS siteId
    , CASE WHEN characteristicName in ('SITE_ID') THEN characteristicValueDescription ELSE NULL END AS siteIdDescription
    , CASE WHEN characteristicName in ('TAP_TESTED') THEN dimDeviceCharacteristics.characteristicInternalId ELSE NULL END AS tapTestedInternalId
    , CASE WHEN characteristicName in ('TAP_TESTED') THEN characteristicValueCode ELSE NULL END AS tapTestedCode
    , CASE WHEN characteristicName in ('TAP_TESTED') THEN characteristicValueDescription ELSE NULL END AS tapTested
    , CASE WHEN characteristicName in ('IOT_TYPE') THEN dimDeviceCharacteristics.characteristicInternalId ELSE NULL END AS IOTTypeInternalId
    , CASE WHEN characteristicName in ('IOT_TYPE') THEN characteristicValueCode ELSE NULL END AS IOTTypeCode
    , CASE WHEN characteristicName in ('IOT_TYPE') THEN characteristicValueDescription ELSE NULL END AS IOTType
    , CASE WHEN characteristicName in ('IOT_NETWORK') THEN dimDeviceCharacteristics.characteristicInternalId ELSE NULL END AS IOTNetworkInternalId
    , CASE WHEN characteristicName in ('IOT_NETWORK') THEN characteristicValueCode ELSE NULL END AS IOTNetworkCode
    , CASE WHEN characteristicName in ('IOT_NETWORK') THEN characteristicValueDescription ELSE NULL END AS IOTNetwork
    , CASE WHEN characteristicName in ('IOT_NETWORK_PROVIDER') THEN dimDeviceCharacteristics.characteristicInternalId ELSE NULL END AS IOTNetworkProviderInternalId
    , CASE WHEN characteristicName in ('IOT_NETWORK_PROVIDER') THEN characteristicValueCode ELSE NULL END AS IOTNetworkProviderCode
    , CASE WHEN characteristicName in ('IOT_NETWORK_PROVIDER') THEN characteristicValueDescription ELSE NULL END AS IOTNetworkProvider
    , CASE WHEN characteristicName in ('IOT_PRESSURE_SENSOR') THEN dimDeviceCharacteristics.characteristicInternalId ELSE NULL END AS IOTPressureSensorInternalId
    , CASE WHEN characteristicName in ('IOT_PRESSURE_SENSOR') THEN characteristicValueCode ELSE NULL END AS IOTPressureSensorCode
    , CASE WHEN characteristicName in ('IOT_PRESSURE_SENSOR') THEN characteristicValueDescription ELSE NULL END AS IOTPressureSensor
    , CASE WHEN characteristicName in ('IOT_PROGRAM_CATEGORY') THEN dimDeviceCharacteristics.characteristicInternalId ELSE NULL END AS IOTProgramCategoryInternalId
    , CASE WHEN characteristicName in ('IOT_PROGRAM_CATEGORY') THEN characteristicValueCode ELSE NULL END AS IOTProgramCategoryCode
    , CASE WHEN characteristicName in ('IOT_PROGRAM_CATEGORY') THEN characteristicValueDescription ELSE NULL END AS IOTProgramCategory
    , CASE WHEN characteristicName in ('IOT_PROGRAM_CATEGORY_2') THEN dimDeviceCharacteristics.characteristicInternalId ELSE NULL END AS IOTProgramCategory2InternalId
    , CASE WHEN characteristicName in ('IOT_PROGRAM_CATEGORY_2') THEN characteristicValueCode ELSE NULL END AS IOTProgramCategory2Code
    , CASE WHEN characteristicName in ('IOT_PROGRAM_CATEGORY_2') THEN characteristicValueDescription ELSE NULL END AS IOTProgramCategory2
    , CASE WHEN characteristicName in ('IOT_PROGRAM_CATEGORY_3') THEN dimDeviceCharacteristics.characteristicInternalId ELSE NULL END AS IOTProgramCategory3InternalId
    , CASE WHEN characteristicName in ('IOT_PROGRAM_CATEGORY_3') THEN characteristicValueCode ELSE NULL END AS IOTProgramCategory3Code
    , CASE WHEN characteristicName in ('IOT_PROGRAM_CATEGORY_3') THEN characteristicValueDescription ELSE NULL END AS IOTProgramCategory3
    , CASE WHEN characteristicName in ('IOT_METER') THEN dimDeviceCharacteristics.characteristicInternalId ELSE NULL END AS IOTMeterInternalId
    , CASE WHEN characteristicName in ('IOT_METER') THEN characteristicValueCode ELSE NULL END AS IOTMeterCode
    , CASE WHEN characteristicName in ('IOT_METER') THEN characteristicValueDescription ELSE NULL END AS IOTMeter
    , CASE WHEN characteristicName in ('METER_GPS_LAT_LONG_SOURCE') THEN dimDeviceCharacteristics.characteristicInternalId ELSE NULL END AS meterGPSSourceInternalId
    , CASE WHEN characteristicName in ('METER_GPS_LAT_LONG_SOURCE') THEN characteristicValueCode ELSE NULL END AS meterGPSSourceCode
    , CASE WHEN characteristicName in ('METER_GPS_LAT_LONG_SOURCE') THEN characteristicValueDescription ELSE NULL END AS meterGPSSource
    , CASE WHEN characteristicName in ('METER_COUPLINGS') THEN dimDeviceCharacteristics.characteristicInternalId ELSE NULL END AS meterCouplingInternalId
    , CASE WHEN characteristicName in ('METER_COUPLINGS') THEN characteristicValueCode ELSE NULL END AS meterCouplingCode
    , CASE WHEN characteristicName in ('METER_COUPLINGS') THEN characteristicValueDescription ELSE NULL END AS meterCoupling
    , CASE WHEN characteristicName in ('IOT_RADIO_MODEL') THEN dimDeviceCharacteristics.characteristicInternalId ELSE NULL END AS IOTRadioModelInternalId
    , CASE WHEN characteristicName in ('IOT_RADIO_MODEL') THEN characteristicValueCode ELSE NULL END AS IOTRadioModelCode
    , CASE WHEN characteristicName in ('IOT_RADIO_MODEL') THEN characteristicValueDescription ELSE NULL END AS IOTRadioModel
    , CASE WHEN characteristicName in ('PULSE_SENSOR') THEN dimDeviceCharacteristics.characteristicInternalId ELSE NULL END AS pulseSensorInternalId
    , CASE WHEN characteristicName in ('PULSE_SENSOR') THEN characteristicValueCode ELSE NULL END AS pulseSensorCode
    , CASE WHEN characteristicName in ('PULSE_SENSOR') THEN characteristicValueDescription ELSE NULL END AS pulseSensor
    , CASE WHEN characteristicName in ('PULSE_SPLITTER') THEN dimDeviceCharacteristics.characteristicInternalId ELSE NULL END AS pulseSplitterInternalId
    , CASE WHEN characteristicName in ('PULSE_SPLITTER') THEN characteristicValueCode ELSE NULL END AS pulseSplitterCode
    , CASE WHEN characteristicName in ('PULSE_SPLITTER') THEN characteristicValueDescription ELSE NULL END AS pulseSplitter
    , CASE WHEN characteristicName in ('MLOC_GPS_LAT') THEN dimDeviceCharacteristics.characteristicInternalId ELSE NULL END AS meterLocationGPSLatitudeInternalId
    , CASE WHEN characteristicName in ('MLOC_GPS_LAT') THEN decimalMinimumValue ELSE NULL END AS meterLocationGPSLatitude
    , CASE WHEN characteristicName in ('MLOC_GPS_LON') THEN dimDeviceCharacteristics.characteristicInternalId ELSE NULL END AS meterLocationGPSLongitudeInternalId
    , CASE WHEN characteristicName in ('MLOC_GPS_LON') THEN decimalMinimumValue ELSE NULL END AS meterLocationGPSLongitude
    , CASE WHEN characteristicName in ('MR_GPS_LAT') THEN dimDeviceCharacteristics.characteristicInternalId ELSE NULL END AS meterReadGPSLatitudeInternalId
    , CASE WHEN characteristicName in ('MR_GPS_LAT') THEN decimalMinimumValue ELSE NULL END AS meterReadGPSLatitude
    , CASE WHEN characteristicName in ('MR_GPS_LON') THEN dimDeviceCharacteristics.characteristicInternalId ELSE NULL END AS meterReadGPSLongitudeInternalId
    , CASE WHEN characteristicName in ('MR_GPS_LON') THEN decimalMinimumValue ELSE NULL END AS meterReadGPSLongitude
    , CASE WHEN characteristicName in ('METER_OFFSET') THEN dimDeviceCharacteristics.characteristicInternalId ELSE NULL END AS meterOffsetInternalId
    , CASE WHEN characteristicName in ('METER_OFFSET') THEN decimalMinimumValue ELSE NULL END AS meterOffset
    , CASE WHEN characteristicName in ('IOT_MULTIPLIER') THEN dimDeviceCharacteristics.characteristicInternalId ELSE NULL END AS IOTMultiplierInternalId
    , CASE WHEN characteristicName in ('IOT_MULTIPLIER') THEN decimalMinimumValue ELSE NULL END AS IOTMultiplier
    , CASE WHEN characteristicName in ('IOT_HIGH_DAILY_CONS') THEN dimDeviceCharacteristics.characteristicInternalId ELSE NULL END AS IOTHighDailyConsumptionInternalId
    , CASE WHEN characteristicName in ('IOT_HIGH_DAILY_CONS') THEN decimalMinimumValue ELSE NULL END AS IOTHighDailyConsumption
    , CASE WHEN characteristicName in ('IOT_LEAK_ALARM_THRESHOLD') THEN dimDeviceCharacteristics.characteristicInternalId ELSE NULL END AS IOTLeakAlarmThresholdInternalId
    , CASE WHEN characteristicName in ('IOT_LEAK_ALARM_THRESHOLD') THEN decimalMinimumValue ELSE NULL END AS IOTLeakAlarmThreshold
    , CASE WHEN characteristicName in ('NUMBER_OF_BEDROOMS') THEN dimDeviceCharacteristics.characteristicInternalId ELSE NULL END AS numberOfBedroomsInternalId
    , CASE WHEN characteristicName in ('NUMBER_OF_BEDROOMS') THEN decimalMinimumValue ELSE NULL END AS numberOfBedrooms
    , CASE WHEN characteristicName in ('CERTIFICATION_DATE') THEN dimDeviceCharacteristics.characteristicInternalId ELSE NULL END AS certificateDateInternalId
    , CASE WHEN characteristicName in ('CERTIFICATION_DATE') THEN characteristicValueMaximumDate ELSE NULL END AS certificateDate
    , CASE WHEN characteristicName in ('IOT_RADIO_FIT_DATE') THEN dimDeviceCharacteristics.characteristicInternalId ELSE NULL END AS IOTRadioFitDateInternalId
    , CASE WHEN characteristicName in ('IOT_RADIO_FIT_DATE') THEN characteristicValueMaximumDate ELSE NULL END AS IOTRadioFitDate
    , CASE WHEN characteristicName in ('METER_GPS_DATE_CAPTURED') THEN dimDeviceCharacteristics.characteristicInternalId ELSE NULL END AS meterGPSCapturedDateInternalId
    , CASE WHEN characteristicName in ('METER_GPS_DATE_CAPTURED') THEN characteristicValueMaximumDate ELSE NULL END AS meterGPSCapturedDate
    , CASE WHEN characteristicName in ('TOTAL_REVENUE_COST') THEN dimDeviceCharacteristics.characteristicInternalId ELSE NULL END AS totalRevenueCostInternalId
    , CASE WHEN characteristicName in ('TOTAL_REVENUE_COST') THEN currencyMaximumValue ELSE NULL END AS totalRevenueCost
    , CASE
      WHEN CURRENT_DATE() BETWEEN effectiveDateRanges._effectiveFrom AND effectiveDateRanges._effectiveTo then 'Y'
      ELSE 'N'
      END AS currentIndicator
FROM effectiveDateRanges
LEFT OUTER JOIN curated_v2.dimDeviceCharacteristics
  ON effectiveDateRanges.deviceNumber = dimDeviceCharacteristics.deviceNumber
        AND effectiveDateRanges.characteristicInternalId = dimDeviceCharacteristics.characteristicInternalId
        AND effectiveDateRanges.classifiedEntityType = dimDeviceCharacteristics.classifiedEntityType
        AND effectiveDateRanges.classTypeCode = dimDeviceCharacteristics.classTypeCode
        AND effectiveDateRanges.archivingObjectsInternalId = dimDeviceCharacteristics.archivingObjectsInternalId
        AND effectiveDateRanges._effectiveFrom <= dimDeviceCharacteristics._RecordEnd
        AND effectiveDateRanges._effectiveTo >= dimDeviceCharacteristics._RecordStart 
ORDER BY effectiveDateRanges._effectiveFrom

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.notebook.exit("1")
