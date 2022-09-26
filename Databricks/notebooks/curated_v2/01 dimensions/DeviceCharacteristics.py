# Databricks notebook source
###########################################################################################################################
# Loads DeviceCharacteristics dimension 
#############################################################################################################################
# Method
# 1.Load Cleansed layer table data into dataframe and transform
# 2.SCHEMA DEFINITION
#############################################################################################################################

# COMMAND ----------

# MAGIC %run ../common/common-curated-includeMain

# COMMAND ----------

def getDeviceCharacteristics():

    #DeviceCharacteristics Data from SAP ISU
    isuDeviceCharacteristicsDf  = spark.sql(f"""
                                    WITH ausp AS (
                                              SELECT characteristicInternalId, 
                                                  CONCAT_WS(',',SORT_ARRAY(COLLECT_LIST(STRUCT(characteristicValueInternalId,characteristicValueCode))).characteristicValueCode) AS characteristicValueCode 
                                              FROM {ADS_DATABASE_CLEANSED}.isu_ausp ausp 
                                              WHERE _RecordCurrent = 1 and _RecordDeleted = 0
                                              GROUP BY characteristicInternalId
                                                 ),
                                         cabn AS (
                                              SELECT DISTINCT isu_cabn.internalcharacteristic, isu_cabn.characteristicName, isu_cabnt.charactericDescription 
                                              FROM {ADS_DATABASE_CLEANSED}.isu_cabn
                                              LEFT OUTER JOIN {ADS_DATABASE_CLEANSED}.isu_cabnt
                                                  ON isu_cabn.internalcharacteristic = isu_cabnt.internalcharacteristic
                                                      AND isu_cabn.internalCounterforArchivingObjectsbyECM = isu_cabnt.internalCounterforArchivingObjectsbyECM
                                        WHERE isu_cabn.characteristicName IN ('MTR_GRID_LOCATION_CODE', 'MR_LOCATION', 'COMMON_AREA_METER', 'MLOC_LEVEL', 'MLOC_DESC', 'MTR_PROPERTY_POSITION_ID', 'MTR_READ_GRID_LOCATION_CODE', 'RFDEVICEMODEL', 'RFDEVICEMAKER', 'RF_ID', 'WARNINGNOTES', 'MTR_ACCESS_NOTE', 'MTR_READING_NOTE', 'METER_SERVES', 'SO_COMMENTS', 'COMMENTS', 'METER_COMPLETION_NOTES', 'SIM_ITEM_NO', 'PRJ_NUM', 'IOTFIRMWARE', 'ADDITIONAL_INFO', 'CANT_DO_CODE', 'BARCODE', 'CENTRALISED_HOTWATER',
        'MLIM_SYSTEM', 'SITE_ID', 'TAP_TESTED', 'IOT_TYPE', 'IOT_NETWORK', 'IOT_NETWORK_PROVIDER', 'IOT_PRESSURE_SENSOR', 'IOT_PROGRAM_CATEGORY', 'IOT_PROGRAM_CATEGORY_2',
        'IOT_PROGRAM_CATEGORY_3', 'IOT_METER', 'METER_GPS_LAT_LONG_SOURCE', 'METER_COUPLINGS', 'IOT_RADIO_MODEL', 'PULSE_SENSOR', 'PULSE_SPLITTER',
        'MLOC_GPS_LAT', 'MLOC_GPS_LON', 'MR_GPS_LAT', 'MR_GPS_LON', 'METER_OFFSET', 'IOT_MULTIPLIER', 'IOT_HIGH_DAILY_CONS', 'IOT_LEAK_ALARM_THRESHOLD', 'NUMBER_OF_BEDROOMS',
         'CERTIFICATION_DATE', 'IOT_RADIO_FIT_DATE', 'METER_GPS_DATE_CAPTURED', 'TOTAL_REVENUE_COST')
                                          AND isu_cabn._RecordCurrent = 1 and  isu_cabn._RecordDeleted = 0
                                          AND isu_cabnt._RecordCurrent = 1 and isu_cabnt._RecordDeleted = 0
                                                 ),
                                        cawnt AS (
                                                SELECT internalcharacteristic, charactericValue,
                                                       CONCAT_WS(',', SORT_ARRAY(COLLECT_LIST(STRUCT(characteristicValueInternalId,characteristicValueCode))).characteristicValueDescription) AS characteristicValueDescription 
                                                 FROM {ADS_DATABASE_CLEANSED}.isu_cawn
                                                 LEFT OUTER JOIN {ADS_DATABASE_CLEANSED}.isu_cawnt
                                                      ON isu_cawn.internalCharacteristic = isu_cawnt.internalcharacteristic
                                                          AND isu_cawn.internalCounter = isu_cawnt.internalCounter 
                                                  GROUP BY isu_cawn.internalCharacteristic
                                                  WHERE isu_cawn._RecordCurrent = 1 AND isu_cawn._RecordDeleted = 0
                                                  AND isu_cawnt._RecordCurrent = 1 AND  isu_cawnt._RecordDeleted = 0
                                                  )
                                    SELECT 'ISU' AS sourceSystemCode,
                                        isu_ausp.classificationObjectInternalId AS deviceNumber,
                                        isu_ausp.characteristicInternalId,
                                        isu_ausp.classifiedEntityType,
                                        isu_ausp.classTypeCode,
                                        isu_ausp.classType,
                                        isu_ausp.archivingObjectsInternalId,
                                        cabnt.characteristicName,
                                        cabnt.charactericDescription,
                                        ausp.characteristicValueCode,
                                        cawnt.characteristicValueDescription,
                                        isu_ausp.validFromDate1 AS characteristicValueMinimumDate,
                                        isu_ausp.validToDate1 AS characteristicValueMaximumDate,
                                        isu_ausp.timeMinimumValue,
                                        isu_ausp.timeMaximumValue,
                                        isu_ausp.validFromDate,
                                        isu_ausp.validToDate,
                                        isu_ausp.decimalMinimumValue,
                                        isu_ausp.decimalMaximumValue,
                                        isu_ausp.currencyMinimumValue,
                                        isu_ausp.currencyMaximumValue
                                    FROM {ADS_DATABASE_CLEANSED}.isu_ausp
                                        INNER JOIN cabnt
                                              ON isu_ausp.characteristicInternalId = cabnt.internalcharacteristic
                                        LEFT OUTER JOIN ausp
                                              ON isu_ausp.characteristicInternalId = ausp.characteristicInternalId
                                        LEFT OUTER JOIN cawnt
                                              ON ausp.characteristicInternalId = cawnt.internalcharacteristic
                                                  AND ausp.characteristicValueCode = cawnt.charactericValue
                                        WHERE isu_ausp._RecordCurrent = 1 AND isu_ausp._RecordDeleted = 0
                              """)
    
    dummyDimRecDf = spark.createDataFrame([("ISU","-1")], ["sourceSystemCode","deviceNumber"])   
    dfResult = isuDeviceCharacteristicsDf.unionByName(dummyDimRecDf, allowMissingColumns = True)    
    
    #5.Apply schema definition
    schema = StructType([
                            StructField('deviceCharacteristicsSK', StringType(), False),
                            StructField('sourceSystemCode', StringType(), False),
                            StructField('deviceNumber', StringType(), False),
                            StructField('characteristicInternalId', StringType(), False),
                            StructField('classifiedEntityType', StringType(), False),
                            StructField('classTypeCode', StringType(), False),
                            StructField('classType', StringType(), True),
                            StructField('archivingObjectsInternalId', StringType(), False),
                            StructField('characteristicName', StringType(), True),
                            StructField('charactericDescription', StringType(), True),
                            StructField('characteristicValueCode', StringType(), True),
                            StructField('characteristicValueDescription', StringType(), True),
                            StructField('characteristicValueMinimumDate', DateType(), True),
                            StructField('characteristicValueMaximumDate', DateType(), True),
                            StructField('timeMinimumValue', StringType(), True),
                            StructField('timeMaximumValue', StringType(), True),
                            StructField('validFromDate', DateType(), True),
                            StructField('validToDate', DateType(), True),
                            StructField('decimalMinimumValue', FloatType(), True),   
                            StructField('decimalMaximumValue', FloatType(), True),
                            StructField('currencyMinimumValue', StringType(), True),   
                            StructField('currencyMaximumValue', StringType(), True)
                      ])    
    
    return dfResult, schema

# COMMAND ----------

df, schema = getDeviceCharacteristics()
TemplateEtlSCD(df, entity="dimDeviceCharacteristics", businessKey="deviceNumber,characteristicInternalId,classifiedEntityType,classTypeCode,archivingObjectsInternalId", schema=schema)

# COMMAND ----------

dbutils.notebook.exit("1")
