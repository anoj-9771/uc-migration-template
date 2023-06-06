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
                                    WITH isu_ausp AS (
                                            SELECT classificationObjectInternalId,characteristicInternalId,characteristicValueInternalId,
                                                    classifiedEntityType,classTypeCode,classType,archivingObjectsInternalId,characteristicValueCode,
                                                    minimumValue,minimumValueUnit,maximumValue,maximumValueUnit,valueDependencyCode,
                                                    minToleranceValue,maxToleranceValue,toleranceIsPercentFlag,IncrementWithinInterval,
                                                    characteristicAuthor,characteristicChangeNumber,validFromDate,
                                                    validToDate,decimalMinimumValue,decimalMaximumValue,currencyMinimumValue,
                                                    currencyMaximumValue,validFromDate1,validToDate1,timeMinimumValue,timeMaximumValue
                                            FROM (
                                                  SELECT *, RANK() OVER (PARTITION BY classificationObjectInternalId,characteristicInternalId 
                                                                              ORDER BY _DLCleansedZoneTimeStamp DESC
                                                                         ) AS rank
                                                  FROM {ADS_DATABASE_CLEANSED}.isu.ausp ausp 
                                                  WHERE _RecordCurrent = 1 
                                                  ) 
                                            WHERE rank = 1
                                                ),
                                        isu_ausp_cawnt AS (
                                                SELECT isu_ausp.*, isu_cawnt.characteristicValueDescription
                                                 FROM isu_ausp LEFT OUTER JOIN {ADS_DATABASE_CLEANSED}.isu.cawn
                                                     ON isu_ausp.characteristicInternalId = isu_cawn.internalcharacteristic
                                                         AND isu_ausp.characteristicValueCode = isu_cawn.characteristicValue
                                                         AND isu_cawn._RecordCurrent = 1 
                                                 LEFT OUTER JOIN {ADS_DATABASE_CLEANSED}.isu.cawnt
                                                      ON isu_cawn.internalCharacteristic = isu_cawnt.internalcharacteristic
                                                          AND isu_cawn.internalCounter = isu_cawnt.internalCounter 
                                                          AND isu_cawnt._RecordCurrent = 1 
                                                  ),
                                         ausp_cawnt AS (
                                              SELECT classificationObjectInternalId,characteristicInternalId
                                                ,classifiedEntityType,classTypeCode,classType
                                                ,archivingObjectsInternalId
                                                ,CONCAT_WS(' ',SORT_ARRAY(COLLECT_LIST(STRUCT(characteristicValueInternalId,characteristicValueCode))).characteristicValueCode) AS characteristicValueCode
                                                ,CONCAT_WS(' ', SORT_ARRAY(COLLECT_LIST(STRUCT(characteristicValueInternalId,characteristicValueDescription))).characteristicValueDescription) AS characteristicValueDescription
                                                ,validFromDate1,validToDate1
                                                ,timeMinimumValue,timeMaximumValue,validFromDate,validToDate
                                                ,decimalMinimumValue,decimalMaximumValue,currencyMinimumValue,currencyMaximumValue 
                                              FROM isu_ausp_cawnt 
                                              WHERE TRIM(characteristicValueCode) <> "" AND characteristicValueCode IS NOT NULL
                                              GROUP BY classificationObjectInternalId,characteristicInternalId,
                                                    classifiedEntityType,classTypeCode,classType,archivingObjectsInternalId,
                                                    minimumValue,minimumValueUnit,maximumValue,maximumValueUnit,valueDependencyCode,
                                                    minToleranceValue,maxToleranceValue,toleranceIsPercentFlag,IncrementWithinInterval,
                                                    characteristicAuthor,characteristicChangeNumber,validFromDate,
                                                    validToDate,decimalMinimumValue,decimalMaximumValue,currencyMinimumValue,
                                                    currencyMaximumValue,validFromDate1,validToDate1,timeMinimumValue,timeMaximumValue
                                                 ),
                                         ausp_cabnt_cawnt AS (
                                              SELECT DISTINCT ausp_cawnt.*, isu_cabn.characteristicName, isu_cabnt.characteristicDescription 
                                              FROM ausp_cawnt INNER JOIN {ADS_DATABASE_CLEANSED}.isu.cabn
                                                  ON ausp_cawnt.characteristicInternalId = isu_cabn.internalcharacteristic
                                                      AND isu_cabn._RecordCurrent = 1 
                                              LEFT OUTER JOIN {ADS_DATABASE_CLEANSED}.isu.cabnt
                                                  ON isu_cabn.internalcharacteristic = isu_cabnt.internalcharacteristic
                                                      AND isu_cabn.internalCounterforArchivingObjectsbyECM = isu_cabnt.internalCounterforArchivingObjectsbyECM
                                                      AND isu_cabnt._RecordCurrent = 1 
                                                WHERE isu_cabn.characteristicName IN ('MTR_GRID_LOCATION_CODE', 'MR_LOCATION', 'COMMON_AREA_METER', 'MLOC_LEVEL', 'MLOC_DESC', 'MTR_PROPERTY_POSITION_ID', 'MTR_READ_GRID_LOCATION_CODE', 'RFDEVICEMODEL', 'RFDEVICEMAKER', 'RF_ID', 'WARNINGNOTES', 'MTR_ACCESS_NOTE', 'MTR_READING_NOTE', 'METER_SERVES', 'SO_COMMENTS', 'COMMENTS', 'METER_COMPLETION_NOTES', 'SIM_ITEM_NO', 'PRJ_NUM', 'IOTFIRMWARE', 'ADDITIONAL_INFO', 'CANT_DO_CODE', 'BARCODE', 'CENTRALISED_HOTWATER',
                'MLIM_SYSTEM', 'SITE_ID', 'TAP_TESTED', 'IOT_TYPE', 'IOT_NETWORK', 'IOT_NETWORK_PROVIDER', 'IOT_PRESSURE_SENSOR', 'IOT_PROGRAM_CATEGORY', 'IOT_PROGRAM_CATEGORY_2',
                'IOT_PROGRAM_CATEGORY_3', 'IOT_METER', 'METER_GPS_LAT_LONG_SOURCE', 'METER_COUPLINGS', 'IOT_RADIO_MODEL', 'PULSE_SENSOR', 'PULSE_SPLITTER',
                'MLOC_GPS_LAT', 'MLOC_GPS_LON', 'MR_GPS_LAT', 'MR_GPS_LON', 'METER_OFFSET', 'IOT_MULTIPLIER', 'IOT_HIGH_DAILY_CONS', 'IOT_LEAK_ALARM_THRESHOLD', 'NUMBER_OF_BEDROOMS',
                 'CERTIFICATION_DATE', 'IOT_RADIO_FIT_DATE', 'METER_GPS_DATE_CAPTURED', 'TOTAL_REVENUE_COST')
                                                 )
                                    SELECT 'ISU' AS sourceSystemCode,
                                        classificationObjectInternalId AS deviceNumber,
                                        characteristicInternalId,
                                        classifiedEntityType,
                                        classTypeCode,
                                        classType,
                                        archivingObjectsInternalId,
                                        characteristicName,
                                        characteristicDescription,
                                        characteristicValueCode,
                                        characteristicValueDescription,
                                        validFromDate1 AS characteristicValueMinimumDate,
                                        validToDate1 AS characteristicValueMaximumDate,
                                        timeMinimumValue,
                                        timeMaximumValue,
                                        validFromDate,
                                        validToDate,
                                        decimalMinimumValue,
                                        decimalMaximumValue,
                                        currencyMinimumValue,
                                        currencyMaximumValue
                                    FROM ausp_cabnt_cawnt
                              """)
    
    dummyDimRecDf = spark.createDataFrame([("-1","-1","-1","-1","-1")], ["deviceNumber","characteristicInternalId","classifiedEntityType","classTypeCode","archivingObjectsInternalId"])   
    dfResult = isuDeviceCharacteristicsDf.unionByName(dummyDimRecDf, allowMissingColumns = True)    
    
    #5.Apply schema definition
    schema = StructType([
                            StructField('deviceCharacteristicsSK', StringType(), False),
                            StructField('sourceSystemCode', StringType(), True),
                            StructField('deviceNumber', StringType(), False),
                            StructField('characteristicInternalId', StringType(), False),
                            StructField('classifiedEntityType', StringType(), False),
                            StructField('classTypeCode', StringType(), False),
                            StructField('classType', StringType(), True),
                            StructField('archivingObjectsInternalId', StringType(), False),
                            StructField('characteristicName', StringType(), True),
                            StructField('characteristicDescription', StringType(), True),
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

curnt_table = f'{ADS_DATABASE_CURATED}.dim.deviceCharacteristics'
curnt_pk = 'deviceNumber,characteristicInternalId,classifiedEntityType,classTypeCode,archivingObjectsInternalId' 
curnt_recordStart_pk = 'deviceNumber'
history_table = f'{ADS_DATABASE_CURATED}.dim.deviceHistory'
history_table_pk = 'deviceNumber'
history_table_pk_convert = 'deviceNumber'

df_ = appendRecordStartFromHistoryTable(df,history_table,history_table_pk,curnt_pk,history_table_pk_convert,curnt_recordStart_pk)
updateDBTableWithLatestRecordStart(df_, curnt_table, curnt_pk)

TemplateEtlSCD(df_, entity="dim.deviceCharacteristics", businessKey="deviceNumber,characteristicInternalId,classifiedEntityType,classTypeCode,archivingObjectsInternalId", schema=schema)

# COMMAND ----------

#dbutils.notebook.exit("1")
