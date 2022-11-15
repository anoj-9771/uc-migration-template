# Databricks notebook source
# MAGIC %run ../../atf-common

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from curated_v2.dimDeviceCharacteristics

# COMMAND ----------

# DBTITLE 1,[Target] Schema Check
target_df = spark.sql("select * from curated_v2.dimDeviceCharacteristics")
target_df.printSchema()

# COMMAND ----------

# DBTITLE 1,Source with mapping for active and deleted records
source_isu = spark.sql("""
WITH isu_ausp AS (
                                            SELECT classificationObjectInternalId,characteristicInternalId,characteristicValueInternalId,
                                                    classifiedEntityType,classTypeCode,classType,archivingObjectsInternalId,characteristicValueCode,
                                                    minimumValue,minimumValueUnit,maximumValue,maximumValueUnit,valueDependencyCode,
                                                    minToleranceValue,maxToleranceValue,toleranceIsPercentFlag,IncrementWithinInterval,
                                                    characteristicAuthor,characteristicChangeNumber,validFromDate,isDeletedFlag,
                                                    validToDate,decimalMinimumValue,decimalMaximumValue,currencyMinimumValue,
                                                    currencyMaximumValue,validFromDate1,validToDate1,timeMinimumValue,timeMaximumValue,_RecordCurrent,_RecordDeleted
                                            FROM (
                                                  SELECT *, RANK() OVER (PARTITION BY classificationObjectInternalId,characteristicInternalId 
                                                                              ORDER BY _DLCleansedZoneTimeStamp DESC
                                                                         ) AS rank
                                                  FROM cleansed.isu_ausp ausp 
                                                 ) 
                                            WHERE rank = 1
                                                ),
                                        isu_ausp_cawnt AS (
                                                SELECT isu_ausp.*, isu_cawnt.characteristicValueDescription
                                                 FROM isu_ausp LEFT OUTER JOIN cleansed.isu_cawn
                                                     ON isu_ausp.characteristicInternalId = isu_cawn.internalcharacteristic
                                                         AND isu_ausp.characteristicValueCode = isu_cawn.characteristicValue
                                                         AND isu_cawn._RecordCurrent = 1 AND isu_cawn._RecordDeleted = 0
                                                 LEFT OUTER JOIN cleansed.isu_cawnt
                                                      ON isu_cawn.internalCharacteristic = isu_cawnt.internalcharacteristic
                                                          AND isu_cawn.internalCounter = isu_cawnt.internalCounter 
                                                 ),
                                         ausp_cawnt AS (
                                              SELECT classificationObjectInternalId,characteristicInternalId
                                                ,classifiedEntityType,classTypeCode,classType
                                                ,archivingObjectsInternalId
                                                ,CONCAT_WS(' ',SORT_ARRAY(COLLECT_LIST(STRUCT(characteristicValueInternalId,characteristicValueCode))).characteristicValueCode) AS characteristicValueCode
                                                ,CONCAT_WS(' ', SORT_ARRAY(COLLECT_LIST(STRUCT(characteristicValueInternalId,characteristicValueDescription))).characteristicValueDescription) AS characteristicValueDescription
                                                ,validFromDate1,validToDate1
                                                ,timeMinimumValue,timeMaximumValue,validFromDate,validToDate
                                                ,decimalMinimumValue,decimalMaximumValue,currencyMinimumValue,currencyMaximumValue,_RecordCurrent,_RecordDeleted
                                              FROM isu_ausp_cawnt 
                                              GROUP BY classificationObjectInternalId,characteristicInternalId,
                                                    classifiedEntityType,classTypeCode,classType,archivingObjectsInternalId,
                                                    minimumValue,minimumValueUnit,maximumValue,maximumValueUnit,valueDependencyCode,
                                                    minToleranceValue,maxToleranceValue,toleranceIsPercentFlag,IncrementWithinInterval,
                                                    characteristicAuthor,characteristicChangeNumber,validFromDate,isDeletedFlag,
                                                    validToDate,decimalMinimumValue,decimalMaximumValue,currencyMinimumValue,
                                                    currencyMaximumValue,validFromDate1,validToDate1,timeMinimumValue,timeMaximumValue,_RecordCurrent,
                                        _RecordDeleted
                                                 ),
                                         ausp_cabnt_cawnt AS (
                                              SELECT DISTINCT ausp_cawnt.*, isu_cabn.characteristicName, isu_cabnt.characteristicDescription 
                                              FROM ausp_cawnt INNER JOIN cleansed.isu_cabn
                                                  ON ausp_cawnt.characteristicInternalId = isu_cabn.internalcharacteristic
                                                      AND isu_cabn._RecordCurrent = 1 and  isu_cabn._RecordDeleted = 0
                                              LEFT OUTER JOIN cleansed.isu_cabnt
                                                  ON isu_cabn.internalcharacteristic = isu_cabnt.internalcharacteristic
                                                      AND isu_cabn.internalCounterforArchivingObjectsbyECM = isu_cabnt.internalCounterforArchivingObjectsbyECM
                                                      AND isu_cabnt._RecordCurrent = 1 and isu_cabnt._RecordDeleted = 0
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
                                        currencyMaximumValue,
                                        validFromDate as _RecordStart,
                                        validToDate as _RecordEnd,
                                        _RecordCurrent,
                                        _RecordDeleted
                                    FROM ausp_cabnt_cawnt
""")
source_isu.createOrReplaceTempView("source_view")
#display(source_isu)
#source_isu.count()

src_a = spark.sql("Select * except(_RecordCurrent,_recordDeleted) from source_view where _RecordCurrent=1 and _recordDeleted=0 ")
src_d = spark.sql("Select * except(_RecordCurrent,_recordDeleted) from source_view where _RecordCurrent=0 and _recordDeleted=1 ")
src_a.createOrReplaceTempView("src_a")
src_d.createOrReplaceTempView("src_d")


# COMMAND ----------

# DBTITLE 1,Define Variables for ATF
keyColumns = 'deviceNumber,characteristicInternalId,classifiedEntityType,classTypeCode,archivingObjectsInternalId'
mandatoryColumns = 'deviceNumber,characteristicInternalId,classifiedEntityType,classTypeCode,archivingObjectsInternalId'

columns = ("""
sourceSystemCode
, deviceNumber
, characteristicInternalId
, classifiedEntityType
, classTypeCode
, classType
, archivingObjectsInternalId
, characteristicName
, characteristicDescription
, characteristicValueCode
, characteristicValueDescription
, characteristicValueMinimumDate
, characteristicValueMaximumDate
, timeMinimumValue
, timeMaximumValue
, validFromDate
, validToDate
, decimalMinimumValue
, decimalMaximumValue
, currencyMinimumValue
, currencyMaximumValue
""")

source_a = spark.sql(f"""
Select {columns}
From src_a
""")

source_d = spark.sql(f"""
Select {columns}
From src_d
""")

# COMMAND ----------

#ALWAYS RUN THIS AT THE END
RunTests()

# COMMAND ----------

# MAGIC %md
# MAGIC #Investigation for failed tests

# COMMAND ----------

# MAGIC %sql
# MAGIC Select deviceNumber,characteristicInternalId,classifiedEntityType,classTypeCode,archivingObjectsInternalId, count(*) from curated_v2.dimDeviceCharacteristics group by deviceNumber,characteristicInternalId,classifiedEntityType,classTypeCode,archivingObjectsInternalId,_RecordStart having count(*) > 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cleansed.isu_ausp where classificationObjectInternalId = '000000000011872141'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT length(archivingObjectsInternalId) from curated_v2.dimDeviceCharacteristics
# MAGIC                       WHERE length(archivingObjectsInternalId)<>2

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from curated_v2.dimDeviceCharacteristics
# MAGIC                       WHERE length(classifiedEntityType) = 7

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct characteristicValueDescription from curated_v2.dimDeviceCharacteristics

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from curated_v2.dimDeviceCharacteristics 
# MAGIC --where characteristicValueCode = lower(characteristicValueCode) and characteristicValueCode != binary(characteristicValueCode)

# COMMAND ----------

# MAGIC %sql
# MAGIC                                            select classificationObjectInternalId,characteristicInternalId,characteristicValueInternalId,characteristicValueCode,rank from ( SELECT classificationObjectInternalId,characteristicInternalId,characteristicValueInternalId,
# MAGIC                                                     classifiedEntityType,classTypeCode,classType,archivingObjectsInternalId,characteristicValueCode,
# MAGIC                                                     minimumValue,minimumValueUnit,maximumValue,maximumValueUnit,valueDependencyCode,
# MAGIC                                                     minToleranceValue,maxToleranceValue,toleranceIsPercentFlag,IncrementWithinInterval,
# MAGIC                                                     characteristicAuthor,characteristicChangeNumber,validFromDate,isDeletedFlag,
# MAGIC                                                     validToDate,decimalMinimumValue,decimalMaximumValue,currencyMinimumValue,
# MAGIC                                                     currencyMaximumValue,validFromDate1,validToDate1,timeMinimumValue,timeMaximumValue,_RecordCurrent,_RecordDeleted,_DLCleansedZoneTimeStamp, rank
# MAGIC                                             FROM (
# MAGIC                                                   SELECT *, rank() OVER (PARTITION BY classificationObjectInternalId,characteristicInternalId 
# MAGIC                                                                               ORDER BY _DLCleansedZoneTimeStamp DESC
# MAGIC                                                                          ) AS rank
# MAGIC                                                   FROM cleansed.isu_ausp ausp 
# MAGIC                                                   ))  WHERE classificationObjectInternalId in ('000000000010477493') and
# MAGIC characteristicInternalId in ('0000000698')

# COMMAND ----------

# MAGIC %sql
# MAGIC select deviceNumber,characteristicInternalId,_DLCuratedZoneTimeStamp,characteristicValueCode from curated_v2.dimdevicecharacteristics where deviceNumber = '000000000010477493'and characteristicInternalId = '0000000698'

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH isu_ausp AS (
# MAGIC                                             SELECT classificationObjectInternalId,characteristicInternalId,characteristicValueInternalId,
# MAGIC                                                     classifiedEntityType,classTypeCode,classType,archivingObjectsInternalId,characteristicValueCode,
# MAGIC                                                     minimumValue,minimumValueUnit,maximumValue,maximumValueUnit,valueDependencyCode,
# MAGIC                                                     minToleranceValue,maxToleranceValue,toleranceIsPercentFlag,IncrementWithinInterval,
# MAGIC                                                     characteristicAuthor,characteristicChangeNumber,validFromDate,isDeletedFlag,
# MAGIC                                                     validToDate,decimalMinimumValue,decimalMaximumValue,currencyMinimumValue,
# MAGIC                                                     currencyMaximumValue,validFromDate1,validToDate1,timeMinimumValue,timeMaximumValue,_RecordCurrent,_RecordDeleted
# MAGIC                                             FROM (
# MAGIC                                                   SELECT *, RANK() OVER (PARTITION BY classificationObjectInternalId,characteristicInternalId 
# MAGIC                                                                               ORDER BY _DLCleansedZoneTimeStamp DESC
# MAGIC                                                                          ) AS rank
# MAGIC                                                   FROM cleansed.isu_ausp ausp 
# MAGIC                                                  ) 
# MAGIC                                             WHERE rank = 1
# MAGIC                                                 ),
# MAGIC                                         isu_ausp_cawnt AS (
# MAGIC                                                 SELECT isu_ausp.*, isu_cawnt.characteristicValueDescription
# MAGIC                                                  FROM isu_ausp LEFT OUTER JOIN cleansed.isu_cawn
# MAGIC                                                      ON isu_ausp.characteristicInternalId = isu_cawn.internalcharacteristic
# MAGIC                                                          AND isu_ausp.characteristicValueCode = isu_cawn.characteristicValue
# MAGIC                                                          LEFT OUTER JOIN cleansed.isu_cawnt
# MAGIC                                                       ON isu_cawn.internalCharacteristic = isu_cawnt.internalcharacteristic
# MAGIC                                                           AND isu_cawn.internalCounter = isu_cawnt.internalCounter 
# MAGIC                                                  ),                                             
# MAGIC                                              t2 as ( SELECT characteristicValueCode,characteristicValueDescription
# MAGIC                                                 , count(*)                                               
# MAGIC                                               FROM isu_ausp_cawnt 
# MAGIC                                               GROUP BY characteristicValueCode,characteristicValueDescription
# MAGIC                                                         having count(*)>1)
# MAGIC --                                                         select * from t2
# MAGIC --                                                       select * from t1 where characteristicValueCode = '3419157-1' --and t1.characteristicValueDescription is null
# MAGIC                                                       SELECT characteristicValueInternalId,characteristicValueCode,characteristicValueDescription
# MAGIC                                                 ,CONCAT_WS(' ',SORT_ARRAY(COLLECT_LIST(STRUCT(characteristicValueInternalId,characteristicValueCode))).characteristicValueCode) AS characteristicValueCode
# MAGIC                                                 ,CONCAT_WS(' ', SORT_ARRAY(COLLECT_LIST(STRUCT(characteristicValueInternalId,characteristicValueDescription))).characteristicValueDescription) AS characteristicValueDescription
# MAGIC                                                
# MAGIC                                               FROM isu_ausp_cawnt
# MAGIC                                               where characteristicValueCode = '2m In'---'3419157-1'
# MAGIC                                               GROUP BY characteristicValueInternalId,characteristicValueCode,characteristicValueDescription

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT struct('Spark', 5);
