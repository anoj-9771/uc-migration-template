# Databricks notebook source
#config parameters
source = 'ISU' #either CRM or ISU
table = '0UC_MTR_DOC'

environment = 'test'
storage_account_name = "sablobdaftest01"
storage_account_access_key = dbutils.secrets.get(scope="TestScope",key="test-sablob-key")
containerName = "archive"


# COMMAND ----------

# MAGIC %run ../../includes/tableEvaluation

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC distinct EXTRACT_DATETIME from test.${vars.table}

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC *  from test.${vars.table}

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC MRESULT
# MAGIC ,ZPREV_MRESULT
# MAGIC ,ZADATTATS
# MAGIC ,ZPREV_ADT
# MAGIC  from test.${vars.table}

# COMMAND ----------

# DBTITLE 1,Source with correct mapping
# MAGIC %sql
# MAGIC select
# MAGIC ABLBELNR as meterReadingId
# MAGIC ,EQUNR as equipmentNumber
# MAGIC ,ZWNUMMER as registerNumber
# MAGIC ,ADAT as meterReadingDate
# MAGIC ,MRESULT as meterReadingTaken
# MAGIC ,MR_BILL  as duplicate
# MAGIC ,AKTIV as meterReadingActive
# MAGIC ,ADATSOLL as scheduledMeterReadingDate
# MAGIC ,ABLSTAT as meterReadingStatus
# MAGIC ,ABLHINW as notefromMeterReader
# MAGIC ,ABLESART as scheduledMeterReadingCategory
# MAGIC ,ABLESER as meterReaderNumber
# MAGIC ,MDEUPL as orderHasBeenOutput 
# MAGIC ,ISTABLART as meterReadingType 
# MAGIC ,ABLESTYP as meterReadingCategory 
# MAGIC ,MASSREAD as unitOfMeasurementMeterReading 
# MAGIC ,UPDMOD as bwDeltaProcess
# MAGIC ,LOEVM as deletedIndicator 
# MAGIC ,PRUEFPKT as independentValidation
# MAGIC ,POPCODE as dependentValidation
# MAGIC ,AMS as advancedMeteringSystem
# MAGIC ,TRANSSTAT as transferStatusCode 
# MAGIC ,TRANSTSTAMP as timeStamp
# MAGIC ,SOURCESYST as sourceSystemOrigin
# MAGIC ,ZPREV_ADT as actualPreviousmeterReadingDate
# MAGIC ,ZPREV_MRESULT as meterPreviousReadingTaken
# MAGIC ,ZZ_PHOTO_IND as meterPhotoIndicator
# MAGIC ,ZZ_FREE_TEXT as freeText
# MAGIC ,ZZ_COMM_CODE as meterReadingCommentCode
# MAGIC ,ZZ_NO_READ_CODE as noReadCode
# MAGIC ,ZGERNR as DeviceNumber
# MAGIC ,ZADATTATS as actualMeterReadingDate
# MAGIC ,ZWNABR as registerNotRelevantToBilling
# MAGIC ,AEDAT as lastChangedDate
# MAGIC from test.${vars.table}

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC ZZ_PHOTO_IND as meterPhotoIndicator
# MAGIC --ZZ_PHOTO_IND as meterPhotoIndicator
# MAGIC --as meterPhotoIndicator
# MAGIC from test.${vars.table}

# COMMAND ----------

# DBTITLE 1,Source with corrected mapping and outer query
# MAGIC %sql
# MAGIC select
# MAGIC meterReadingId
# MAGIC ,equipmentNumber
# MAGIC ,registerNumber
# MAGIC ,meterReadingDate
# MAGIC ,meterReadingTaken
# MAGIC ,duplicate
# MAGIC ,meterReadingActive
# MAGIC ,scheduledMeterReadingDate
# MAGIC ,meterReadingStatus
# MAGIC ,notefromMeterReader
# MAGIC ,scheduledMeterReadingCategory
# MAGIC ,meterReaderNumber
# MAGIC ,orderHasBeenOutput
# MAGIC ,meterReadingType
# MAGIC ,meterReadingCategory
# MAGIC ,unitOfMeasurementMeterReading
# MAGIC ,bwDeltaProcess
# MAGIC ,deletedIndicator
# MAGIC ,independentValidation
# MAGIC ,dependentValidation
# MAGIC ,advancedMeteringSystem
# MAGIC ,transferStatusCode
# MAGIC ,timeStamp
# MAGIC ,sourceSystemOrigin
# MAGIC ,actualPreviousmeterReadingDate
# MAGIC ,meterPreviousReadingTaken
# MAGIC --,meterPhotoIndicator 
# MAGIC ,freeText 
# MAGIC ,meterReadingCommentCode 
# MAGIC ,noReadCode 
# MAGIC ,DeviceNumber
# MAGIC ,actualMeterReadingDate 
# MAGIC ,registerNotRelevantToBilling 
# MAGIC ,lastChangedDate
# MAGIC from
# MAGIC (select
# MAGIC ABLBELNR as meterReadingId
# MAGIC ,EQUNR as equipmentNumber
# MAGIC ,ZWNUMMER as registerNumber
# MAGIC ,ADAT as meterReadingDate
# MAGIC ,MRESULT as meterReadingTaken
# MAGIC ,MR_BILL  as duplicate
# MAGIC ,AKTIV as meterReadingActive
# MAGIC ,ADATSOLL as scheduledMeterReadingDate
# MAGIC ,ABLSTAT as meterReadingStatus
# MAGIC ,ABLHINW as notefromMeterReader
# MAGIC ,ABLESART as scheduledMeterReadingCategory
# MAGIC ,ABLESER as meterReaderNumber
# MAGIC ,MDEUPL as orderHasBeenOutput 
# MAGIC ,ISTABLART as meterReadingType 
# MAGIC ,ABLESTYP as meterReadingCategory 
# MAGIC ,MASSREAD as unitOfMeasurementMeterReading 
# MAGIC ,UPDMOD as bwDeltaProcess
# MAGIC ,LOEVM as deletedIndicator 
# MAGIC ,PRUEFPKT as independentValidation
# MAGIC ,POPCODE as dependentValidation
# MAGIC ,AMS as advancedMeteringSystem
# MAGIC ,TRANSSTAT as transferStatusCode 
# MAGIC ,TRANSTSTAMP as timeStamp
# MAGIC ,SOURCESYST as sourceSystemOrigin
# MAGIC ,ZPREV_ADT as actualPreviousmeterReadingDate
# MAGIC ,ZPREV_MRESULT as meterPreviousReadingTaken
# MAGIC ,ZZ_PHOTO_IND as meterPhotoIndicator
# MAGIC ,ZZ_FREE_TEXT as freeText
# MAGIC ,ZZ_COMM_CODE as meterReadingCommentCode
# MAGIC ,ZZ_NO_READ_CODE as noReadCode
# MAGIC ,ZGERNR as DeviceNumber
# MAGIC ,ZADATTATS as actualMeterReadingDate
# MAGIC ,ZWNABR as registerNotRelevantToBilling
# MAGIC ,AEDAT as lastChangedDate
# MAGIC ,row_number() over (partition by ABLBELNR,EQUNR order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table}
# MAGIC )a where  a.rn = 1

# COMMAND ----------

# DBTITLE 1,[Source] with mapping
# MAGIC %sql
# MAGIC select
# MAGIC meterReadingId
# MAGIC ,equipmentNumber
# MAGIC ,registerNumber
# MAGIC ,meterReadingDate
# MAGIC ,meterReadingTaken  
# MAGIC ,duplicate
# MAGIC ,meterReadingActive 
# MAGIC ,scheduledMeterReadingDate 
# MAGIC ,meterReadingStatus 
# MAGIC ,notefromMeterReader 
# MAGIC ,scheduledMeterReadingCategory 
# MAGIC ,meterReaderNumber 
# MAGIC ,orderHasBeenOutput 
# MAGIC ,meterReadingType 
# MAGIC ,meterReadingCategory 
# MAGIC ,unitOfMeasurementMeterReading 
# MAGIC ,bwDeltaProcess
# MAGIC ,deletedIndicator 
# MAGIC ,independentValidation 
# MAGIC ,dependentValidation 
# MAGIC ,advancedMeteringSystem 
# MAGIC ,transferStatusCode 
# MAGIC ,timeStamp
# MAGIC ,sourceSystemOrigin
# MAGIC ,actualmeterReadingDate 
# MAGIC ,meterReadingTaken 
# MAGIC ,meterPhotoIndicator 
# MAGIC ,freeText 
# MAGIC ,meterReadingCommentCode 
# MAGIC ,noReadCode 
# MAGIC ,DeviceNumber
# MAGIC ,actualMeterReadingDate 
# MAGIC ,registerNotRelevantToBilling 
# MAGIC ,lastChangedDate
# MAGIC from
# MAGIC (select
# MAGIC ABLBELNR as meterReadingId
# MAGIC ,EQUNR as equipmentNumber
# MAGIC ,ZWNUMMER as registerNumber
# MAGIC ,ADAT as meterReadingDate
# MAGIC ,MRESULT as meterReadingTaken
# MAGIC ,MR_BILL  as duplicate
# MAGIC ,AKTIV as meterReadingActive
# MAGIC ,ADATSOLL as scheduledMeterReadingDate
# MAGIC ,ABLSTAT as meterReadingStatus
# MAGIC ,ABLHINW as notefromMeterReader
# MAGIC ,ABLESART as scheduledMeterReadingCategory
# MAGIC ,ABLESER as meterReaderNumber
# MAGIC ,MDEUPL as orderHasBeenOutput 
# MAGIC ,ISTABLART as meterReadingType 
# MAGIC ,ABLESTYP as meterReadingCategory 
# MAGIC ,MASSREAD as unitOfMeasurementMeterReading 
# MAGIC ,UPDMOD as bwDeltaProcess
# MAGIC ,LOEVM as deletedIndicator 
# MAGIC ,PRUEFPKT as independentValidation
# MAGIC ,POPCODE as dependentValidation
# MAGIC ,AMS as advancedMeteringSystem
# MAGIC ,TRANSSTAT as transferStatusCode 
# MAGIC ,TRANSTSTAMP as timeStamp
# MAGIC ,SOURCESYST as sourceSystemOrigin
# MAGIC ,ZPREV_ADT as actualmeterReadingDate
# MAGIC ,ZPREV_MRESULT as meterReadingTaken
# MAGIC ,ZZ_PHOTO_IND as meterPhotoIndicator
# MAGIC ,ZZ_FREE_TEXT as freeText
# MAGIC ,ZZ_COMM_CODE as meterReadingCommentCode
# MAGIC ,ZZ_NO_READ_CODE as noReadCode
# MAGIC ,ZGERNR as DeviceNumber
# MAGIC ,ZADATTATS as actualMeterReadingDate
# MAGIC ,ZWNABR as registerNotRelevantToBilling
# MAGIC ,AEDAT as lastChangedDate
# MAGIC from test.${vars.table}
# MAGIC ,row_number() over (partition by ABLBELNR,EQUNR order by EXTRACT_DATETIME desc) as rn
# MAGIC )a where  a.rn = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC ABLBELNR as meterReadingId
# MAGIC ,EQUNR as equipmentNumber
# MAGIC ,ZWNUMMER as registerNumber
# MAGIC ,ADAT as meterReadingDate
# MAGIC ,MRESULT as meterReadingTaken 
# MAGIC ,MR_BILL as duplicate
# MAGIC ,AKTIV as meterReadingActive 
# MAGIC ,ADATSOLL as scheduledMeterReadingDate 
# MAGIC ,ABLSTAT as meterReadingStatus 
# MAGIC ,ABLHINW as notefromMeterReader 
# MAGIC ,ABLESART as scheduledMeterReadingCategory 
# MAGIC ,ABLESER as meterReaderNumber 
# MAGIC ,MDEUPL as orderHasBeenOutput 
# MAGIC ,ISTABLART as meterReadingType 
# MAGIC ,ABLESTYP as meterReadingCategory 
# MAGIC ,MASSREAD as unitOfMeasurementMeterReading 
# MAGIC ,UPDMOD as bwDeltaProcess
# MAGIC ,LOEVM as deletedIndicator 
# MAGIC ,PRUEFPKT as independentValidation 
# MAGIC ,POPCODE as dependentValidation 
# MAGIC ,AMS as advancedMeteringSystem 
# MAGIC ,TRANSSTAT as transferStatusCode 
# MAGIC ,TRANSTSTAMP as timeStamp
# MAGIC ,SOURCESYST as sourceSystemOrigin
# MAGIC ,ZPREV_ADT as actualmeterReadingDate 
# MAGIC ,ZPREV_MRESULT as meterReadingTaken 
# MAGIC ,ZZ_PHOTO_IND as meterPhotoIndicator 
# MAGIC ,ZZ_FREE_TEXT as freeText 
# MAGIC ,ZZ_COMM_CODE as meterReadingCommentCode 
# MAGIC ,ZZ_NO_READ_CODE as noReadCode 
# MAGIC ,ZGERNR as DeviceNumber
# MAGIC ,ZADATTATS as actualMeterReadingDate 
# MAGIC ,ZWNABR as registerNotRelevantToBilling 
# MAGIC ,AEDAT as lastChangedDate
# MAGIC ,row_number() over (partition by ABLBELNR,EQUNR order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table}
# MAGIC --)a where  a.rn = 1

# COMMAND ----------

# DBTITLE 1,[Verification] Count Check
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from cleansed.${vars.table}
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from (select
# MAGIC ABLBELNR as meterReadingId
# MAGIC ,EQUNR as equipmentNumber
# MAGIC ,ZWNUMMER as registerNumber
# MAGIC ,ADAT as meterReadingDate
# MAGIC ,MRESULT as meterReadingTaken
# MAGIC ,MR_BILL  as duplicate
# MAGIC ,AKTIV as meterReadingActive
# MAGIC ,ADATSOLL as scheduledMeterReadingDate
# MAGIC ,ABLSTAT as meterReadingStatus
# MAGIC ,ABLHINW as notefromMeterReader
# MAGIC ,ABLESART as scheduledMeterReadingCategory
# MAGIC ,ABLESER as meterReaderNumber
# MAGIC ,MDEUPL as orderHasBeenOutput 
# MAGIC ,ISTABLART as meterReadingType 
# MAGIC ,ABLESTYP as meterReadingCategory 
# MAGIC ,MASSREAD as unitOfMeasurementMeterReading 
# MAGIC ,UPDMOD as bwDeltaProcess
# MAGIC ,LOEVM as deletedIndicator 
# MAGIC ,PRUEFPKT as independentValidation
# MAGIC ,POPCODE as dependentValidation
# MAGIC ,AMS as advancedMeteringSystem
# MAGIC ,TRANSSTAT as transferStatusCode 
# MAGIC ,TRANSTSTAMP as timeStamp
# MAGIC ,SOURCESYST as sourceSystemOrigin
# MAGIC ,ZPREV_ADT as actualPreviousmeterReadingDate
# MAGIC ,ZPREV_MRESULT as meterPreviousReadingTaken
# MAGIC ,ZZ_PHOTO_IND as meterPhotoIndicator
# MAGIC ,ZZ_FREE_TEXT as freeText
# MAGIC ,ZZ_COMM_CODE as meterReadingCommentCode
# MAGIC ,ZZ_NO_READ_CODE as noReadCode
# MAGIC ,ZGERNR as DeviceNumber
# MAGIC ,ZADATTATS as actualMeterReadingDate
# MAGIC ,ZWNABR as registerNotRelevantToBilling
# MAGIC ,AEDAT as lastChangedDate
# MAGIC from test.${vars.table})

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT meterReadingId,equipmentNumber
# MAGIC , COUNT (*) as count
# MAGIC FROM cleansed.${vars.table}
# MAGIC GROUP BY meterReadingId,equipmentNumber
# MAGIC HAVING COUNT (*) > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT * FROM (
# MAGIC SELECT
# MAGIC *,
# MAGIC row_number() OVER(PARTITION BY meterReadingId,equipmentNumber  order by meterReadingId,equipmentNumber) as rn
# MAGIC FROM  cleansed.${vars.table}
# MAGIC )a where a.rn > 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select meterReadingTaken from cleansed.${vars.table}

# COMMAND ----------

# DBTITLE 1,target query
# MAGIC %sql
# MAGIC select 
# MAGIC meterReadingId
# MAGIC ,equipmentNumber
# MAGIC ,registerNumber
# MAGIC ,meterReadingDate
# MAGIC ,meterReadingTaken
# MAGIC ,duplicate
# MAGIC ,meterReadingActive
# MAGIC ,scheduledMeterReadingDate
# MAGIC ,meterReadingStatus
# MAGIC ,notefromMeterReader
# MAGIC ,scheduledMeterReadingCategory
# MAGIC ,meterReaderNumber
# MAGIC ,scheduledMeterReadingCategory
# MAGIC from cleansed.${vars.table}

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC actualPreviousmeterReadingDate
# MAGIC ,meterPreviousReadingTaken
# MAGIC from
# MAGIC from cleansed.${vars.table}

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC meterReadingId
# MAGIC ,equipmentNumber
# MAGIC ,registerNumber
# MAGIC ,meterReadingDate
# MAGIC ,meterReadingTaken
# MAGIC ,duplicate
# MAGIC ,meterReadingActive
# MAGIC ,scheduledMeterReadingDate
# MAGIC ,meterReadingStatus
# MAGIC ,notefromMeterReader
# MAGIC ,scheduledMeterReadingCategory
# MAGIC ,meterReaderNumber
# MAGIC ,orderHasBeenOutput
# MAGIC ,meterReadingType
# MAGIC ,meterReadingCategory
# MAGIC ,unitOfMeasurementMeterReading
# MAGIC ,bwDeltaProcess
# MAGIC ,deletedIndicator
# MAGIC ,independentValidation
# MAGIC ,dependentValidation
# MAGIC ,advancedMeteringSystem
# MAGIC ,transferStatusCode
# MAGIC ,timeStamp
# MAGIC ,sourceSystemOrigin
# MAGIC ,actualPreviousmeterReadingDate
# MAGIC ,meterPreviousReadingTaken
# MAGIC ,meterPhotoIndicator
# MAGIC ,freeText
# MAGIC ,meterReadingCommentCode
# MAGIC ,noReadCode
# MAGIC ,DeviceNumber
# MAGIC ,actualMeterReadingDate
# MAGIC ,registerNotRelevantToBilling
# MAGIC ,lastChangedDate
# MAGIC from
# MAGIC cleansed.${vars.table}

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Source and Target Data
# MAGIC %sql
# MAGIC select
# MAGIC ABLBELNR as meterReadingId
# MAGIC ,EQUNR as equipmentNumber
# MAGIC ,ZWNUMMER as registerNumber
# MAGIC ,ADAT as meterReadingDate
# MAGIC ,MRESULT as meterReadingTaken
# MAGIC ,MR_BILL  as duplicate
# MAGIC ,AKTIV as meterReadingActive
# MAGIC ,ADATSOLL as scheduledMeterReadingDate
# MAGIC ,ABLSTAT as meterReadingStatus
# MAGIC ,ABLHINW as notefromMeterReader
# MAGIC ,ABLESART as scheduledMeterReadingCategory
# MAGIC ,ABLESER as meterReaderNumber
# MAGIC ,MDEUPL as orderHasBeenOutput 
# MAGIC ,ISTABLART as meterReadingType 
# MAGIC ,ABLESTYP as meterReadingCategory 
# MAGIC ,MASSREAD as unitOfMeasurementMeterReading 
# MAGIC ,UPDMOD as bwDeltaProcess
# MAGIC ,LOEVM as deletedIndicator 
# MAGIC ,PRUEFPKT as independentValidation
# MAGIC ,POPCODE as dependentValidation
# MAGIC ,AMS as advancedMeteringSystem
# MAGIC ,TRANSSTAT as transferStatusCode 
# MAGIC ,TRANSTSTAMP as timeStamp
# MAGIC ,SOURCESYST as sourceSystemOrigin
# MAGIC ,ZPREV_ADT as actualPreviousmeterReadingDate
# MAGIC ,ZPREV_MRESULT as meterPreviousReadingTaken
# MAGIC ,ZZ_PHOTO_IND as meterPhotoIndicator
# MAGIC ,ZZ_FREE_TEXT as freeText
# MAGIC ,ZZ_COMM_CODE as meterReadingCommentCode
# MAGIC ,ZZ_NO_READ_CODE as noReadCode
# MAGIC ,ZGERNR as DeviceNumber
# MAGIC ,ZADATTATS as actualMeterReadingDate
# MAGIC ,ZWNABR as registerNotRelevantToBilling
# MAGIC ,AEDAT as lastChangedDate
# MAGIC from test.${vars.table}
# MAGIC 
# MAGIC except
# MAGIC select
# MAGIC meterReadingId
# MAGIC ,equipmentNumber
# MAGIC ,registerNumber
# MAGIC ,meterReadingDate
# MAGIC ,meterReadingTaken  
# MAGIC ,duplicate
# MAGIC ,meterReadingActive 
# MAGIC ,scheduledMeterReadingDate 
# MAGIC ,meterReadingStatus 
# MAGIC ,notefromMeterReader 
# MAGIC ,scheduledMeterReadingCategory 
# MAGIC ,meterReaderNumber 
# MAGIC ,orderHasBeenOutput 
# MAGIC ,meterReadingType 
# MAGIC ,meterReadingCategory 
# MAGIC ,unitOfMeasurementMeterReading 
# MAGIC ,bwDeltaProcess
# MAGIC ,deletedIndicator 
# MAGIC ,independentValidation 
# MAGIC ,dependentValidation 
# MAGIC ,advancedMeteringSystem 
# MAGIC ,transferStatusCode 
# MAGIC ,timeStamp
# MAGIC ,sourceSystemOrigin
# MAGIC ,actualPreviousmeterReadingDate 
# MAGIC ,meterPreviousReadingTaken 
# MAGIC ,meterPhotoIndicator 
# MAGIC ,freeText 
# MAGIC ,meterReadingCommentCode 
# MAGIC ,noReadCode 
# MAGIC ,DeviceNumber
# MAGIC ,actualMeterReadingDate 
# MAGIC ,registerNotRelevantToBilling 
# MAGIC ,lastChangedDate
# MAGIC from
# MAGIC cleansed.${vars.table}

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Target and Source Data
# MAGIC %sql
# MAGIC select
# MAGIC meterReadingId
# MAGIC ,equipmentNumber
# MAGIC ,registerNumber
# MAGIC ,meterReadingDate
# MAGIC ,meterReadingTaken  
# MAGIC ,duplicate
# MAGIC ,meterReadingActive 
# MAGIC ,scheduledMeterReadingDate 
# MAGIC ,meterReadingStatus 
# MAGIC ,notefromMeterReader 
# MAGIC ,scheduledMeterReadingCategory 
# MAGIC ,meterReaderNumber 
# MAGIC ,orderHasBeenOutput 
# MAGIC ,meterReadingType 
# MAGIC ,meterReadingCategory 
# MAGIC ,unitOfMeasurementMeterReading 
# MAGIC ,bwDeltaProcess
# MAGIC ,deletedIndicator 
# MAGIC ,independentValidation 
# MAGIC ,dependentValidation 
# MAGIC ,advancedMeteringSystem 
# MAGIC ,transferStatusCode 
# MAGIC ,timeStamp
# MAGIC ,sourceSystemOrigin
# MAGIC ,actualmeterReadingDate 
# MAGIC ,meterReadingTaken 
# MAGIC ,meterPhotoIndicator 
# MAGIC ,freeText 
# MAGIC ,meterReadingCommentCode 
# MAGIC ,noReadCode 
# MAGIC ,DeviceNumber
# MAGIC ,actualMeterReadingDate 
# MAGIC ,registerNotRelevantToBilling 
# MAGIC ,lastChangedDate
# MAGIC from
# MAGIC cleansed.${vars.table}
# MAGIC except
# MAGIC select
# MAGIC meterReadingId
# MAGIC ,equipmentNumber
# MAGIC ,registerNumber
# MAGIC ,meterReadingDate
# MAGIC ,meterReadingTaken  
# MAGIC ,duplicate
# MAGIC ,meterReadingActive 
# MAGIC ,scheduledMeterReadingDate 
# MAGIC ,meterReadingStatus 
# MAGIC ,notefromMeterReader 
# MAGIC ,scheduledMeterReadingCategory 
# MAGIC ,meterReaderNumber 
# MAGIC ,orderHasBeenOutput 
# MAGIC ,meterReadingType 
# MAGIC ,meterReadingCategory 
# MAGIC ,unitOfMeasurementMeterReading 
# MAGIC ,bwDeltaProcess
# MAGIC ,deletedIndicator 
# MAGIC ,independentValidation 
# MAGIC ,dependentValidation 
# MAGIC ,advancedMeteringSystem 
# MAGIC ,transferStatusCode 
# MAGIC ,timeStamp
# MAGIC ,sourceSystemOrigin
# MAGIC ,actualmeterReadingDate 
# MAGIC ,meterReadingTaken 
# MAGIC ,meterPhotoIndicator 
# MAGIC ,freeText 
# MAGIC ,meterReadingCommentCode 
# MAGIC ,noReadCode 
# MAGIC ,DeviceNumber
# MAGIC ,actualMeterReadingDate 
# MAGIC ,registerNotRelevantToBilling 
# MAGIC ,lastChangedDate
# MAGIC from
# MAGIC (select
# MAGIC ABLBELNR as meterReadingId
# MAGIC ,EQUNR as equipmentNumber
# MAGIC ,ZWNUMMER as registerNumber
# MAGIC ,ADAT as meterReadingDate
# MAGIC ,MRESULT as meterReadingTaken 
# MAGIC ,MR_BILL as duplicate
# MAGIC ,AKTIV as meterReadingActive 
# MAGIC ,ADATSOLL as scheduledMeterReadingDate 
# MAGIC ,ABLSTAT as meterReadingStatus 
# MAGIC ,ABLHINW as notefromMeterReader 
# MAGIC ,ABLESART as scheduledMeterReadingCategory 
# MAGIC ,ABLESER as meterReaderNumber 
# MAGIC ,MDEUPL as orderHasBeenOutput 
# MAGIC ,ISTABLART as meterReadingType 
# MAGIC ,ABLESTYP as meterReadingCategory 
# MAGIC ,MASSREAD as unitOfMeasurementMeterReading 
# MAGIC ,UPDMOD as bwDeltaProcess
# MAGIC ,LOEVM as deletedIndicator 
# MAGIC ,PRUEFPKT as independentValidation 
# MAGIC ,POPCODE as dependentValidation 
# MAGIC ,AMS as advancedMeteringSystem 
# MAGIC ,TRANSSTAT as transferStatusCode 
# MAGIC ,TRANSTSTAMP as timeStamp
# MAGIC ,SOURCESYST as sourceSystemOrigin
# MAGIC ,ZPREV_ADT as actualmeterReadingDate 
# MAGIC ,ZPREV_MRESULT as meterReadingTaken 
# MAGIC ,ZZ_PHOTO_IND as meterPhotoIndicator 
# MAGIC ,ZZ_FREE_TEXT as freeText 
# MAGIC ,ZZ_COMM_CODE as meterReadingCommentCode 
# MAGIC ,ZZ_NO_READ_CODE as noReadCode 
# MAGIC ,ZGERNR as DeviceNumber
# MAGIC ,ZADATTATS as actualMeterReadingDate 
# MAGIC ,ZWNABR as registerNotRelevantToBilling 
# MAGIC ,AEDAT as lastChangedDate
# MAGIC ,row_number() over (partition by ABLBELNR,EQUNR order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table}
# MAGIC )a where  a.rn = 1
