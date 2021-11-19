# Databricks notebook source
# DBTITLE 1,[Config] Connection Setup
storage_account_name = "sadaftest01"
storage_account_access_key = dbutils.secrets.get(scope="Test-Access",key="test-datalake-key")
container_name = "raw"
file_location = "wasbs://raw@sadaftest01.blob.core.windows.net/sapisu/dberchz2/json/year=2021/month=09/day=27/DBO.DBERCHZ2_2021-09-27_165356_320.json.gz"
file_type = "json"
print(storage_account_name)

# COMMAND ----------

spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
  storage_account_access_key)


# COMMAND ----------

# DBTITLE 1,[Source] Loading data into Dataframe
df = spark.read.format(file_type).option("inferSchema", "true").load(file_location)
#df2 = spark.read.format(file_type).option("inferSchema", "true").load(file_location2)

# COMMAND ----------

# DBTITLE 1,[Source] Schema Check - Refer to Raw2Cleansed Mapping
df.printSchema()
#df2.printSchema()

# COMMAND ----------

# DBTITLE 0,[Result] Load Count Result into DataFrame
lakedf = spark.sql("select * from cleansed.t_isu_dberchz2")

# COMMAND ----------

display(lakedf)

# COMMAND ----------

# DBTITLE 1,[Target] Schema Check
lakedf.printSchema()

# COMMAND ----------

# DBTITLE 1,[Source] Creating Temporary Table
df.createOrReplaceTempView("Source")
#df2.createOrReplaceTempView("Source22")

# COMMAND ----------

df = spark.sql("select * from Source")
#df2 = spark.sql("select * from Source22")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cleansed.t_sapisu_dberchz2

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cleansed.t_sapisu_0UC_AKLASSE_TEXT

# COMMAND ----------

# DBTITLE 1,[Source] with mapping
# MAGIC %sql
# MAGIC SELECT
# MAGIC BELNR	as	billingDocumentNumber
# MAGIC ,BELZEILE	as	billingDocumentLineItemID
# MAGIC ,EQUNR	as	equipmentNumber
# MAGIC ,GERAET	as	deviceNumber
# MAGIC ,MATNR	as	materialNumber
# MAGIC ,ZWNUMMER	as	registerNumber
# MAGIC ,INDEXNR	as	registerRelationshipConsecutiveNumber
# MAGIC ,ABLESGR	as	meterReadingReasonCode
# MAGIC ,ABLESGRV	as	previousMeterReadingReasonCode
# MAGIC ,ATIM	as	billingMeterReadingTime
# MAGIC ,ATIMVA	as	previousMeterReadingTime,
# MAGIC case
# MAGIC when ADATMAX='0' then null 
# MAGIC when ADATMAX='00000000' then null
# MAGIC when ADATMAX <> 'null' then CONCAT(LEFT(ADATMAX,4),'-',SUBSTRING(ADATMAX,5,2),'-',RIGHT(ADATMAX,2))
# MAGIC else ADATMAX end as maxMeterReadingDate
# MAGIC --,ADATMAX	as	maxMeterReadingDate
# MAGIC ,ATIMMAX	as	maxMeterReadingTime
# MAGIC ,THGDATUM	as	serviceAllocationDate
# MAGIC ,ZUORDDAT	as	meterReadingAllocationDate
# MAGIC ,ABLBELNR	as	suppressedMeterReadingDocumentID
# MAGIC ,LOGIKNR	as	logicalDeviceNumber
# MAGIC ,LOGIKZW	as	logicalRegisterNumber
# MAGIC ,ISTABLART	as	meterReadingTypeCode
# MAGIC ,ISTABLARTVA	as	previousMeterReadingTypeCode
# MAGIC ,EXTPKZ	as	meterReadingResultsSimulationIndicator
# MAGIC ,BEGPROG	as	forecastPeriodStartDate
# MAGIC ,ENDEPROG	as	forecastPeriodEndDate
# MAGIC ,ABLHINW	as	meterReaderNoteText
# MAGIC ,V_ZWSTAND	as	meterReadingBeforeDecimalPoint
# MAGIC ,N_ZWSTAND	as	meterReadingAfterDecimalPoint
# MAGIC ,V_ZWSTNDAB	as	billedMeterReadingBeforeDecimalPlaces
# MAGIC ,N_ZWSTNDAB	as	billedMeterReadingAfterDecimalPlaces
# MAGIC ,V_ZWSTVOR	as	billedMeterReadingBeforeDecimalPlaces
# MAGIC ,N_ZWSTVOR	as	previousMeterReadingAfterDecimalPlaces
# MAGIC ,V_ZWSTDIFF	as	meterReadingDifferenceBeforeDecimalPlaces
# MAGIC ,N_ZWSTDIFF	as	meterReadingDifferenceAfterDecimalPlaces
# MAGIC FROM Source

# COMMAND ----------

lakedf.createOrReplaceTempView("Target")

# COMMAND ----------

# DBTITLE 1,[Verification] Count Checks
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from cleansed.t_sapisu_dberchz2
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from (
# MAGIC SELECT
# MAGIC BELNR	as	billingDocumentNumber
# MAGIC ,BELZEILE	as	billingDocumentLineItemID
# MAGIC ,EQUNR	as	equipmentNumber
# MAGIC ,GERAET	as	deviceNumber
# MAGIC ,MATNR	as	materialNumber
# MAGIC ,ZWNUMMER	as	registerNumber
# MAGIC ,INDEXNR	as	registerRelationshipConsecutiveNumber
# MAGIC ,ABLESGR	as	meterReadingReasonCode
# MAGIC ,ABLESGRV	as	previousMeterReadingReasonCode
# MAGIC ,ATIM	as	billingMeterReadingTime
# MAGIC ,ATIMVA	as	previousMeterReadingTime
# MAGIC ,case
# MAGIC when ADATMAX='0' then null 
# MAGIC when ADATMAX='00000000' then null
# MAGIC when ADATMAX <> 'null' then CONCAT(LEFT(ADATMAX,4),'-',SUBSTRING(ADATMAX,5,2),'-',RIGHT(ADATMAX,2))
# MAGIC else ADATMAX end as maxMeterReadingDate
# MAGIC ,ATIMMAX	as	maxMeterReadingTime
# MAGIC ,THGDATUM	as	serviceAllocationDate
# MAGIC ,ZUORDDAT	as	meterReadingAllocationDate
# MAGIC ,ABLBELNR	as	suppressedMeterReadingDocumentID
# MAGIC ,LOGIKNR	as	logicalDeviceNumber
# MAGIC ,LOGIKZW	as	logicalRegisterNumber
# MAGIC ,ISTABLART	as	meterReadingTypeCode
# MAGIC ,ISTABLARTVA	as	previousMeterReadingTypeCode
# MAGIC ,EXTPKZ	as	meterReadingResultsSimulationIndicator
# MAGIC ,BEGPROG	as	forecastPeriodStartDate
# MAGIC ,ENDEPROG	as	forecastPeriodEndDate
# MAGIC ,ABLHINW	as	meterReaderNoteText
# MAGIC ,V_ZWSTAND	as	meterReadingBeforeDecimalPoint
# MAGIC ,N_ZWSTAND	as	meterReadingAfterDecimalPoint
# MAGIC ,V_ZWSTNDAB	as	billedMeterReadingBeforeDecimalPlaces
# MAGIC ,N_ZWSTNDAB	as	billedMeterReadingAfterDecimalPlaces
# MAGIC ,V_ZWSTVOR	as	billedMeterReadingBeforeDecimalPlaces
# MAGIC ,N_ZWSTVOR	as	previousMeterReadingAfterDecimalPlaces
# MAGIC ,V_ZWSTDIFF	as	meterReadingDifferenceBeforeDecimalPlaces
# MAGIC ,N_ZWSTDIFF	as	meterReadingDifferenceAfterDecimalPlaces
# MAGIC FROM Source )

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT billingDocumentNumber,billingDocumentLineItemId, COUNT (*) as count
# MAGIC FROM cleansed.t_sapisu_dberchz2
# MAGIC GROUP BY billingDocumentNumber, billingDocumentLineItemId
# MAGIC HAVING COUNT (*) > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT * FROM (
# MAGIC SELECT
# MAGIC *,
# MAGIC row_number() OVER(PARTITION BY billingDocumentNumber, billingDocumentLineItemId order by billingDocumentNumber) as rn
# MAGIC FROM cleansed.t_sapisu_dberchz2
# MAGIC )a where a.rn > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Source and Target Data
# MAGIC %sql
# MAGIC SELECT
# MAGIC BELNR	as	billingDocumentNumber
# MAGIC ,BELZEILE	as	billingDocumentLineItemID
# MAGIC ,EQUNR	as	equipmentNumber
# MAGIC ,GERAET	as	deviceNumber
# MAGIC ,MATNR	as	materialNumber
# MAGIC ,ZWNUMMER	as	registerNumber
# MAGIC ,INDEXNR	as	registerRelationshipConsecutiveNumber
# MAGIC ,ABLESGR	as	meterReadingReasonCode
# MAGIC ,ABLESGRV	as	previousMeterReadingReasonCode
# MAGIC ,ATIM	as	billingMeterReadingTime
# MAGIC ,ATIMVA	as	previousMeterReadingTime
# MAGIC ,case
# MAGIC when ADATMAX='0' then null 
# MAGIC when ADATMAX='00000000' then null
# MAGIC when ADATMAX <> 'null' then CONCAT(LEFT(ADATMAX,4),'-',SUBSTRING(ADATMAX,5,2),'-',RIGHT(ADATMAX,2))
# MAGIC else ADATMAX end as maxMeterReadingDate
# MAGIC --,ADATMAX	as	maxMeterReadingDate
# MAGIC ,ATIMMAX	as	maxMeterReadingTime
# MAGIC --,THGDATUM	as	serviceAllocationDate
# MAGIC --,ZUORDDAT	as	meterReadingAllocationDate
# MAGIC ,ABLBELNR	as	suppressedMeterReadingDocumentID
# MAGIC ,LOGIKNR	as	logicalDeviceNumber
# MAGIC ,LOGIKZW	as	logicalRegisterNumber
# MAGIC ,ISTABLART	as	meterReadingTypeCode
# MAGIC ,ISTABLARTVA	as	previousMeterReadingTypeCode
# MAGIC ,EXTPKZ	as	meterReadingResultsSimulationIndicator
# MAGIC --,BEGPROG	as	forecastPeriodStartDate
# MAGIC --,ENDEPROG	as	forecastPeriodEndDate
# MAGIC ,ABLHINW	as	meterReaderNoteText
# MAGIC ,V_ZWSTAND	as	meterReadingBeforeDecimalPoint
# MAGIC ,N_ZWSTAND	as	meterReadingAfterDecimalPoint
# MAGIC ,V_ZWSTNDAB	as	billedMeterReadingBeforeDecimalPlaces
# MAGIC ,N_ZWSTNDAB	as	billedMeterReadingAfterDecimalPlaces
# MAGIC ,V_ZWSTVOR	as	previousMeterReadingBeforeDecimalPlaces
# MAGIC ,N_ZWSTVOR	as	previousMeterReadingAfterDecimalPlaces
# MAGIC ,V_ZWSTDIFF	as	meterReadingDifferenceBeforeDecimalPlaces
# MAGIC ,N_ZWSTDIFF	as	meterReadingDifferenceAfterDecimalPlaces
# MAGIC FROM Source
# MAGIC except
# MAGIC select
# MAGIC billingDocumentNumber
# MAGIC ,billingDocumentLineItemID
# MAGIC ,equipmentNumber
# MAGIC ,deviceNumber
# MAGIC ,materialNumber
# MAGIC ,registerNumber
# MAGIC ,registerRelationshipConsecutiveNumber
# MAGIC ,meterReadingReasonCode
# MAGIC ,previousMeterReadingReasonCode
# MAGIC ,billingMeterReadingTime
# MAGIC ,previousMeterReadingTime
# MAGIC --,maxMeterReadingDate
# MAGIC ,maxMeterReadingTime
# MAGIC --,serviceAllocationDate
# MAGIC --,meterReadingAllocationDate
# MAGIC ,suppressedMeterReadingDocumentID
# MAGIC ,logicalDeviceNumber
# MAGIC ,logicalRegisterNumber
# MAGIC ,meterReadingTypeCode
# MAGIC ,previousMeterReadingTypeCode
# MAGIC ,meterReadingResultsSimulationIndicator
# MAGIC --,forecastPeriodStartDate
# MAGIC --,forecastPeriodEndDate
# MAGIC ,meterReaderNoteText
# MAGIC ,meterReadingBeforeDecimalPoint
# MAGIC ,meterReadingAfterDecimalPoint
# MAGIC ,billedMeterReadingBeforeDecimalPlaces
# MAGIC ,billedMeterReadingAfterDecimalPlaces
# MAGIC ,billedMeterReadingBeforeDecimalPlaces
# MAGIC ,previousMeterReadingAfterDecimalPlaces
# MAGIC ,meterReadingDifferenceBeforeDecimalPlaces
# MAGIC ,meterReadingDifferenceAfterDecimalPlaces
# MAGIC FROM
# MAGIC cleansed.t_sapisu_dberchz2

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Target and Source Data
# MAGIC %sql
# MAGIC select
# MAGIC billingDocumentNumber
# MAGIC ,billingDocumentLineItemID
# MAGIC ,equipmentNumber
# MAGIC ,deviceNumber
# MAGIC ,materialNumber
# MAGIC ,registerNumber
# MAGIC ,registerRelationshipConsecutiveNumber
# MAGIC ,meterReadingReasonCode
# MAGIC ,previousMeterReadingReasonCode
# MAGIC ,billingMeterReadingTime
# MAGIC ,previousMeterReadingTime
# MAGIC --,maxMeterReadingDate
# MAGIC ,maxMeterReadingTime
# MAGIC --,serviceAllocationDate
# MAGIC --,meterReadingAllocationDate
# MAGIC ,suppressedMeterReadingDocumentID
# MAGIC ,logicalDeviceNumber
# MAGIC ,logicalRegisterNumber
# MAGIC ,meterReadingTypeCode
# MAGIC ,previousMeterReadingTypeCode
# MAGIC ,meterReadingResultsSimulationIndicator
# MAGIC --,forecastPeriodStartDate
# MAGIC --,forecastPeriodEndDate
# MAGIC ,meterReaderNoteText
# MAGIC ,meterReadingBeforeDecimalPoint
# MAGIC --,meterReadingAfterDecimalPoint
# MAGIC ,billedMeterReadingBeforeDecimalPlaces
# MAGIC --,billedMeterReadingAfterDecimalPlaces
# MAGIC ,previousMeterReadingBeforeDecimalPlaces
# MAGIC ,previousMeterReadingAfterDecimalPlaces
# MAGIC ,meterReadingDifferenceBeforeDecimalPlaces
# MAGIC --,meterReadingDifferenceAfterDecimalPlaces
# MAGIC FROM
# MAGIC cleansed.t_sapisu_dberchz2
# MAGIC EXCEPT
# MAGIC SELECT
# MAGIC BELNR	as	billingDocumentNumber
# MAGIC ,BELZEILE	as	billingDocumentLineItemID
# MAGIC ,EQUNR	as	equipmentNumber
# MAGIC ,GERAET	as	deviceNumber
# MAGIC ,MATNR	as	materialNumber
# MAGIC ,ZWNUMMER	as	registerNumber
# MAGIC ,INDEXNR	as	registerRelationshipConsecutiveNumber
# MAGIC ,ABLESGR	as	meterReadingReasonCode
# MAGIC ,ABLESGRV	as	previousMeterReadingReasonCode
# MAGIC ,ATIM	as	billingMeterReadingTime
# MAGIC ,ATIMVA	as	previousMeterReadingTime
# MAGIC ,case
# MAGIC when ADATMAX='0' then null 
# MAGIC when ADATMAX='00000000' then null
# MAGIC when ADATMAX <> 'null' then CONCAT(LEFT(ADATMAX,4),'-',SUBSTRING(ADATMAX,5,2),'-',RIGHT(ADATMAX,2))
# MAGIC else ADATMAX end as maxMeterReadingDate
# MAGIC --,ADATMAX	as	maxMeterReadingDate
# MAGIC ,ATIMMAX	as	maxMeterReadingTime
# MAGIC --,THGDATUM	as	serviceAllocationDate
# MAGIC --,ZUORDDAT	as	meterReadingAllocationDate
# MAGIC ,ABLBELNR	as	suppressedMeterReadingDocumentID
# MAGIC ,LOGIKNR	as	logicalDeviceNumber
# MAGIC ,LOGIKZW	as	logicalRegisterNumber
# MAGIC ,ISTABLART	as	meterReadingTypeCode
# MAGIC ,ISTABLARTVA	as	previousMeterReadingTypeCode
# MAGIC ,EXTPKZ	as	meterReadingResultsSimulationIndicator
# MAGIC --,BEGPROG	as	forecastPeriodStartDate
# MAGIC --,ENDEPROG	as	forecastPeriodEndDate
# MAGIC ,ABLHINW	as	meterReaderNoteText
# MAGIC ,V_ZWSTAND	as	meterReadingBeforeDecimalPoint
# MAGIC --,N_ZWSTAND	as	meterReadingAfterDecimalPoint
# MAGIC ,V_ZWSTNDAB	as	billedMeterReadingBeforeDecimalPlaces
# MAGIC --,N_ZWSTNDAB	as	billedMeterReadingAfterDecimalPlaces
# MAGIC ,V_ZWSTVOR	as	previousMeterReadingBeforeDecimalPlaces
# MAGIC ,N_ZWSTVOR	as	previousMeterReadingAfterDecimalPlaces
# MAGIC ,V_ZWSTDIFF	as	meterReadingDifferenceBeforeDecimalPlaces
# MAGIC --,N_ZWSTDIFF	as	meterReadingDifferenceAfterDecimalPlaces
# MAGIC FROM Source

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from source where BEGPROG = '20171230'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC BELNR	as	billingDocumentNumber
# MAGIC ,BELZEILE	as	billingDocumentLineItemID
# MAGIC ,EQUNR	as	equipmentNumber
# MAGIC ,GERAET	as	deviceNumber
# MAGIC ,MATNR	as	materialNumber
# MAGIC ,ZWNUMMER	as	registerNumber
# MAGIC ,INDEXNR	as	registerRelationshipConsecutiveNumber
# MAGIC ,ABLESGR	as	meterReadingReasonCode
# MAGIC ,ABLESGRV	as	previousMeterReadingReasonCode
# MAGIC ,ATIM	as	billingMeterReadingTime
# MAGIC ,ATIMVA	as	previousMeterReadingTime
# MAGIC --,ADATMAX	as	maxMeterReadingDate
# MAGIC ,ATIMMAX	as	maxMeterReadingTime
# MAGIC --,THGDATUM	as	serviceAllocationDate
# MAGIC --,ZUORDDAT	as	meterReadingAllocationDate
# MAGIC ,ABLBELNR	as	suppressedMeterReadingDocumentID
# MAGIC ,LOGIKNR	as	logicalDeviceNumber
# MAGIC ,LOGIKZW	as	logicalRegisterNumber
# MAGIC ,ISTABLART	as	meterReadingTypeCode
# MAGIC ,ISTABLARTVA	as	previousMeterReadingTypeCode
# MAGIC ,EXTPKZ	as	meterReadingResultsSimulationIndicator
# MAGIC --,BEGPROG	as	forecastPeriodStartDate
# MAGIC --,ENDEPROG	as	forecastPeriodEndDate
# MAGIC ,ABLHINW	as	meterReaderNoteText
# MAGIC ,V_ZWSTAND	as	meterReadingBeforeDecimalPoint
# MAGIC ,N_ZWSTAND	as	meterReadingAfterDecimalPoint
# MAGIC ,V_ZWSTNDAB	as	billedMeterReadingBeforeDecimalPlaces
# MAGIC ,N_ZWSTNDAB	as	billedMeterReadingAfterDecimalPlaces
# MAGIC ,V_ZWSTVOR	as	previousMeterReadingBeforeDecimalPlaces
# MAGIC ,N_ZWSTVOR	as	previousMeterReadingAfterDecimalPlaces
# MAGIC ,V_ZWSTDIFF	as	meterReadingDifferenceBeforeDecimalPlaces
# MAGIC ,N_ZWSTDIFF	as	meterReadingDifferenceAfterDecimalPlaces
# MAGIC FROM Source
# MAGIC where BELNR = '616000519573' and BELZEILE = '000003'-- BELNR = '300000002646' and BELZEILE = '000009'

# COMMAND ----------

# MAGIC %sql
# MAGIC select billingDocumentNumber
# MAGIC ,billingDocumentLineItemID
# MAGIC ,equipmentNumber
# MAGIC ,deviceNumber
# MAGIC ,materialNumber
# MAGIC ,registerNumber
# MAGIC ,registerRelationshipConsecutiveNumber
# MAGIC ,meterReadingReasonCode
# MAGIC ,previousMeterReadingReasonCode
# MAGIC ,billingMeterReadingTime
# MAGIC ,previousMeterReadingTime
# MAGIC ,maxMeterReadingDate
# MAGIC ,maxMeterReadingTime
# MAGIC --,serviceAllocationDate
# MAGIC --,meterReadingAllocationDate
# MAGIC ,suppressedMeterReadingDocumentID
# MAGIC ,logicalDeviceNumber
# MAGIC ,logicalRegisterNumber
# MAGIC ,meterReadingTypeCode
# MAGIC ,previousMeterReadingTypeCode
# MAGIC ,meterReadingResultsSimulationIndicator
# MAGIC --,forecastPeriodStartDate
# MAGIC --,forecastPeriodEndDate
# MAGIC ,meterReaderNoteText
# MAGIC ,meterReadingBeforeDecimalPoint
# MAGIC ,meterReadingAfterDecimalPoint
# MAGIC ,billedMeterReadingBeforeDecimalPlaces
# MAGIC ,billedMeterReadingAfterDecimalPlaces
# MAGIC ,previousMeterReadingBeforeDecimalPlaces
# MAGIC ,previousMeterReadingAfterDecimalPlaces
# MAGIC ,meterReadingDifferenceBeforeDecimalPlaces
# MAGIC ,meterReadingDifferenceAfterDecimalPlaces from cleansed.t_sapisu_dberchz2 --where billingdocumentnumber = '616000519573' and billingDocumentLineItemId = '000003'

# COMMAND ----------

# MAGIC %sql
# MAGIC select V_ZWSTAND	as	meterReadingBeforeDecimalPoint
# MAGIC ,N_ZWSTAND	as	meterReadingAfterDecimalPoint
# MAGIC ,V_ZWSTNDAB	as	billedMeterReadingBeforeDecimalPlaces
# MAGIC ,N_ZWSTNDAB	as	billedMeterReadingAfterDecimalPlaces
# MAGIC ,V_ZWSTVOR	as	previousMeterReadingBeforeDecimalPlaces
# MAGIC ,N_ZWSTVOR	as	previousMeterReadingAfterDecimalPlaces
# MAGIC ,V_ZWSTDIFF	as	meterReadingDifferenceBeforeDecimalPlaces
# MAGIC ,N_ZWSTDIFF	as	meterReadingDifferenceAfterDecimalPlaces from source where BELNR = '616000703805' and BELZEILE = '000016' 	

# COMMAND ----------

# MAGIC %sql
# MAGIC select meterReadingBeforeDecimalPoint
# MAGIC ,meterReadingAfterDecimalPoint
# MAGIC ,billedMeterReadingBeforeDecimalPlaces
# MAGIC ,billedMeterReadingAfterDecimalPlaces
# MAGIC ,previousMeterReadingBeforeDecimalPlaces
# MAGIC ,previousMeterReadingAfterDecimalPlaces
# MAGIC ,meterReadingDifferenceBeforeDecimalPlaces from cleansed.t_isu_dberchz2 where billingdocumentnumber = '616000703805' and billingDocumentLineItemId = '000016'

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC distinct
# MAGIC case
# MAGIC when ADATMAX='0' then null 
# MAGIC when ADATMAX='00000000' then null
# MAGIC when ADATMAX <> 'null' then CONCAT(LEFT(ADATMAX,4),'-',SUBSTRING(ADATMAX,5,2),'-',RIGHT(ADATMAX,2))
# MAGIC else ADATMAX end as maxMeterReadingDate
# MAGIC from source

# COMMAND ----------

# MAGIC 
# MAGIC %sql
# MAGIC select distinct ADATMAX	as	maxMeterReadingDate from Source
