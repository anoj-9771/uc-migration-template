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

hostName = 'nt097dslt01.adsdev.swcdev'
databaseName = 'SLT_EQ1CLNT100'
port = 1433
dbURL = f'jdbc:sqlserver://{hostName}:{port};database={databaseName};user=SLTADMIN;password=Edpgroup01#!'
qry = '(select * from EQ1.DBERCHZ2) myTable'
df = spark.read.jdbc(url=dbURL, table=qry, lowerBound=1, upperBound=100000, numPartitions=10)
display(df)

# COMMAND ----------

df.write.format("json").saveAsTable ("test" + "." + "isu_dberchz2")

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
lakedf = spark.sql("select * from cleansed.isu_dberchz2")

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
# MAGIC select * from cleansed.isu_dberchz2

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cleansed.isu_0UC_AKLASSE_TEXT

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

timestampfield = spark.sql("select max(_recordstart) from cleansed.isu_dberchz2")

# COMMAND ----------

# DBTITLE 1,[Verification] Count Checks
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from cleansed.isu_dberchz2
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
# MAGIC FROM test.isu_dberchz2 where DELTA_TS<'20211117055649')

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT billingDocumentNumber,billingDocumentLineItemId, COUNT (*) as count
# MAGIC FROM cleansed.isu_dberchz2
# MAGIC GROUP BY billingDocumentNumber, billingDocumentLineItemId
# MAGIC HAVING COUNT (*) > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT * FROM (
# MAGIC SELECT
# MAGIC *,
# MAGIC row_number() OVER(PARTITION BY billingDocumentNumber, billingDocumentLineItemId order by billingDocumentNumber) as rn
# MAGIC FROM cleansed.isu_dberchz2
# MAGIC )a where a.rn > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Source and Target Data
# MAGIC %sql
# MAGIC select BELNR	as	billingDocumentNumber
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
# MAGIC ,case
# MAGIC when THGDATUM='0' then null 
# MAGIC when THGDATUM='00000000' then null
# MAGIC when THGDATUM <> 'null' then CONCAT(LEFT(THGDATUM,4),'-',SUBSTRING(THGDATUM,5,2),'-',RIGHT(THGDATUM,2))
# MAGIC else THGDATUM end as serviceAllocationDate
# MAGIC --,THGDATUM	as	serviceAllocationDate
# MAGIC ,case
# MAGIC when ZUORDDAT='0' then null 
# MAGIC when ZUORDDAT='00000000' then null
# MAGIC when ZUORDDAT <> 'null' then CONCAT(LEFT(ZUORDDAT,4),'-',SUBSTRING(ZUORDDAT,5,2),'-',RIGHT(ZUORDDAT,2))
# MAGIC else ZUORDDAT end as meterReadingAllocationDate
# MAGIC --,ZUORDDAT	as	meterReadingAllocationDate
# MAGIC ,ABLBELNR	as	suppressedMeterReadingDocumentID
# MAGIC ,LOGIKNR	as	logicalDeviceNumber
# MAGIC ,LOGIKZW	as	logicalRegisterNumber
# MAGIC ,ISTABLART	as	meterReadingTypeCode
# MAGIC ,ISTABLARTVA	as	previousMeterReadingTypeCode
# MAGIC ,EXTPKZ	as	meterReadingResultsSimulationIndicator
# MAGIC ,case
# MAGIC when BEGPROG='0' then null 
# MAGIC when BEGPROG='00000000' then null
# MAGIC when BEGPROG <> 'null' then CONCAT(LEFT(BEGPROG,4),'-',SUBSTRING(BEGPROG,5,2),'-',RIGHT(BEGPROG,2))
# MAGIC else BEGPROG end as forecastPeriodStartDate
# MAGIC --,BEGPROG	as	forecastPeriodStartDate
# MAGIC ,case
# MAGIC when ENDEPROG='0' then null 
# MAGIC when ENDEPROG='00000000' then null
# MAGIC when ENDEPROG <> 'null' then CONCAT(LEFT(ENDEPROG,4),'-',SUBSTRING(ENDEPROG,5,2),'-',RIGHT(ENDEPROG,2))
# MAGIC else ENDEPROG end as forecastPeriodEndDate
# MAGIC --,ENDEPROG	as	forecastPeriodEndDate
# MAGIC ,ABLHINW	as	meterReaderNoteText
# MAGIC ,V_ZWSTAND	as	meterReadingBeforeDecimalPoint
# MAGIC ,N_ZWSTAND	as	meterReadingAfterDecimalPoint
# MAGIC ,V_ZWSTNDAB	as	billedMeterReadingBeforeDecimalPlaces
# MAGIC ,N_ZWSTNDAB	as	billedMeterReadingAfterDecimalPlaces
# MAGIC ,V_ZWSTVOR	as	previousMeterReadingBeforeDecimalPlaces
# MAGIC ,N_ZWSTVOR	as	previousMeterReadingAfterDecimalPlaces
# MAGIC ,V_ZWSTDIFF	as	meterReadingDifferenceBeforeDecimalPlaces
# MAGIC ,N_ZWSTDIFF	as	meterReadingDifferenceAfterDecimalPlaces from test.isu_dberchz2 where DELTA_TS<'20211117055649'
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
# MAGIC ,maxMeterReadingDate
# MAGIC ,maxMeterReadingTime
# MAGIC ,serviceAllocationDate
# MAGIC ,meterReadingAllocationDate
# MAGIC ,suppressedMeterReadingDocumentID
# MAGIC ,logicalDeviceNumber
# MAGIC ,logicalRegisterNumber
# MAGIC ,meterReadingTypeCode
# MAGIC ,previousMeterReadingTypeCode
# MAGIC ,meterReadingResultsSimulationIndicator
# MAGIC ,forecastPeriodStartDate
# MAGIC ,forecastPeriodEndDate
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
# MAGIC cleansed.isu_dberchz2

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
# MAGIC ,maxMeterReadingDate
# MAGIC ,maxMeterReadingTime
# MAGIC ,serviceAllocationDate
# MAGIC ,meterReadingAllocationDate
# MAGIC ,suppressedMeterReadingDocumentID
# MAGIC ,logicalDeviceNumber
# MAGIC ,logicalRegisterNumber
# MAGIC ,meterReadingTypeCode
# MAGIC ,previousMeterReadingTypeCode
# MAGIC ,meterReadingResultsSimulationIndicator
# MAGIC ,forecastPeriodStartDate
# MAGIC ,forecastPeriodEndDate
# MAGIC ,meterReaderNoteText
# MAGIC ,meterReadingBeforeDecimalPoint
# MAGIC ,meterReadingAfterDecimalPoint
# MAGIC ,billedMeterReadingBeforeDecimalPlaces
# MAGIC ,billedMeterReadingAfterDecimalPlaces
# MAGIC ,previousMeterReadingBeforeDecimalPlaces
# MAGIC ,previousMeterReadingAfterDecimalPlaces
# MAGIC ,meterReadingDifferenceBeforeDecimalPlaces
# MAGIC ,meterReadingDifferenceAfterDecimalPlaces
# MAGIC FROM
# MAGIC cleansed.isu_dberchz2
# MAGIC EXCEPT
# MAGIC select BELNR	as	billingDocumentNumber
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
# MAGIC ,case
# MAGIC when THGDATUM='0' then null 
# MAGIC when THGDATUM='00000000' then null
# MAGIC when THGDATUM <> 'null' then CONCAT(LEFT(THGDATUM,4),'-',SUBSTRING(THGDATUM,5,2),'-',RIGHT(THGDATUM,2))
# MAGIC else THGDATUM end as serviceAllocationDate
# MAGIC --,THGDATUM	as	serviceAllocationDate
# MAGIC ,case
# MAGIC when ZUORDDAT='0' then null 
# MAGIC when ZUORDDAT='00000000' then null
# MAGIC when ZUORDDAT <> 'null' then CONCAT(LEFT(ZUORDDAT,4),'-',SUBSTRING(ZUORDDAT,5,2),'-',RIGHT(ZUORDDAT,2))
# MAGIC else ZUORDDAT end as meterReadingAllocationDate
# MAGIC --,ZUORDDAT	as	meterReadingAllocationDate
# MAGIC ,ABLBELNR	as	suppressedMeterReadingDocumentID
# MAGIC ,LOGIKNR	as	logicalDeviceNumber
# MAGIC ,LOGIKZW	as	logicalRegisterNumber
# MAGIC ,ISTABLART	as	meterReadingTypeCode
# MAGIC ,ISTABLARTVA	as	previousMeterReadingTypeCode
# MAGIC ,EXTPKZ	as	meterReadingResultsSimulationIndicator
# MAGIC ,case
# MAGIC when BEGPROG='0' then null 
# MAGIC when BEGPROG='00000000' then null
# MAGIC when BEGPROG <> 'null' then CONCAT(LEFT(BEGPROG,4),'-',SUBSTRING(BEGPROG,5,2),'-',RIGHT(BEGPROG,2))
# MAGIC else BEGPROG end as forecastPeriodStartDate
# MAGIC --,BEGPROG	as	forecastPeriodStartDate
# MAGIC ,case
# MAGIC when ENDEPROG='0' then null 
# MAGIC when ENDEPROG='00000000' then null
# MAGIC when ENDEPROG <> 'null' then CONCAT(LEFT(ENDEPROG,4),'-',SUBSTRING(ENDEPROG,5,2),'-',RIGHT(ENDEPROG,2))
# MAGIC else ENDEPROG end as forecastPeriodEndDate
# MAGIC --,ENDEPROG	as	forecastPeriodEndDate
# MAGIC ,ABLHINW	as	meterReaderNoteText
# MAGIC ,V_ZWSTAND	as	meterReadingBeforeDecimalPoint
# MAGIC ,N_ZWSTAND	as	meterReadingAfterDecimalPoint
# MAGIC ,V_ZWSTNDAB	as	billedMeterReadingBeforeDecimalPlaces
# MAGIC ,N_ZWSTNDAB	as	billedMeterReadingAfterDecimalPlaces
# MAGIC ,V_ZWSTVOR	as	previousMeterReadingBeforeDecimalPlaces
# MAGIC ,N_ZWSTVOR	as	previousMeterReadingAfterDecimalPlaces
# MAGIC ,V_ZWSTDIFF	as	meterReadingDifferenceBeforeDecimalPlaces
# MAGIC ,N_ZWSTDIFF	as	meterReadingDifferenceAfterDecimalPlaces from test.isu_dberchz2 where DELTA_TS<'20211117055649'

# COMMAND ----------

# MAGIC %sql
# MAGIC select sum(billingQuantityPlaceAfterDecimalPoint) as total from cleansed.isu_dberchz1

# COMMAND ----------

# MAGIC %sql
# MAGIC select ,V_ZWSTAND	as	meterReadingBeforeDecimalPoint
# MAGIC ,N_ZWSTAND	as	meterReadingAfterDecimalPoint
# MAGIC ,V_ZWSTNDAB	as	billedMeterReadingBeforeDecimalPlaces
# MAGIC ,N_ZWSTNDAB	as	billedMeterReadingAfterDecimalPlaces
# MAGIC ,V_ZWSTVOR	as	previousMeterReadingBeforeDecimalPlaces
# MAGIC ,N_ZWSTVOR	as	previousMeterReadingAfterDecimalPlaces
# MAGIC ,V_ZWSTDIFF	as	meterReadingDifferenceBeforeDecimalPlaces
# MAGIC ,N_ZWSTDIFF	as	meterReadingDifferenceAfterDecimalPlaces from test.isu_dberchz2 where BELNR = '193006781302' and BELZEILE = '000001'

# COMMAND ----------

# MAGIC %sql
# MAGIC select  *, billingQuantityPlaceBeforeDecimalPoint,billingQuantityPlaceAfterDecimalPoint from cleansed.isu_dberchz1 where billingQuantityPlaceAfterDecimalPoint in (0.42465753424660,0.80273972602731,0.88767123287665)
# MAGIC --cast(N_ZWSTVOR as decimal(15,5)) as num from test.isu_dberchz2

# COMMAND ----------

# MAGIC %sql
# MAGIC select BELNR	as	billingDocumentNumber
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
# MAGIC ,case
# MAGIC when THGDATUM='0' then null 
# MAGIC when THGDATUM='00000000' then null
# MAGIC when THGDATUM <> 'null' then CONCAT(LEFT(THGDATUM,4),'-',SUBSTRING(THGDATUM,5,2),'-',RIGHT(THGDATUM,2))
# MAGIC else THGDATUM end as serviceAllocationDate
# MAGIC --,THGDATUM	as	serviceAllocationDate
# MAGIC ,case
# MAGIC when ZUORDDAT='0' then null 
# MAGIC when ZUORDDAT='00000000' then null
# MAGIC when ZUORDDAT <> 'null' then CONCAT(LEFT(ZUORDDAT,4),'-',SUBSTRING(ZUORDDAT,5,2),'-',RIGHT(ZUORDDAT,2))
# MAGIC else ZUORDDAT end as meterReadingAllocationDate
# MAGIC --,ZUORDDAT	as	meterReadingAllocationDate
# MAGIC ,ABLBELNR	as	suppressedMeterReadingDocumentID
# MAGIC ,LOGIKNR	as	logicalDeviceNumber
# MAGIC ,LOGIKZW	as	logicalRegisterNumber
# MAGIC ,ISTABLART	as	meterReadingTypeCode
# MAGIC ,ISTABLARTVA	as	previousMeterReadingTypeCode
# MAGIC ,EXTPKZ	as	meterReadingResultsSimulationIndicator
# MAGIC ,case
# MAGIC when BEGPROG='0' then null 
# MAGIC when BEGPROG='00000000' then null
# MAGIC when BEGPROG <> 'null' then CONCAT(LEFT(BEGPROG,4),'-',SUBSTRING(BEGPROG,5,2),'-',RIGHT(BEGPROG,2))
# MAGIC else BEGPROG end as forecastPeriodStartDate
# MAGIC --,BEGPROG	as	forecastPeriodStartDate
# MAGIC ,case
# MAGIC when ENDEPROG='0' then null 
# MAGIC when ENDEPROG='00000000' then null
# MAGIC when ENDEPROG <> 'null' then CONCAT(LEFT(ENDEPROG,4),'-',SUBSTRING(ENDEPROG,5,2),'-',RIGHT(ENDEPROG,2))
# MAGIC else ENDEPROG end as forecastPeriodEndDate
# MAGIC --,ENDEPROG	as	forecastPeriodEndDate
# MAGIC ,ABLHINW	as	meterReaderNoteText
# MAGIC ,V_ZWSTAND	as	meterReadingBeforeDecimalPoint
# MAGIC ,N_ZWSTAND	as	meterReadingAfterDecimalPoint
# MAGIC ,V_ZWSTNDAB	as	billedMeterReadingBeforeDecimalPlaces
# MAGIC ,N_ZWSTNDAB	as	billedMeterReadingAfterDecimalPlaces
# MAGIC ,V_ZWSTVOR	as	previousMeterReadingBeforeDecimalPlaces
# MAGIC ,N_ZWSTVOR	as	previousMeterReadingAfterDecimalPlaces
# MAGIC ,V_ZWSTDIFF	as	meterReadingDifferenceBeforeDecimalPlaces
# MAGIC ,N_ZWSTDIFF	as	meterReadingDifferenceAfterDecimalPlaces from test.isu_dberchz2
# MAGIC where BELNR = '010000004117' and BELZEILE ='002739'

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC *
# MAGIC from cleansed.isu_dberchz2 where billingDocumentNumber = '010000004117' and billingDocumentLineItemID = '002739'

# COMMAND ----------

# MAGIC %sql
# MAGIC select BELNR	as	billingDocumentNumber
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
# MAGIC ,case
# MAGIC when THGDATUM='0' then null 
# MAGIC when THGDATUM='00000000' then null
# MAGIC when THGDATUM <> 'null' then CONCAT(LEFT(THGDATUM,4),'-',SUBSTRING(THGDATUM,5,2),'-',RIGHT(THGDATUM,2))
# MAGIC else THGDATUM end as serviceAllocationDate
# MAGIC --,THGDATUM	as	serviceAllocationDate
# MAGIC ,case
# MAGIC when ZUORDDAT='0' then null 
# MAGIC when ZUORDDAT='00000000' then null
# MAGIC when ZUORDDAT <> 'null' then CONCAT(LEFT(ZUORDDAT,4),'-',SUBSTRING(ZUORDDAT,5,2),'-',RIGHT(ZUORDDAT,2))
# MAGIC else ZUORDDAT end as meterReadingAllocationDate
# MAGIC --,ZUORDDAT	as	meterReadingAllocationDate
# MAGIC ,ABLBELNR	as	suppressedMeterReadingDocumentID
# MAGIC ,LOGIKNR	as	logicalDeviceNumber
# MAGIC ,LOGIKZW	as	logicalRegisterNumber
# MAGIC ,ISTABLART	as	meterReadingTypeCode
# MAGIC ,ISTABLARTVA	as	previousMeterReadingTypeCode
# MAGIC ,EXTPKZ	as	meterReadingResultsSimulationIndicator
# MAGIC ,case
# MAGIC when BEGPROG='0' then null 
# MAGIC when BEGPROG='00000000' then null
# MAGIC when BEGPROG <> 'null' then CONCAT(LEFT(BEGPROG,4),'-',SUBSTRING(BEGPROG,5,2),'-',RIGHT(BEGPROG,2))
# MAGIC else BEGPROG end as forecastPeriodStartDate
# MAGIC --,BEGPROG	as	forecastPeriodStartDate
# MAGIC ,case
# MAGIC when ENDEPROG='0' then null 
# MAGIC when ENDEPROG='00000000' then null
# MAGIC when ENDEPROG <> 'null' then CONCAT(LEFT(ENDEPROG,4),'-',SUBSTRING(ENDEPROG,5,2),'-',RIGHT(ENDEPROG,2))
# MAGIC else ENDEPROG end as forecastPeriodEndDate
# MAGIC --,ENDEPROG	as	forecastPeriodEndDate
# MAGIC ,ABLHINW	as	meterReaderNoteText
# MAGIC ,V_ZWSTAND	as	meterReadingBeforeDecimalPoint
# MAGIC ,N_ZWSTAND	as	meterReadingAfterDecimalPoint
# MAGIC ,V_ZWSTNDAB	as	billedMeterReadingBeforeDecimalPlaces
# MAGIC ,N_ZWSTNDAB	as	billedMeterReadingAfterDecimalPlaces
# MAGIC ,V_ZWSTVOR	as	previousMeterReadingBeforeDecimalPlaces
# MAGIC ,N_ZWSTVOR	as	previousMeterReadingAfterDecimalPlaces
# MAGIC ,V_ZWSTDIFF	as	meterReadingDifferenceBeforeDecimalPlaces
# MAGIC ,N_ZWSTDIFF	as	meterReadingDifferenceAfterDecimalPlaces from source
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
# MAGIC ,maxMeterReadingDate
# MAGIC ,maxMeterReadingTime
# MAGIC ,serviceAllocationDate
# MAGIC ,meterReadingAllocationDate
# MAGIC ,suppressedMeterReadingDocumentID
# MAGIC ,logicalDeviceNumber
# MAGIC ,logicalRegisterNumber
# MAGIC ,meterReadingTypeCode
# MAGIC ,previousMeterReadingTypeCode
# MAGIC ,meterReadingResultsSimulationIndicator
# MAGIC ,forecastPeriodStartDate
# MAGIC ,forecastPeriodEndDate
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
# MAGIC cleansed.isu_dberchz2
# MAGIC WHERE NOT EXISTS
# MAGIC (
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
# MAGIC ,maxMeterReadingDate
# MAGIC ,maxMeterReadingTime
# MAGIC ,serviceAllocationDate
# MAGIC ,meterReadingAllocationDate
# MAGIC ,suppressedMeterReadingDocumentID
# MAGIC ,logicalDeviceNumber
# MAGIC ,logicalRegisterNumber
# MAGIC ,meterReadingTypeCode
# MAGIC ,previousMeterReadingTypeCode
# MAGIC ,meterReadingResultsSimulationIndicator
# MAGIC ,forecastPeriodStartDate
# MAGIC ,forecastPeriodEndDate
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
# MAGIC cleansed.isu_dberchz2
# MAGIC )

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
# MAGIC ,meterReadingDifferenceBeforeDecimalPlaces from cleansed.isu_dberchz2 -- where billingdocumentnumber = '616000703805' and billingDocumentLineItemId = '000016'

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
# MAGIC select distinct maxMeterReadingDate from cleansed.isu_dberchz2 

# COMMAND ----------

# MAGIC 
# MAGIC %sql
# MAGIC --select distinct serviceAllocationDate from cleansed.isu_dberchz2
# MAGIC select distinct THGDATUM from source

# COMMAND ----------

# MAGIC 
# MAGIC %sql
# MAGIC select distinct meterReadingAllocationDate from cleansed.isu_dberchz2

# COMMAND ----------

# MAGIC 
# MAGIC %sql
# MAGIC select distinct forecastPeriodStartDate from cleansed.isu_dberchz2

# COMMAND ----------

# MAGIC 
# MAGIC %sql
# MAGIC select distinct forecastPeriodEndDate from cleansed.isu_dberchz2
