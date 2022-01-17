# Databricks notebook source
# DBTITLE 1,[Config] Connection Setup to SLT
hostName = 'nt097dslt01.adsdev.swcdev'
databaseName = 'SLT_EQ1CLNT100'
port = 1433
sltkey = dbutils.secrets.get(scope="TestScope",key="slt-key")
sltuser = dbutils.secrets.get(scope="TestScope",key="slt-user")
dbURL = f'jdbc:sqlserver://{hostName}:{port};database={databaseName};user={sltuser};password={sltkey}'
qry = '(select * from EQ1.DBERCHZ2) myTable'
df = spark.read.jdbc(url=dbURL, table=qry, lowerBound=1, upperBound=100000, numPartitions=10)
display(df)

# COMMAND ----------

# DBTITLE 1,Delete if exists
# MAGIC %sql
# MAGIC drop table if exists test.isu_dberchz2

# COMMAND ----------

# DBTITLE 1,Delete if exists
# MAGIC %fs rm -r dbfs:/user/hive/warehouse/test.db/isu_dberchz2

# COMMAND ----------

# DBTITLE 1,Create SLT table in Test
df.write.format("json").saveAsTable ("test" + "." + "isu_dberchz2a")

# COMMAND ----------

# DBTITLE 1,[Source] Schema Check - Refer to Raw2Cleansed Mapping
lakedfs = spark.sql("select * from test.isu_dberchz2a")
display(lakedfs)
lakedfs.printSchema()

# COMMAND ----------

# DBTITLE 1,[Target] Schema Check
lakedf = spark.sql("select * from cleansed.isu_dberchz2")
display(lakedf)
lakedf.printSchema()

# COMMAND ----------

# DBTITLE 1,[Source] with mapping
# MAGIC %sql
# MAGIC 
# MAGIC select 
# MAGIC BELNR as billingDocumentNumber
# MAGIC ,BELZEILE as billingDocumentLineItemID
# MAGIC ,EQUNR as equipmentNumber
# MAGIC ,GERAET as deviceNumber
# MAGIC ,MATNR as materialNumber
# MAGIC ,ZWNUMMER as registerNumber
# MAGIC ,INDEXNR as registerRelationshipConsecutiveNumber
# MAGIC ,ABLESGR as meterReadingReasonCode
# MAGIC ,ABLESGRV as previousMeterReadingReasonCode
# MAGIC ,ATIM as billingMeterReadingTime
# MAGIC ,ATIMVA as previousMeterReadingTime
# MAGIC ,case
# MAGIC when ADATMAX='0' then null 
# MAGIC when ADATMAX='00000000' then null
# MAGIC when ADATMAX <> 'null' then CONCAT(LEFT(ADATMAX,4),'-',SUBSTRING(ADATMAX,5,2),'-',RIGHT(ADATMAX,2))
# MAGIC else ADATMAX end as maxMeterReadingDate
# MAGIC --,ADATMAX as maxMeterReadingDate
# MAGIC ,ATIMMAX as maxMeterReadingTime
# MAGIC ,case
# MAGIC when THGDATUM='0' then null 
# MAGIC when THGDATUM='00000000' then null
# MAGIC when THGDATUM <> 'null' then CONCAT(LEFT(THGDATUM,4),'-',SUBSTRING(THGDATUM,5,2),'-',RIGHT(THGDATUM,2))
# MAGIC else THGDATUM end as serviceAllocationDate
# MAGIC --,THGDATUM as serviceAllocationDate
# MAGIC ,case
# MAGIC when ZUORDDAT='0' then null 
# MAGIC when ZUORDDAT='00000000' then null
# MAGIC when ZUORDDAT <> 'null' then CONCAT(LEFT(ZUORDDAT,4),'-',SUBSTRING(ZUORDDAT,5,2),'-',RIGHT(ZUORDDAT,2))
# MAGIC else ZUORDDAT end as meterReadingAllocationDate
# MAGIC --,ZUORDDAT as meterReadingAllocationDate
# MAGIC ,ABLBELNR as suppressedMeterReadingDocumentID
# MAGIC ,LOGIKNR as logicalDeviceNumber
# MAGIC ,LOGIKZW as logicalRegisterNumber
# MAGIC ,ISTABLART as meterReadingTypeCode
# MAGIC ,ISTABLARTVA as previousMeterReadingTypeCode
# MAGIC ,EXTPKZ as meterReadingResultsSimulationIndicator
# MAGIC ,case
# MAGIC when BEGPROG='0' then null 
# MAGIC when BEGPROG='00000000' then null
# MAGIC when BEGPROG <> 'null' then CONCAT(LEFT(BEGPROG,4),'-',SUBSTRING(BEGPROG,5,2),'-',RIGHT(BEGPROG,2))
# MAGIC else BEGPROG end as forecastPeriodStartDate
# MAGIC --,BEGPROG as forecastPeriodStartDate
# MAGIC ,case
# MAGIC when ENDEPROG='0' then null 
# MAGIC when ENDEPROG='00000000' then null
# MAGIC when ENDEPROG <> 'null' then CONCAT(LEFT(ENDEPROG,4),'-',SUBSTRING(ENDEPROG,5,2),'-',RIGHT(ENDEPROG,2))
# MAGIC else ENDEPROG end as forecastPeriodEndDate
# MAGIC --,ENDEPROG as forecastPeriodEndDate
# MAGIC ,ABLHINW as meterReaderNoteText
# MAGIC ,V_ZWSTAND as meterReadingBeforeDecimalPoint
# MAGIC ,N_ZWSTAND as meterReadingAfterDecimalPoint
# MAGIC ,V_ZWSTNDAB as billedMeterReadingBeforeDecimalPlaces
# MAGIC ,N_ZWSTNDAB as billedMeterReadingAfterDecimalPlaces
# MAGIC ,V_ZWSTVOR as previousMeterReadingBeforeDecimalPlaces
# MAGIC ,N_ZWSTVOR as previousMeterReadingAfterDecimalPlaces
# MAGIC ,V_ZWSTDIFF as meterReadingDifferenceBeforeDecimalPlaces
# MAGIC ,N_ZWSTDIFF as meterReadingDifferenceAfterDecimalPlaces 
# MAGIC from test.isu_dberchz2a

# COMMAND ----------

# DBTITLE 1,[Verification] Count Checks
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from cleansed.isu_dberchz2
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from (
# MAGIC select 
# MAGIC BELNR as billingDocumentNumber
# MAGIC ,BELZEILE as billingDocumentLineItemID
# MAGIC ,EQUNR as equipmentNumber
# MAGIC ,GERAET as deviceNumber
# MAGIC ,MATNR as materialNumber
# MAGIC ,ZWNUMMER as registerNumber
# MAGIC ,INDEXNR as registerRelationshipConsecutiveNumber
# MAGIC ,ABLESGR as meterReadingReasonCode
# MAGIC ,ABLESGRV as previousMeterReadingReasonCode
# MAGIC ,ATIM as billingMeterReadingTime
# MAGIC ,ATIMVA as previousMeterReadingTime
# MAGIC ,case
# MAGIC when ADATMAX='0' then null 
# MAGIC when ADATMAX='00000000' then null
# MAGIC when ADATMAX <> 'null' then CONCAT(LEFT(ADATMAX,4),'-',SUBSTRING(ADATMAX,5,2),'-',RIGHT(ADATMAX,2))
# MAGIC else ADATMAX end as maxMeterReadingDate
# MAGIC --,ADATMAX as maxMeterReadingDate
# MAGIC ,ATIMMAX as maxMeterReadingTime
# MAGIC ,case
# MAGIC when THGDATUM='0' then null 
# MAGIC when THGDATUM='00000000' then null
# MAGIC when THGDATUM <> 'null' then CONCAT(LEFT(THGDATUM,4),'-',SUBSTRING(THGDATUM,5,2),'-',RIGHT(THGDATUM,2))
# MAGIC else THGDATUM end as serviceAllocationDate
# MAGIC --,THGDATUM as serviceAllocationDate
# MAGIC ,case
# MAGIC when ZUORDDAT='0' then null 
# MAGIC when ZUORDDAT='00000000' then null
# MAGIC when ZUORDDAT <> 'null' then CONCAT(LEFT(ZUORDDAT,4),'-',SUBSTRING(ZUORDDAT,5,2),'-',RIGHT(ZUORDDAT,2))
# MAGIC else ZUORDDAT end as meterReadingAllocationDate
# MAGIC --,ZUORDDAT as meterReadingAllocationDate
# MAGIC ,ABLBELNR as suppressedMeterReadingDocumentID
# MAGIC ,LOGIKNR as logicalDeviceNumber
# MAGIC ,LOGIKZW as logicalRegisterNumber
# MAGIC ,ISTABLART as meterReadingTypeCode
# MAGIC ,ISTABLARTVA as previousMeterReadingTypeCode
# MAGIC ,EXTPKZ as meterReadingResultsSimulationIndicator
# MAGIC ,case
# MAGIC when BEGPROG='0' then null 
# MAGIC when BEGPROG='00000000' then null
# MAGIC when BEGPROG <> 'null' then CONCAT(LEFT(BEGPROG,4),'-',SUBSTRING(BEGPROG,5,2),'-',RIGHT(BEGPROG,2))
# MAGIC else BEGPROG end as forecastPeriodStartDate
# MAGIC --,BEGPROG as forecastPeriodStartDate
# MAGIC ,case
# MAGIC when ENDEPROG='0' then null 
# MAGIC when ENDEPROG='00000000' then null
# MAGIC when ENDEPROG <> 'null' then CONCAT(LEFT(ENDEPROG,4),'-',SUBSTRING(ENDEPROG,5,2),'-',RIGHT(ENDEPROG,2))
# MAGIC else ENDEPROG end as forecastPeriodEndDate
# MAGIC --,ENDEPROG as forecastPeriodEndDate
# MAGIC ,ABLHINW as meterReaderNoteText
# MAGIC ,V_ZWSTAND as meterReadingBeforeDecimalPoint
# MAGIC ,N_ZWSTAND as meterReadingAfterDecimalPoint
# MAGIC ,V_ZWSTNDAB as billedMeterReadingBeforeDecimalPlaces
# MAGIC ,N_ZWSTNDAB as billedMeterReadingAfterDecimalPlaces
# MAGIC ,V_ZWSTVOR as previousMeterReadingBeforeDecimalPlaces
# MAGIC ,N_ZWSTVOR as previousMeterReadingAfterDecimalPlaces
# MAGIC ,V_ZWSTDIFF as meterReadingDifferenceBeforeDecimalPlaces
# MAGIC ,N_ZWSTDIFF as meterReadingDifferenceAfterDecimalPlaces 
# MAGIC from test.isu_dberchz2a --where DELTA_TS<'20211120044035'
# MAGIC )

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
# MAGIC select 
# MAGIC BELNR as billingDocumentNumber
# MAGIC ,BELZEILE as billingDocumentLineItemID
# MAGIC ,EQUNR as equipmentNumber
# MAGIC ,GERAET as deviceNumber
# MAGIC ,MATNR as materialNumber
# MAGIC ,ZWNUMMER as registerNumber
# MAGIC ,INDEXNR as registerRelationshipConsecutiveNumber
# MAGIC ,ABLESGR as meterReadingReasonCode
# MAGIC ,ABLESGRV as previousMeterReadingReasonCode
# MAGIC ,ATIM as billingMeterReadingTime
# MAGIC ,ATIMVA as previousMeterReadingTime
# MAGIC ,case
# MAGIC when ADATMAX='0' then null 
# MAGIC when ADATMAX='00000000' then null
# MAGIC when ADATMAX <> 'null' then CONCAT(LEFT(ADATMAX,4),'-',SUBSTRING(ADATMAX,5,2),'-',RIGHT(ADATMAX,2))
# MAGIC else ADATMAX end as maxMeterReadingDate
# MAGIC --,ADATMAX as maxMeterReadingDate
# MAGIC ,ATIMMAX as maxMeterReadingTime
# MAGIC ,case
# MAGIC when THGDATUM='0' then null 
# MAGIC when THGDATUM='00000000' then null
# MAGIC when THGDATUM <> 'null' then CONCAT(LEFT(THGDATUM,4),'-',SUBSTRING(THGDATUM,5,2),'-',RIGHT(THGDATUM,2))
# MAGIC else THGDATUM end as serviceAllocationDate
# MAGIC --,THGDATUM as serviceAllocationDate
# MAGIC ,case
# MAGIC when ZUORDDAT='0' then null 
# MAGIC when ZUORDDAT='00000000' then null
# MAGIC when ZUORDDAT <> 'null' then CONCAT(LEFT(ZUORDDAT,4),'-',SUBSTRING(ZUORDDAT,5,2),'-',RIGHT(ZUORDDAT,2))
# MAGIC else ZUORDDAT end as meterReadingAllocationDate
# MAGIC --,ZUORDDAT as meterReadingAllocationDate
# MAGIC ,ABLBELNR as suppressedMeterReadingDocumentID
# MAGIC ,LOGIKNR as logicalDeviceNumber
# MAGIC ,LOGIKZW as logicalRegisterNumber
# MAGIC ,ISTABLART as meterReadingTypeCode
# MAGIC ,ISTABLARTVA as previousMeterReadingTypeCode
# MAGIC ,EXTPKZ as meterReadingResultsSimulationIndicator
# MAGIC ,case
# MAGIC when BEGPROG='0' then null 
# MAGIC when BEGPROG='00000000' then null
# MAGIC when BEGPROG <> 'null' then CONCAT(LEFT(BEGPROG,4),'-',SUBSTRING(BEGPROG,5,2),'-',RIGHT(BEGPROG,2))
# MAGIC else BEGPROG end as forecastPeriodStartDate
# MAGIC --,BEGPROG as forecastPeriodStartDate
# MAGIC ,case
# MAGIC when ENDEPROG='0' then null 
# MAGIC when ENDEPROG='00000000' then null
# MAGIC when ENDEPROG <> 'null' then CONCAT(LEFT(ENDEPROG,4),'-',SUBSTRING(ENDEPROG,5,2),'-',RIGHT(ENDEPROG,2))
# MAGIC else ENDEPROG end as forecastPeriodEndDate
# MAGIC --,ENDEPROG as forecastPeriodEndDate
# MAGIC ,ABLHINW as meterReaderNoteText
# MAGIC ,V_ZWSTAND as meterReadingBeforeDecimalPoint
# MAGIC ,N_ZWSTAND as meterReadingAfterDecimalPoint
# MAGIC ,V_ZWSTNDAB as billedMeterReadingBeforeDecimalPlaces
# MAGIC ,N_ZWSTNDAB as billedMeterReadingAfterDecimalPlaces
# MAGIC ,V_ZWSTVOR as previousMeterReadingBeforeDecimalPlaces
# MAGIC ,N_ZWSTVOR as previousMeterReadingAfterDecimalPlaces
# MAGIC ,V_ZWSTDIFF as meterReadingDifferenceBeforeDecimalPlaces
# MAGIC ,N_ZWSTDIFF as meterReadingDifferenceAfterDecimalPlaces 
# MAGIC from test.isu_dberchz2a --where DELTA_TS<'20211117055649'
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
# MAGIC ,previousMeterReadingBeforeDecimalPlaces
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
# MAGIC ,N_ZWSTDIFF	as	meterReadingDifferenceAfterDecimalPlaces from test.isu_dberchz2a --where DELTA_TS<'20211117055649'
