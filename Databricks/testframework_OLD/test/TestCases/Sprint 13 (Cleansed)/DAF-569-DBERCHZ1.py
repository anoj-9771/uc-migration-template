# Databricks notebook source
# DBTITLE 1,[Config] Connection Setup to SLT
hostName = 'nt097dslt01.adsdev.swcdev'
databaseName = 'SLT_EQ1CLNT100'
port = 1433
sltkey = dbutils.secrets.get(scope="TestScope",key="slt-key")
sltuser = dbutils.secrets.get(scope="TestScope",key="slt-user")
dbURL = f'jdbc:sqlserver://{hostName}:{port};database={databaseName};user={sltuser};password={sltkey}'
qry = '(SELECT top 1000 MANDT ,BELNR ,BELZEILE ,CSNO ,BELZART ,ABSLKZ ,DIFFKZ ,BUCHREL ,MENGESTREL ,BETRSTREL ,STGRQNT ,STGRAMT ,PRINTREL ,AKLASSE ,BRANCHE ,TVORG ,GEGEN_TVORG ,LINESORT ,AB ,BIS ,TIMTYPZA ,SCHEMANR ,SNO ,PROGRAMM ,MASSBILL ,SAISON ,TIMBASIS ,TIMTYP ,FRAN_TYPE ,KONZIGR ,TARIFTYP ,TARIFNR ,KONDIGR ,STTARIF ,GEWKEY ,WDHFAKT ,TEMP_AREA ,DYNCANC01 ,DYNCANC02 ,DYNCANC03 ,DYNCANC04 ,DYNCANC05 ,DYNCANC ,DYNEXEC ,LRATESTEP ,PEB ,STAFO ,ARTMENGE ,STATTART ,TIMECONTRL ,TCNUMTOR ,TCDENOMTOR ,TIMTYPQUOT ,AKTIV ,KONZVER ,PERTYP ,OUCONTRACT ,V_ABRMENGE ,N_ABRMENGE ,DELTA_TS FROM SLT_EQ1CLNT100.eq1.DBERCHZ1 order by BELNR, BELZEILE, AB) myTable'
df = spark.read.jdbc(url=dbURL, table=qry, lowerBound=1, upperBound=100000, numPartitions=10)
display(df)

# COMMAND ----------

hostName = 'nt097dslt01.adsdev.swcdev'
databaseName = 'SLT_EQ1CLNT100'
port = 1433
sltkey = dbutils.secrets.get(scope="TestScope",key="slt-key")
sltuser = dbutils.secrets.get(scope="TestScope",key="slt-user")
dbURL = f'jdbc:sqlserver://{hostName}:{port};database={databaseName};user={sltuser};password={sltkey}'
qry = "(SELECT MANDT ,BELNR ,BELZEILE ,CSNO ,BELZART ,ABSLKZ ,DIFFKZ ,BUCHREL ,MENGESTREL ,BETRSTREL ,STGRQNT ,STGRAMT ,PRINTREL ,AKLASSE ,BRANCHE ,TVORG ,GEGEN_TVORG ,LINESORT ,AB ,BIS ,TIMTYPZA ,SCHEMANR ,SNO ,PROGRAMM ,MASSBILL ,SAISON ,TIMBASIS ,TIMTYP ,FRAN_TYPE ,KONZIGR ,TARIFTYP ,TARIFNR ,KONDIGR ,STTARIF ,GEWKEY ,WDHFAKT ,TEMP_AREA ,DYNCANC01 ,DYNCANC02 ,DYNCANC03 ,DYNCANC04 ,DYNCANC05 ,DYNCANC ,DYNEXEC ,LRATESTEP ,PEB ,STAFO ,ARTMENGE ,STATTART ,TIMECONTRL ,TCNUMTOR ,TCDENOMTOR ,TIMTYPQUOT ,AKTIV ,KONZVER ,PERTYP ,OUCONTRACT ,V_ABRMENGE ,N_ABRMENGE ,DELTA_TS FROM SLT_EQ1CLNT100.eq1.DBERCHZ1 WHERE BELNR = '010000000169') myTable"
df = spark.read.jdbc(url=dbURL, table=qry, lowerBound=1, upperBound=100000, numPartitions=10)
display(df)

# COMMAND ----------

# DBTITLE 1,Delete if exists
# MAGIC %sql
# MAGIC drop table if exists test.isu_dberchz1

# COMMAND ----------

# DBTITLE 1,Delete if exists
# MAGIC %fs rm -r dbfs:/user/hive/warehouse/test.db/isu_dberchz1

# COMMAND ----------

# DBTITLE 1,Create SLT table in Test
df.write.format("json").saveAsTable ("test" + "." + "isu_dberchz1_subset")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count (*) from test.isu_dberchz1_subset

# COMMAND ----------

# DBTITLE 1,[Source] Schema Check - Refer to Raw2Cleansed Mapping
lakedfs = spark.sql("select * from test.isu_dberchz1")
display(lakedfs)
lakedfs.printSchema()

# COMMAND ----------

# DBTITLE 1,[Target] Schema Check
lakedf = spark.sql("select * from cleansed.isu_dberchz1")
display(lakedf)
lakedf.printSchema()

# COMMAND ----------

# DBTITLE 1,[Source] with mapping
# MAGIC %sql
# MAGIC SELECT
# MAGIC BELNR as billingDocumentNumber
# MAGIC ,BELZEILE as billingDocumentLineItemId
# MAGIC ,CSNO as billingSequenceNumber
# MAGIC ,BELZART as lineItemTypeCode
# MAGIC ,ABSLKZ as billingLineItemBudgetBillingIndicator
# MAGIC ,DIFFKZ as lineItemDiscountStatisticsIndicator
# MAGIC ,BUCHREL as billingLineItemReleventPostingIndicator
# MAGIC ,MENGESTREL as billedValueStatisticallyReleventIndicator
# MAGIC ,BETRSTREL as billingLineItemStatisticallyReleventAmount
# MAGIC ,STGRQNT as quantityStatisticsGroupCode
# MAGIC ,STGRAMT as amountStatisticsGroupCoide
# MAGIC ,PRINTREL as billingLinePrintReleventIndicator
# MAGIC ,AKLASSE as billingClassCode
# MAGIC ,b.billingClass as billingClass
# MAGIC ,BRANCHE as industryText
# MAGIC ,TVORG as subtransactionForDocumentItem
# MAGIC ,GEGEN_TVORG as offsettingTransactionSubtransactionForDocumentItem
# MAGIC ,LINESORT as poresortingBillingLineItems
# MAGIC ,AB as validFromDate
# MAGIC ,BIS as validToDate
# MAGIC ,TIMTYPZA as billingLineItemTimeCategoryCode
# MAGIC ,SCHEMANR as billingSchemaNumber
# MAGIC ,SNO as billingSchemaStepSequenceNumber
# MAGIC ,PROGRAMM as variantProgramNumber
# MAGIC ,MASSBILL as billingMeasurementUnitCode
# MAGIC ,SAISON as seasonNumber
# MAGIC ,TIMBASIS as timeBasisCode
# MAGIC ,TIMTYP as timeCategoryCode
# MAGIC ,FRAN_TYPE as franchiseFeeTypeCode
# MAGIC ,KONZIGR as franchiseFeeGroupNumber
# MAGIC ,TARIFTYP as rateTypeCode
# MAGIC ,TARIFNR as rateId
# MAGIC ,KONDIGR as rateFactGroupNumber
# MAGIC ,STTARIF as statisticalRate
# MAGIC ,GEWKEY as weightingKeyId
# MAGIC ,WDHFAKT as referenceValuesForRepetitionFactor
# MAGIC ,TEMP_AREA as tempratureArea
# MAGIC ,DYNCANC01 as reversalDynamicPeriodControl1
# MAGIC ,DYNCANC02 as reversalDynamicPeriodControl2
# MAGIC ,DYNCANC03 as reversalDynamicPeriodControl3
# MAGIC ,DYNCANC04 as reversalDynamicPeriodControl4
# MAGIC ,DYNCANC05 as reversalDynamicPeriodControl5
# MAGIC ,DYNCANC as reverseBackbillingIndicator
# MAGIC ,DYNEXEC as allocateBackbillingIndicator
# MAGIC ,LRATESTEP as eateStepLogicalNumber
# MAGIC ,PEB as periodEndBillingIndicator
# MAGIC ,STAFO as statististicsUpdateGroupCode
# MAGIC ,ARTMENGE as billedQuantityStatisticsCode
# MAGIC ,STATTART as statisticalAnalysisRateType
# MAGIC ,TIMECONTRL as periodControlCode
# MAGIC ,TCNUMTOR as timesliceNumeratorTimePortion
# MAGIC ,TCDENOMTOR as timesliceDenominatorTimePortion
# MAGIC ,TIMTYPQUOT as timesliceTimeCatogoryTimePortion
# MAGIC ,AKTIV as meterReadingActiveIndicator
# MAGIC ,KONZVER as franchiseContractIndicator
# MAGIC ,PERTYP as billingPeriodInternalCategoryCode
# MAGIC ,OUCONTRACT as individualContractID
# MAGIC ,V_ABRMENGE as billingQuantityPlaceBeforeDecimalPoint
# MAGIC ,N_ABRMENGE as billingQuantityPlaceAfterDecimalPoint
# MAGIC from test.isu_dberchz1 a
# MAGIC left join cleansed.isu_0UC_AKLASSE_TEXT b
# MAGIC on a.AKLASSE = b.billingClassCode --and b.SPRAS ='E'

# COMMAND ----------

# DBTITLE 1,[Verification] Count Checks - N/A **Manually done for this table**
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from cleansed.isu_dberchz1
# MAGIC /**select count (*) as RecordCount, 'Target' as TableName from cleansed.isu_dberchz1
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from (
# MAGIC SELECT 
# MAGIC BELNR as billingDocumentNumber
# MAGIC ,BELZEILE as billingDocumentLineItemId
# MAGIC ,CSNO as billingSequenceNumber
# MAGIC ,BELZART as lineItemTypeCode
# MAGIC ,ABSLKZ as billingLineItemBudgetBillingIndicator
# MAGIC ,DIFFKZ as lineItemDiscountStatisticsIndicator
# MAGIC ,BUCHREL as billingLineItemReleventPostingIndicator
# MAGIC ,MENGESTREL as billedValueStatisticallyReleventIndicator
# MAGIC ,BETRSTREL as billingLineItemStatisticallyReleventAmount
# MAGIC ,STGRQNT as quantityStatisticsGroupCode
# MAGIC ,STGRAMT as amountStatisticsGroupCoide
# MAGIC ,PRINTREL as billingLinePrintReleventIndicator
# MAGIC ,AKLASSE as billingClassCode
# MAGIC ,b.billingClass as billingClass
# MAGIC ,BRANCHE as industryText
# MAGIC ,TVORG as subtransactionForDocumentItem
# MAGIC ,GEGEN_TVORG as offsettingTransactionSubtransactionForDocumentItem
# MAGIC ,LINESORT as poresortingBillingLineItems
# MAGIC ,AB as validFromDate
# MAGIC ,BIS as validToDate
# MAGIC ,TIMTYPZA as billingLineItemTimeCategoryCode
# MAGIC ,SCHEMANR as billingSchemaNumber
# MAGIC ,SNO as billingSchemaStepSequenceNumber
# MAGIC ,PROGRAMM as variantProgramNumber
# MAGIC ,MASSBILL as billingMeasurementUnitCode
# MAGIC ,SAISON as seasonNumber
# MAGIC ,TIMBASIS as timeBasisCode
# MAGIC ,TIMTYP as timeCategoryCode
# MAGIC ,FRAN_TYPE as franchiseFeeTypeCode
# MAGIC ,KONZIGR as franchiseFeeGroupNumber
# MAGIC ,TARIFTYP as rateTypeCode
# MAGIC ,TARIFNR as rateId
# MAGIC ,KONDIGR as rateFactGroupNumber
# MAGIC ,STTARIF as statisticalRate
# MAGIC ,GEWKEY as weightingKeyId
# MAGIC ,WDHFAKT as referenceValuesForRepetitionFactor
# MAGIC ,TEMP_AREA as tempratureArea
# MAGIC ,DYNCANC01 as reversalDynamicPeriodControl1
# MAGIC ,DYNCANC02 as reversalDynamicPeriodControl2
# MAGIC ,DYNCANC03 as reversalDynamicPeriodControl3
# MAGIC ,DYNCANC04 as reversalDynamicPeriodControl4
# MAGIC ,DYNCANC05 as reversalDynamicPeriodControl5
# MAGIC ,DYNCANC as reverseBackbillingIndicator
# MAGIC ,DYNEXEC as allocateBackbillingIndicator
# MAGIC ,LRATESTEP as eateStepLogicalNumber
# MAGIC ,PEB as periodEndBillingIndicator
# MAGIC ,STAFO as statististicsUpdateGroupCode
# MAGIC ,ARTMENGE as billedQuantityStatisticsCode
# MAGIC ,STATTART as statisticalAnalysisRateType
# MAGIC ,TIMECONTRL as periodControlCode
# MAGIC ,TCNUMTOR as timesliceNumeratorTimePortion
# MAGIC ,TCDENOMTOR as timesliceDenominatorTimePortion
# MAGIC ,TIMTYPQUOT as timesliceTimeCatogoryTimePortion
# MAGIC ,AKTIV as meterReadingActiveIndicator
# MAGIC ,KONZVER as franchiseContractIndicator
# MAGIC ,PERTYP as billingPeriodInternalCategoryCode
# MAGIC ,OUCONTRACT as individualContractID
# MAGIC ,V_ABRMENGE as billingQuantityPlaceBeforeDecimalPoint
# MAGIC ,N_ABRMENGE as billingQuantityPlaceAfterDecimalPoint
# MAGIC from  a
# MAGIC left join cleansed.isu_0UC_AKLASSE_TEXT b
# MAGIC on a.AKLASSE = b.billingClassCode --limit 10000 --and DELTA_TS < '20211117055649' )--and b.SPRAS ='E' **/

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT billingDocumentNumber,billingDocumentLineItemId, COUNT (*) as count
# MAGIC FROM cleansed.isu_dberchz1
# MAGIC GROUP BY billingDocumentNumber, billingDocumentLineItemId
# MAGIC HAVING COUNT (*) > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT * FROM (
# MAGIC SELECT
# MAGIC *,
# MAGIC row_number() OVER(PARTITION BY billingDocumentNumber, billingDocumentLineItemId order by billingDocumentNumber) as rn
# MAGIC FROM cleansed.isu_dberchz1
# MAGIC )a where a.rn > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Source and Target Data
# MAGIC %sql
# MAGIC (SELECT
# MAGIC BELNR as billingDocumentNumber
# MAGIC ,BELZEILE as billingDocumentLineItemId
# MAGIC ,CSNO as billingSequenceNumber
# MAGIC ,BELZART as lineItemTypeCode
# MAGIC ,ABSLKZ as billingLineItemBudgetBillingIndicator
# MAGIC ,DIFFKZ as lineItemDiscountStatisticsIndicator
# MAGIC ,BUCHREL as billingLineItemRelevantPostingIndicator
# MAGIC ,MENGESTREL as billedValueStatisticallyRelevantIndicator
# MAGIC ,BETRSTREL as billingLineItemStatisticallyRelevantAmount
# MAGIC ,STGRQNT as quantityStatisticsGroupCode
# MAGIC ,STGRAMT as amountStatisticsGroupCode
# MAGIC ,PRINTREL as billingLinePrintRelevantIndicator
# MAGIC ,AKLASSE as billingClassCode
# MAGIC ,b.billingClass as billingClass
# MAGIC ,BRANCHE as industryText
# MAGIC ,TVORG as subtransactionForDocumentItem
# MAGIC ,GEGEN_TVORG as offsettingTransactionSubtransactionForDocumentItem
# MAGIC ,LINESORT as presortingBillingLineItems
# MAGIC ,case when (CONCAT(LEFT(AB,4),'-',SUBSTRING(AB,5,2),'-',SUBSTRING(AB,7,2))) < '1900-01-01' then '1900-01-01'
# MAGIC else CONCAT(LEFT(AB,4),'-',SUBSTRING(AB,5,2),'-',SUBSTRING(AB,7,2)) end as validFromDate
# MAGIC ,CONCAT(LEFT(BIS,4),'-',SUBSTRING(BIS,5,2),'-',SUBSTRING(BIS,7,2)) as validToDate
# MAGIC ,TIMTYPZA as billingLineItemTimeCategoryCode
# MAGIC ,SCHEMANR as billingSchemaNumber
# MAGIC ,SNO as billingSchemaStepSequenceNumber
# MAGIC ,PROGRAMM as variantProgramNumber
# MAGIC ,MASSBILL as billingMeasurementUnitCode
# MAGIC ,SAISON as seasonNumber
# MAGIC ,TIMBASIS as timeBasisCode
# MAGIC ,TIMTYP as timeCategoryCode
# MAGIC ,FRAN_TYPE as franchiseFeeTypeCode
# MAGIC ,KONZIGR as franchiseFeeGroupNumber
# MAGIC ,TARIFTYP as rateTypeCode
# MAGIC ,TARIFNR as rateId
# MAGIC ,KONDIGR as rateFactGroupNumber
# MAGIC ,STTARIF as statisticalRate
# MAGIC ,GEWKEY as weightingKeyId
# MAGIC ,WDHFAKT as referenceValuesForRepetitionFactor
# MAGIC ,TEMP_AREA as tempratureArea
# MAGIC ,DYNCANC01 as reversalDynamicPeriodControl1
# MAGIC ,DYNCANC02 as reversalDynamicPeriodControl2
# MAGIC ,DYNCANC03 as reversalDynamicPeriodControl3
# MAGIC ,DYNCANC04 as reversalDynamicPeriodControl4
# MAGIC ,DYNCANC05 as reversalDynamicPeriodControl5
# MAGIC ,DYNCANC as reverseBackbillingIndicator
# MAGIC ,DYNEXEC as allocateBackbillingIndicator
# MAGIC ,LRATESTEP as eateStepLogicalNumber
# MAGIC ,PEB as periodEndBillingIndicator
# MAGIC ,STAFO as statististicsUpdateGroupCode
# MAGIC ,ARTMENGE as billedQuantityStatisticsCode
# MAGIC ,STATTART as statisticalAnalysisRateType
# MAGIC ,TIMECONTRL as periodControlCode
# MAGIC --,TCNUMTOR as timesliceNumeratorTimePortion
# MAGIC --,TCDENOMTOR as timesliceDenominatorTimePortion
# MAGIC ,TIMTYPQUOT as timesliceTimeCatogoryTimePortion
# MAGIC ,AKTIV as meterReadingActiveIndicator
# MAGIC ,KONZVER as franchiseContractIndicator
# MAGIC ,PERTYP as billingPeriodInternalCategoryCode
# MAGIC ,OUCONTRACT as individualContractID
# MAGIC ,V_ABRMENGE as billingQuantityPlaceBeforeDecimalPoint
# MAGIC ,N_ABRMENGE as billingQuantityPlaceAfterDecimalPoint
# MAGIC from test.isu_dberchz1_subset a
# MAGIC left join cleansed.isu_0UC_AKLASSE_TEXT b
# MAGIC on a.AKLASSE = b.billingClassCode -- and DELTA_TS < '20211117055649' 
# MAGIC order by BELZEILE)
# MAGIC 
# MAGIC EXCEPT
# MAGIC select
# MAGIC billingDocumentNumber
# MAGIC ,billingDocumentLineItemId
# MAGIC ,billingSequenceNumber
# MAGIC ,lineItemTypeCode
# MAGIC ,billingLineItemBudgetBillingIndicator
# MAGIC ,lineItemDiscountStatisticsIndicator
# MAGIC ,billingLineItemRelevantPostingIndicator
# MAGIC ,billedValueStatisticallyRelevantIndicator
# MAGIC ,billingLineItemStatisticallyRelevantAmount
# MAGIC ,quantityStatisticsGroupCode
# MAGIC ,amountStatisticsGroupCode
# MAGIC ,billingLinePrintRelevantIndicator
# MAGIC ,billingClassCode
# MAGIC ,billingClass
# MAGIC ,industryText
# MAGIC ,subtransactionForDocumentItem
# MAGIC ,offsettingTransactionSubtransactionForDocumentItem
# MAGIC ,presortingBillingLineItems
# MAGIC ,validFromDate
# MAGIC ,validToDate
# MAGIC ,billingLineItemTimeCategoryCode
# MAGIC ,billingSchemaNumber
# MAGIC ,billingSchemaStepSequenceNumber
# MAGIC ,variantProgramNumber
# MAGIC ,billingMeasurementUnitCode
# MAGIC ,seasonNumber
# MAGIC ,timeBasisCode
# MAGIC ,timeCategoryCode
# MAGIC ,franchiseFeeTypeCode
# MAGIC ,franchiseFeeGroupNumber
# MAGIC ,rateTypeCode
# MAGIC ,rateId
# MAGIC ,rateFactGroupNumber
# MAGIC ,statisticalRate
# MAGIC ,weightingKeyId
# MAGIC ,referenceValuesForRepetitionFactor
# MAGIC ,temperatureArea
# MAGIC ,reversalDynamicPeriodControl1
# MAGIC ,reversalDynamicPeriodControl2
# MAGIC ,reversalDynamicPeriodControl3
# MAGIC ,reversalDynamicPeriodControl4
# MAGIC ,reversalDynamicPeriodControl5
# MAGIC ,reverseBackbillingIndicator
# MAGIC ,allocateBackbillingIndicator
# MAGIC ,rateStepLogicalNumber
# MAGIC ,periodEndBillingIndicator
# MAGIC ,statisticsUpdateGroupCode
# MAGIC ,billedQuantityStatisticsCode
# MAGIC ,statisticalAnalysisRateType
# MAGIC ,periodControlCode
# MAGIC --,timesliceNumeratorTimePortion
# MAGIC --,timesliceDenominatorTimePortion
# MAGIC ,timesliceTimeCategoryTimePortion
# MAGIC ,meterReadingActiveIndicator
# MAGIC ,franchiseContractIndicator
# MAGIC ,billingPeriodInternalCategoryCode
# MAGIC ,individualContractID
# MAGIC ,billingQuantityPlaceBeforeDecimalPoint
# MAGIC ,billingQuantityPlaceAfterDecimalPoint
# MAGIC FROM
# MAGIC cleansed.isu_dberchz1 where billingDocumentNumber = '010000000169' order by billingDocumentLineItemId

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Target and Source Data
# MAGIC %sql
# MAGIC (select
# MAGIC billingDocumentNumber
# MAGIC ,billingDocumentLineItemId
# MAGIC ,billingSequenceNumber
# MAGIC ,lineItemTypeCode
# MAGIC ,billingLineItemBudgetBillingIndicator
# MAGIC ,lineItemDiscountStatisticsIndicator
# MAGIC ,billingLineItemRelevantPostingIndicator
# MAGIC ,billedValueStatisticallyRelevantIndicator
# MAGIC ,billingLineItemStatisticallyRelevantAmount
# MAGIC ,quantityStatisticsGroupCode
# MAGIC ,amountStatisticsGroupCode
# MAGIC ,billingLinePrintRelevantIndicator
# MAGIC ,billingClassCode
# MAGIC ,billingClass
# MAGIC ,industryText
# MAGIC ,subtransactionForDocumentItem
# MAGIC ,offsettingTransactionSubtransactionForDocumentItem
# MAGIC ,presortingBillingLineItems
# MAGIC ,validFromDate
# MAGIC ,validToDate
# MAGIC ,billingLineItemTimeCategoryCode
# MAGIC ,billingSchemaNumber
# MAGIC ,billingSchemaStepSequenceNumber
# MAGIC ,variantProgramNumber
# MAGIC ,billingMeasurementUnitCode
# MAGIC ,seasonNumber
# MAGIC ,timeBasisCode
# MAGIC ,timeCategoryCode
# MAGIC ,franchiseFeeTypeCode
# MAGIC ,franchiseFeeGroupNumber
# MAGIC ,rateTypeCode
# MAGIC ,rateId
# MAGIC ,rateFactGroupNumber
# MAGIC ,statisticalRate
# MAGIC ,weightingKeyId
# MAGIC ,referenceValuesForRepetitionFactor
# MAGIC ,temperatureArea
# MAGIC ,reversalDynamicPeriodControl1
# MAGIC ,reversalDynamicPeriodControl2
# MAGIC ,reversalDynamicPeriodControl3
# MAGIC ,reversalDynamicPeriodControl4
# MAGIC ,reversalDynamicPeriodControl5
# MAGIC ,reverseBackbillingIndicator
# MAGIC ,allocateBackbillingIndicator
# MAGIC ,rateStepLogicalNumber
# MAGIC ,periodEndBillingIndicator
# MAGIC ,statisticsUpdateGroupCode
# MAGIC ,billedQuantityStatisticsCode
# MAGIC ,statisticalAnalysisRateType
# MAGIC ,periodControlCode
# MAGIC --,timesliceNumeratorTimePortion
# MAGIC --,timesliceDenominatorTimePortion
# MAGIC ,timesliceTimeCategoryTimePortion
# MAGIC ,meterReadingActiveIndicator
# MAGIC ,franchiseContractIndicator
# MAGIC ,billingPeriodInternalCategoryCode
# MAGIC ,individualContractID
# MAGIC ,billingQuantityPlaceBeforeDecimalPoint
# MAGIC ,billingQuantityPlaceAfterDecimalPoint
# MAGIC FROM
# MAGIC cleansed.isu_dberchz1 where billingDocumentNumber = '010000000169' order by billingDocumentLineItemId)
# MAGIC except
# MAGIC (SELECT
# MAGIC BELNR as billingDocumentNumber
# MAGIC ,BELZEILE as billingDocumentLineItemId
# MAGIC ,CSNO as billingSequenceNumber
# MAGIC ,BELZART as lineItemTypeCode
# MAGIC ,ABSLKZ as billingLineItemBudgetBillingIndicator
# MAGIC ,DIFFKZ as lineItemDiscountStatisticsIndicator
# MAGIC ,BUCHREL as billingLineItemRelevantPostingIndicator
# MAGIC ,MENGESTREL as billedValueStatisticallyRelevantIndicator
# MAGIC ,BETRSTREL as billingLineItemStatisticallyRelevantAmount
# MAGIC ,STGRQNT as quantityStatisticsGroupCode
# MAGIC ,STGRAMT as amountStatisticsGroupCode
# MAGIC ,PRINTREL as billingLinePrintRelevantIndicator
# MAGIC ,AKLASSE as billingClassCode
# MAGIC ,b.billingClass as billingClass
# MAGIC ,BRANCHE as industryText
# MAGIC ,TVORG as subtransactionForDocumentItem
# MAGIC ,GEGEN_TVORG as offsettingTransactionSubtransactionForDocumentItem
# MAGIC ,LINESORT as presortingBillingLineItems
# MAGIC ,case when (CONCAT(LEFT(AB,4),'-',SUBSTRING(AB,5,2),'-',SUBSTRING(AB,7,2))) < '1900-01-01' then '1900-01-01'
# MAGIC else CONCAT(LEFT(AB,4),'-',SUBSTRING(AB,5,2),'-',SUBSTRING(AB,7,2)) end as validFromDate
# MAGIC ,CONCAT(LEFT(BIS,4),'-',SUBSTRING(BIS,5,2),'-',SUBSTRING(BIS,7,2)) as validToDate
# MAGIC ,TIMTYPZA as billingLineItemTimeCategoryCode
# MAGIC ,SCHEMANR as billingSchemaNumber
# MAGIC ,SNO as billingSchemaStepSequenceNumber
# MAGIC ,PROGRAMM as variantProgramNumber
# MAGIC ,MASSBILL as billingMeasurementUnitCode
# MAGIC ,SAISON as seasonNumber
# MAGIC ,TIMBASIS as timeBasisCode
# MAGIC ,TIMTYP as timeCategoryCode
# MAGIC ,FRAN_TYPE as franchiseFeeTypeCode
# MAGIC ,KONZIGR as franchiseFeeGroupNumber
# MAGIC ,TARIFTYP as rateTypeCode
# MAGIC ,TARIFNR as rateId
# MAGIC ,KONDIGR as rateFactGroupNumber
# MAGIC ,STTARIF as statisticalRate
# MAGIC ,GEWKEY as weightingKeyId
# MAGIC ,WDHFAKT as referenceValuesForRepetitionFactor
# MAGIC ,TEMP_AREA as temperatureArea
# MAGIC ,DYNCANC01 as reversalDynamicPeriodControl1
# MAGIC ,DYNCANC02 as reversalDynamicPeriodControl2
# MAGIC ,DYNCANC03 as reversalDynamicPeriodControl3
# MAGIC ,DYNCANC04 as reversalDynamicPeriodControl4
# MAGIC ,DYNCANC05 as reversalDynamicPeriodControl5
# MAGIC ,DYNCANC as reverseBackbillingIndicator
# MAGIC ,DYNEXEC as allocateBackbillingIndicator
# MAGIC ,LRATESTEP as eateStepLogicalNumber
# MAGIC ,PEB as periodEndBillingIndicator
# MAGIC ,STAFO as statisticsUpdateGroupCode
# MAGIC ,ARTMENGE as billedQuantityStatisticsCode
# MAGIC ,STATTART as statisticalAnalysisRateType
# MAGIC ,TIMECONTRL as periodControlCode
# MAGIC --,TCNUMTOR as timesliceNumeratorTimePortion
# MAGIC --,TCDENOMTOR as timesliceDenominatorTimePortion
# MAGIC ,TIMTYPQUOT as timesliceTimeCatogoryTimePortion
# MAGIC ,AKTIV as meterReadingActiveIndicator
# MAGIC ,KONZVER as franchiseContractIndicator
# MAGIC ,PERTYP as billingPeriodInternalCategoryCode
# MAGIC ,OUCONTRACT as individualContractID
# MAGIC ,V_ABRMENGE as billingQuantityPlaceBeforeDecimalPoint
# MAGIC ,N_ABRMENGE as billingQuantityPlaceAfterDecimalPoint
# MAGIC from test.isu_dberchz1_subset a
# MAGIC left join cleansed.isu_0UC_AKLASSE_TEXT b
# MAGIC on a.AKLASSE = b.billingClassCode -- and DELTA_TS < '20211117055649' 
# MAGIC order by BELZEILE)

# COMMAND ----------

# DBTITLE 1,Additional Test #1
# MAGIC %sql
# MAGIC select count (*) from (
# MAGIC select distinct billingDocumentNumber from
# MAGIC cleansed.isu_dberchz1)a
# MAGIC 
# MAGIC --select count (*) from (select distinct BELNR from SLT_EQ1CLNT100.EQ1.DBERCHZ1)a

# COMMAND ----------

# DBTITLE 1,Additional Test #2
# MAGIC %sql
# MAGIC select count (*) from (
# MAGIC select distinct billingDocumentLineItemId from
# MAGIC cleansed.isu_dberchz1)a
# MAGIC 
# MAGIC --select count (*) from (select distinct BELZEILE from SLT_EQ1CLNT100.EQ1.DBERCHZ1)a

# COMMAND ----------

# DBTITLE 1,Additional Test #3
# MAGIC %sql
# MAGIC select count (*) from (
# MAGIC select distinct amountStatisticsGroupCode from
# MAGIC cleansed.isu_dberchz1)a
# MAGIC 
# MAGIC --select count (*) from (select distinct STGRAMT from SLT_EQ1CLNT100.EQ1.DBERCHZ1)a

# COMMAND ----------

# DBTITLE 1,Additional Test #4
# MAGIC %sql
# MAGIC select distinct billingClassCode from
# MAGIC cleansed.isu_dberchz1
# MAGIC 
# MAGIC --select distinct AKLASSE from SLT_EQ1CLNT100.EQ1.DBERCHZ1

# COMMAND ----------

# DBTITLE 1,Additional Test #4.5
# MAGIC %sql
# MAGIC select billingClassCode, count(*) from
# MAGIC cleansed.isu_dberchz1
# MAGIC group by billingClassCode
# MAGIC 
# MAGIC --select distinct AKLASSE from SLT_EQ1CLNT100.EQ1.DBERCHZ1

# COMMAND ----------

# DBTITLE 1,Additional Test #5
# MAGIC %sql
# MAGIC select validfromdate,validtodate from
# MAGIC cleansed.isu_dberchz1 
# MAGIC where validtodate < validfromdate
# MAGIC order by validfromdate asc
# MAGIC 
# MAGIC --select AB,BIS from SLT_EQ1CLNT100.EQ1.DBERCHZ1 where BIS < AB order by AB

# COMMAND ----------

# DBTITLE 1,Additional Test #6
# MAGIC %sql
# MAGIC select billingdocumentnumber
# MAGIC from cleansed.isu_dberchz1
# MAGIC where billingdocumentnumber in ('', 'null') or billingdocumentnumber is null

# COMMAND ----------

# DBTITLE 1,Additional Test #7
# MAGIC %sql
# MAGIC select billingDocumentLineItemId
# MAGIC from cleansed.isu_dberchz1
# MAGIC where billingDocumentLineItemId in ('', 'null') or billingdocumentnumber is null

# COMMAND ----------

# MAGIC %sql
# MAGIC select max(billingDocumentNumber) from cleansed.isu_dberchz1

# COMMAND ----------

# MAGIC %sql
# MAGIC select max(billingDocumentLineItemId) from cleansed.isu_dberchz1

# COMMAND ----------

# MAGIC %sql
# MAGIC select validFromDate, validToDate, * from
# MAGIC cleansed.isu_dberchz1
# MAGIC where 
# MAGIC billingDocumentNumber = '010000002929' and billingDocumentLineItemId = '000005'
