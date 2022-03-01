# Databricks notebook source
# DBTITLE 1,[Config] Connection Setup
storage_account_name = "sadaftest01"
storage_account_access_key = dbutils.secrets.get(scope="Test-Access",key="test-datalake-key")
container_name = "raw"
file_location = "wasbs://raw@sadaftest01.blob.core.windows.net/sapisu/dberchz1/json/year=2021/month=09/day=27/DBO.DBERCHZ1_2021-09-27_165357_260.json.gz"
file_type = "json"
print(storage_account_name)

# COMMAND ----------

spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
  storage_account_access_key)


# COMMAND ----------

# MAGIC %sql
# MAGIC select * From cleansed.isu_dberchz1 where billingdocumentnumber = '193006781302'

# COMMAND ----------

hostName = 'nt097dslt01.adsdev.swcdev'
databaseName = 'SLT_EQ1CLNT100'
port = 1433
dbURL =
qry = '(select * from EQ1.DBERCHZ1 where BELNR = 193006781302) myTable'
df = spark.read.jdbc(url=dbURL, table=qry, lowerBound=1, upperBound=100000, numPartitions=10)
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE test.isu_dberchz1
# MAGIC dbfs:/user/hive/warehouse/test.db/isu_dberchz1

# COMMAND ----------

df.write.format("json").saveAsTable ("test" + "." + "isu_dberchz1a")

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
lakedf = spark.sql("select * from cleansed.isu_dberchz1")

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
# MAGIC select * from cleansed.isu_0UC_AKLASSE_TEXT

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
# MAGIC from Source a
# MAGIC left join cleansed.isu_0UC_AKLASSE_TEXT b
# MAGIC on a.AKLASSE = b.billingClassCode --and b.SPRAS ='E'

# COMMAND ----------

# DBTITLE 1,[Verification] Count Checks
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from cleansed.isu_dberchz1
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
# MAGIC from Source a
# MAGIC left join cleansed.isu_0UC_AKLASSE_TEXT b
# MAGIC on a.AKLASSE = b.billingClassCode and DELTA_TS < '20211117055649' )--and b.SPRAS ='E' 

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
# MAGIC from Source a
# MAGIC left join cleansed.isu_0UC_AKLASSE_TEXT b
# MAGIC on a.AKLASSE = b.billingClassCode  and DELTA_TS < '20211117055649'
# MAGIC 
# MAGIC EXCEPT
# MAGIC select
# MAGIC billingDocumentNumber
# MAGIC ,billingDocumentLineItemId
# MAGIC ,billingSequenceNumber
# MAGIC ,lineItemTypeCode
# MAGIC ,billingLineItemBudgetBillingIndicator
# MAGIC ,lineItemDiscountStatisticsIndicator
# MAGIC ,billingLineItemReleventPostingIndicator
# MAGIC ,billedValueStatisticallyReleventIndicator
# MAGIC ,billingLineItemStatisticallyReleventAmount
# MAGIC ,quantityStatisticsGroupCode
# MAGIC ,amountStatisticsGroupCoide
# MAGIC ,billingLinePrintReleventIndicator
# MAGIC ,billingClassCode
# MAGIC ,billingClass
# MAGIC ,industryText
# MAGIC ,subtransactionForDocumentItem
# MAGIC ,offsettingTransactionSubtransactionForDocumentItem
# MAGIC ,poresortingBillingLineItems
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
# MAGIC ,tempratureArea
# MAGIC ,reversalDynamicPeriodControl1
# MAGIC ,reversalDynamicPeriodControl2
# MAGIC ,reversalDynamicPeriodControl3
# MAGIC ,reversalDynamicPeriodControl4
# MAGIC ,reversalDynamicPeriodControl5
# MAGIC ,reverseBackbillingIndicator
# MAGIC ,allocateBackbillingIndicator
# MAGIC ,eateStepLogicalNumber
# MAGIC ,periodEndBillingIndicator
# MAGIC ,statististicsUpdateGroupCode
# MAGIC ,billedQuantityStatisticsCode
# MAGIC ,statisticalAnalysisRateType
# MAGIC ,periodControlCode
# MAGIC ,timesliceNumeratorTimePortion
# MAGIC ,timesliceDenominatorTimePortion
# MAGIC ,timesliceTimeCatogoryTimePortion
# MAGIC ,meterReadingActiveIndicator
# MAGIC ,franchiseContractIndicator
# MAGIC ,billingPeriodInternalCategoryCode
# MAGIC ,individualContractID
# MAGIC ,billingQuantityPlaceBeforeDecimalPoint
# MAGIC ,billingQuantityPlaceAfterDecimalPoint
# MAGIC 
# MAGIC 
# MAGIC FROM
# MAGIC cleansed.isu_dberchz1

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Target and Source Data
# MAGIC %sql
# MAGIC select
# MAGIC billingDocumentNumber
# MAGIC ,billingDocumentLineItemId
# MAGIC ,billingSequenceNumber
# MAGIC ,lineItemTypeCode
# MAGIC ,billingLineItemBudgetBillingIndicator
# MAGIC ,lineItemDiscountStatisticsIndicator
# MAGIC ,billingLineItemReleventPostingIndicator
# MAGIC ,billedValueStatisticallyReleventIndicator
# MAGIC ,billingLineItemStatisticallyReleventAmount
# MAGIC ,quantityStatisticsGroupCode
# MAGIC ,amountStatisticsGroupCoide
# MAGIC ,billingLinePrintReleventIndicator
# MAGIC ,billingClassCode
# MAGIC ,billingClass
# MAGIC ,industryText
# MAGIC ,subtransactionForDocumentItem
# MAGIC ,offsettingTransactionSubtransactionForDocumentItem
# MAGIC ,poresortingBillingLineItems
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
# MAGIC ,tempratureArea
# MAGIC ,reversalDynamicPeriodControl1
# MAGIC ,reversalDynamicPeriodControl2
# MAGIC ,reversalDynamicPeriodControl3
# MAGIC ,reversalDynamicPeriodControl4
# MAGIC ,reversalDynamicPeriodControl5
# MAGIC ,reverseBackbillingIndicator
# MAGIC ,allocateBackbillingIndicator
# MAGIC ,eateStepLogicalNumber
# MAGIC ,periodEndBillingIndicator
# MAGIC ,statististicsUpdateGroupCode
# MAGIC ,billedQuantityStatisticsCode
# MAGIC ,statisticalAnalysisRateType
# MAGIC ,periodControlCode
# MAGIC ,timesliceNumeratorTimePortion
# MAGIC ,timesliceDenominatorTimePortion
# MAGIC ,timesliceTimeCatogoryTimePortion
# MAGIC ,meterReadingActiveIndicator
# MAGIC ,franchiseContractIndicator
# MAGIC ,billingPeriodInternalCategoryCode
# MAGIC ,individualContractID
# MAGIC ,billingQuantityPlaceBeforeDecimalPoint
# MAGIC ,billingQuantityPlaceAfterDecimalPoint
# MAGIC 
# MAGIC 
# MAGIC FROM
# MAGIC cleansed.isu_dberchz1
# MAGIC EXCEPT
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
# MAGIC from Source a
# MAGIC left join cleansed.isu_0UC_AKLASSE_TEXT b
# MAGIC on a.AKLASSE = b.billingClassCode and DELTA_TS < '20211117055649'

# COMMAND ----------

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
# MAGIC from Source a
# MAGIC left join cleansed.isu_0UC_AKLASSE_TEXT b
# MAGIC on a.AKLASSE = b.billingClassCode   where BELNR = '010000004111' and BELZEILE = '000696'
# MAGIC --billingdocumentnumber = '616000659963' and billingDocumentLineItemId = '000001'-- 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cleansed.isu_dberchz1 where billingdocumentnumber = '010000004111' and billingDocumentLineItemId = '000696'

# COMMAND ----------

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
# MAGIC from Source a
# MAGIC left join cleansed.isu_0UC_AKLASSE_TEXT b
# MAGIC on a.AKLASSE = b.billingClassCode   --where BELNR = '010000004111' and BELZEILE = '000696'
# MAGIC --billingdocumentnumber = '616000659963' and billingDocumentLineItemId = '000001'-- 
# MAGIC --select * from source 
# MAGIC where BELNR ='193006781302'

# COMMAND ----------

# MAGIC %sql
# MAGIC select billingdocumentnumber from cleansed.isu_dberchz1 where billingdocumentnumber = '193006781302'--billingQuantityPlaceAfterDecimalPoint in ('0.02191780821927' )

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct billingQuantityPlaceBeforeDecimalPoint, billingQuantityPlaceAfterDecimalPoint  from cleansed.isu_dberchz1 where billingQuantityPlaceBeforeDecimalPoint > 1 and  billingQuantityPlaceAfterDecimalPoint <> 0E-14

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct billingQuantityPlaceAfterDecimalPoint from cleansed.isu_dberchz1 --where billingQuantityPlaceAfterDecimalPoint > 1
