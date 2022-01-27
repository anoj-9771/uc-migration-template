# Databricks notebook source
# DBTITLE 1,[Config] Connection Setup to SLT
hostName = 'nt097dslt01.adsdev.swcdev'
databaseName = 'SLT_EQ1CLNT100'
port = 1433
sltkey = dbutils.secrets.get(scope="TestScope",key="slt-key")
sltuser = dbutils.secrets.get(scope="TestScope",key="slt-user")
dbURL = f'jdbc:sqlserver://{hostName}:{port};database={databaseName};user={sltuser};password={sltkey}'
qry = '(select top 10000 * from EQ1.DBERCHZ1 order by AB asc) myTable'
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
df.write.format("json").saveAsTable ("test" + "." + "isu_dberchz1")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count (*) from test.isu_dberchz1

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
# MAGIC --,TCNUMTOR as timesliceNumeratorTimePortion
# MAGIC --,TCDENOMTOR as timesliceDenominatorTimePortion
# MAGIC ,TIMTYPQUOT as timesliceTimeCatogoryTimePortion
# MAGIC ,AKTIV as meterReadingActiveIndicator
# MAGIC ,KONZVER as franchiseContractIndicator
# MAGIC ,PERTYP as billingPeriodInternalCategoryCode
# MAGIC ,OUCONTRACT as individualContractID
# MAGIC ,V_ABRMENGE as billingQuantityPlaceBeforeDecimalPoint
# MAGIC ,N_ABRMENGE as billingQuantityPlaceAfterDecimalPoint
# MAGIC from test.isu_dberchz1 a
# MAGIC left join cleansed.isu_0UC_AKLASSE_TEXT b
# MAGIC on a.AKLASSE = b.billingClassCode -- and DELTA_TS < '20211117055649'
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
# MAGIC --,timesliceNumeratorTimePortion
# MAGIC --,timesliceDenominatorTimePortion
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
# MAGIC cleansed.isu_dberchz1 order by validFromDate asc limit 10000 

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
# MAGIC --,timesliceNumeratorTimePortion
# MAGIC --,timesliceDenominatorTimePortion
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
# MAGIC cleansed.isu_dberchz1 order by validFromDate asc limit 10000
# MAGIC )
# MAGIC EXCEPT
# MAGIC (SELECT
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
# MAGIC ,case when AB < '19000101' then '1900-01-01' else CONCAT(substring(AB,1,4),'-',substring(AB,5,2),'-',substring (AB,7,2)) end as validFromDate
# MAGIC ,CONCAT(substring(BIS,1,4),'-',substring(BIS,5,2),'-',substring (BIS,7,2)) as validToDate
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
# MAGIC ,TIMTYPQUOT as timesliceTimeCategoryTimePortion
# MAGIC ,AKTIV as meterReadingActiveIndicator
# MAGIC ,KONZVER as franchiseContractIndicator
# MAGIC ,PERTYP as billingPeriodInternalCategoryCode
# MAGIC ,OUCONTRACT as individualContractID
# MAGIC ,V_ABRMENGE as billingQuantityPlaceBeforeDecimalPoint
# MAGIC ,N_ABRMENGE as billingQuantityPlaceAfterDecimalPoint
# MAGIC from test.isu_dberchz1 a
# MAGIC left join cleansed.isu_0UC_AKLASSE_TEXT b
# MAGIC on a.AKLASSE = b.billingClassCode )-- and DELTA_TS < '20211117055649'

# COMMAND ----------

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
# MAGIC cleansed.isu_dberchz1 where billingDocumentNumber = 315021342771 and billingDocumentLineItemId = 000001 -- order by billingdocumentnumber limit 10000 

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
# MAGIC ,case when AB < '19000101' then '1900-01-01' else CONCAT(substring(AB,1,4),'-',substring(AB,5,2),'-',substring (AB,7,2)) end as validFromDate
# MAGIC ,CONCAT(substring(BIS,1,4),'-',substring(BIS,5,2),'-',substring (BIS,7,2)) as validToDate
# MAGIC --,cast (BIS as date) as validToDate
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
# MAGIC on a.AKLASSE = b.billingClassCode where BELNR = 315021342771 and BELZEILE = 000001

# COMMAND ----------

# MAGIC %sql
# MAGIC (select billingdocumentnumber, billingdocumentlineitemid 
# MAGIC from cleansed.isu_dberchz1 order by validFromDate asc limit 10000)
# MAGIC except
# MAGIC (SELECT
# MAGIC BELNR as billingDocumentNumber
# MAGIC ,BELZEILE as billingDocumentLineItemId
# MAGIC from test.isu_dberchz1)

# COMMAND ----------

# MAGIC %sql
# MAGIC (SELECT
# MAGIC BELNR as billingDocumentNumber
# MAGIC ,BELZEILE as billingDocumentLineItemId
# MAGIC from test.isu_dberchz1)
# MAGIC except
# MAGIC (select billingdocumentnumber, billingdocumentlineitemid 
# MAGIC from cleansed.isu_dberchz1 order by validFromDate asc limit 10000)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC BELNR as billingDocumentNumber
# MAGIC --M,BELZEILE as billingDocumentLineItemId
# MAGIC --M,CSNO as billingSequenceNumber
# MAGIC --M,BELZART as lineItemTypeCode
# MAGIC --M,ABSLKZ as billingLineItemBudgetBillingIndicator
# MAGIC --M,DIFFKZ as lineItemDiscountStatisticsIndicator
# MAGIC --M,BUCHREL as billingLineItemReleventPostingIndicator
# MAGIC --M,MENGESTREL as billedValueStatisticallyReleventIndicator
# MAGIC --M,BETRSTREL as billingLineItemStatisticallyReleventAmount
# MAGIC --M,STGRQNT as quantityStatisticsGroupCode
# MAGIC --M,STGRAMT as amountStatisticsGroupCoide
# MAGIC --M,PRINTREL as billingLinePrintReleventIndicator
# MAGIC --M,AKLASSE as billingClassCode
# MAGIC --M,b.billingClass as billingClass
# MAGIC --M,BRANCHE as industryText
# MAGIC --M,TVORG as subtransactionForDocumentItem
# MAGIC --M,GEGEN_TVORG as offsettingTransactionSubtransactionForDocumentItem
# MAGIC --M,LINESORT as poresortingBillingLineItems
# MAGIC --M,AB as validFromDate
# MAGIC --M,BIS as validToDate
# MAGIC --M,TIMTYPZA as billingLineItemTimeCategoryCode
# MAGIC --M,SCHEMANR as billingSchemaNumber
# MAGIC --M,SNO as billingSchemaStepSequenceNumber
# MAGIC --M,PROGRAMM as variantProgramNumber
# MAGIC --M,MASSBILL as billingMeasurementUnitCode
# MAGIC --M,SAISON as seasonNumber
# MAGIC --M,TIMBASIS as timeBasisCode
# MAGIC --M,TIMTYP as timeCategoryCode
# MAGIC --M,FRAN_TYPE as franchiseFeeTypeCode
# MAGIC --M,KONZIGR as franchiseFeeGroupNumber
# MAGIC --M,TARIFTYP as rateTypeCode
# MAGIC --M,TARIFNR as rateId
# MAGIC --M,KONDIGR as rateFactGroupNumber
# MAGIC --M,STTARIF as statisticalRate
# MAGIC --M,GEWKEY as weightingKeyId
# MAGIC --M,WDHFAKT as referenceValuesForRepetitionFactor
# MAGIC --M,TEMP_AREA as tempratureArea
# MAGIC --M,DYNCANC01 as reversalDynamicPeriodControl1
# MAGIC --M,DYNCANC02 as reversalDynamicPeriodControl2
# MAGIC --M,DYNCANC03 as reversalDynamicPeriodControl3
# MAGIC --M,DYNCANC04 as reversalDynamicPeriodControl4
# MAGIC --M,DYNCANC05 as reversalDynamicPeriodControl5
# MAGIC --M,DYNCANC as reverseBackbillingIndicator
# MAGIC --M,DYNEXEC as allocateBackbillingIndicator
# MAGIC --M,LRATESTEP as eateStepLogicalNumber
# MAGIC --M,PEB as periodEndBillingIndicator
# MAGIC --M,STAFO as statististicsUpdateGroupCode
# MAGIC --M,ARTMENGE as billedQuantityStatisticsCode
# MAGIC --M,STATTART as statisticalAnalysisRateType
# MAGIC --M,TIMECONTRL as periodControlCode
# MAGIC --M--,TCNUMTOR as timesliceNumeratorTimePortion
# MAGIC --M--,TCDENOMTOR as timesliceDenominatorTimePortion
# MAGIC --M,TIMTYPQUOT as timesliceTimeCatogoryTimePortion
# MAGIC --M,AKTIV as meterReadingActiveIndicator
# MAGIC --M,KONZVER as franchiseContractIndicator
# MAGIC --M,PERTYP as billingPeriodInternalCategoryCode
# MAGIC --M,OUCONTRACT as individualContractID
# MAGIC --M,V_ABRMENGE as billingQuantityPlaceBeforeDecimalPoint
# MAGIC --M,N_ABRMENGE as billingQuantityPlaceAfterDecimalPoint
# MAGIC from test.isu_dberchz1 a
# MAGIC left join cleansed.isu_0UC_AKLASSE_TEXT b
# MAGIC on a.AKLASSE = b.billingClassCode -- and DELTA_TS < '20211117055649'
# MAGIC EXCEPT
# MAGIC (select
# MAGIC billingDocumentNumber
# MAGIC --M,billingDocumentLineItemId
# MAGIC --M,billingSequenceNumber
# MAGIC --M,lineItemTypeCode
# MAGIC --M,billingLineItemBudgetBillingIndicator
# MAGIC --M,lineItemDiscountStatisticsIndicator
# MAGIC --M,billingLineItemReleventPostingIndicator
# MAGIC --M,billedValueStatisticallyReleventIndicator
# MAGIC --M,billingLineItemStatisticallyReleventAmount
# MAGIC --M,quantityStatisticsGroupCode
# MAGIC --M,amountStatisticsGroupCoide
# MAGIC --M,billingLinePrintReleventIndicator
# MAGIC --M,billingClassCode
# MAGIC --M,billingClass
# MAGIC --M,industryText
# MAGIC --M,subtransactionForDocumentItem
# MAGIC --M,offsettingTransactionSubtransactionForDocumentItem
# MAGIC --M,poresortingBillingLineItems
# MAGIC --M,validFromDate
# MAGIC --M,validToDate
# MAGIC --M,billingLineItemTimeCategoryCode
# MAGIC --M,billingSchemaNumber
# MAGIC --M,billingSchemaStepSequenceNumber
# MAGIC --M,variantProgramNumber
# MAGIC --M,billingMeasurementUnitCode
# MAGIC --M,seasonNumber
# MAGIC --M,timeBasisCode
# MAGIC --M,timeCategoryCode
# MAGIC --M,franchiseFeeTypeCode
# MAGIC --M,franchiseFeeGroupNumber
# MAGIC --M,rateTypeCode
# MAGIC --M,rateId
# MAGIC --M,rateFactGroupNumber
# MAGIC --M,statisticalRate
# MAGIC --M,weightingKeyId
# MAGIC --M,referenceValuesForRepetitionFactor
# MAGIC --M,tempratureArea
# MAGIC --M,reversalDynamicPeriodControl1
# MAGIC --M,reversalDynamicPeriodControl2
# MAGIC --M,reversalDynamicPeriodControl3
# MAGIC --M,reversalDynamicPeriodControl4
# MAGIC --M,reversalDynamicPeriodControl5
# MAGIC --M,reverseBackbillingIndicator
# MAGIC --M,allocateBackbillingIndicator
# MAGIC --M,eateStepLogicalNumber
# MAGIC --M,periodEndBillingIndicator
# MAGIC --M,statististicsUpdateGroupCode
# MAGIC --M,billedQuantityStatisticsCode
# MAGIC --M,statisticalAnalysisRateType
# MAGIC --M,periodControlCode
# MAGIC --M--,timesliceNumeratorTimePortion
# MAGIC --M--,timesliceDenominatorTimePortion
# MAGIC --M,timesliceTimeCatogoryTimePortion
# MAGIC --M,meterReadingActiveIndicator
# MAGIC --M,franchiseContractIndicator
# MAGIC --M,billingPeriodInternalCategoryCode
# MAGIC --M,individualContractID
# MAGIC --M,billingQuantityPlaceBeforeDecimalPoint
# MAGIC --M,billingQuantityPlaceAfterDecimalPoint
# MAGIC 
# MAGIC 
# MAGIC FROM
# MAGIC cleansed.isu_dberchz1 order by validFromDate asc limit 10000)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from test.isu_dberchz1 where BELNR = 193000025020
