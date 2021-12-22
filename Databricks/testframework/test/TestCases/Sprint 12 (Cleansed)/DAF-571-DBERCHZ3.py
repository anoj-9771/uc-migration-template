# Databricks notebook source
hostName = 'nt097dslt01.adsdev.swcdev'
databaseName = 'SLT_EQ1CLNT100'
port = 1433
sltkey = dbutils.secrets.get(scope="TestScope",key="slt-key")
sltuser = dbutils.secrets.get(scope="TestScope",key="slt-user")
dbURL = f'jdbc:sqlserver://{hostName}:{port};database={databaseName};user={sltuser};password={sltkey}'
qry = '(select * from EQ1.DBERCHZ3) myTable'
df = spark.read.jdbc(url=dbURL, table=qry, lowerBound=1, upperBound=100000, numPartitions=10)
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists test.isu_dberchz3

# COMMAND ----------

df.write.format("json").saveAsTable ("test" + "." + "isu_dberchz3")

# COMMAND ----------

sourcedf=spark.sql("select * from test.isu_dberchz3")
sourcedf.printSchema()

# COMMAND ----------

# DBTITLE 0,[Result] Load Count Result into DataFrame
lakedf = spark.sql("select * from cleansed.isu_dberchz3")

# COMMAND ----------

display(lakedf)

# COMMAND ----------

# DBTITLE 1,[Target] Schema Check
lakedf.printSchema()

# COMMAND ----------

# DBTITLE 1,[Source] with mapping
# MAGIC %sql
# MAGIC SELECT
# MAGIC BELNR as billingDocumentNumber
# MAGIC ,BELZEILE as billingDocumentLineItemId
# MAGIC ,MWSKZ as taxSalesCode
# MAGIC ,ERMWSKZ as texDeterminationCode
# MAGIC ,NETTOBTR as billingLineItemNetAmount
# MAGIC ,TWAERS as transactionCurrency
# MAGIC ,PREISTUF as priceLevel
# MAGIC ,PREISTYP as priceCategory
# MAGIC ,PREIS as price
# MAGIC ,PREISZUS as priceSummaryIndicator
# MAGIC ,VONZONE as fromBlock
# MAGIC ,BISZONE as toBlock
# MAGIC ,ZONENNR as numberOfPriceBlock
# MAGIC ,PREISBTR as priceAmount
# MAGIC ,MNGBASIS as amountLongQuantityBase
# MAGIC ,PREIGKL as priceAdjustemntClause
# MAGIC ,URPREIS as priceAdjustemntClauseBasePrice
# MAGIC ,PREIADD as addedAdjustmentPrice
# MAGIC ,PREIFAKT as priceAdjustmentFactor
# MAGIC ,OPMULT as additionFirst
# MAGIC ,TXDAT_KK as taxDecisiveDate
# MAGIC ,PRCTR as profitCenter
# MAGIC ,KOSTL as costCenter
# MAGIC ,PS_PSP_PNR as wbsElement
# MAGIC ,AUFNR as orderNumber
# MAGIC ,PAOBJNR as profitabilitySegmentNumber
# MAGIC ,PAOBJNR_S as profitabilitySegmentNumberForPost
# MAGIC ,GSBER as businessArea
# MAGIC ,APERIODIC as nonPeriodicPosting
# MAGIC ,GROSSGROUP as grossGroup
# MAGIC ,BRUTTOZEILE as grossBillingLineItem
# MAGIC ,BUPLA as businessPlace
# MAGIC ,LINE_CLASS as billingLineClassificationIndicator
# MAGIC ,PREISART as priceType
# MAGIC ,V_NETTOBTR_L as longNetAmountPredecimalPlaces
# MAGIC ,N_NETTOBTR_L as longNetAmountDecimalPlaces
# MAGIC FROM test.isu_dberchz3

# COMMAND ----------

lakedf.createOrReplaceTempView("Target")

# COMMAND ----------

# DBTITLE 1,[Verification] Count Checks
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from cleansed.isu_dberchz3
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from (
# MAGIC SELECT
# MAGIC BELNR as billingDocumentNumber
# MAGIC ,BELZEILE as billingDocumentLineItemId
# MAGIC ,MWSKZ as taxSalesCode
# MAGIC ,ERMWSKZ as texDeterminationCode
# MAGIC ,NETTOBTR as billingLineItemNetAmount
# MAGIC ,TWAERS as transactionCurrency
# MAGIC ,PREISTUF as priceLevel
# MAGIC ,PREISTYP as priceCategory
# MAGIC ,PREIS as price
# MAGIC ,PREISZUS as priceSummaryIndicator
# MAGIC ,VONZONE as fromBlock
# MAGIC ,BISZONE as toBlock
# MAGIC ,ZONENNR as numberOfPriceBlock
# MAGIC ,PREISBTR as priceAmount
# MAGIC ,MNGBASIS as amountLongQuantityBase
# MAGIC ,PREIGKL as priceAdjustemntClause
# MAGIC ,URPREIS as priceAdjustemntClauseBasePrice
# MAGIC ,PREIADD as addedAdjustmentPrice
# MAGIC ,PREIFAKT as priceAdjustmentFactor
# MAGIC ,OPMULT as additionFirst
# MAGIC ,TXDAT_KK as taxDecisiveDate
# MAGIC ,PRCTR as profitCenter
# MAGIC ,KOSTL as costCenter
# MAGIC ,PS_PSP_PNR as wbsElement
# MAGIC ,AUFNR as orderNumber
# MAGIC ,PAOBJNR as profitabilitySegmentNumber
# MAGIC ,PAOBJNR_S as profitabilitySegmentNumberForPost
# MAGIC ,GSBER as businessArea
# MAGIC ,APERIODIC as nonPeriodicPosting
# MAGIC ,GROSSGROUP as grossGroup
# MAGIC ,BRUTTOZEILE as grossBillingLineItem
# MAGIC ,BUPLA as businessPlace
# MAGIC ,LINE_CLASS as billingLineClassificationIndicator
# MAGIC ,PREISART as priceType
# MAGIC ,V_NETTOBTR_L as longNetAmountPredecimalPlaces
# MAGIC ,N_NETTOBTR_L as longNetAmountDecimalPlaces
# MAGIC FROM test.isu_dberchz3 --where DELTA_TS<'20211120044035'
# MAGIC )

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT billingDocumentNumber,billingDocumentLineItemId, COUNT (*) as count
# MAGIC FROM cleansed.isu_dberchz3
# MAGIC GROUP BY billingDocumentNumber,billingDocumentLineItemId
# MAGIC HAVING COUNT (*) > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT * FROM (
# MAGIC SELECT
# MAGIC *,
# MAGIC row_number() OVER(PARTITION BY billingDocumentNumber, billingDocumentLineItemId order by billingDocumentNumber,billingDocumentLineItemId) as rn
# MAGIC FROM cleansed.isu_dberchz3
# MAGIC )a where a.rn > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Source and Target Data
# MAGIC %sql
# MAGIC select 
# MAGIC BELNR as billingDocumentNumber
# MAGIC ,BELZEILE as billingDocumentLineItemId
# MAGIC ,MWSKZ as taxSalesCode
# MAGIC ,ERMWSKZ as texDeterminationCode
# MAGIC ,NETTOBTR as billingLineItemNetAmount
# MAGIC ,TWAERS as transactionCurrency
# MAGIC ,PREISTUF as priceLevel
# MAGIC ,PREISTYP as priceCategory
# MAGIC ,PREIS as price
# MAGIC ,PREISZUS as priceSummaryIndicator
# MAGIC ,VONZONE as fromBlock
# MAGIC ,BISZONE as toBlock
# MAGIC ,ZONENNR as numberOfPriceBlock
# MAGIC ,PREISBTR as priceAmount
# MAGIC ,MNGBASIS as amountLongQuantityBase
# MAGIC ,PREIGKL as priceAdjustemntClause
# MAGIC ,URPREIS as priceAdjustemntClauseBasePrice
# MAGIC ,PREIADD as addedAdjustmentPrice
# MAGIC ,PREIFAKT as priceAdjustmentFactor
# MAGIC ,OPMULT as additionFirst
# MAGIC ,case when TXDAT_KK = '00000000' then null else TXDAT_KK end as taxDecisiveDate
# MAGIC ,PRCTR as profitCenter
# MAGIC ,KOSTL as costCenter
# MAGIC ,PS_PSP_PNR as wbsElement
# MAGIC ,AUFNR as orderNumber
# MAGIC ,PAOBJNR as profitabilitySegmentNumber
# MAGIC ,PAOBJNR_S as profitabilitySegmentNumberForPost
# MAGIC ,GSBER as businessArea
# MAGIC ,APERIODIC as nonPeriodicPosting
# MAGIC ,GROSSGROUP as grossGroup
# MAGIC ,BRUTTOZEILE as grossBillingLineItem
# MAGIC ,BUPLA as businessPlace
# MAGIC ,LINE_CLASS as billingLineClassificationIndicator
# MAGIC ,PREISART as priceType
# MAGIC ,V_NETTOBTR_L as longNetAmountPredecimalPlaces
# MAGIC ,N_NETTOBTR_L as longNetAmountDecimalPlaces
# MAGIC from test.isu_dberchz3 --where DELTA_TS<'20211117055649'
# MAGIC except
# MAGIC select
# MAGIC billingDocumentNumber
# MAGIC ,billingDocumentLineItemId
# MAGIC ,taxSalesCode
# MAGIC ,texDeterminationCode
# MAGIC ,billingLineItemNetAmount
# MAGIC ,transactionCurrency
# MAGIC ,priceLevel
# MAGIC ,priceCategory
# MAGIC ,price
# MAGIC ,priceSummaryIndicator
# MAGIC ,fromBlock
# MAGIC ,toBlock
# MAGIC ,numberOfPriceBlock
# MAGIC ,priceAmount
# MAGIC ,amountLongQuantityBase
# MAGIC ,priceAdjustemntClause
# MAGIC ,priceAdjustemntClauseBasePrice
# MAGIC ,addedAdjustmentPrice
# MAGIC ,priceAdjustmentFactor
# MAGIC ,additionFirst
# MAGIC ,taxDecisiveDate
# MAGIC ,profitCenter
# MAGIC ,costCenter
# MAGIC ,wbsElement
# MAGIC ,orderNumber
# MAGIC ,profitabilitySegmentNumber
# MAGIC ,profitabilitySegmentNumberForPost
# MAGIC ,businessArea
# MAGIC ,nonPeriodicPosting
# MAGIC ,grossGroup
# MAGIC ,grossBillingLineItem
# MAGIC ,businessPlace
# MAGIC ,billingLineClassificationIndicator
# MAGIC ,priceType
# MAGIC ,longNetAmountPredecimalPlaces
# MAGIC ,longNetAmountDecimalPlaces
# MAGIC FROM
# MAGIC cleansed.isu_dberchz3

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Target and Source Data
# MAGIC %sql
# MAGIC select
# MAGIC billingDocumentNumber
# MAGIC ,billingDocumentLineItemId
# MAGIC ,taxSalesCode
# MAGIC ,texDeterminationCode
# MAGIC ,billingLineItemNetAmount
# MAGIC ,transactionCurrency
# MAGIC ,priceLevel
# MAGIC ,priceCategory
# MAGIC ,price
# MAGIC ,priceSummaryIndicator
# MAGIC ,fromBlock
# MAGIC ,toBlock
# MAGIC ,numberOfPriceBlock
# MAGIC ,priceAmount
# MAGIC ,amountLongQuantityBase
# MAGIC ,priceAdjustemntClause
# MAGIC ,priceAdjustemntClauseBasePrice
# MAGIC ,addedAdjustmentPrice
# MAGIC ,priceAdjustmentFactor
# MAGIC ,additionFirst
# MAGIC ,taxDecisiveDate
# MAGIC ,profitCenter
# MAGIC ,costCenter
# MAGIC ,wbsElement
# MAGIC ,orderNumber
# MAGIC ,profitabilitySegmentNumber
# MAGIC ,profitabilitySegmentNumberForPost
# MAGIC ,businessArea
# MAGIC ,nonPeriodicPosting
# MAGIC ,grossGroup
# MAGIC ,grossBillingLineItem
# MAGIC ,businessPlace
# MAGIC ,billingLineClassificationIndicator
# MAGIC ,priceType
# MAGIC ,longNetAmountPredecimalPlaces
# MAGIC ,longNetAmountDecimalPlaces
# MAGIC FROM
# MAGIC cleansed.isu_dberchz3
# MAGIC EXCEPT
# MAGIC select
# MAGIC BELNR as billingDocumentNumber
# MAGIC ,BELZEILE as billingDocumentLineItemId
# MAGIC ,MWSKZ as taxSalesCode
# MAGIC ,ERMWSKZ as texDeterminationCode
# MAGIC ,NETTOBTR as billingLineItemNetAmount
# MAGIC ,TWAERS as transactionCurrency
# MAGIC ,PREISTUF as priceLevel
# MAGIC ,PREISTYP as priceCategory
# MAGIC ,PREIS as price
# MAGIC ,PREISZUS as priceSummaryIndicator
# MAGIC ,VONZONE as fromBlock
# MAGIC ,BISZONE as toBlock
# MAGIC ,ZONENNR as numberOfPriceBlock
# MAGIC ,PREISBTR as priceAmount
# MAGIC ,MNGBASIS as amountLongQuantityBase
# MAGIC ,PREIGKL as priceAdjustemntClause
# MAGIC ,URPREIS as priceAdjustemntClauseBasePrice
# MAGIC ,PREIADD as addedAdjustmentPrice
# MAGIC ,PREIFAKT as priceAdjustmentFactor
# MAGIC ,OPMULT as additionFirst
# MAGIC ,case when TXDAT_KK = '00000000' then null else TXDAT_KK end as taxDecisiveDate
# MAGIC ,PRCTR as profitCenter
# MAGIC ,KOSTL as costCenter
# MAGIC ,PS_PSP_PNR as wbsElement
# MAGIC ,AUFNR as orderNumber
# MAGIC ,PAOBJNR as profitabilitySegmentNumber
# MAGIC ,PAOBJNR_S as profitabilitySegmentNumberForPost
# MAGIC ,GSBER as businessArea
# MAGIC ,APERIODIC as nonPeriodicPosting
# MAGIC ,GROSSGROUP as grossGroup
# MAGIC ,BRUTTOZEILE as grossBillingLineItem
# MAGIC ,BUPLA as businessPlace
# MAGIC ,LINE_CLASS as billingLineClassificationIndicator
# MAGIC ,PREISART as priceType
# MAGIC ,V_NETTOBTR_L as longNetAmountPredecimalPlaces
# MAGIC ,N_NETTOBTR_L as longNetAmountDecimalPlaces
# MAGIC from
# MAGIC test.isu_dberchz3 --where DELTA_TS<'20211117055649'

# COMMAND ----------

# DBTITLE 1,Records count for Target vs Source Data
# MAGIC %sql
# MAGIC select count (*) from (select
# MAGIC billingDocumentNumber
# MAGIC ,billingDocumentLineItemId
# MAGIC ,taxSalesCode
# MAGIC ,texDeterminationCode
# MAGIC ,billingLineItemNetAmount
# MAGIC ,transactionCurrency
# MAGIC ,priceLevel
# MAGIC ,priceCategory
# MAGIC ,price
# MAGIC ,priceSummaryIndicator
# MAGIC ,fromBlock
# MAGIC ,toBlock
# MAGIC ,numberOfPriceBlock
# MAGIC ,priceAmount
# MAGIC ,amountLongQuantityBase
# MAGIC ,priceAdjustemntClause
# MAGIC ,priceAdjustemntClauseBasePrice
# MAGIC ,addedAdjustmentPrice
# MAGIC ,priceAdjustmentFactor
# MAGIC ,additionFirst
# MAGIC ,taxDecisiveDate
# MAGIC ,profitCenter
# MAGIC ,costCenter
# MAGIC ,wbsElement
# MAGIC ,orderNumber
# MAGIC ,profitabilitySegmentNumber
# MAGIC ,profitabilitySegmentNumberForPost
# MAGIC ,businessArea
# MAGIC ,nonPeriodicPosting
# MAGIC ,grossGroup
# MAGIC ,grossBillingLineItem
# MAGIC ,businessPlace
# MAGIC ,billingLineClassificationIndicator
# MAGIC ,priceType
# MAGIC ,longNetAmountPredecimalPlaces
# MAGIC ,longNetAmountDecimalPlaces
# MAGIC FROM
# MAGIC cleansed.isu_dberchz3
# MAGIC EXCEPT
# MAGIC select
# MAGIC BELNR as billingDocumentNumber
# MAGIC ,BELZEILE as billingDocumentLineItemId
# MAGIC ,MWSKZ as taxSalesCode
# MAGIC ,ERMWSKZ as texDeterminationCode
# MAGIC ,NETTOBTR as billingLineItemNetAmount
# MAGIC ,TWAERS as transactionCurrency
# MAGIC ,PREISTUF as priceLevel
# MAGIC ,PREISTYP as priceCategory
# MAGIC ,PREIS as price
# MAGIC ,PREISZUS as priceSummaryIndicator
# MAGIC ,VONZONE as fromBlock
# MAGIC ,BISZONE as toBlock
# MAGIC ,ZONENNR as numberOfPriceBlock
# MAGIC ,PREISBTR as priceAmount
# MAGIC ,MNGBASIS as amountLongQuantityBase
# MAGIC ,PREIGKL as priceAdjustemntClause
# MAGIC ,URPREIS as priceAdjustemntClauseBasePrice
# MAGIC ,PREIADD as addedAdjustmentPrice
# MAGIC ,PREIFAKT as priceAdjustmentFactor
# MAGIC ,OPMULT as additionFirst
# MAGIC ,case when TXDAT_KK = '00000000' then null else TXDAT_KK end as taxDecisiveDate
# MAGIC ,PRCTR as profitCenter
# MAGIC ,KOSTL as costCenter
# MAGIC ,PS_PSP_PNR as wbsElement
# MAGIC ,AUFNR as orderNumber
# MAGIC ,PAOBJNR as profitabilitySegmentNumber
# MAGIC ,PAOBJNR_S as profitabilitySegmentNumberForPost
# MAGIC ,GSBER as businessArea
# MAGIC ,APERIODIC as nonPeriodicPosting
# MAGIC ,GROSSGROUP as grossGroup
# MAGIC ,BRUTTOZEILE as grossBillingLineItem
# MAGIC ,BUPLA as businessPlace
# MAGIC ,LINE_CLASS as billingLineClassificationIndicator
# MAGIC ,PREISART as priceType
# MAGIC ,V_NETTOBTR_L as longNetAmountPredecimalPlaces
# MAGIC ,N_NETTOBTR_L as longNetAmountDecimalPlaces
# MAGIC from
# MAGIC test.isu_dberchz3) --where DELTA_TS<'20211117055649'
