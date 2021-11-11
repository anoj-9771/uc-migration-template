# Databricks notebook source
# DBTITLE 1,[Config] Connection Setup
storage_account_name = "sadaftest01"
storage_account_access_key = dbutils.secrets.get(scope="Test-Access",key="test-datalake-key")
container_name = "raw"
file_location = "wasbs://raw@sadaftest01.blob.core.windows.net/sapisu/dberchz3/json/year=2021/month=09/day=27/DBO.DBERCHZ3_2021-09-27_165356_190.json.gz"
file_type = "json"
print(storage_account_name)

# COMMAND ----------

spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
  storage_account_access_key)


# COMMAND ----------

# DBTITLE 1,[Source] Loading data into Dataframe
df = spark.read.format(file_type).option("inferSchema", "true").load(file_location)


# COMMAND ----------

# DBTITLE 1,[Source] Schema Check - Refer to Raw2Cleansed Mapping
df.printSchema()


# COMMAND ----------

# DBTITLE 0,[Result] Load Count Result into DataFrame
lakedf = spark.sql("select * from cleansed.t_sapisu_dberchz3")

# COMMAND ----------

display(lakedf)

# COMMAND ----------

# DBTITLE 1,[Target] Schema Check
lakedf.printSchema()

# COMMAND ----------

df.createOrReplaceTempView("Source")

# COMMAND ----------

# DBTITLE 1,[Source] with mapping
# MAGIC %sql
# MAGIC SELECT
# MAGIC BELNR	as	billingDocumentNumber
# MAGIC ,BELZEILE	as	billingDocumentLineItemId
# MAGIC ,MWSKZ	as	taxSalesCode
# MAGIC ,ERMWSKZ	as	texDeterminationCode
# MAGIC ,NETTOBTR	as	billingLineItemNetAmount
# MAGIC ,TWAERS	as	transactionCurrency
# MAGIC ,PREISTUF	as	priceLevel
# MAGIC ,PREISTYP	as	priceCategory
# MAGIC ,PREIS	as	price
# MAGIC ,PREISZUS	as	priceSummaryIndicator
# MAGIC ,VONZONE	as	fromBlock
# MAGIC ,BISZONE	as	toBlock
# MAGIC ,ZONENNR	as	numberOfPriceBlock
# MAGIC ,PREISBTR	as	priceAmount
# MAGIC ,MNGBASIS	as	amountLongQuantityBase
# MAGIC ,PREIGKL	as	priceAdjustemntClause
# MAGIC ,URPREIS	as	priceAdjustemntClauseBasePrice
# MAGIC ,PREIADD	as	PREIADD
# MAGIC ,PREIFAKT	as	priceAdjustmentFactor
# MAGIC ,OPMULT	as	additionFirst
# MAGIC ,TXDAT_KK	as	taxDecisiveDate
# MAGIC ,PRCTR	as	profitCenter
# MAGIC ,KOSTL	as	costCenter
# MAGIC ,PS_PSP_PNR	as	wbsElement
# MAGIC ,AUFNR	as	orderNumber
# MAGIC ,PAOBJNR	as	profitabilitySegmentNumber
# MAGIC ,PAOBJNR_S	as	profitabilitySegmentNumberForPost
# MAGIC ,GSBER	as	businessArea
# MAGIC ,APERIODIC	as	nonPeriodicPosting
# MAGIC ,GROSSGROUP	as	grossGroup
# MAGIC ,BRUTTOZEILE	as	grossBillingLineItem
# MAGIC ,BUPLA	as	businessPlace
# MAGIC ,LINE_CLASS	as	billingLineClassificationIndicator
# MAGIC ,PREISART	as	priceType
# MAGIC ,V_NETTOBTR_L	as	longNetAmountPredecimalPlaces
# MAGIC ,N_NETTOBTR_L	as	longNetAmountDecimalPlaces
# MAGIC FROM Source

# COMMAND ----------

lakedf.createOrReplaceTempView("Target")

# COMMAND ----------

# DBTITLE 1,[Verification] Count Checks
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from cleansed.t_sapisu_dberchz3
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from Source

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT billingDocumentNumber,billingDocumentLineItemId, COUNT (*) as count
# MAGIC FROM cleansed.t_sapisu_dberchz3
# MAGIC GROUP BY billingDocumentNumber,billingDocumentLineItemId
# MAGIC HAVING COUNT (*) > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT * FROM (
# MAGIC SELECT
# MAGIC *,
# MAGIC row_number() OVER(PARTITION BY billingDocumentNumber ,billingDocumentLineItemId order by billingDocumentNumber) as rn
# MAGIC FROM cleansed.t_sapisu_dberchz3
# MAGIC )a where a.rn > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Source and Target Data
# MAGIC %sql
# MAGIC SELECT
# MAGIC BELNR	as	billingDocumentNumber
# MAGIC ,BELZEILE	as	billingDocumentLineItemId
# MAGIC ,MWSKZ	as	taxSalesCode
# MAGIC ,ERMWSKZ	as	texDeterminationCode
# MAGIC ,NETTOBTR	as	billingLineItemNetAmount
# MAGIC ,TWAERS	as	transactionCurrency
# MAGIC ,PREISTUF	as	priceLevel
# MAGIC ,PREISTYP	as	priceCategory
# MAGIC ,PREIS	as	price
# MAGIC ,PREISZUS	as	priceSummaryIndicator
# MAGIC ,VONZONE	as	fromBlock
# MAGIC ,BISZONE	as	toBlock
# MAGIC ,ZONENNR	as	numberOfPriceBlock
# MAGIC --,PREISBTR	as	priceAmount
# MAGIC ,MNGBASIS	as	amountLongQuantityBase
# MAGIC ,PREIGKL	as	priceAdjustemntClause
# MAGIC --,URPREIS	as	priceAdjustemntClauseBasePrice
# MAGIC --,PREIADD	as	PREIADD
# MAGIC ,PREIFAKT	as	priceAdjustmentFactor
# MAGIC ,OPMULT	as	additionFirst
# MAGIC ,case when TXDAT_KK ='0' then 'null' else TXDAT_KK end as taxDecisiveDate
# MAGIC ,PRCTR	as	profitCenter
# MAGIC ,KOSTL	as	costCenter
# MAGIC ,PS_PSP_PNR	as	wbsElement
# MAGIC ,AUFNR	as	orderNumber
# MAGIC ,PAOBJNR	as	profitabilitySegmentNumber
# MAGIC ,PAOBJNR_S	as	profitabilitySegmentNumberForPost
# MAGIC ,GSBER	as	businessArea
# MAGIC ,APERIODIC	as	nonPeriodicPosting
# MAGIC ,GROSSGROUP	as	grossGroup
# MAGIC ,BRUTTOZEILE	as	grossBillingLineItem
# MAGIC ,BUPLA	as	businessPlace
# MAGIC ,LINE_CLASS	as	billingLineClassificationIndicator
# MAGIC ,PREISART	as	priceType
# MAGIC ,V_NETTOBTR_L	as	longNetAmountPredecimalPlaces
# MAGIC --,N_NETTOBTR_L	as	longNetAmountDecimalPlaces
# MAGIC FROM Source
# MAGIC except
# MAGIC select
# MAGIC billingDocumentNumber,
# MAGIC billingDocumentLineItemId,
# MAGIC taxSalesCode,
# MAGIC texDeterminationCode,
# MAGIC billingLineItemNetAmount,
# MAGIC transactionCurrency,
# MAGIC priceLevel,
# MAGIC priceCategory,
# MAGIC price,
# MAGIC priceSummaryIndicator,
# MAGIC fromBlock,
# MAGIC toBlock,
# MAGIC numberOfPriceBlock,
# MAGIC --priceAmount,
# MAGIC amountLongQuantityBase,
# MAGIC priceAdjustemntClause,
# MAGIC --priceAdjustemntClauseBasePrice,
# MAGIC --prceAdjustmentPrceAddition,
# MAGIC priceAdjustmentFactor,
# MAGIC additionFirst,
# MAGIC taxDecisiveDate,
# MAGIC profitCenter,
# MAGIC costCenter,
# MAGIC wbsElement,
# MAGIC orderNumber,
# MAGIC profitabilitySegmentNumber,
# MAGIC profitabilitySegmentNumberForPost,
# MAGIC businessArea,
# MAGIC nonPeriodicPosting,
# MAGIC grossGroup,
# MAGIC grossBillingLineItem,
# MAGIC businessPlace,
# MAGIC billingLineClassificationIndicator,
# MAGIC priceType,
# MAGIC longNetAmountPredecimalPlaces
# MAGIC --,longNetAmountDecimalPlaces
# MAGIC from
# MAGIC cleansed.t_sapisu_dberchz3

# COMMAND ----------

# DBTITLE 1,[Source]
# MAGIC %sql
# MAGIC SELECT
# MAGIC BELNR	as	billingDocumentNumber
# MAGIC ,BELZEILE	as	billingDocumentLineItemId
# MAGIC ,MWSKZ	as	taxSalesCode
# MAGIC ,ERMWSKZ	as	texDeterminationCode
# MAGIC ,NETTOBTR	as	billingLineItemNetAmount
# MAGIC ,TWAERS	as	transactionCurrency
# MAGIC ,PREISTUF	as	priceLevel
# MAGIC ,PREISTYP	as	priceCategory
# MAGIC ,PREIS	as	price
# MAGIC ,PREISZUS	as	priceSummaryIndicator
# MAGIC ,VONZONE	as	fromBlock
# MAGIC ,BISZONE	as	toBlock
# MAGIC ,ZONENNR	as	numberOfPriceBlock
# MAGIC ,PREISBTR	as	priceAmount
# MAGIC ,MNGBASIS	as	amountLongQuantityBase
# MAGIC ,PREIGKL	as	priceAdjustemntClause
# MAGIC ,URPREIS	as	priceAdjustemntClauseBasePrice
# MAGIC ,PREIADD	as	PREIADD
# MAGIC ,PREIFAKT	as	priceAdjustmentFactor
# MAGIC ,OPMULT	as	additionFirst,
# MAGIC case when TXDAT_KK = '0' then null else TXDAT_KK end as taxDecisiveDate,
# MAGIC PRCTR	as	profitCenter
# MAGIC ,KOSTL	as	costCenter
# MAGIC ,PS_PSP_PNR	as	wbsElement
# MAGIC ,AUFNR	as	orderNumber
# MAGIC ,PAOBJNR	as	profitabilitySegmentNumber
# MAGIC ,PAOBJNR_S	as	profitabilitySegmentNumberForPost
# MAGIC ,GSBER	as	businessArea
# MAGIC ,APERIODIC	as	nonPeriodicPosting
# MAGIC ,GROSSGROUP	as	grossGroup
# MAGIC ,BRUTTOZEILE	as	grossBillingLineItem
# MAGIC ,BUPLA	as	businessPlace
# MAGIC ,LINE_CLASS	as	billingLineClassificationIndicator
# MAGIC ,PREISART	as	priceType
# MAGIC ,V_NETTOBTR_L	as	longNetAmountPredecimalPlaces
# MAGIC ,N_NETTOBTR_L	as	longNetAmountDecimalPlaces
# MAGIC FROM Source where BELNR = '335000007062'and BELZEILE ='000002'

# COMMAND ----------

# DBTITLE 1,Target
# MAGIC %sql
# MAGIC select
# MAGIC billingDocumentNumber,
# MAGIC billingDocumentLineItemId,
# MAGIC taxSalesCode,
# MAGIC texDeterminationCode,
# MAGIC billingLineItemNetAmount,
# MAGIC transactionCurrency,
# MAGIC priceLevel,
# MAGIC priceCategory,
# MAGIC price,
# MAGIC priceSummaryIndicator,
# MAGIC fromBlock,
# MAGIC toBlock,
# MAGIC numberOfPriceBlock,
# MAGIC priceAmount,
# MAGIC amountLongQuantityBase,
# MAGIC priceAdjustemntClause,
# MAGIC priceAdjustemntClauseBasePrice,
# MAGIC prceAdjustmentPrceAddition,
# MAGIC priceAdjustmentFactor,
# MAGIC additionFirst,
# MAGIC taxDecisiveDate,
# MAGIC profitCenter,
# MAGIC costCenter,
# MAGIC wbsElement,
# MAGIC orderNumber,
# MAGIC profitabilitySegmentNumber,
# MAGIC profitabilitySegmentNumberForPost,
# MAGIC businessArea,
# MAGIC nonPeriodicPosting,
# MAGIC grossGroup,
# MAGIC grossBillingLineItem,
# MAGIC businessPlace,
# MAGIC billingLineClassificationIndicator,
# MAGIC priceType,
# MAGIC longNetAmountPredecimalPlaces,
# MAGIC longNetAmountDecimalPlaces
# MAGIC from
# MAGIC cleansed.t_sapisu_dberchz3 where billingDocumentNumber ='335000007062'and billingDocumentLineItemId='000002'

# COMMAND ----------

# DBTITLE 1,[Compare S vs T for single values]
# MAGIC %sql
# MAGIC SELECT
# MAGIC BELNR	as	billingDocumentNumber
# MAGIC ,BELZEILE	as	billingDocumentLineItemId
# MAGIC ,MWSKZ	as	taxSalesCode
# MAGIC ,ERMWSKZ	as	texDeterminationCode
# MAGIC ,NETTOBTR	as	billingLineItemNetAmount
# MAGIC ,TWAERS	as	transactionCurrency
# MAGIC ,PREISTUF	as	priceLevel
# MAGIC ,PREISTYP	as	priceCategory
# MAGIC ,PREIS	as	price
# MAGIC ,PREISZUS	as	priceSummaryIndicator
# MAGIC ,VONZONE	as	fromBlock
# MAGIC ,BISZONE	as	toBlock
# MAGIC ,ZONENNR	as	numberOfPriceBlock
# MAGIC --,PREISBTR	as	priceAmount
# MAGIC ,MNGBASIS	as	amountLongQuantityBase
# MAGIC ,PREIGKL	as	priceAdjustemntClause
# MAGIC --,URPREIS	as	priceAdjustemntClauseBasePrice
# MAGIC --,PREIADD	as	PREIADD
# MAGIC ,PREIFAKT	as	priceAdjustmentFactor
# MAGIC ,OPMULT	as	additionFirst
# MAGIC --,TXDAT_KK	as	taxDecisiveDate
# MAGIC ,PRCTR	as	profitCenter
# MAGIC ,KOSTL	as	costCenter
# MAGIC ,PS_PSP_PNR	as	wbsElement
# MAGIC ,AUFNR	as	orderNumber
# MAGIC ,PAOBJNR	as	profitabilitySegmentNumber
# MAGIC ,PAOBJNR_S	as	profitabilitySegmentNumberForPost
# MAGIC ,GSBER	as	businessArea
# MAGIC ,APERIODIC	as	nonPeriodicPosting
# MAGIC ,GROSSGROUP	as	grossGroup
# MAGIC ,BRUTTOZEILE	as	grossBillingLineItem
# MAGIC ,BUPLA	as	businessPlace
# MAGIC ,LINE_CLASS	as	billingLineClassificationIndicator
# MAGIC ,PREISART	as	priceType
# MAGIC ,V_NETTOBTR_L	as	longNetAmountPredecimalPlaces
# MAGIC ,N_NETTOBTR_L	as	longNetAmountDecimalPlaces
# MAGIC FROM Source where BELNR = '300000001126'and BELZEILE ='000003'
# MAGIC except
# MAGIC select
# MAGIC billingDocumentNumber,
# MAGIC billingDocumentLineItemId,
# MAGIC taxSalesCode,
# MAGIC texDeterminationCode,
# MAGIC billingLineItemNetAmount,
# MAGIC transactionCurrency,
# MAGIC priceLevel,
# MAGIC priceCategory,
# MAGIC price,
# MAGIC priceSummaryIndicator,
# MAGIC fromBlock,
# MAGIC toBlock,
# MAGIC numberOfPriceBlock,
# MAGIC --priceAmount,
# MAGIC amountLongQuantityBase,
# MAGIC priceAdjustemntClause,
# MAGIC --priceAdjustemntClauseBasePrice,
# MAGIC --prceAdjustmentPrceAddition,
# MAGIC priceAdjustmentFactor,
# MAGIC additionFirst,
# MAGIC --taxDecisiveDate,
# MAGIC profitCenter,
# MAGIC costCenter,
# MAGIC wbsElement,
# MAGIC orderNumber,
# MAGIC profitabilitySegmentNumber,
# MAGIC profitabilitySegmentNumberForPost,
# MAGIC businessArea,
# MAGIC nonPeriodicPosting,
# MAGIC grossGroup,
# MAGIC grossBillingLineItem,
# MAGIC businessPlace,
# MAGIC billingLineClassificationIndicator,
# MAGIC priceType,
# MAGIC longNetAmountPredecimalPlaces,
# MAGIC longNetAmountDecimalPlaces
# MAGIC from
# MAGIC cleansed.t_sapisu_dberchz3 where billingDocumentNumber ='300000001126'and billingDocumentLineItemId='000003'

# COMMAND ----------

# DBTITLE 1,[Compare S vs T for single values]
# MAGIC %sql
# MAGIC SELECT
# MAGIC BELNR	as	billingDocumentNumber
# MAGIC ,BELZEILE	as	billingDocumentLineItemId
# MAGIC ,MWSKZ	as	taxSalesCode
# MAGIC ,ERMWSKZ	as	texDeterminationCode
# MAGIC ,NETTOBTR	as	billingLineItemNetAmount
# MAGIC ,TWAERS	as	transactionCurrency
# MAGIC ,PREISTUF	as	priceLevel
# MAGIC ,PREISTYP	as	priceCategory
# MAGIC ,PREIS	as	price
# MAGIC ,PREISZUS	as	priceSummaryIndicator
# MAGIC ,VONZONE	as	fromBlock
# MAGIC ,BISZONE	as	toBlock
# MAGIC ,ZONENNR	as	numberOfPriceBlock
# MAGIC ,PREISBTR	as	priceAmount
# MAGIC ,MNGBASIS	as	amountLongQuantityBase
# MAGIC ,PREIGKL	as	priceAdjustemntClause
# MAGIC ,URPREIS	as	priceAdjustemntClauseBasePrice
# MAGIC --,PREIADD	as	PREIADD
# MAGIC ,PREIFAKT	as	priceAdjustmentFactor
# MAGIC ,OPMULT	as	additionFirst
# MAGIC --,TXDAT_KK	as	taxDecisiveDate
# MAGIC ,PRCTR	as	profitCenter
# MAGIC ,KOSTL	as	costCenter
# MAGIC ,PS_PSP_PNR	as	wbsElement
# MAGIC ,AUFNR	as	orderNumber
# MAGIC ,PAOBJNR	as	profitabilitySegmentNumber
# MAGIC ,PAOBJNR_S	as	profitabilitySegmentNumberForPost
# MAGIC ,GSBER	as	businessArea
# MAGIC ,APERIODIC	as	nonPeriodicPosting
# MAGIC ,GROSSGROUP	as	grossGroup
# MAGIC ,BRUTTOZEILE	as	grossBillingLineItem
# MAGIC ,BUPLA	as	businessPlace
# MAGIC ,LINE_CLASS	as	billingLineClassificationIndicator
# MAGIC ,PREISART	as	priceType
# MAGIC ,V_NETTOBTR_L	as	longNetAmountPredecimalPlaces
# MAGIC ,N_NETTOBTR_L	as	longNetAmountDecimalPlaces
# MAGIC FROM Source where BELNR = '300000001126'and BELZEILE ='000003'
# MAGIC except
# MAGIC select
# MAGIC billingDocumentNumber,
# MAGIC billingDocumentLineItemId,
# MAGIC taxSalesCode,
# MAGIC texDeterminationCode,
# MAGIC billingLineItemNetAmount,
# MAGIC transactionCurrency,
# MAGIC priceLevel,
# MAGIC priceCategory,
# MAGIC price,
# MAGIC priceSummaryIndicator,
# MAGIC fromBlock,
# MAGIC toBlock,
# MAGIC numberOfPriceBlock,
# MAGIC priceAmount,
# MAGIC amountLongQuantityBase,
# MAGIC priceAdjustemntClause,
# MAGIC priceAdjustemntClauseBasePrice,
# MAGIC --prceAdjustmentPrceAddition,
# MAGIC priceAdjustmentFactor,
# MAGIC additionFirst,
# MAGIC --taxDecisiveDate,
# MAGIC profitCenter,
# MAGIC costCenter,
# MAGIC wbsElement,
# MAGIC orderNumber,
# MAGIC profitabilitySegmentNumber,
# MAGIC profitabilitySegmentNumberForPost,
# MAGIC businessArea,
# MAGIC nonPeriodicPosting,
# MAGIC grossGroup,
# MAGIC grossBillingLineItem,
# MAGIC businessPlace,
# MAGIC billingLineClassificationIndicator,
# MAGIC priceType,
# MAGIC longNetAmountPredecimalPlaces,
# MAGIC longNetAmountDecimalPlaces
# MAGIC from
# MAGIC cleansed.t_sapisu_dberchz3 where billingDocumentNumber ='300000001126'and billingDocumentLineItemId='000003'

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Target and Source Data
# MAGIC %sql
# MAGIC select
# MAGIC billingDocumentNumber,
# MAGIC billingDocumentLineItemId,
# MAGIC taxSalesCode,
# MAGIC texDeterminationCode,
# MAGIC billingLineItemNetAmount,
# MAGIC transactionCurrency,
# MAGIC priceLevel,
# MAGIC priceCategory,
# MAGIC price,
# MAGIC priceSummaryIndicator,
# MAGIC fromBlock,
# MAGIC toBlock,
# MAGIC numberOfPriceBlock,
# MAGIC --priceAmount,
# MAGIC amountLongQuantityBase,
# MAGIC priceAdjustemntClause,
# MAGIC --priceAdjustemntClauseBasePrice,
# MAGIC --prceAdjustmentPrceAddition,
# MAGIC priceAdjustmentFactor,
# MAGIC additionFirst,
# MAGIC --taxDecisiveDate,
# MAGIC profitCenter,
# MAGIC costCenter,
# MAGIC wbsElement,
# MAGIC orderNumber,
# MAGIC profitabilitySegmentNumber,
# MAGIC profitabilitySegmentNumberForPost,
# MAGIC businessArea,
# MAGIC nonPeriodicPosting,
# MAGIC grossGroup,
# MAGIC grossBillingLineItem,
# MAGIC businessPlace,
# MAGIC billingLineClassificationIndicator,
# MAGIC priceType,
# MAGIC longNetAmountPredecimalPlaces
# MAGIC --,longNetAmountDecimalPlaces
# MAGIC from
# MAGIC cleansed.t_sapisu_dberchz3
# MAGIC except
# MAGIC SELECT
# MAGIC BELNR	as	billingDocumentNumber
# MAGIC ,BELZEILE	as	billingDocumentLineItemId
# MAGIC ,MWSKZ	as	taxSalesCode
# MAGIC ,ERMWSKZ	as	texDeterminationCode
# MAGIC ,NETTOBTR	as	billingLineItemNetAmount
# MAGIC ,TWAERS	as	transactionCurrency
# MAGIC ,PREISTUF	as	priceLevel
# MAGIC ,PREISTYP	as	priceCategory
# MAGIC ,PREIS	as	price
# MAGIC ,PREISZUS	as	priceSummaryIndicator
# MAGIC ,VONZONE	as	fromBlock
# MAGIC ,BISZONE	as	toBlock
# MAGIC ,ZONENNR	as	numberOfPriceBlock
# MAGIC --,PREISBTR	as	priceAmount
# MAGIC ,MNGBASIS	as	amountLongQuantityBase
# MAGIC ,PREIGKL	as	priceAdjustemntClause
# MAGIC --,URPREIS	as	priceAdjustemntClauseBasePrice
# MAGIC --,PREIADD	as	PREIADD
# MAGIC ,PREIFAKT	as	priceAdjustmentFactor
# MAGIC ,OPMULT	as	additionFirst
# MAGIC --,TXDAT_KK	as	taxDecisiveDate
# MAGIC ,PRCTR	as	profitCenter
# MAGIC ,KOSTL	as	costCenter
# MAGIC ,PS_PSP_PNR	as	wbsElement
# MAGIC ,AUFNR	as	orderNumber
# MAGIC ,PAOBJNR	as	profitabilitySegmentNumber
# MAGIC ,PAOBJNR_S	as	profitabilitySegmentNumberForPost
# MAGIC ,GSBER	as	businessArea
# MAGIC ,APERIODIC	as	nonPeriodicPosting
# MAGIC ,GROSSGROUP	as	grossGroup
# MAGIC ,BRUTTOZEILE	as	grossBillingLineItem
# MAGIC ,BUPLA	as	businessPlace
# MAGIC ,LINE_CLASS	as	billingLineClassificationIndicator
# MAGIC ,PREISART	as	priceType
# MAGIC ,V_NETTOBTR_L	as	longNetAmountPredecimalPlaces
# MAGIC --,N_NETTOBTR_L	as	longNetAmountDecimalPlaces
# MAGIC FROM Source
