# Databricks notebook source
# DBTITLE 1,[Config] Connection Setup
storage_account_name = "sadaftest01"
storage_account_access_key = dbutils.secrets.get(scope="Test-Access",key="test-datalake-key")
container_name = "raw"
file_location = "wasbs://raw@sadaftest01.blob.core.windows.net/sapisu/erch/json/year=2021/month=09/day=27/DBO.ERCH_2021-09-27_165356_097.json.gz"
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
lakedf = spark.sql("select * from cleansed.t_sapisu_erch")

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

# MAGIC %sql
# MAGIC select * from cleansed.t_sapisu_0COMP_CODE_TEXT

# COMMAND ----------

# DBTITLE 1,[Source] with mapping
# MAGIC %sql
# MAGIC SELECT
# MAGIC BELNR as billingDocumentNumber
# MAGIC ,BUKRS as companyCode
# MAGIC ,b.companyName as companyName
# MAGIC ,SPARTE as divisonCode
# MAGIC ,GPARTNER as businessPartnerNumber
# MAGIC ,VKONT as contractAccountNumber
# MAGIC ,VERTRAG as contractID
# MAGIC ,case
# MAGIC when BEGABRPE='0' then null 
# MAGIC when BEGABRPE='00000000' then null
# MAGIC when BEGABRPE <> 'null' then CONCAT(LEFT(BEGABRPE,4),'-',SUBSTRING(BEGABRPE,5,2),'-',RIGHT(BEGABRPE,2))
# MAGIC else BEGABRPE end as startBillingPeriod
# MAGIC ,case
# MAGIC when ENDABRPE='0' then null 
# MAGIC when ENDABRPE='00000000' then null
# MAGIC when ENDABRPE <> 'null' then CONCAT(LEFT(ENDABRPE,4),'-',SUBSTRING(ENDABRPE,5,2),'-',RIGHT(ENDABRPE,2))
# MAGIC else ENDABRPE end as endBillingPeriod
# MAGIC ,case
# MAGIC when ENDABRPE='0' then null 
# MAGIC when ENDABRPE='00000000' then null
# MAGIC when ENDABRPE <> 'null' then CONCAT(LEFT(ENDABRPE,4),'-',SUBSTRING(ENDABRPE,5,2),'-',RIGHT(ENDABRPE,2))
# MAGIC else ENDABRPE end as endBillingPeriod
# MAGIC ,case
# MAGIC when ABRDATS='0' then null 
# MAGIC when ABRDATS='00000000' then null
# MAGIC when ABRDATS <> 'null' then CONCAT(LEFT(ABRDATS,4),'-',SUBSTRING(ABRDATS,5,2),'-',RIGHT(ABRDATS,2))
# MAGIC else ABRDATS end as billingScheduleDate
# MAGIC ,case
# MAGIC when ADATSOLL='0' then null 
# MAGIC when ADATSOLL='00000000' then null
# MAGIC when ADATSOLL <> 'null' then CONCAT(LEFT(ADATSOLL,4),'-',SUBSTRING(ADATSOLL,5,2),'-',RIGHT(ADATSOLL,2))
# MAGIC else ADATSOLL end as meterReadingScheduleDate
# MAGIC ,case
# MAGIC when PTERMTDAT='0' then null 
# MAGIC when PTERMTDAT='00000000' then null
# MAGIC when PTERMTDAT <> 'null' then CONCAT(LEFT(PTERMTDAT,4),'-',SUBSTRING(PTERMTDAT,5,2),'-',RIGHT(PTERMTDAT,2))
# MAGIC else PTERMTDAT end as billingPeriodEndDate
# MAGIC ,case
# MAGIC when BELEGDAT='0' then null 
# MAGIC when BELEGDAT='00000000' then null
# MAGIC when BELEGDAT <> 'null' then CONCAT(LEFT(BELEGDAT,4),'-',SUBSTRING(BELEGDAT,5,2),'-',RIGHT(BELEGDAT,2))
# MAGIC else BELEGDAT end as billingDocumentCreateDate
# MAGIC ,ABWVK as alternativeContractAccountForCollectiveBills
# MAGIC ,BELNRALT as previousDocumentNumber
# MAGIC ,case
# MAGIC when STORNODAT='0' then null 
# MAGIC when STORNODAT='00000000' then null
# MAGIC when STORNODAT <> 'null' then CONCAT(LEFT(STORNODAT,4),'-',SUBSTRING(STORNODAT,5,2),'-',RIGHT(STORNODAT,2))
# MAGIC else STORNODAT end as reversalDate
# MAGIC ,ABRVORG as billingTransactionCode
# MAGIC ,HVORG as mainTransactionLineItemCode
# MAGIC ,KOFIZ as contractAccountDeterminationID
# MAGIC ,PORTION as portionNumber
# MAGIC ,FORMULAR as formName
# MAGIC ,SIMULATION as billingSimulationIndicator
# MAGIC ,BELEGART as documentTypeCode
# MAGIC ,BERGRUND as backbillingCreditReasonCode
# MAGIC ,case
# MAGIC when BEGNACH='0' then null 
# MAGIC when BEGNACH='00000000' then null
# MAGIC when BEGNACH <> 'null' then CONCAT(LEFT(BEGNACH,4),'-',SUBSTRING(BEGNACH,5,2),'-',RIGHT(BEGNACH,2))
# MAGIC else BEGNACH end as backbillingStartPeriod
# MAGIC ,TOBRELEASD as DocumentNotReleasedIndicator
# MAGIC ,TXJCD as taxJurisdictionDescription
# MAGIC ,KONZVER as franchiseContractCode
# MAGIC ,EROETIM as billingDocumentCreateTime
# MAGIC ,ERCHP_V as ERCHP_Exist_IND
# MAGIC ,ABRVORG2 as periodEndBillingTransactionCode
# MAGIC ,ABLEINH as meterReadingUnit
# MAGIC ,ENDPRIO as billingEndingPriorityCodfe
# MAGIC ,ERDAT as createdDate
# MAGIC ,ERNAM as createdBy
# MAGIC ,case
# MAGIC when AEDAT='0' then null 
# MAGIC when AEDAT='00000000' then null
# MAGIC when AEDAT <> 'null' then CONCAT(LEFT(AEDAT,4),'-',SUBSTRING(AEDAT,5,2),'-',RIGHT(AEDAT,2))
# MAGIC else AEDAT end as lastChangedDate
# MAGIC ,AENAM as changedBy
# MAGIC ,BEGRU as authorizationGroupCode
# MAGIC ,LOEVM as deletedIndicator
# MAGIC ,case
# MAGIC when ABRDATSU='0' then null 
# MAGIC when ABRDATSU='00000000' then null
# MAGIC when ABRDATSU <> 'null' then CONCAT(LEFT(ABRDATSU,4),'-',SUBSTRING(ABRDATSU,5,2),'-',RIGHT(ABRDATSU,2))
# MAGIC else ABRDATSU end as suppressedBillingOrderScheduleDate
# MAGIC ,ABRVORGU as suppressedBillingOrderTransactionCode
# MAGIC ,N_INVSEP as jointInvoiceAutomaticDocumentIndicator
# MAGIC ,ABPOPBEL as BudgetBillingPlanCode
# MAGIC ,MANBILLREL as manualDocumentReleasedInvoicingIndicator
# MAGIC ,BACKBI as backbillingTypeCode
# MAGIC ,PERENDBI as billingPeriodEndType
# MAGIC ,NUMPERBB as backbillingPeriodNumber
# MAGIC ,case
# MAGIC when BEGEND='0' then null 
# MAGIC when BEGEND='00000000' then null
# MAGIC when BEGEND <> 'null' then CONCAT(LEFT(BEGEND,4),'-',SUBSTRING(BEGEND,5,2),'-',RIGHT(BEGEND,2))
# MAGIC else BEGEND end as periodEndBillingStartDate
# MAGIC ,ENDOFBB as backbillingPeriodEndIndicator
# MAGIC ,ENDOFPEB as billingPeriodEndIndicator
# MAGIC ,NUMPERPEB as billingPeriodEndCount
# MAGIC ,SC_BELNR_H as billingDoumentAdjustmentReversalCount
# MAGIC ,SC_BELNR_N as billingDocumentNumberForAdjustmentReverssal
# MAGIC ,case
# MAGIC when ZUORDDAA='0' then null 
# MAGIC when ZUORDDAA='00000000' then null
# MAGIC when ZUORDDAA <> 'null' then CONCAT(LEFT(ZUORDDAA,4),'-',SUBSTRING(ZUORDDAA,5,2),'-',RIGHT(ZUORDDAA,2))
# MAGIC else ZUORDDAA end as billingAllocationDate
# MAGIC ,BILLINGRUNNO as billingRunNumber
# MAGIC ,SIMRUNID as simulationPeriodID
# MAGIC ,KTOKLASSE as accountClassCode
# MAGIC ,ORIGDOC as billingDocumentOriginCode
# MAGIC ,NOCANC as billingDonotExecuteIndicator
# MAGIC ,ABSCHLPAN as billingPlanAdjustIndicator
# MAGIC ,MEM_OPBEL as newBillingDocumentNumberForReversedInvoicing
# MAGIC ,case
# MAGIC when MEM_BUDAT='0' then null 
# MAGIC when MEM_BUDAT='00000000' then null
# MAGIC when MEM_BUDAT <> 'null' then CONCAT(LEFT(MEM_BUDAT,4),'-',SUBSTRING(MEM_BUDAT,5,2),'-',RIGHT(MEM_BUDAT,2))
# MAGIC else MEM_BUDAT end as billingPostingDateInDocument
# MAGIC ,EXBILLDOCNO as externalDocumentNumber
# MAGIC ,BCREASON as reversalReasonCode
# MAGIC ,NINVOICE as billingDocumentWithoutInvoicingCode
# MAGIC ,NBILLREL as billingRelavancyIndicator
# MAGIC ,case
# MAGIC when CORRECTION_DATE='0' then null 
# MAGIC when CORRECTION_DATE='00000000' then null
# MAGIC when CORRECTION_DATE <> 'null' then CONCAT(LEFT(CORRECTION_DATE,4),'-',SUBSTRING(CORRECTION_DATE,5,2),'-',RIGHT(CORRECTION_DATE,2))
# MAGIC else CORRECTION_DATE end as errorDetectedDate
# MAGIC ,BASDYPER as basicCategoryDynamicPeriodControlCode
# MAGIC ,ESTINBILL as meterReadingResultEstimatedBillingIndicator
# MAGIC ,ESTINBILLU as SuppressedOrderEstimateBillingIndicator
# MAGIC ,ESTINBILL_SAV as originalValueEstimateBillingIndicator
# MAGIC ,ESTINBILL_USAV as suppressedOrderBillingIndicator
# MAGIC ,ACTPERIOD as currentBillingPeriodCategoryCode
# MAGIC ,ACTPERORG as toBeBilledPeriodOriginalCategoryCode
# MAGIC ,EZAWE as incomingPaymentMethodCode
# MAGIC ,DAUBUCH as standingOrderIndicator
# MAGIC ,FDGRP as planningGroupNumber
# MAGIC ,case
# MAGIC when BILLING_PERIOD='0' then null 
# MAGIC when BILLING_PERIOD='00000000' then null
# MAGIC when BILLING_PERIOD <> 'null' then CONCAT(LEFT(BILLING_PERIOD,4),'-',SUBSTRING(BILLING_PERIOD,5,2),'-',RIGHT(BILLING_PERIOD,2))
# MAGIC else BILLING_PERIOD end as billingKeyDate
# MAGIC ,OSB_GROUP as onsiteBillingGroupCode
# MAGIC ,BP_BILL as resultingBillingPeriodIndicator
# MAGIC ,MAINDOCNO as billingDocumentPrimaryInstallationNumber
# MAGIC ,INSTGRTYPE as instalGroupTypeCode
# MAGIC ,INSTROLE as instalGroupRoleCode
# MAGIC FROM Source a
# MAGIC LEFT JOIN cleansed.t_sapisu_0COMP_CODE_TEXT b
# MAGIC ON a.BUKRS = b.companyCode-- AND b.LANGU='E'

# COMMAND ----------

lakedf.createOrReplaceTempView("Target")

# COMMAND ----------

# DBTITLE 1,[Verification] Count Checks
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from cleansed.t_sapisu_erch
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from (
# MAGIC SELECT
# MAGIC BELNR as billingDocumentNumber
# MAGIC ,BUKRS as companyCode
# MAGIC ,b.companyName as companyName
# MAGIC ,SPARTE as divisonCode
# MAGIC ,GPARTNER as businessPartnerNumber
# MAGIC ,VKONT as contractAccountNumber
# MAGIC ,VERTRAG as contractID
# MAGIC ,case
# MAGIC when BEGABRPE='0' then null 
# MAGIC when BEGABRPE='00000000' then null
# MAGIC when BEGABRPE <> 'null' then CONCAT(LEFT(BEGABRPE,4),'-',SUBSTRING(BEGABRPE,5,2),'-',RIGHT(BEGABRPE,2))
# MAGIC else BEGABRPE end as startBillingPeriod
# MAGIC ,case
# MAGIC when ENDABRPE='0' then null 
# MAGIC when ENDABRPE='00000000' then null
# MAGIC when ENDABRPE <> 'null' then CONCAT(LEFT(ENDABRPE,4),'-',SUBSTRING(ENDABRPE,5,2),'-',RIGHT(ENDABRPE,2))
# MAGIC else ENDABRPE end as endBillingPeriod
# MAGIC ,case
# MAGIC when ENDABRPE='0' then null 
# MAGIC when ENDABRPE='00000000' then null
# MAGIC when ENDABRPE <> 'null' then CONCAT(LEFT(ENDABRPE,4),'-',SUBSTRING(ENDABRPE,5,2),'-',RIGHT(ENDABRPE,2))
# MAGIC else ENDABRPE end as endBillingPeriod
# MAGIC ,case
# MAGIC when ABRDATS='0' then null 
# MAGIC when ABRDATS='00000000' then null
# MAGIC when ABRDATS <> 'null' then CONCAT(LEFT(ABRDATS,4),'-',SUBSTRING(ABRDATS,5,2),'-',RIGHT(ABRDATS,2))
# MAGIC else ABRDATS end as billingScheduleDate
# MAGIC ,case
# MAGIC when ADATSOLL='0' then null 
# MAGIC when ADATSOLL='00000000' then null
# MAGIC when ADATSOLL <> 'null' then CONCAT(LEFT(ADATSOLL,4),'-',SUBSTRING(ADATSOLL,5,2),'-',RIGHT(ADATSOLL,2))
# MAGIC else ADATSOLL end as meterReadingScheduleDate
# MAGIC ,case
# MAGIC when PTERMTDAT='0' then null 
# MAGIC when PTERMTDAT='00000000' then null
# MAGIC when PTERMTDAT <> 'null' then CONCAT(LEFT(PTERMTDAT,4),'-',SUBSTRING(PTERMTDAT,5,2),'-',RIGHT(PTERMTDAT,2))
# MAGIC else PTERMTDAT end as billingPeriodEndDate
# MAGIC ,case
# MAGIC when BELEGDAT='0' then null 
# MAGIC when BELEGDAT='00000000' then null
# MAGIC when BELEGDAT <> 'null' then CONCAT(LEFT(BELEGDAT,4),'-',SUBSTRING(BELEGDAT,5,2),'-',RIGHT(BELEGDAT,2))
# MAGIC else BELEGDAT end as billingDocumentCreateDate
# MAGIC ,ABWVK as alternativeContractAccountForCollectiveBills
# MAGIC ,BELNRALT as previousDocumentNumber
# MAGIC ,case
# MAGIC when STORNODAT='0' then null 
# MAGIC when STORNODAT='00000000' then null
# MAGIC when STORNODAT <> 'null' then CONCAT(LEFT(STORNODAT,4),'-',SUBSTRING(STORNODAT,5,2),'-',RIGHT(STORNODAT,2))
# MAGIC else STORNODAT end as reversalDate
# MAGIC ,ABRVORG as billingTransactionCode
# MAGIC ,HVORG as mainTransactionLineItemCode
# MAGIC ,KOFIZ as contractAccountDeterminationID
# MAGIC ,PORTION as portionNumber
# MAGIC ,FORMULAR as formName
# MAGIC ,SIMULATION as billingSimulationIndicator
# MAGIC ,BELEGART as documentTypeCode
# MAGIC ,BERGRUND as backbillingCreditReasonCode
# MAGIC ,case
# MAGIC when BEGNACH='0' then null 
# MAGIC when BEGNACH='00000000' then null
# MAGIC when BEGNACH <> 'null' then CONCAT(LEFT(BEGNACH,4),'-',SUBSTRING(BEGNACH,5,2),'-',RIGHT(BEGNACH,2))
# MAGIC else BEGNACH end as backbillingStartPeriod
# MAGIC ,TOBRELEASD as DocumentNotReleasedIndicator
# MAGIC ,TXJCD as taxJurisdictionDescription
# MAGIC ,KONZVER as franchiseContractCode
# MAGIC ,EROETIM as billingDocumentCreateTime
# MAGIC ,ERCHP_V as ERCHP_Exist_IND
# MAGIC ,ABRVORG2 as periodEndBillingTransactionCode
# MAGIC ,ABLEINH as meterReadingUnit
# MAGIC ,ENDPRIO as billingEndingPriorityCodfe
# MAGIC ,ERDAT as createdDate
# MAGIC ,ERNAM as createdBy
# MAGIC ,case
# MAGIC when AEDAT='0' then null 
# MAGIC when AEDAT='00000000' then null
# MAGIC when AEDAT <> 'null' then CONCAT(LEFT(AEDAT,4),'-',SUBSTRING(AEDAT,5,2),'-',RIGHT(AEDAT,2))
# MAGIC else AEDAT end as lastChangedDate
# MAGIC ,AENAM as changedBy
# MAGIC ,BEGRU as authorizationGroupCode
# MAGIC ,LOEVM as deletedIndicator
# MAGIC ,case
# MAGIC when ABRDATSU='0' then null 
# MAGIC when ABRDATSU='00000000' then null
# MAGIC when ABRDATSU <> 'null' then CONCAT(LEFT(ABRDATSU,4),'-',SUBSTRING(ABRDATSU,5,2),'-',RIGHT(ABRDATSU,2))
# MAGIC else ABRDATSU end as suppressedBillingOrderScheduleDate
# MAGIC ,ABRVORGU as suppressedBillingOrderTransactionCode
# MAGIC ,N_INVSEP as jointInvoiceAutomaticDocumentIndicator
# MAGIC ,ABPOPBEL as BudgetBillingPlanCode
# MAGIC ,MANBILLREL as manualDocumentReleasedInvoicingIndicator
# MAGIC ,BACKBI as backbillingTypeCode
# MAGIC ,PERENDBI as billingPeriodEndType
# MAGIC ,NUMPERBB as backbillingPeriodNumber
# MAGIC ,case
# MAGIC when BEGEND='0' then null 
# MAGIC when BEGEND='00000000' then null
# MAGIC when BEGEND <> 'null' then CONCAT(LEFT(BEGEND,4),'-',SUBSTRING(BEGEND,5,2),'-',RIGHT(BEGEND,2))
# MAGIC else BEGEND end as periodEndBillingStartDate
# MAGIC ,ENDOFBB as backbillingPeriodEndIndicator
# MAGIC ,ENDOFPEB as billingPeriodEndIndicator
# MAGIC ,NUMPERPEB as billingPeriodEndCount
# MAGIC ,SC_BELNR_H as billingDoumentAdjustmentReversalCount
# MAGIC ,SC_BELNR_N as billingDocumentNumberForAdjustmentReverssal
# MAGIC ,case
# MAGIC when ZUORDDAA='0' then null 
# MAGIC when ZUORDDAA='00000000' then null
# MAGIC when ZUORDDAA <> 'null' then CONCAT(LEFT(ZUORDDAA,4),'-',SUBSTRING(ZUORDDAA,5,2),'-',RIGHT(ZUORDDAA,2))
# MAGIC else ZUORDDAA end as billingAllocationDate
# MAGIC ,BILLINGRUNNO as billingRunNumber
# MAGIC ,SIMRUNID as simulationPeriodID
# MAGIC ,KTOKLASSE as accountClassCode
# MAGIC ,ORIGDOC as billingDocumentOriginCode
# MAGIC ,NOCANC as billingDonotExecuteIndicator
# MAGIC ,ABSCHLPAN as billingPlanAdjustIndicator
# MAGIC ,MEM_OPBEL as newBillingDocumentNumberForReversedInvoicing
# MAGIC ,case
# MAGIC when MEM_BUDAT='0' then null 
# MAGIC when MEM_BUDAT='00000000' then null
# MAGIC when MEM_BUDAT <> 'null' then CONCAT(LEFT(MEM_BUDAT,4),'-',SUBSTRING(MEM_BUDAT,5,2),'-',RIGHT(MEM_BUDAT,2))
# MAGIC else MEM_BUDAT end as billingPostingDateInDocument
# MAGIC ,EXBILLDOCNO as externalDocumentNumber
# MAGIC ,BCREASON as reversalReasonCode
# MAGIC ,NINVOICE as billingDocumentWithoutInvoicingCode
# MAGIC ,NBILLREL as billingRelavancyIndicator
# MAGIC ,case
# MAGIC when CORRECTION_DATE='0' then null 
# MAGIC when CORRECTION_DATE='00000000' then null
# MAGIC when CORRECTION_DATE <> 'null' then CONCAT(LEFT(CORRECTION_DATE,4),'-',SUBSTRING(CORRECTION_DATE,5,2),'-',RIGHT(CORRECTION_DATE,2))
# MAGIC else CORRECTION_DATE end as errorDetectedDate
# MAGIC ,BASDYPER as basicCategoryDynamicPeriodControlCode
# MAGIC ,ESTINBILL as meterReadingResultEstimatedBillingIndicator
# MAGIC ,ESTINBILLU as SuppressedOrderEstimateBillingIndicator
# MAGIC ,ESTINBILL_SAV as originalValueEstimateBillingIndicator
# MAGIC ,ESTINBILL_USAV as suppressedOrderBillingIndicator
# MAGIC ,ACTPERIOD as currentBillingPeriodCategoryCode
# MAGIC ,ACTPERORG as toBeBilledPeriodOriginalCategoryCode
# MAGIC ,EZAWE as incomingPaymentMethodCode
# MAGIC ,DAUBUCH as standingOrderIndicator
# MAGIC ,FDGRP as planningGroupNumber
# MAGIC ,case
# MAGIC when BILLING_PERIOD='0' then null 
# MAGIC when BILLING_PERIOD='00000000' then null
# MAGIC when BILLING_PERIOD <> 'null' then CONCAT(LEFT(BILLING_PERIOD,4),'-',SUBSTRING(BILLING_PERIOD,5,2),'-',RIGHT(BILLING_PERIOD,2))
# MAGIC else BILLING_PERIOD end as billingKeyDate
# MAGIC ,OSB_GROUP as onsiteBillingGroupCode
# MAGIC ,BP_BILL as resultingBillingPeriodIndicator
# MAGIC ,MAINDOCNO as billingDocumentPrimaryInstallationNumber
# MAGIC ,INSTGRTYPE as instalGroupTypeCode
# MAGIC ,INSTROLE as instalGroupRoleCode
# MAGIC FROM Source a
# MAGIC LEFT JOIN cleansed.t_sapisu_0COMP_CODE_TEXT b
# MAGIC ON a.BUKRS = b.companyCode)-- AND b.LANGU='E'

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT billingDocumentNumber, COUNT (*) as count
# MAGIC FROM cleansed.t_sapisu_erch
# MAGIC GROUP BY billingDocumentNumber
# MAGIC HAVING COUNT (*) > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT * FROM (
# MAGIC SELECT
# MAGIC *,
# MAGIC row_number() OVER(PARTITION BY billingDocumentNumber order by billingDocumentNumber) as rn
# MAGIC FROM cleansed.t_sapisu_erch
# MAGIC )a where a.rn > 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cleansed.t_isu_erch

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Source and Target Data
# MAGIC %sql
# MAGIC SELECT
# MAGIC BELNR as billingDocumentNumber
# MAGIC ,BUKRS as companyCode
# MAGIC ,b.companyName as companyName
# MAGIC ,SPARTE as divisonCode
# MAGIC ,GPARTNER as businessPartnerNumber
# MAGIC ,VKONT as contractAccountNumber
# MAGIC ,VERTRAG as contractID
# MAGIC ,case
# MAGIC when BEGABRPE='0' then null 
# MAGIC when BEGABRPE='00000000' then null
# MAGIC when BEGABRPE <> 'null' then CONCAT(LEFT(BEGABRPE,4),'-',SUBSTRING(BEGABRPE,5,2),'-',RIGHT(BEGABRPE,2))
# MAGIC else BEGABRPE end as startBillingPeriod
# MAGIC ,case
# MAGIC when ENDABRPE='0' then null 
# MAGIC when ENDABRPE='00000000' then null
# MAGIC when ENDABRPE <> 'null' then CONCAT(LEFT(ENDABRPE,4),'-',SUBSTRING(ENDABRPE,5,2),'-',RIGHT(ENDABRPE,2))
# MAGIC else ENDABRPE end as endBillingPeriod
# MAGIC ,case
# MAGIC when ENDABRPE='0' then null 
# MAGIC when ENDABRPE='00000000' then null
# MAGIC when ENDABRPE <> 'null' then CONCAT(LEFT(ENDABRPE,4),'-',SUBSTRING(ENDABRPE,5,2),'-',RIGHT(ENDABRPE,2))
# MAGIC else ENDABRPE end as endBillingPeriod
# MAGIC ,case
# MAGIC when ABRDATS='0' then null 
# MAGIC when ABRDATS='00000000' then null
# MAGIC when ABRDATS <> 'null' then CONCAT(LEFT(ABRDATS,4),'-',SUBSTRING(ABRDATS,5,2),'-',RIGHT(ABRDATS,2))
# MAGIC else ABRDATS end as billingScheduleDate
# MAGIC ,case
# MAGIC when ADATSOLL='0' then null 
# MAGIC when ADATSOLL='00000000' then null
# MAGIC when ADATSOLL <> 'null' then CONCAT(LEFT(ADATSOLL,4),'-',SUBSTRING(ADATSOLL,5,2),'-',RIGHT(ADATSOLL,2))
# MAGIC else ADATSOLL end as meterReadingScheduleDate
# MAGIC ,case
# MAGIC when PTERMTDAT='0' then null 
# MAGIC when PTERMTDAT='00000000' then null
# MAGIC when PTERMTDAT <> 'null' then CONCAT(LEFT(PTERMTDAT,4),'-',SUBSTRING(PTERMTDAT,5,2),'-',RIGHT(PTERMTDAT,2))
# MAGIC else PTERMTDAT end as billingPeriodEndDate
# MAGIC ,case
# MAGIC when BELEGDAT='0' then null 
# MAGIC when BELEGDAT='00000000' then null
# MAGIC when BELEGDAT <> 'null' then CONCAT(LEFT(BELEGDAT,4),'-',SUBSTRING(BELEGDAT,5,2),'-',RIGHT(BELEGDAT,2))
# MAGIC else BELEGDAT end as billingDocumentCreateDate
# MAGIC ,ABWVK as alternativeContractAccountForCollectiveBills
# MAGIC ,BELNRALT as previousDocumentNumber
# MAGIC ,case
# MAGIC when STORNODAT='0' then null 
# MAGIC when STORNODAT='00000000' then null
# MAGIC when STORNODAT <> 'null' then CONCAT(LEFT(STORNODAT,4),'-',SUBSTRING(STORNODAT,5,2),'-',RIGHT(STORNODAT,2))
# MAGIC else STORNODAT end as reversalDate
# MAGIC ,ABRVORG as billingTransactionCode
# MAGIC ,HVORG as mainTransactionLineItemCode
# MAGIC ,KOFIZ as contractAccountDeterminationID
# MAGIC ,PORTION as portionNumber
# MAGIC ,FORMULAR as formName
# MAGIC ,SIMULATION as billingSimulationIndicator
# MAGIC ,BELEGART as documentTypeCode
# MAGIC ,BERGRUND as backbillingCreditReasonCode
# MAGIC ,case
# MAGIC when BEGNACH='0' then null 
# MAGIC when BEGNACH='00000000' then null
# MAGIC when BEGNACH <> 'null' then CONCAT(LEFT(BEGNACH,4),'-',SUBSTRING(BEGNACH,5,2),'-',RIGHT(BEGNACH,2))
# MAGIC else BEGNACH end as backbillingStartPeriod
# MAGIC ,TOBRELEASD as DocumentNotReleasedIndicator
# MAGIC ,TXJCD as taxJurisdictionDescription
# MAGIC ,KONZVER as franchiseContractCode
# MAGIC ,EROETIM as billingDocumentCreateTime
# MAGIC ,ERCHP_V as ERCHP_Exist_IND
# MAGIC ,ABRVORG2 as periodEndBillingTransactionCode
# MAGIC ,ABLEINH as meterReadingUnit
# MAGIC ,ENDPRIO as billingEndingPriorityCodfe
# MAGIC ,ERDAT as createdDate
# MAGIC ,ERNAM as createdBy
# MAGIC ,case
# MAGIC when AEDAT='0' then null 
# MAGIC when AEDAT='00000000' then null
# MAGIC when AEDAT <> 'null' then CONCAT(LEFT(AEDAT,4),'-',SUBSTRING(AEDAT,5,2),'-',RIGHT(AEDAT,2))
# MAGIC else AEDAT end as lastChangedDate
# MAGIC ,AENAM as changedBy
# MAGIC ,BEGRU as authorizationGroupCode
# MAGIC ,LOEVM as deletedIndicator
# MAGIC ,case
# MAGIC when ABRDATSU='0' then null 
# MAGIC when ABRDATSU='00000000' then null
# MAGIC when ABRDATSU <> 'null' then CONCAT(LEFT(ABRDATSU,4),'-',SUBSTRING(ABRDATSU,5,2),'-',RIGHT(ABRDATSU,2))
# MAGIC else ABRDATSU end as suppressedBillingOrderScheduleDate
# MAGIC ,ABRVORGU as suppressedBillingOrderTransactionCode
# MAGIC ,N_INVSEP as jointInvoiceAutomaticDocumentIndicator
# MAGIC ,ABPOPBEL as BudgetBillingPlanCode
# MAGIC ,MANBILLREL as manualDocumentReleasedInvoicingIndicator
# MAGIC ,BACKBI as backbillingTypeCode
# MAGIC ,PERENDBI as billingPeriodEndType
# MAGIC ,NUMPERBB as backbillingPeriodNumber
# MAGIC ,case
# MAGIC when BEGEND='0' then null 
# MAGIC when BEGEND='00000000' then null
# MAGIC when BEGEND <> 'null' then CONCAT(LEFT(BEGEND,4),'-',SUBSTRING(BEGEND,5,2),'-',RIGHT(BEGEND,2))
# MAGIC else BEGEND end as periodEndBillingStartDate
# MAGIC ,ENDOFBB as backbillingPeriodEndIndicator
# MAGIC ,ENDOFPEB as billingPeriodEndIndicator
# MAGIC ,NUMPERPEB as billingPeriodEndCount
# MAGIC ,SC_BELNR_H as billingDoumentAdjustmentReversalCount
# MAGIC ,SC_BELNR_N as billingDocumentNumberForAdjustmentReverssal
# MAGIC ,case
# MAGIC when ZUORDDAA='0' then null 
# MAGIC when ZUORDDAA='00000000' then null
# MAGIC when ZUORDDAA <> 'null' then CONCAT(LEFT(ZUORDDAA,4),'-',SUBSTRING(ZUORDDAA,5,2),'-',RIGHT(ZUORDDAA,2))
# MAGIC else ZUORDDAA end as billingAllocationDate
# MAGIC ,BILLINGRUNNO as billingRunNumber
# MAGIC ,SIMRUNID as simulationPeriodID
# MAGIC ,KTOKLASSE as accountClassCode
# MAGIC ,ORIGDOC as billingDocumentOriginCode
# MAGIC ,NOCANC as billingDonotExecuteIndicator
# MAGIC ,ABSCHLPAN as billingPlanAdjustIndicator
# MAGIC ,MEM_OPBEL as newBillingDocumentNumberForReversedInvoicing
# MAGIC ,case
# MAGIC when MEM_BUDAT='0' then null 
# MAGIC when MEM_BUDAT='00000000' then null
# MAGIC when MEM_BUDAT <> 'null' then CONCAT(LEFT(MEM_BUDAT,4),'-',SUBSTRING(MEM_BUDAT,5,2),'-',RIGHT(MEM_BUDAT,2))
# MAGIC else MEM_BUDAT end as billingPostingDateInDocument
# MAGIC ,EXBILLDOCNO as externalDocumentNumber
# MAGIC ,BCREASON as reversalReasonCode
# MAGIC ,NINVOICE as billingDocumentWithoutInvoicingCode
# MAGIC ,NBILLREL as billingRelavancyIndicator
# MAGIC ,case
# MAGIC when CORRECTION_DATE='0' then null 
# MAGIC when CORRECTION_DATE='00000000' then null
# MAGIC when CORRECTION_DATE <> 'null' then CONCAT(LEFT(CORRECTION_DATE,4),'-',SUBSTRING(CORRECTION_DATE,5,2),'-',RIGHT(CORRECTION_DATE,2))
# MAGIC else CORRECTION_DATE end as errorDetectedDate
# MAGIC ,BASDYPER as basicCategoryDynamicPeriodControlCode
# MAGIC ,ESTINBILL as meterReadingResultEstimatedBillingIndicator
# MAGIC ,ESTINBILLU as SuppressedOrderEstimateBillingIndicator
# MAGIC ,ESTINBILL_SAV as originalValueEstimateBillingIndicator
# MAGIC ,ESTINBILL_USAV as suppressedOrderBillingIndicator
# MAGIC ,ACTPERIOD as currentBillingPeriodCategoryCode
# MAGIC ,ACTPERORG as toBeBilledPeriodOriginalCategoryCode
# MAGIC ,EZAWE as incomingPaymentMethodCode
# MAGIC ,DAUBUCH as standingOrderIndicator
# MAGIC ,FDGRP as planningGroupNumber
# MAGIC ,case
# MAGIC when BILLING_PERIOD='0' then null 
# MAGIC when BILLING_PERIOD='00000000' then null
# MAGIC when BILLING_PERIOD <> 'null' then CONCAT(LEFT(BILLING_PERIOD,4),'-',SUBSTRING(BILLING_PERIOD,5,2),'-',RIGHT(BILLING_PERIOD,2))
# MAGIC else BILLING_PERIOD end as billingKeyDate
# MAGIC ,OSB_GROUP as onsiteBillingGroupCode
# MAGIC ,BP_BILL as resultingBillingPeriodIndicator
# MAGIC ,MAINDOCNO as billingDocumentPrimaryInstallationNumber
# MAGIC ,INSTGRTYPE as instalGroupTypeCode
# MAGIC ,INSTROLE as instalGroupRoleCode
# MAGIC FROM Source a
# MAGIC LEFT JOIN cleansed.t_sapisu_0COMP_CODE_TEXT b
# MAGIC ON a.BUKRS = b.companyCode-- AND b.LANGU='E'
# MAGIC except
# MAGIC select
# MAGIC billingDocumentNumber
# MAGIC ,companyCode
# MAGIC ,companyName
# MAGIC ,divisonCode
# MAGIC ,businessPartnerNumber
# MAGIC ,contractAccountNumber
# MAGIC ,contractID
# MAGIC ,startBillingPeriod
# MAGIC ,endBillingPeriod
# MAGIC ,billingScheduleDate
# MAGIC ,meterReadingScheduleDate
# MAGIC ,billingPeriodEndDate
# MAGIC ,billingDocumentCreateDate
# MAGIC ,alternativeContractAccountForCollectiveBills
# MAGIC ,previousDocumentNumber
# MAGIC ,reversalDate
# MAGIC ,billingTransactionCode
# MAGIC ,mainTransactionLineItemCode
# MAGIC ,contractAccountDeterminationID
# MAGIC ,portionNumber
# MAGIC ,formName
# MAGIC ,billingSimulationIndicator
# MAGIC ,documentTypeCode
# MAGIC ,backbillingCreditReasonCode
# MAGIC ,backbillingStartPeriod
# MAGIC ,DocumentNotReleasedIndicator
# MAGIC ,taxJurisdictionDescription
# MAGIC ,franchiseContractCode
# MAGIC ,billingDocumentCreateTime
# MAGIC ,periodEndBillingTransactionCode
# MAGIC ,meterReadingUnit
# MAGIC ,billingEndingPriorityCodfe
# MAGIC ,createdDate
# MAGIC ,createdBy
# MAGIC ,lastChangedDate
# MAGIC ,changedBy
# MAGIC ,authorizationGroupCode
# MAGIC ,deletedIndicator
# MAGIC ,suppressedBillingOrderScheduleDate
# MAGIC ,suppressedBillingOrderTransactionCode
# MAGIC ,jointInvoiceAutomaticDocumentIndicator
# MAGIC ,BudgetBillingPlanCode
# MAGIC ,manualDocumentReleasedInvoicingIndicator
# MAGIC ,backbillingTypeCode
# MAGIC ,billingPeriodEndType
# MAGIC ,backbillingPeriodNumber
# MAGIC ,periodEndBillingStartDate
# MAGIC ,backbillingPeriodEndIndicator
# MAGIC ,billingPeriodEndIndicator
# MAGIC ,billingPeriodEndCount
# MAGIC ,billingDoumentAdjustmentReversalCount
# MAGIC ,billingDocumentNumberForAdjustmentReverssal
# MAGIC ,billingAllocationDate
# MAGIC ,billingRunNumber
# MAGIC ,simulationPeriodID
# MAGIC ,accountClassCode
# MAGIC ,billingDocumentOriginCode
# MAGIC ,billingDonotExecuteIndicator
# MAGIC ,billingPlanAdjustIndicator
# MAGIC ,newBillingDocumentNumberForReversedInvoicing
# MAGIC ,billingPostingDateInDocument
# MAGIC ,externalDocumentNumber
# MAGIC ,reversalReasonCode
# MAGIC ,billingDocumentWithoutInvoicingCode
# MAGIC ,billingRelavancyIndicator
# MAGIC ,errorDetectedDate
# MAGIC ,basicCategoryDynamicPeriodControlCode
# MAGIC ,meterReadingResultEstimatedBillingIndicator
# MAGIC ,SuppressedOrderEstimateBillingIndicator
# MAGIC ,originalValueEstimateBillingIndicator
# MAGIC ,suppressedOrderBillingIndicator
# MAGIC ,currentBillingPeriodCategoryCode
# MAGIC ,toBeBilledPeriodOriginalCategoryCode
# MAGIC ,incomingPaymentMethodCode
# MAGIC ,standingOrderIndicator
# MAGIC ,planningGroupNumber
# MAGIC ,billingKeyDate
# MAGIC ,onsiteBillingGroupCode
# MAGIC ,resultingBillingPeriodIndicator
# MAGIC ,billingDocumentPrimaryInstallationNumber
# MAGIC ,instalGroupTypeCode
# MAGIC ,instalGroupRoleCode
# MAGIC FROM
# MAGIC cleansed.t_sapisu_erch

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
# MAGIC BELNR as billingDocumentNumber
# MAGIC ,BUKRS as companyCode
# MAGIC ,b.companyName as companyName
# MAGIC ,SPARTE as divisonCode
# MAGIC ,GPARTNER as businessPartnerNumber
# MAGIC ,VKONT as contractAccountNumber
# MAGIC ,VERTRAG as contractID
# MAGIC ,case
# MAGIC when BEGABRPE='0' then null 
# MAGIC when BEGABRPE='00000000' then null
# MAGIC when BEGABRPE <> 'null' then CONCAT(LEFT(BEGABRPE,4),'-',SUBSTRING(BEGABRPE,5,2),'-',RIGHT(BEGABRPE,2))
# MAGIC else BEGABRPE end as startBillingPeriod
# MAGIC ,case
# MAGIC when ENDABRPE='0' then null 
# MAGIC when ENDABRPE='00000000' then null
# MAGIC when ENDABRPE <> 'null' then CONCAT(LEFT(ENDABRPE,4),'-',SUBSTRING(ENDABRPE,5,2),'-',RIGHT(ENDABRPE,2))
# MAGIC else ENDABRPE end as endBillingPeriod
# MAGIC ,case
# MAGIC when ENDABRPE='0' then null 
# MAGIC when ENDABRPE='00000000' then null
# MAGIC when ENDABRPE <> 'null' then CONCAT(LEFT(ENDABRPE,4),'-',SUBSTRING(ENDABRPE,5,2),'-',RIGHT(ENDABRPE,2))
# MAGIC else ENDABRPE end as endBillingPeriod
# MAGIC ,case
# MAGIC when ABRDATS='0' then null 
# MAGIC when ABRDATS='00000000' then null
# MAGIC when ABRDATS <> 'null' then CONCAT(LEFT(ABRDATS,4),'-',SUBSTRING(ABRDATS,5,2),'-',RIGHT(ABRDATS,2))
# MAGIC else ABRDATS end as billingScheduleDate
# MAGIC ,case
# MAGIC when ADATSOLL='0' then null 
# MAGIC when ADATSOLL='00000000' then null
# MAGIC when ADATSOLL <> 'null' then CONCAT(LEFT(ADATSOLL,4),'-',SUBSTRING(ADATSOLL,5,2),'-',RIGHT(ADATSOLL,2))
# MAGIC else ADATSOLL end as meterReadingScheduleDate
# MAGIC ,case
# MAGIC when PTERMTDAT='0' then null 
# MAGIC when PTERMTDAT='00000000' then null
# MAGIC when PTERMTDAT <> 'null' then CONCAT(LEFT(PTERMTDAT,4),'-',SUBSTRING(PTERMTDAT,5,2),'-',RIGHT(PTERMTDAT,2))
# MAGIC else PTERMTDAT end as billingPeriodEndDate
# MAGIC ,case
# MAGIC when BELEGDAT='0' then null 
# MAGIC when BELEGDAT='00000000' then null
# MAGIC when BELEGDAT <> 'null' then CONCAT(LEFT(BELEGDAT,4),'-',SUBSTRING(BELEGDAT,5,2),'-',RIGHT(BELEGDAT,2))
# MAGIC else BELEGDAT end as billingDocumentCreateDate
# MAGIC ,ABWVK as alternativeContractAccountForCollectiveBills
# MAGIC ,BELNRALT as previousDocumentNumber
# MAGIC ,case
# MAGIC when STORNODAT='0' then null 
# MAGIC when STORNODAT='00000000' then null
# MAGIC when STORNODAT <> 'null' then CONCAT(LEFT(STORNODAT,4),'-',SUBSTRING(STORNODAT,5,2),'-',RIGHT(STORNODAT,2))
# MAGIC else STORNODAT end as reversalDate
# MAGIC ,ABRVORG as billingTransactionCode
# MAGIC ,HVORG as mainTransactionLineItemCode
# MAGIC ,KOFIZ as contractAccountDeterminationID
# MAGIC ,PORTION as portionNumber
# MAGIC ,FORMULAR as formName
# MAGIC ,SIMULATION as billingSimulationIndicator
# MAGIC ,BELEGART as documentTypeCode
# MAGIC ,BERGRUND as backbillingCreditReasonCode
# MAGIC ,case
# MAGIC when BEGNACH='0' then null 
# MAGIC when BEGNACH='00000000' then null
# MAGIC when BEGNACH <> 'null' then CONCAT(LEFT(BEGNACH,4),'-',SUBSTRING(BEGNACH,5,2),'-',RIGHT(BEGNACH,2))
# MAGIC else BEGNACH end as backbillingStartPeriod
# MAGIC ,TOBRELEASD as DocumentNotReleasedIndicator
# MAGIC ,TXJCD as taxJurisdictionDescription
# MAGIC ,KONZVER as franchiseContractCode
# MAGIC ,EROETIM as billingDocumentCreateTime
# MAGIC ,ERCHP_V as ERCHP_Exist_IND
# MAGIC ,ABRVORG2 as periodEndBillingTransactionCode
# MAGIC ,ABLEINH as meterReadingUnit
# MAGIC ,ENDPRIO as billingEndingPriorityCodfe
# MAGIC ,ERDAT as createdDate
# MAGIC ,ERNAM as createdBy
# MAGIC ,case
# MAGIC when AEDAT='0' then null 
# MAGIC when AEDAT='00000000' then null
# MAGIC when AEDAT <> 'null' then CONCAT(LEFT(AEDAT,4),'-',SUBSTRING(AEDAT,5,2),'-',RIGHT(AEDAT,2))
# MAGIC else AEDAT end as lastChangedDate
# MAGIC ,AENAM as changedBy
# MAGIC ,BEGRU as authorizationGroupCode
# MAGIC ,LOEVM as deletedIndicator
# MAGIC ,case
# MAGIC when ABRDATSU='0' then null 
# MAGIC when ABRDATSU='00000000' then null
# MAGIC when ABRDATSU <> 'null' then CONCAT(LEFT(ABRDATSU,4),'-',SUBSTRING(ABRDATSU,5,2),'-',RIGHT(ABRDATSU,2))
# MAGIC else ABRDATSU end as suppressedBillingOrderScheduleDate
# MAGIC ,ABRVORGU as suppressedBillingOrderTransactionCode
# MAGIC ,N_INVSEP as jointInvoiceAutomaticDocumentIndicator
# MAGIC ,ABPOPBEL as BudgetBillingPlanCode
# MAGIC ,MANBILLREL as manualDocumentReleasedInvoicingIndicator
# MAGIC ,BACKBI as backbillingTypeCode
# MAGIC ,PERENDBI as billingPeriodEndType
# MAGIC ,NUMPERBB as backbillingPeriodNumber
# MAGIC ,case
# MAGIC when BEGEND='0' then null 
# MAGIC when BEGEND='00000000' then null
# MAGIC when BEGEND <> 'null' then CONCAT(LEFT(BEGEND,4),'-',SUBSTRING(BEGEND,5,2),'-',RIGHT(BEGEND,2))
# MAGIC else BEGEND end as periodEndBillingStartDate
# MAGIC ,ENDOFBB as backbillingPeriodEndIndicator
# MAGIC ,ENDOFPEB as billingPeriodEndIndicator
# MAGIC ,NUMPERPEB as billingPeriodEndCount
# MAGIC ,SC_BELNR_H as billingDoumentAdjustmentReversalCount
# MAGIC ,SC_BELNR_N as billingDocumentNumberForAdjustmentReverssal
# MAGIC ,case
# MAGIC when ZUORDDAA='0' then null 
# MAGIC when ZUORDDAA='00000000' then null
# MAGIC when ZUORDDAA <> 'null' then CONCAT(LEFT(ZUORDDAA,4),'-',SUBSTRING(ZUORDDAA,5,2),'-',RIGHT(ZUORDDAA,2))
# MAGIC else ZUORDDAA end as billingAllocationDate
# MAGIC ,BILLINGRUNNO as billingRunNumber
# MAGIC ,SIMRUNID as simulationPeriodID
# MAGIC ,KTOKLASSE as accountClassCode
# MAGIC ,ORIGDOC as billingDocumentOriginCode
# MAGIC ,NOCANC as billingDonotExecuteIndicator
# MAGIC ,ABSCHLPAN as billingPlanAdjustIndicator
# MAGIC ,MEM_OPBEL as newBillingDocumentNumberForReversedInvoicing
# MAGIC ,case
# MAGIC when MEM_BUDAT='0' then null 
# MAGIC when MEM_BUDAT='00000000' then null
# MAGIC when MEM_BUDAT <> 'null' then CONCAT(LEFT(MEM_BUDAT,4),'-',SUBSTRING(MEM_BUDAT,5,2),'-',RIGHT(MEM_BUDAT,2))
# MAGIC else MEM_BUDAT end as billingPostingDateInDocument
# MAGIC ,EXBILLDOCNO as externalDocumentNumber
# MAGIC ,BCREASON as reversalReasonCode
# MAGIC ,NINVOICE as billingDocumentWithoutInvoicingCode
# MAGIC ,NBILLREL as billingRelavancyIndicator
# MAGIC ,case
# MAGIC when CORRECTION_DATE='0' then null 
# MAGIC when CORRECTION_DATE='00000000' then null
# MAGIC when CORRECTION_DATE <> 'null' then CONCAT(LEFT(CORRECTION_DATE,4),'-',SUBSTRING(CORRECTION_DATE,5,2),'-',RIGHT(CORRECTION_DATE,2))
# MAGIC else CORRECTION_DATE end as errorDetectedDate
# MAGIC ,BASDYPER as basicCategoryDynamicPeriodControlCode
# MAGIC ,ESTINBILL as meterReadingResultEstimatedBillingIndicator
# MAGIC ,ESTINBILLU as SuppressedOrderEstimateBillingIndicator
# MAGIC ,ESTINBILL_SAV as originalValueEstimateBillingIndicator
# MAGIC ,ESTINBILL_USAV as suppressedOrderBillingIndicator
# MAGIC ,ACTPERIOD as currentBillingPeriodCategoryCode
# MAGIC ,ACTPERORG as toBeBilledPeriodOriginalCategoryCode
# MAGIC ,EZAWE as incomingPaymentMethodCode
# MAGIC ,DAUBUCH as standingOrderIndicator
# MAGIC ,FDGRP as planningGroupNumber
# MAGIC ,case
# MAGIC when BILLING_PERIOD='0' then null 
# MAGIC when BILLING_PERIOD='00000000' then null
# MAGIC when BILLING_PERIOD <> 'null' then CONCAT(LEFT(BILLING_PERIOD,4),'-',SUBSTRING(BILLING_PERIOD,5,2),'-',RIGHT(BILLING_PERIOD,2))
# MAGIC else BILLING_PERIOD end as billingKeyDate
# MAGIC ,OSB_GROUP as onsiteBillingGroupCode
# MAGIC ,BP_BILL as resultingBillingPeriodIndicator
# MAGIC ,MAINDOCNO as billingDocumentPrimaryInstallationNumber
# MAGIC ,INSTGRTYPE as instalGroupTypeCode
# MAGIC ,INSTROLE as instalGroupRoleCode
# MAGIC FROM Source a
# MAGIC LEFT JOIN cleansed.t_sapisu_0COMP_CODE_TEXT b
# MAGIC ON a.BUKRS = b.companyCode-- AND b.LANGU='E'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC BELNR as billingDocumentNumber
# MAGIC ,BUKRS as companyCode
# MAGIC ,b.companyName as companyName
# MAGIC ,SPARTE as divisonCode
# MAGIC ,GPARTNER as businessPartnerNumber
# MAGIC ,VKONT as contractAccountNumber
# MAGIC ,VERTRAG as contractID
# MAGIC ,BEGABRPE as startBillingPeriod
# MAGIC ,ENDABRPE as endBillingPeriod
# MAGIC ,ABRDATS as billingScheduleDate
# MAGIC ,ADATSOLL as meterReadingScheduleDate
# MAGIC ,PTERMTDAT as billingPeriodEndDate
# MAGIC ,BELEGDAT as billingDocumentCreateDate
# MAGIC ,ABWVK as alternativeContractAccountForCollectiveBills
# MAGIC ,BELNRALT as previousDocumentNumber
# MAGIC ,STORNODAT as reversalDate
# MAGIC ,ABRVORG as billingTransactionCode
# MAGIC ,HVORG as mainTransactionLineItemCode
# MAGIC ,KOFIZ as contractAccountDeterminationID
# MAGIC ,PORTION as portionNumber
# MAGIC ,FORMULAR as formName
# MAGIC ,SIMULATION as billingSimulationIndicator
# MAGIC ,BELEGART as documentTypeCode
# MAGIC ,BERGRUND as backbillingCreditReasonCode
# MAGIC ,BEGNACH as backbillingStartPeriod
# MAGIC ,TOBRELEASD as DocumentNotReleasedIndicator
# MAGIC ,TXJCD as taxJurisdictionDescription
# MAGIC ,KONZVER as franchiseContractCode
# MAGIC ,EROETIM as billingDocumentCreateTime
# MAGIC --,ERCHP_V as ERCHP_Exist_IND
# MAGIC ,ABRVORG2 as periodEndBillingTransactionCode
# MAGIC ,ABLEINH as meterReadingUnit
# MAGIC ,ENDPRIO as billingEndingPriorityCodfe
# MAGIC ,ERDAT as createdDate
# MAGIC ,ERNAM as createdBy
# MAGIC ,AEDAT as lastChangedDate
# MAGIC ,AENAM as changedBy
# MAGIC ,BEGRU as authorizationGroupCode
# MAGIC ,LOEVM as deletedIndicator
# MAGIC ,ABRDATSU as suppressedBillingOrderScheduleDate
# MAGIC ,ABRVORGU as suppressedBillingOrderTransactionCode
# MAGIC ,N_INVSEP as jointInvoiceAutomaticDocumentIndicator
# MAGIC ,ABPOPBEL as BudgetBillingPlanCode
# MAGIC ,MANBILLREL as manualDocumentReleasedInvoicingIndicator
# MAGIC ,BACKBI as backbillingTypeCode
# MAGIC ,PERENDBI as billingPeriodEndType
# MAGIC ,NUMPERBB as backbillingPeriodNumber
# MAGIC ,BEGEND as periodEndBillingStartDate
# MAGIC ,ENDOFBB as backbillingPeriodEndIndicator
# MAGIC ,ENDOFPEB as billingPeriodEndIndicator
# MAGIC ,NUMPERPEB as billingPeriodEndCount
# MAGIC ,SC_BELNR_H as billingDoumentAdjustmentReversalCount
# MAGIC ,SC_BELNR_N as billingDocumentNumberForAdjustmentReverssal
# MAGIC ,ZUORDDAA as billingAllocationDate
# MAGIC ,BILLINGRUNNO as billingRunNumber
# MAGIC ,SIMRUNID as simulationPeriodID
# MAGIC ,KTOKLASSE as accountClassCode
# MAGIC ,ORIGDOC as billingDocumentOriginCode
# MAGIC ,NOCANC as billingDonotExecuteIndicator
# MAGIC ,ABSCHLPAN as billingPlanAdjustIndicator
# MAGIC ,MEM_OPBEL as newBillingDocumentNumberForReversedInvoicing
# MAGIC ,MEM_BUDAT as billingPostingDateInDocument
# MAGIC ,EXBILLDOCNO as externalDocumentNumber
# MAGIC ,BCREASON as reversalReasonCode
# MAGIC ,NINVOICE as billingDocumentWithoutInvoicingCode
# MAGIC ,NBILLREL as billingRelavancyIndicator
# MAGIC ,CORRECTION_DATE as errorDetectedDate
# MAGIC ,BASDYPER as basicCategoryDynamicPeriodControlCode
# MAGIC ,ESTINBILL as meterReadingResultEstimatedBillingIndicator
# MAGIC ,ESTINBILLU as SuppressedOrderEstimateBillingIndicator
# MAGIC ,ESTINBILL_SAV as originalValueEstimateBillingIndicator
# MAGIC ,ESTINBILL_USAV as suppressedOrderBillingIndicator
# MAGIC ,ACTPERIOD as currentBillingPeriodCategoryCode
# MAGIC ,ACTPERORG as toBeBilledPeriodOriginalCategoryCode
# MAGIC ,EZAWE as incomingPaymentMethodCode
# MAGIC ,DAUBUCH as standingOrderIndicator
# MAGIC ,FDGRP as planningGroupNumber
# MAGIC ,BILLING_PERIOD as billingKeyDate
# MAGIC ,OSB_GROUP as onsiteBillingGroupCode
# MAGIC ,BP_BILL as resultingBillingPeriodIndicator
# MAGIC ,MAINDOCNO as billingDocumentPrimaryInstallationNumber
# MAGIC ,INSTGRTYPE as instalGroupTypeCode
# MAGIC ,INSTROLE as instalGroupRoleCode
# MAGIC FROM Source a
# MAGIC LEFT JOIN cleansed.t_sapisu_0COMP_CODE_TEXT b
# MAGIC ON a.BUKRS = b.companyCode
# MAGIC where a.BELNR = '300000003548' -- BELNR = '300000002646' and BELZEILE = '000009'

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC billingDocumentNumber
# MAGIC ,companyCode
# MAGIC ,companyName
# MAGIC ,divisonCode
# MAGIC ,businessPartnerNumber
# MAGIC ,contractAccountNumber
# MAGIC ,contractID
# MAGIC ,startBillingPeriod
# MAGIC ,endBillingPeriod
# MAGIC ,billingScheduleDate
# MAGIC ,meterReadingScheduleDate
# MAGIC ,billingPeriodEndDate
# MAGIC ,billingDocumentCreateDate
# MAGIC ,alternativeContractAccountForCollectiveBills
# MAGIC ,previousDocumentNumber
# MAGIC ,reversalDate
# MAGIC ,billingTransactionCode
# MAGIC ,mainTransactionLineItemCode
# MAGIC ,contractAccountDeterminationID
# MAGIC ,portionNumber
# MAGIC ,formName
# MAGIC ,billingSimulationIndicator
# MAGIC ,documentTypeCode
# MAGIC ,backbillingCreditReasonCode
# MAGIC ,backbillingStartPeriod
# MAGIC ,DocumentNotReleasedIndicator
# MAGIC ,taxJurisdictionDescription
# MAGIC ,franchiseContractCode
# MAGIC ,billingDocumentCreateTime
# MAGIC ,periodEndBillingTransactionCode
# MAGIC ,meterReadingUnit
# MAGIC ,billingEndingPriorityCodfe
# MAGIC ,createdDate
# MAGIC ,createdBy
# MAGIC ,lastChangedDate
# MAGIC ,changedBy
# MAGIC ,authorizationGroupCode
# MAGIC ,deletedIndicator
# MAGIC ,suppressedBillingOrderScheduleDate
# MAGIC ,suppressedBillingOrderTransactionCode
# MAGIC ,jointInvoiceAutomaticDocumentIndicator
# MAGIC ,BudgetBillingPlanCode
# MAGIC ,manualDocumentReleasedInvoicingIndicator
# MAGIC ,backbillingTypeCode
# MAGIC ,billingPeriodEndType
# MAGIC ,backbillingPeriodNumber
# MAGIC ,periodEndBillingStartDate
# MAGIC ,backbillingPeriodEndIndicator
# MAGIC ,billingPeriodEndIndicator
# MAGIC ,billingPeriodEndCount
# MAGIC ,billingDoumentAdjustmentReversalCount
# MAGIC ,billingDocumentNumberForAdjustmentReverssal
# MAGIC ,billingAllocationDate
# MAGIC ,billingRunNumber
# MAGIC ,simulationPeriodID
# MAGIC ,accountClassCode
# MAGIC ,billingDocumentOriginCode
# MAGIC ,billingDonotExecuteIndicator
# MAGIC ,billingPlanAdjustIndicator
# MAGIC ,newBillingDocumentNumberForReversedInvoicing
# MAGIC ,billingPostingDateInDocument
# MAGIC ,externalDocumentNumber
# MAGIC ,reversalReasonCode
# MAGIC ,billingDocumentWithoutInvoicingCode
# MAGIC ,billingRelavancyIndicator
# MAGIC ,errorDetectedDate
# MAGIC ,basicCategoryDynamicPeriodControlCode
# MAGIC ,meterReadingResultEstimatedBillingIndicator
# MAGIC ,SuppressedOrderEstimateBillingIndicator
# MAGIC ,originalValueEstimateBillingIndicator
# MAGIC ,suppressedOrderBillingIndicator
# MAGIC ,currentBillingPeriodCategoryCode
# MAGIC ,toBeBilledPeriodOriginalCategoryCode
# MAGIC ,incomingPaymentMethodCode
# MAGIC ,standingOrderIndicator
# MAGIC ,planningGroupNumber
# MAGIC ,billingKeyDate
# MAGIC ,onsiteBillingGroupCode
# MAGIC ,resultingBillingPeriodIndicator
# MAGIC ,billingDocumentPrimaryInstallationNumber
# MAGIC ,instalGroupTypeCode
# MAGIC ,instalGroupRoleCode
# MAGIC FROM
# MAGIC cleansed.t_sapisu_erch where billingdocumentnumber = '300000003548'

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct
# MAGIC startBillingPeriod
# MAGIC endBillingPeriod
# MAGIC billingScheduleDate
# MAGIC meterReadingScheduleDate
# MAGIC billingPeriodEndDate
# MAGIC billingDocumentCreateDate
# MAGIC reversalDate
# MAGIC backbillingStartPeriod
# MAGIC lastChangedDate
# MAGIC suppressedBillingOrderScheduleDate
# MAGIC periodEndBillingStartDate
# MAGIC billingAllocationDate
# MAGIC billingPostingDateInDocument
# MAGIC errorDetectedDate
# MAGIC billingKeyDate

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC startBillingPeriod
# MAGIC ,endBillingPeriod
# MAGIC ,billingScheduleDate
# MAGIC ,meterReadingScheduleDate
# MAGIC ,billingPeriodEndDate
# MAGIC ,billingDocumentCreateDate
# MAGIC ,reversalDate
# MAGIC ,backbillingStartPeriod
# MAGIC ,lastChangedDate
# MAGIC ,suppressedBillingOrderScheduleDate
# MAGIC ,periodEndBillingStartDate
# MAGIC ,billingAllocationDate
# MAGIC ,billingPostingDateInDocument
# MAGIC ,errorDetectedDate
# MAGIC billingKeyDate
# MAGIC 
# MAGIC from cleansed.t_isu_erch

# COMMAND ----------

# DBTITLE 1,[Source] with mapping
# MAGIC %sql
# MAGIC SELECT
# MAGIC BELNR as billingDocumentNumber
# MAGIC ,BUKRS as companyCode
# MAGIC ,b.companyName as companyName
# MAGIC ,SPARTE as divisonCode
# MAGIC ,GPARTNER as businessPartnerNumber
# MAGIC ,VKONT as contractAccountNumber
# MAGIC ,VERTRAG as contractID
# MAGIC ,case
# MAGIC when BEGABRPE='0' then null 
# MAGIC when BEGABRPE='00000000' then null
# MAGIC when BEGABRPE <> 'null' then CONCAT(LEFT(BEGABRPE,4),'-',SUBSTRING(BEGABRPE,5,2),'-',RIGHT(BEGABRPE,2))
# MAGIC else BEGABRPE end as startBillingPeriod
# MAGIC ,case
# MAGIC when ENDABRPE='0' then null 
# MAGIC when ENDABRPE='00000000' then null
# MAGIC when ENDABRPE <> 'null' then CONCAT(LEFT(ENDABRPE,4),'-',SUBSTRING(ENDABRPE,5,2),'-',RIGHT(ENDABRPE,2))
# MAGIC else ENDABRPE end as endBillingPeriod
# MAGIC ,case
# MAGIC when ENDABRPE='0' then null 
# MAGIC when ENDABRPE='00000000' then null
# MAGIC when ENDABRPE <> 'null' then CONCAT(LEFT(ENDABRPE,4),'-',SUBSTRING(ENDABRPE,5,2),'-',RIGHT(ENDABRPE,2))
# MAGIC else ENDABRPE end as endBillingPeriod
# MAGIC ,case
# MAGIC when ABRDATS='0' then null 
# MAGIC when ABRDATS='00000000' then null
# MAGIC when ABRDATS <> 'null' then CONCAT(LEFT(ABRDATS,4),'-',SUBSTRING(ABRDATS,5,2),'-',RIGHT(ABRDATS,2))
# MAGIC else ABRDATS end as billingScheduleDate
# MAGIC ,case
# MAGIC when ADATSOLL='0' then null 
# MAGIC when ADATSOLL='00000000' then null
# MAGIC when ADATSOLL <> 'null' then CONCAT(LEFT(ADATSOLL,4),'-',SUBSTRING(ADATSOLL,5,2),'-',RIGHT(ADATSOLL,2))
# MAGIC else ADATSOLL end as meterReadingScheduleDate
# MAGIC ,case
# MAGIC when PTERMTDAT='0' then null 
# MAGIC when PTERMTDAT='00000000' then null
# MAGIC when PTERMTDAT <> 'null' then CONCAT(LEFT(PTERMTDAT,4),'-',SUBSTRING(PTERMTDAT,5,2),'-',RIGHT(PTERMTDAT,2))
# MAGIC else PTERMTDAT end as billingPeriodEndDate
# MAGIC ,case
# MAGIC when BELEGDAT='0' then null 
# MAGIC when BELEGDAT='00000000' then null
# MAGIC when BELEGDAT <> 'null' then CONCAT(LEFT(BELEGDAT,4),'-',SUBSTRING(BELEGDAT,5,2),'-',RIGHT(BELEGDAT,2))
# MAGIC else BELEGDAT end as billingDocumentCreateDate
# MAGIC ,ABWVK as alternativeContractAccountForCollectiveBills
# MAGIC ,BELNRALT as previousDocumentNumber
# MAGIC ,case
# MAGIC when STORNODAT='0' then null 
# MAGIC when STORNODAT='00000000' then null
# MAGIC when STORNODAT <> 'null' then CONCAT(LEFT(STORNODAT,4),'-',SUBSTRING(STORNODAT,5,2),'-',RIGHT(STORNODAT,2))
# MAGIC else STORNODAT end as reversalDate
# MAGIC ,ABRVORG as billingTransactionCode
# MAGIC ,HVORG as mainTransactionLineItemCode
# MAGIC ,KOFIZ as contractAccountDeterminationID
# MAGIC ,PORTION as portionNumber
# MAGIC ,FORMULAR as formName
# MAGIC ,SIMULATION as billingSimulationIndicator
# MAGIC ,BELEGART as documentTypeCode
# MAGIC ,BERGRUND as backbillingCreditReasonCode
# MAGIC ,case
# MAGIC when BEGNACH='0' then null 
# MAGIC when BEGNACH='00000000' then null
# MAGIC when BEGNACH <> 'null' then CONCAT(LEFT(BEGNACH,4),'-',SUBSTRING(BEGNACH,5,2),'-',RIGHT(BEGNACH,2))
# MAGIC else BEGNACH end as backbillingStartPeriod
# MAGIC ,TOBRELEASD as DocumentNotReleasedIndicator
# MAGIC ,TXJCD as taxJurisdictionDescription
# MAGIC ,KONZVER as franchiseContractCode
# MAGIC ,EROETIM as billingDocumentCreateTime
# MAGIC ,ERCHP_V as ERCHP_Exist_IND
# MAGIC ,ABRVORG2 as periodEndBillingTransactionCode
# MAGIC ,ABLEINH as meterReadingUnit
# MAGIC ,ENDPRIO as billingEndingPriorityCodfe
# MAGIC ,ERDAT as createdDate
# MAGIC ,ERNAM as createdBy
# MAGIC ,case
# MAGIC when AEDAT='0' then null 
# MAGIC when AEDAT='00000000' then null
# MAGIC when AEDAT <> 'null' then CONCAT(LEFT(AEDAT,4),'-',SUBSTRING(AEDAT,5,2),'-',RIGHT(AEDAT,2))
# MAGIC else AEDAT end as lastChangedDate
# MAGIC ,AENAM as changedBy
# MAGIC ,BEGRU as authorizationGroupCode
# MAGIC ,LOEVM as deletedIndicator
# MAGIC ,case
# MAGIC when ABRDATSU='0' then null 
# MAGIC when ABRDATSU='00000000' then null
# MAGIC when ABRDATSU <> 'null' then CONCAT(LEFT(ABRDATSU,4),'-',SUBSTRING(ABRDATSU,5,2),'-',RIGHT(ABRDATSU,2))
# MAGIC else ABRDATSU end as suppressedBillingOrderScheduleDate
# MAGIC ,ABRVORGU as suppressedBillingOrderTransactionCode
# MAGIC ,N_INVSEP as jointInvoiceAutomaticDocumentIndicator
# MAGIC ,ABPOPBEL as BudgetBillingPlanCode
# MAGIC ,MANBILLREL as manualDocumentReleasedInvoicingIndicator
# MAGIC ,BACKBI as backbillingTypeCode
# MAGIC ,PERENDBI as billingPeriodEndType
# MAGIC ,NUMPERBB as backbillingPeriodNumber
# MAGIC ,case
# MAGIC when BEGEND='0' then null 
# MAGIC when BEGEND='00000000' then null
# MAGIC when BEGEND <> 'null' then CONCAT(LEFT(BEGEND,4),'-',SUBSTRING(BEGEND,5,2),'-',RIGHT(BEGEND,2))
# MAGIC else BEGEND end as periodEndBillingStartDate
# MAGIC ,ENDOFBB as backbillingPeriodEndIndicator
# MAGIC ,ENDOFPEB as billingPeriodEndIndicator
# MAGIC ,NUMPERPEB as billingPeriodEndCount
# MAGIC ,SC_BELNR_H as billingDoumentAdjustmentReversalCount
# MAGIC ,SC_BELNR_N as billingDocumentNumberForAdjustmentReverssal
# MAGIC ,case
# MAGIC when ZUORDDAA='0' then null 
# MAGIC when ZUORDDAA='00000000' then null
# MAGIC when ZUORDDAA <> 'null' then CONCAT(LEFT(ZUORDDAA,4),'-',SUBSTRING(ZUORDDAA,5,2),'-',RIGHT(ZUORDDAA,2))
# MAGIC else ZUORDDAA end as billingAllocationDate
# MAGIC ,BILLINGRUNNO as billingRunNumber
# MAGIC ,SIMRUNID as simulationPeriodID
# MAGIC ,KTOKLASSE as accountClassCode
# MAGIC ,ORIGDOC as billingDocumentOriginCode
# MAGIC ,NOCANC as billingDonotExecuteIndicator
# MAGIC ,ABSCHLPAN as billingPlanAdjustIndicator
# MAGIC ,MEM_OPBEL as newBillingDocumentNumberForReversedInvoicing
# MAGIC ,case
# MAGIC when MEM_BUDAT='0' then null 
# MAGIC when MEM_BUDAT='00000000' then null
# MAGIC when MEM_BUDAT <> 'null' then CONCAT(LEFT(MEM_BUDAT,4),'-',SUBSTRING(MEM_BUDAT,5,2),'-',RIGHT(MEM_BUDAT,2))
# MAGIC else MEM_BUDAT end as billingPostingDateInDocument
# MAGIC ,EXBILLDOCNO as externalDocumentNumber
# MAGIC ,BCREASON as reversalReasonCode
# MAGIC ,NINVOICE as billingDocumentWithoutInvoicingCode
# MAGIC ,NBILLREL as billingRelavancyIndicator
# MAGIC ,case
# MAGIC when CORRECTION_DATE='0' then null 
# MAGIC when CORRECTION_DATE='00000000' then null
# MAGIC when CORRECTION_DATE <> 'null' then CONCAT(LEFT(CORRECTION_DATE,4),'-',SUBSTRING(CORRECTION_DATE,5,2),'-',RIGHT(CORRECTION_DATE,2))
# MAGIC else CORRECTION_DATE end as errorDetectedDate
# MAGIC ,BASDYPER as basicCategoryDynamicPeriodControlCode
# MAGIC ,ESTINBILL as meterReadingResultEstimatedBillingIndicator
# MAGIC ,ESTINBILLU as SuppressedOrderEstimateBillingIndicator
# MAGIC ,ESTINBILL_SAV as originalValueEstimateBillingIndicator
# MAGIC ,ESTINBILL_USAV as suppressedOrderBillingIndicator
# MAGIC ,ACTPERIOD as currentBillingPeriodCategoryCode
# MAGIC ,ACTPERORG as toBeBilledPeriodOriginalCategoryCode
# MAGIC ,EZAWE as incomingPaymentMethodCode
# MAGIC ,DAUBUCH as standingOrderIndicator
# MAGIC ,FDGRP as planningGroupNumber
# MAGIC ,case
# MAGIC when BILLING_PERIOD='0' then null 
# MAGIC when BILLING_PERIOD='00000000' then null
# MAGIC when BILLING_PERIOD <> 'null' then CONCAT(LEFT(BILLING_PERIOD,4),'-',SUBSTRING(BILLING_PERIOD,5,2),'-',RIGHT(BILLING_PERIOD,2))
# MAGIC else BILLING_PERIOD end as billingKeyDate
# MAGIC ,OSB_GROUP as onsiteBillingGroupCode
# MAGIC ,BP_BILL as resultingBillingPeriodIndicator
# MAGIC ,MAINDOCNO as billingDocumentPrimaryInstallationNumber
# MAGIC ,INSTGRTYPE as instalGroupTypeCode
# MAGIC ,INSTROLE as instalGroupRoleCode
# MAGIC FROM Source a
# MAGIC LEFT JOIN cleansed.t_sapisu_0COMP_CODE_TEXT b
# MAGIC ON a.BUKRS = b.companyCode-- AND b.LANGU='E'
