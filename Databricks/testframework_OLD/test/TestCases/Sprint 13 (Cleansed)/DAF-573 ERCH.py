# Databricks notebook source
hostName = 'nt097dslt01.adsdev.swcdev'
databaseName = 'SLT_EQ1CLNT100'
port = 1433
dbURL = f'jdbc:sqlserver://{hostName}:{port};database={databaseName};user=SLTADMIN;password=Edpgroup01#!'
qry = '(select * from EQ1.ERCH) myTable'
df = spark.read.jdbc(url=dbURL, table=qry, lowerBound=1, upperBound=100000, numPartitions=10)
display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.write.format("json").saveAsTable("test" + "." + "erch")

# COMMAND ----------

# DBTITLE 1,[Source] with mapping
# MAGIC %sql
# MAGIC select
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
# MAGIC ,ERCHO_V as ERCHO_Exist_IND
# MAGIC ,ERCHZ_V as ERCHZ_Exist_IND
# MAGIC ,ERCHU_V as ERCHU_Exist_IND
# MAGIC ,ERCHR_V as ERCHR_Exist_IND
# MAGIC ,ERCHC_V as ERCHC_Exist_IND
# MAGIC ,ERCHV_V as ERCHV_Exist_IND
# MAGIC ,ERCHT_V as ERCHT_Exist_IND
# MAGIC ,ERCHP_V as ERCHP_Exist_IND
# MAGIC ,ABRVORG2 as periodEndBillingTransactionCode
# MAGIC ,ABLEINH as meterReadingUnit
# MAGIC ,ENDPRIO as billingEndingPriorityCode
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
# MAGIC FROM test.erch a
# MAGIC LEFT JOIN cleansed.isu_0comp_code_text b
# MAGIC ON a.BUKRS = b.companyCode

# COMMAND ----------

lakedf = spark.sql("select * from cleansed.isu_erch")
display(lakedf)

# COMMAND ----------

lakedf.printSchema()

# COMMAND ----------

# DBTITLE 1,[Verification] Count Check
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from cleansed.isu_erch
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from (select
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
# MAGIC ,ERCHO_V as ERCHO_Exist_IND
# MAGIC ,ERCHZ_V as ERCHZ_Exist_IND
# MAGIC ,ERCHU_V as ERCHU_Exist_IND
# MAGIC ,ERCHR_V as ERCHR_Exist_IND
# MAGIC ,ERCHC_V as ERCHC_Exist_IND
# MAGIC ,ERCHV_V as ERCHV_Exist_IND
# MAGIC ,ERCHT_V as ERCHT_Exist_IND
# MAGIC ,ERCHP_V as ERCHP_Exist_IND
# MAGIC ,ABRVORG2 as periodEndBillingTransactionCode
# MAGIC ,ABLEINH as meterReadingUnit
# MAGIC ,ENDPRIO as billingEndingPriorityCode
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
# MAGIC FROM test.erch a
# MAGIC LEFT JOIN cleansed.isu_0comp_code_text b
# MAGIC ON a.BUKRS = b.companyCode
# MAGIC )

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT billingDocumentNumber, COUNT (*) as count
# MAGIC FROM cleansed.isu_erch
# MAGIC GROUP BY billingDocumentNumber
# MAGIC HAVING COUNT (*) > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT * FROM (
# MAGIC SELECT
# MAGIC *,
# MAGIC row_number() OVER(PARTITION BY billingDocumentNumber order by billingDocumentNumber) as rn
# MAGIC FROM  cleansed.isu_erch
# MAGIC )a where a.rn > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Source and Target Data
# MAGIC %sql
# MAGIC select
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
# MAGIC ,ERCHO_V as ERCHO_Exist_IND
# MAGIC ,ERCHZ_V as ERCHZ_Exist_IND
# MAGIC ,ERCHU_V as ERCHU_Exist_IND
# MAGIC ,ERCHR_V as ERCHR_Exist_IND
# MAGIC ,ERCHC_V as ERCHC_Exist_IND
# MAGIC ,ERCHV_V as ERCHV_Exist_IND
# MAGIC ,ERCHT_V as ERCHT_Exist_IND
# MAGIC ,ERCHP_V as ERCHP_Exist_IND
# MAGIC ,ABRVORG2 as periodEndBillingTransactionCode
# MAGIC ,ABLEINH as meterReadingUnit
# MAGIC ,ENDPRIO as billingEndingPriorityCode
# MAGIC ,to_date(ERDAT, 'yyyyMMdd') as createdDate
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
# MAGIC ,NBILLREL as billingRelevancyIndicator
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
# MAGIC ,BILLING_PERIOD as billingKeyDate
# MAGIC ,OSB_GROUP as onsiteBillingGroupCode
# MAGIC ,BP_BILL as resultingBillingPeriodIndicator
# MAGIC ,MAINDOCNO as billingDocumentPrimaryInstallationNumber
# MAGIC ,INSTGRTYPE as instalGroupTypeCode
# MAGIC ,INSTROLE as instalGroupRoleCode
# MAGIC FROM test.erch a
# MAGIC LEFT JOIN cleansed.isu_0comp_code_text b
# MAGIC ON a.BUKRS = b.companyCode
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
# MAGIC ,ERCHO_Exist_IND
# MAGIC ,ERCHZ_Exist_IND
# MAGIC ,ERCHU_Exist_IND
# MAGIC ,ERCHR_Exist_IND
# MAGIC ,ERCHC_Exist_IND
# MAGIC ,ERCHV_Exist_IND
# MAGIC ,ERCHT_Exist_IND
# MAGIC ,ERCHP_Exist_IND
# MAGIC ,periodEndBillingTransactionCode
# MAGIC ,meterReadingUnit
# MAGIC ,billingEndingPriorityCode
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
# MAGIC ,billingDocumentNumberForAdjustmentReversal
# MAGIC ,billingAllocationDate
# MAGIC ,billingRunNumber
# MAGIC ,simulationPeriodID
# MAGIC ,accountClassCode
# MAGIC ,billingDocumentOriginCode
# MAGIC ,billingDoNotExecuteIndicator
# MAGIC ,billingPlanAdjustIndicator
# MAGIC ,newBillingDocumentNumberForReversedInvoicing
# MAGIC ,billingPostingDateInDocument
# MAGIC ,externalDocumentNumber
# MAGIC ,reversalReasonCode
# MAGIC ,billingDocumentWithoutInvoicingCode
# MAGIC ,billingRelevancyIndicator
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
# MAGIC from
# MAGIC cleansed.isu_erch

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Target and Source Data
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
# MAGIC ,ERCHO_Exist_IND
# MAGIC ,ERCHZ_Exist_IND
# MAGIC ,ERCHU_Exist_IND
# MAGIC ,ERCHR_Exist_IND
# MAGIC ,ERCHC_Exist_IND
# MAGIC ,ERCHV_Exist_IND
# MAGIC ,ERCHT_Exist_IND
# MAGIC ,ERCHP_Exist_IND
# MAGIC ,periodEndBillingTransactionCode
# MAGIC ,meterReadingUnit
# MAGIC ,billingEndingPriorityCode
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
# MAGIC ,billingDocumentNumberForAdjustmentReversal
# MAGIC ,billingAllocationDate
# MAGIC ,billingRunNumber
# MAGIC ,simulationPeriodID
# MAGIC ,accountClassCode
# MAGIC ,billingDocumentOriginCode
# MAGIC ,billingDoNotExecuteIndicator
# MAGIC ,billingPlanAdjustIndicator
# MAGIC ,newBillingDocumentNumberForReversedInvoicing
# MAGIC ,billingPostingDateInDocument
# MAGIC ,externalDocumentNumber
# MAGIC ,reversalReasonCode
# MAGIC ,billingDocumentWithoutInvoicingCode
# MAGIC ,billingRelevancyIndicator
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
# MAGIC from
# MAGIC cleansed.isu_erch
# MAGIC except
# MAGIC select
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
# MAGIC ,ERCHO_V as ERCHO_Exist_IND
# MAGIC ,ERCHZ_V as ERCHZ_Exist_IND
# MAGIC ,ERCHU_V as ERCHU_Exist_IND
# MAGIC ,ERCHR_V as ERCHR_Exist_IND
# MAGIC ,ERCHC_V as ERCHC_Exist_IND
# MAGIC ,ERCHV_V as ERCHV_Exist_IND
# MAGIC ,ERCHT_V as ERCHT_Exist_IND
# MAGIC ,ERCHP_V as ERCHP_Exist_IND
# MAGIC ,ABRVORG2 as periodEndBillingTransactionCode
# MAGIC ,ABLEINH as meterReadingUnit
# MAGIC ,ENDPRIO as billingEndingPriorityCode
# MAGIC ,to_date(ERDAT, 'yyyyMMdd') as createdDate
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
# MAGIC ,NBILLREL as billingRelevancyIndicator
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
# MAGIC ,BILLING_PERIOD as billingKeyDate
# MAGIC ,OSB_GROUP as onsiteBillingGroupCode
# MAGIC ,BP_BILL as resultingBillingPeriodIndicator
# MAGIC ,MAINDOCNO as billingDocumentPrimaryInstallationNumber
# MAGIC ,INSTGRTYPE as instalGroupTypeCode
# MAGIC ,INSTROLE as instalGroupRoleCode
# MAGIC FROM test.erch a
# MAGIC LEFT JOIN cleansed.isu_0comp_code_text b
# MAGIC ON a.BUKRS = b.companyCode

# COMMAND ----------

# DBTITLE 1,Source
# MAGIC %sql
# MAGIC select
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
# MAGIC ,ERCHO_V as ERCHO_Exist_IND
# MAGIC ,ERCHZ_V as ERCHZ_Exist_IND
# MAGIC ,ERCHU_V as ERCHU_Exist_IND
# MAGIC ,ERCHR_V as ERCHR_Exist_IND
# MAGIC ,ERCHC_V as ERCHC_Exist_IND
# MAGIC ,ERCHV_V as ERCHV_Exist_IND
# MAGIC ,ERCHT_V as ERCHT_Exist_IND
# MAGIC ,ERCHP_V as ERCHP_Exist_IND
# MAGIC ,ABRVORG2 as periodEndBillingTransactionCode
# MAGIC ,ABLEINH as meterReadingUnit
# MAGIC ,ENDPRIO as billingEndingPriorityCode
# MAGIC --,ERDAT as createdDate
# MAGIC ,to_date(ERDAT, 'yyyyMMdd') as createdDate
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
# MAGIC ,BILLING_PERIOD as billingKeyDate
# MAGIC --,case
# MAGIC --when BILLING_PERIOD='0' then null 
# MAGIC --when BILLING_PERIOD='00000000' then null
# MAGIC --when BILLING_PERIOD <> 'null' then CONCAT(LEFT(BILLING_PERIOD,4),'-',SUBSTRING(BILLING_PERIOD,5,2),'-',RIGHT(BILLING_PERIOD,2))
# MAGIC --else BILLING_PERIOD end as billingKeyDate
# MAGIC ,OSB_GROUP as onsiteBillingGroupCode
# MAGIC ,BP_BILL as resultingBillingPeriodIndicator
# MAGIC ,MAINDOCNO as billingDocumentPrimaryInstallationNumber
# MAGIC ,INSTGRTYPE as instalGroupTypeCode
# MAGIC ,INSTROLE as instalGroupRoleCode
# MAGIC FROM test.erch a
# MAGIC LEFT JOIN cleansed.isu_0comp_code_text b
# MAGIC ON a.BUKRS = b.companyCode where BELNR='040000017300'

# COMMAND ----------

# DBTITLE 1,Target
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
# MAGIC ,ERCHO_Exist_IND
# MAGIC ,ERCHZ_Exist_IND
# MAGIC ,ERCHU_Exist_IND
# MAGIC ,ERCHR_Exist_IND
# MAGIC ,ERCHC_Exist_IND
# MAGIC ,ERCHV_Exist_IND
# MAGIC ,ERCHT_Exist_IND
# MAGIC ,ERCHP_Exist_IND
# MAGIC ,periodEndBillingTransactionCode
# MAGIC ,meterReadingUnit
# MAGIC ,billingEndingPriorityCode
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
# MAGIC ,billingDocumentNumberForAdjustmentReversal
# MAGIC ,billingAllocationDate
# MAGIC ,billingRunNumber
# MAGIC ,simulationPeriodID
# MAGIC ,accountClassCode
# MAGIC ,billingDocumentOriginCode
# MAGIC ,billingDoNotExecuteIndicator
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
# MAGIC from
# MAGIC cleansed.isu_erch WHERE billingDocumentNumber='040000017300'

# COMMAND ----------

# DBTITLE 1,def
# MAGIC %sql
# MAGIC select
# MAGIC billingRelevancyIndicator
# MAGIC from cleansed.isu_erch

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select
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
# MAGIC --,ERCHP_V as ERCHP_Exist_IND
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
# MAGIC FROM test.erch a
# MAGIC LEFT JOIN cleansed.isu_0comp_code_text b
# MAGIC ON a.BUKRS = b.companyCode
# MAGIC where BELNR = '010000024982'
# MAGIC EXCEPT
# MAGIC SELECT
# MAGIC * from cleansed.isu_erch where billingDocumentNumber = '010000024982'

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC to_date(ERDAT, 'yyyyMMdd') as createdDate
# MAGIC from
# MAGIC test.erch a
# MAGIC LEFT JOIN cleansed.isu_0comp_code_text b
# MAGIC ON a.BUKRS = b.companyCode
# MAGIC where BELNR = '010000007692'
# MAGIC except
# MAGIC 
# MAGIC select createdDate from cleansed.isu_erch where billingDocumentNumber = '010000007692'

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC to_date(ERDAT, 'yyyyMMdd') as createdDate
# MAGIC from
# MAGIC test.erch a
# MAGIC LEFT JOIN cleansed.isu_0comp_code_text b
# MAGIC ON a.BUKRS = b.companyCode
# MAGIC where BELNR = '010000007692'
# MAGIC except
# MAGIC 
# MAGIC select createdDate from cleansed.isu_erch where billingDocumentNumber = '010000007692'

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC BILLING_PERIOD  as billingKeyDate
# MAGIC from
# MAGIC test.erch a
# MAGIC LEFT JOIN cleansed.isu_0comp_code_text b
# MAGIC ON a.BUKRS = b.companyCode
# MAGIC where BELNR = '010000007692'
# MAGIC except
# MAGIC 
# MAGIC select billingKeyDate from cleansed.isu_erch where billingDocumentNumber = '010000007692'

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
# MAGIC billingKeyDate from cleansed.isu_erch

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC startBillingPeriod
# MAGIC ,endBillingPeriod
# MAGIC  from cleansed.isu_erch

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct 
# MAGIC startBillingPeriod
# MAGIC from cleansed.isu_erch

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct 
# MAGIC billingPeriodEndDate
# MAGIC from cleansed.isu_erch

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct 
# MAGIC billingDocumentCreateDate
# MAGIC from cleansed.isu_erch

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct 
# MAGIC reversalDate
# MAGIC from cleansed.isu_erch

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct 
# MAGIC backbillingStartPeriod
# MAGIC from cleansed.isu_erch

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct 
# MAGIC BEGNACH
# MAGIC from Source

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct 
# MAGIC lastChangedDate
# MAGIC from cleansed.isu_erch

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct 
# MAGIC suppressedBillingOrderScheduleDate
# MAGIC from cleansed.isu_erch

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct 
# MAGIC periodEndBillingStartDate
# MAGIC from cleansed.isu_erch

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct BEGEND from source

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct 
# MAGIC 
# MAGIC 
# MAGIC billingAllocationDate
# MAGIC from cleansed.isu_erch

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct 
# MAGIC billingPostingDateInDocument
# MAGIC from cleansed.isu_erch

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct 
# MAGIC errorDetectedDate
# MAGIC from cleansed.isu_erch

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC distinct
# MAGIC billingKeyDate from cleansed.isu_erch

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC BILLING_PERIOD as billingKeyDate from source
