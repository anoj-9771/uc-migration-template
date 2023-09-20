# Databricks notebook source
# MAGIC %run ../common/common-curated-includeMain

# COMMAND ----------

def getMeterConsumptionBillingDocument():
    
    df_isu = spark.sql(f"""
        select 
        'ISU' as sourceSystemCode,
        erch.billingDocumentNumber as billingDocumentNumber,
        erch.startBillingPeriod as billingPeriodStartDate,
        erch.endBillingPeriod as billingPeriodEndDate,
        erch.billingDocumentCreateDate as billCreatedDate,
        erch.documentNotReleasedFlag as isOutsortedFlag,
        case erch.reversalDate
          when NULL then 'N'
          when '1900-01-01' then 'N'
          when '9999-12-31' then 'N'
          else 'Y'
        end as isReversedFlag,
        erch.reversalDate as reversalDate,
        erch.portionNumber as portionNumber,
        erch.portion as portion,
        erch.documentTypeCode as documentTypeCode,
        erch.documentType as documentType,
        erch.meterReadingUnit as meterReadingUnit,
        erch.billingTransactionCode as billingTransactionCode,
        erch.billingTransaction as billingTransaction,
        erch.divisionCode as divisionCode,
        erch.division as division,
        erch.lastChangedDate as lastChangedDate,
        erch.createdDate as createdDate,
        erch.billingDocumentCreateDate as billingDocumentCreateDate,
        erch.erchcExistFlag as erchcExistFlag,
        erch.billingDocumentWithoutInvoicingCode as billingDocumentWithoutInvoicingCode,
        erch.billingDocumentWithoutInvoicing as billingDocumentWithoutInvoicing,
        erch.newBillingDocumentNumberForReversedInvoicing as newBillingDocumentNumberForReversedInvoicing,
        erchc.postingDate as invoicePostingDate,
        erchc.documentNotReleasedFlag as documentNotReleasedFlag,
        erchc.invoiceReversalPostingDate as invoiceReversalPostingDate,
        erchc.sequenceNumber as invoiceMaxSequenceNumber,
        erch._RecordDeleted as _RecordDeleted,
        erch._DLCleansedZoneTimeStamp 
        from {ADS_DATABASE_CLEANSED}.isu.erch erch 
           inner join {ADS_DATABASE_CLEANSED}.isu.dberchz1 as dberchz1 on erch.billingDocumentNumber = dberchz1.billingDocumentNumber                               
                                          and (dberchz1.lineItemTypeCode = 'ZDQUAN' or dberchz1.lineItemTypeCode = 'ZRQUAN')         
                                          and trim(erch.billingSimulationIndicator) = ''    
           inner join {ADS_DATABASE_CLEANSED}.isu.dberchz2 as dberchz2 on dberchz1.billingDocumentNumber = dberchz2.billingDocumentNumber
                                          and dberchz1.billingDocumentLineItemId  = dberchz2.billingDocumentLineItemId
                                          and trim(dberchz2.suppressedMeterReadingDocumentId) <> ''
           left outer join (select *, row_number() over(partition by billingDocumentNumber order by sequenceNumber desc) rec_num 
                               from {ADS_DATABASE_CLEANSED}.isu.erchc where _RecordCurrent = 1 ) erchc
                                          on erch.billingDocumentNumber =  erchc.billingDocumentNumber and erchc.rec_num = 1 
           where erch._RecordCurrent = 1 and dberchz1._RecordCurrent = 1 and dberchz2._RecordCurrent = 1 
        """).dropDuplicates()
    
    dummyDimRecDf = spark.createDataFrame(["-1"], "string").toDF("billingDocumentNumber") 
    df = df_isu.unionByName(dummyDimRecDf, allowMissingColumns = True)
    
    schema = StructType([
                            StructField('meterConsumptionBillingDocumentSK', StringType(), False),
                            StructField("sourceSystemCode", StringType(), True),
                            StructField("billingDocumentNumber", StringType(), False),
                            StructField("billingPeriodStartDate", DateType(), True),
                            StructField("billingPeriodEndDate", DateType(), True),
                            StructField("billCreatedDate", DateType(), True),
                            StructField("isOutsortedFlag", StringType(), True),
                            StructField("isReversedFlag", StringType(), True),
                            StructField("reversalDate", DateType(), True),
                            StructField("portionNumber", StringType(), True),
                            StructField("portion", StringType(), True),
                            StructField("documentTypeCode", StringType(), True),
                            StructField("documentType", StringType(), True),
                            StructField("meterReadingUnit", StringType(), True),
                            StructField("billingTransactionCode", StringType(), True),
                            StructField("billingTransaction", StringType(), True),
                            StructField("divisionCode", StringType(), True),
                            StructField("division", StringType(), True),
                            StructField("lastChangedDate", DateType(), True),
                            StructField("createdDate", DateType(), True),
                            StructField("billingDocumentCreateDate", DateType(), True),
                            StructField("erchcExistFlag", StringType(), True),
                            StructField("billingDocumentWithoutInvoicingCode", StringType(), True),
                            StructField("billingDocumentWithoutInvoicing", StringType(), True),
                            StructField("newBillingDocumentNumberForReversedInvoicing", IntegerType(), True),
                            StructField("invoicePostingDate", DateType(), True),
                            StructField("documentNotReleasedFlag", StringType(), True),
                            StructField("invoiceReversalPostingDate", DateType(), True),
                            StructField("invoiceMaxSequenceNumber", StringType(), True)
                      ])

    return df, schema

# COMMAND ----------

df, schema = getMeterConsumptionBillingDocument()
#TemplateEtl(df, entity="dim.meterConsumptionBillingDocument", businessKey="sourceSystemCode,billingDocumentNumber", schema=schema, writeMode=ADS_WRITE_MODE_OVERWRITE, AddSK=True)
TemplateTimeSliceEtlSCD(df, entity="dim.meterConsumptionBillingDocument", businessKey="billingDocumentNumber", schema=schema, fromDateCol='billingPeriodStartDate', toDateCol='billingPeriodEndDate')

# COMMAND ----------

dbutils.notebook.exit("1")
