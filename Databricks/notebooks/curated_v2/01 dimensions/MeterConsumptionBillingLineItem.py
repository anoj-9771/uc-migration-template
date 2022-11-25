# Databricks notebook source
# MAGIC %run ../common/common-curated-includeMain

# COMMAND ----------

def getMeterConsumptionBillingLineItem():
    
    df_isu = spark.sql(f"""
                    select 
                    'ISU' as sourceSystemCode,
                    erch.billingDocumentNumber as billingDocumentNumber,
                    dberchz1.billingDocumentLineItemId as billingDocumentLineItemId,
                    dberchz1.lineItemTypeCode as lineItemTypeCode,
                    dberchz1.lineItemType as lineItemType,
                    dberchz1.billingLineItemBudgetBillingFlag as billingLineItemBudgetBillingFlag,
                    dberchz1.subTransactioncode as subTransactioncode,
                    tvorg_text.subTransaction as subTransaction,
                    dberchz1.industryCode,
                    dberchz1.billingClassCode,
                    dberchz1.billingClass,
                    dberchz1.rateTypeCode,
                    dberchz1.rateType,
                    dberchz1.rateId,
                    dberchz1.rateDescription,
                    dberchz1.statisticalAnalysisRateType as statisticalAnalysisRateTypeCode,
                    dberchz1.statisticalAnalysisRateTypeDescription as statisticalAnalysisRateType,
                    dberchz1.validFromDate,
                    dberchz1.validToDate,
                    dberchz2.logicalRegisterNumber,
                    dberchz2.registerNumber,
                    dberchz2.suppressedMeterReadingDocumentId,
                    dberchz2.meterReadingReasonCode,
                    dberchz2.meterReadingReason,
                    dberchz2.previousMeterReadingReasonCode,
                    dberchz2.previousMeterReadingReason,
                    dberchz2.meterReadingTypeCode,
                    dberchz2.meterReadingType,
                    dberchz2.previousMeterReadingTypeCode,
                    dberchz2.previousMeterReadingType,
                    dberchz2.maxMeterReadingDate,
                    dberchz2.maxMeterReadingTime,
                    dberchz2.billingMeterReadingTime,
                    dberchz2.previousMeterReadingTime,
                    dberchz2.meterReadingResultsSimulationIndicator,
                    dberchz2.registerRelationshipConsecutiveNumber,
                    dberchz2.meterReadingAllocationDate,
                    dberchz2.logicalDeviceNumber,
                    dberchz2.registerRelationshipSortHelpCode,
                    dberchz2.registerRelationshipSortHelp,
                    dberchz2.meterReaderNoteText,
                    dberchz2.quantityDeterminationProcedureCode,
                    dberchz2.meterReadingDocumentId,
                    erch._RecordDeleted as _RecordDeleted 
                    from cleansed.isu_erch erch 
                               inner join cleansed.isu_dberchz1 as dberchz1 on erch.billingDocumentNumber = dberchz1.billingDocumentNumber                               
                                                              and (dberchz1.lineItemTypeCode = 'ZDQUAN' or dberchz1.lineItemTypeCode = 'ZRQUAN')         
                                                              and trim(erch.billingSimulationIndicator) = ''    
                               inner join cleansed.isu_dberchz2 as dberchz2 on dberchz1.billingDocumentNumber = dberchz2.billingDocumentNumber
                                                              and dberchz1.billingDocumentLineItemId  = dberchz2.billingDocumentLineItemId
                                                              and trim(dberchz2.suppressedMeterReadingDocumentId) <> '' 
                               left join cleansed.isu_0uc_tvorg_text as tvorg_text on tvorg_text.mainTransactionCode = erch.mainTransactionCode 
                                                              and tvorg_text.subTransactionCode = dberchz1.subTransactionCode 
                               where erch._RecordCurrent = 1 and dberchz1._RecordCurrent = 1 and dberchz2._RecordCurrent = 1
                   """).dropDuplicates()
    
    dummyDimRecDf = spark.createDataFrame([("-1","-1")], ["billingDocumentNumber","billingDocumentLineItemId"])
    df = df_isu.unionByName(dummyDimRecDf, allowMissingColumns = True)
    
    schema = StructType([
                            StructField('meterConsumptionBillingLineItemSK', StringType(), False),
                            StructField("sourceSystemCode", StringType(), True),
                            StructField("billingDocumentNumber", StringType(), False),
                            StructField("billingDocumentLineItemId", StringType(), False),
                            StructField("lineItemTypeCode", StringType(), True),
                            StructField("lineItemType", StringType(), True),
                            StructField("billingLineItemBudgetBillingFlag", StringType(), True),
                            StructField("subTransactioncode", StringType(), True),
                            StructField("subTransaction", StringType(), True),
                            StructField("industryCode", StringType(), True),
                            StructField("billingClassCode", StringType(), True),
                            StructField("billingClass", StringType(), True),
                            StructField("rateTypeCode", StringType(), True),
                            StructField("rateType", StringType(), True),
                            StructField("rateId", StringType(), True),
                            StructField("rateDescription", StringType(), True),
                            StructField("statisticalAnalysisRateTypeCode", StringType(), True),
                            StructField("statisticalAnalysisRateType", StringType(), True),
                            StructField("validFromDate", DateType(), True),
                            StructField("validToDate", DateType(), True),
                            StructField("logicalRegisterNumber", StringType(), True),
                            StructField("registerNumber", StringType(), True),
                            StructField("suppressedMeterReadingDocumentId", StringType(), True),
                            StructField("meterReadingReasonCode", StringType(), True),
                            StructField("meterReadingReason", StringType(), True),
                            StructField("previousMeterReadingReasonCode", StringType(), True),
                            StructField("previousMeterReadingReason", StringType(), True),
                            StructField("meterReadingTypeCode", StringType(), True),
                            StructField("meterReadingType", StringType(), True),
                            StructField("previousMeterReadingTypeCode", StringType(), True),
                            StructField("previousMeterReadingType", StringType(), True),
                            StructField("maxMeterReadingDate", DateType(), True),
                            StructField("maxMeterReadingTime", StringType(), True),
                            StructField("billingMeterReadingTime", StringType(), True),
                            StructField("previousMeterReadingTime", StringType(), True),
                            StructField("meterReadingResultsSimulationIndicator", StringType(), True),
                            StructField("registerRelationshipConsecutiveNumber", StringType(), True),
                            StructField("meterReadingAllocationDate", DateType(), True),
                            StructField("logicalDeviceNumber", StringType(), True),
                            StructField("registerRelationshipSortHelpCode", StringType(), True),
                            StructField("registerRelationshipSortHelp", StringType(), True),
                            StructField("meterReaderNoteText", StringType(), True),
                            StructField("quantityDeterminationProcedureCode", StringType(), True),
                            StructField("meterReadingDocumentId", StringType(), True)
                      ])

    return df, schema

# COMMAND ----------

df, schema = getMeterConsumptionBillingLineItem()
#TemplateEtl(df, entity="dimMeterConsumptionBillingLineItem", businessKey="sourceSystemCode,billingDocumentNumber,billingDocumentLineItemId", schema=schema, writeMode=ADS_WRITE_MODE_OVERWRITE, AddSK=True)
TemplateEtlSCD(df, entity="dimMeterConsumptionBillingLineItem", businessKey="billingDocumentNumber,billingDocumentLineItemId", schema=schema)

# COMMAND ----------

dbutils.notebook.exit("1")
