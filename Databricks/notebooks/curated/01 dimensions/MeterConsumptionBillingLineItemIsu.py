# Databricks notebook source
###########################################################################################################################
# Loads MeterConsumptionBillingLineItem dimension 
#############################################################################################################################
# Method
# 1.Load Cleansed layer table data into dataframe and transform
# 2.JOIN TABLES
# 3.UNION TABLES
# 4.SELECT / TRANSFORM
# 5.SCHEMA DEFINITION
#############################################################################################################################

# COMMAND ----------

# MAGIC %run ../common/common-curated-includeMain

# COMMAND ----------

# MAGIC %run ../common/functions/commonBilledWaterConsumptionIsu

# COMMAND ----------

def getMeterConsumptionBillingLineItemIsu():

    #1.Load Cleansed layer table data into dataframe
    billedConsIsuDf = getBilledWaterConsumptionIsu()

    dummyDimRecDf = spark.createDataFrame([("Unknown", "-1", "-1", "1900-01-01", "9999-12-31")], ["sourceSystemCode", "billingDocumentNumber", "billingDocumentLineItemId", "validFromDate", "validToDate"])
    dummyDimRecDf = dummyDimRecDf.withColumn("validFromDate",(col("validFromDate").cast("date"))).withColumn("validToDate",(col("validToDate").cast("date"))) 
    
    #2.JOIN TABLES  

    #3.UNION TABLES
    billedConsIsuDf = billedConsIsuDf.unionByName(dummyDimRecDf, allowMissingColumns = True)

    #4.SELECT / TRANSFORM
    df = billedConsIsuDf.selectExpr \
                                ( \
                                   "sourceSystemCode" \
                                    ,"billingDocumentNumber" \
                                    ,"billingDocumentLineItemId" \
                                    ,"lineItemTypeCode" \
                                    ,"lineItemType" \
                                    ,"billingLineItemBudgetBillingIndicator" \
                                    ,"subtransactionForDocumentItem" \
                                    ,"industryCode" \
                                    ,"billingClassCode" \
                                    ,"billingClass" \
                                    ,"rateTypeCode" \
                                    ,"rateType" \
                                    ,"rateId" \
                                    ,"rateDescription" \
                                    ,"statisticalAnalysisRateTypeCode" \
                                    ,"statisticalAnalysisRateType" \
                                    ,"validFromDate" \
                                    ,"validToDate" \
                                    ,"logicalRegisterNumber" \
                                    ,"registerNumber" \
                                    ,"suppressedMeterReadingDocumentId" \
                                    ,"meterReadingReasonCode" \
                                    ,"previousMeterReadingReasonCode" \
                                    ,"meterReadingTypeCode" \
                                    ,"previousMeterReadingTypeCode" \
                                    ,"maxMeterReadingDate" \
                                    ,"maxMeterReadingTime" \
                                    ,"billingMeterReadingTime" \
                                    ,"previousMeterReadingTime" \
                                    ,"meterReadingResultsSimulationIndicator" \
                                    ,"registerRelationshipConsecutiveNumber" \
                                    ,"meterReadingAllocationDate" \
                                    ,"logicalDeviceNumber" \
                                    ,"registerRelationshipSortHelpCode" \
                                    ,"meterReaderNoteText" \
                                    ,"quantityDeterminationProcedureCode" \
                                    ,"meterReadingDocumentId" \
                                ).dropDuplicates()
    #5.Apply schema definition
    schema = StructType([
                            StructField('meterConsumptionBillingLineItemSK', LongType(), False),
                            StructField("sourceSystemCode", StringType(), False),
                            StructField("billingDocumentNumber", StringType(), False),
                            StructField("billingDocumentLineItemId", StringType(), False),
                            StructField("lineItemTypeCode", StringType(), True),
                            StructField("lineItemType", StringType(), True),
                            StructField("billingLineItemBudgetBillingIndicator", StringType(), True),
                            StructField("subtransactionForDocumentItem", StringType(), True),
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
                            StructField("previousMeterReadingReasonCode", StringType(), True),
                            StructField("meterReadingTypeCode", StringType(), True),
                            StructField("previousMeterReadingTypeCode", StringType(), True),
                            StructField("maxMeterReadingDate", DateType(), True),
                            StructField("maxMeterReadingTime", StringType(), True),
                            StructField("billingMeterReadingTime", StringType(), True),
                            StructField("previousMeterReadingTime", StringType(), True),
                            StructField("meterReadingResultsSimulationIndicator", StringType(), True),
                            StructField("registerRelationshipConsecutiveNumber", StringType(), True),
                            StructField("meterReadingAllocationDate", DateType(), True),
                            StructField("logicalDeviceNumber", StringType(), True),
                            StructField("registerRelationshipSortHelpCode", StringType(), True),
                            StructField("meterReaderNoteText", StringType(), True),
                            StructField("quantityDeterminationProcedureCode", StringType(), True),
                            StructField("meterReadingDocumentId", StringType(), True),
                      ])

    return df, schema

# COMMAND ----------

df, schema = getMeterConsumptionBillingLineItemIsu()
TemplateEtl(df, entity="dimMeterConsumptionBillingLineItem", businessKey="sourceSystemCode,billingDocumentNumber,billingDocumentLineItemId", schema=schema, writeMode=ADS_WRITE_MODE_MERGE, AddSK=True)

# COMMAND ----------

dbutils.notebook.exit("1")
