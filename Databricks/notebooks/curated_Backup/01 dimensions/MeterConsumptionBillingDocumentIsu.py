# Databricks notebook source
###########################################################################################################################
# Loads MeterConsumptionBillingDocument dimension 
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

def getMeterConsumptionBillingDocumentIsu():

    #1.Load Cleansed layer table data into dataframe
    billedConsIsuDf = getBilledWaterConsumptionIsu()

    dummyDimRecDf = spark.createDataFrame([("-1", "1900-01-01", "9999-12-31")], ["billingDocumentNumber", "billingPeriodStartDate", "billingPeriodEndDate"])
    dummyDimRecDf = dummyDimRecDf.withColumn("billingPeriodStartDate",(col("billingPeriodStartDate").cast("date"))).withColumn("billingPeriodEndDate",(col("billingPeriodEndDate").cast("date")))  
    
    #2.JOIN TABLES  

    #3.UNION TABLES
    billedConsIsuDf = billedConsIsuDf.unionByName(dummyDimRecDf, allowMissingColumns = True)

    #4.SELECT / TRANSFORM
    df = billedConsIsuDf.selectExpr \
                                ( \
                                   "sourceSystemCode" \
                                  ,"billingDocumentNumber" \
                                  ,"billingPeriodStartDate" \
                                  ,"billingPeriodEndDate" \
                                  ,"billCreatedDate" \
                                  ,"isOutsortedFlag" \
                                  ,"isReversedFlag" \
                                  ,"reversalDate" \
                                  ,"portionNumber" \
                                  ,"documentTypeCode" \
                                  ,"meterReadingUnit" \
                                  ,"billingReasonCode" \
                                  ,"divisionCode" \
                                  ,"lastChangedDate" \
                                  ,"createdDate" \
                                  ,"billingDocumentCreateDate" \
                                  ,"erchcExistFlag" \
                                  ,"billingDocumentWithoutInvoicingCode" \
                                  ,"newBillingDocumentNumberForReversedInvoicing" \
                                  ,"invoicePostingDate" \
                                  ,"invoiceNotReleasedIndicator" \
                                  ,"invoiceReversalPostingDate" \
                                  ,"invoiceMaxSequenceNumber" \
                                ).dropDuplicates()
    #5.Apply schema definition
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
                            StructField("documentTypeCode", StringType(), True),
                            StructField("meterReadingUnit", StringType(), True),
                            StructField("billingReasonCode", StringType(), True),
                            StructField("divisionCode", StringType(), True),
                            StructField("lastChangedDate", DateType(), True),
                            StructField("createdDate", DateType(), True),
                            StructField("billingDocumentCreateDate", DateType(), True),
                            StructField("erchcExistFlag", StringType(), True),
                            StructField("billingDocumentWithoutInvoicingCode", StringType(), True),
                            StructField("newBillingDocumentNumberForReversedInvoicing", IntegerType(), True),
                            StructField("invoicePostingDate", DateType(), True),
                            StructField("invoiceNotReleasedIndicator", StringType(), True),
                            StructField("invoiceReversalPostingDate", DateType(), True),
                            StructField("invoiceMaxSequenceNumber", StringType(), True)  
                      ])

    return df, schema

# COMMAND ----------

df, schema = getMeterConsumptionBillingDocumentIsu()
TemplateEtl(df, entity="dimMeterConsumptionBillingDocument", businessKey="billingDocumentNumber", schema=schema, writeMode=ADS_WRITE_MODE_OVERWRITE, AddSK=True)

# COMMAND ----------

dbutils.notebook.exit("1")
