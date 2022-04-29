# Databricks notebook source
###########################################################################################################################
# Loads BILLINGDOCUMENT dimension 
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

def getBillingDocumentIsu():

    #1.Load Cleansed layer table data into dataframe
    billedConsIsuDf = getBilledWaterConsumptionIsu()

    dummyDimRecDf = spark.createDataFrame([("ISU", "-1", "1900-01-01", "9999-12-31"), ("ACCESS", "-2", "1900-01-01", "9999-12-31"),("ISU", "-3", "1900-01-01", "9999-12-31"),("ACCESS", "-4", "1900-01-01", "9999-12-31")], ["sourceSystemCode", "billingDocumentNumber", "billingPeriodStartDate", "billingPeriodEndDate"])
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
                                  ,"billingTransactionCode"
                                
                                ).dropDuplicates()
    #5.Apply schema definition
    schema = StructType([
                            StructField('dimBillingDocumentSK', LongType(), False),
                            StructField("sourceSystemCode", StringType(), False),
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
                            StructField("billingTransactionCode", StringType(), True)
                      ])

    return df, schema

# COMMAND ----------

df, schema = getBillingDocumentIsu()
TemplateEtl(df, entity="dimBillingDocument", businessKey="sourceSystemCode,billingDocumentNumber", schema=schema, AddSK=True)

# COMMAND ----------

dbutils.notebook.exit("1")
