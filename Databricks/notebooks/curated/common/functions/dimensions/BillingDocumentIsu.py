# Databricks notebook source
#%run ../../includes/util-common

# COMMAND ----------

#%run ../commonBilledWaterConsumptionIsu

# COMMAND ----------

# Run the above commands only when running this notebook independently, otherwise the curated master notebook would take care of calling the above notebooks

# COMMAND ----------

###########################################################################################################################
# Function: getBillingDocumentIsu
#  GETS BillingDocument DIMENSION 
# Returns:
#  Dataframe of transformed BillingDocument
#############################################################################################################################
# Method
# 1.Create Function
# 2.Load Cleansed layer table data into dataframe and transform
# 3.JOIN TABLES
# 4.UNION TABLES
# 5.SELECT / TRANSFORM
#############################################################################################################################
#1.Create Function
def getBillingDocumentIsu():
  
  spark.udf.register("TidyCase", GeneralToTidyCase)  
  
  #DimBillingDocument
  #2.Load Cleansed layer table data into dataframe

  billedConsIsuDf = getBilledWaterConsumptionIsu()

  dummyDimRecDf = spark.createDataFrame([("ISU", "-1", "1900-01-01", "9999-12-31"), ("ACCESS", "-2", "1900-01-01", "9999-12-31"),("ISU", "-3", "1900-01-01", "9999-12-31"),("ACCESS", "-4", "1900-01-01", "9999-12-31")], ["sourceSystemCode", "billingDocumentNumber", "billingPeriodStartDate", "billingPeriodEndDate"])
  dummyDimRecDf = dummyDimRecDf.withColumn("billingPeriodStartDate",(col("billingPeriodStartDate").cast("date"))).withColumn("billingPeriodEndDate",(col("billingPeriodEndDate").cast("date")))  
    
  #3.JOIN TABLES  
  
  #4.UNION TABLES
  billedConsIsuDf = billedConsIsuDf.unionByName(dummyDimRecDf, allowMissingColumns = True)
  
  #5.SELECT / TRANSFORM
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
  #6.Apply schema definition
  schema = StructType([
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
