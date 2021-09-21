# Databricks notebook source
#%run ../../includes/util-common

# COMMAND ----------

#%run ../commonBilledWaterConsumptionSapisu

# COMMAND ----------

# Run the above commands only when running this notebook independently, otherwise the curated master notebook would take care of calling the above notebooks

# COMMAND ----------

###########################################################################################################################
# Function: getCommonBillingDocumentSapisu
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
def getCommonBillingDocumentSapisu():
  
  spark.udf.register("TidyCase", GeneralToTidyCase)  
  
  #DimBillingDocument
  #2.Load Cleansed layer table data into dataframe

  billedConsSapisuDf = getBilledWaterConsumptionSapisu()

  dummyDimRecDf = spark.createDataFrame([("SAPISU", "-1", "1900-01-01", "9999-12-31"), ("Access", "-2", "1900-01-01", "9999-12-31")], ["sourceSystemCode", "billingDocumentNumber", "billingPeriodStartDate", "billingPeriodEndDate"])
  
  #3.JOIN TABLES  
  
  #4.UNION TABLES
  billedConsSapisuDf = billedConsSapisuDf.unionByName(dummyDimRecDf, allowMissingColumns = True)
  
  #5.SELECT / TRANSFORM
  billDocDf = billedConsSapisuDf.selectExpr \
                                ( \
                                   "sourceSystemCode" \
                                  ,"billingDocumentNumber" \
                                  ,"billingPeriodStartDate" \
                                  ,"billingPeriodEndDate" \
                                  ,"billCreatedDate" \
                                  ,"isOutsortedFlag" \
                                  ,"isReversedFlag" \
                                  ,"reversalDate" \
                                )
  #6.Apply schema definition
  newSchema = StructType([
                            StructField("sourceSystemCode", StringType(), False),
                            StructField("billingDocumentNumber", StringType(), False),
                            StructField("billingPeriodStartDate", DateType(), False),
                            StructField("billingPeriodEndDate", DateType(), True),
                            StructField("billCreatedDate", StringType(), True),
                            StructField("isOutsortedFlag", StringType(), True),
                            StructField("isReversedFlag", DecimalType(18,6), True),
                            StructField("reversalDate", DateType(), True)
                      ])
  
  billDocDf = spark.createDataFrame(billDocDf.rdd, schema=newSchema)
  
  return billDocDf

# COMMAND ----------


