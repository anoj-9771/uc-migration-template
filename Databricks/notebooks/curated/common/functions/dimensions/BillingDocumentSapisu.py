# Databricks notebook source
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
  
  #3.JOIN TABLES  
  
  #4.UNION TABLES
  
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

  return billDocDf
