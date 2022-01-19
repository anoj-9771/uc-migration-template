# Databricks notebook source
#%run ../includes/util-common

# COMMAND ----------


###########################################################################################################################
# Function: getBilledWaterConsumptionisu
#  GETS ISU Billed Water Consumption from cleansed layer
# Returns:
#  Dataframe of transformed ISU Billed Water Consumption
#############################################################################################################################
# Method
# 1.Create Function
# 2.Load Cleansed layer table data into dataframe and transform
# 3.JOIN TABLES
# 4.UNION TABLES
# 5.SELECT / TRANSFORM
#############################################################################################################################
#1.Create Function
def getBilledWaterConsumptionIsu():
  
  spark.udf.register("TidyCase", GeneralToTidyCase)  
  
  #2.Load Cleansed layer table data into dataframe
  erchDf = spark.sql(f"select 'ISU' as sourceSystemCode, billingDocumentNumber, \
                             case when ltrim('0', businessPartnerNumber) is null then 'Unknown' else ltrim('0', businessPartnerNumber) end as businessPartnerNumber, \
                             case when startBillingPeriod is null then to_date('19000101', 'yyyymmdd') else startBillingPeriod end as startBillingPeriod, \
                             case when endBillingPeriod is null then to_date('19000101', 'yyyymmdd') else endBillingPeriod end as endBillingPeriod, \
                             billingDocumentCreateDate, documentNotReleasedIndicator, reversalDate, \
                             portionNumber, \
                             documentTypeCode, \
                             meterReadingUnit, \
                             billingTransactionCode \
                             contractID \
                         from {ADS_DATABASE_CLEANSED}.isu_erch \
                         where trim(billingSimulationIndicator) = ''")

  dberchz1Df = spark.sql(f"select billingDocumentNumber, billingDocumentLineItemId \
                                ,validFromDate, validToDate \
                                ,billingQuantityPlaceBeforeDecimalPoint \
                             from {ADS_DATABASE_CLEANSED}.isu_dberchz1 \
                             where lineItemTypeCode in ('ZDQUAN', 'ZRQUAN') \
                             and trim(billingLineItemBudgetBillingIndicator) = ''")
  
  dberchz2Df = spark.sql(f"select billingDocumentNumber, billingDocumentLineItemId \
                                ,equipmentNumber \
                             from {ADS_DATABASE_CLEANSED}.isu_dberchz2 \
                             where trim(suppressedMeterReadingDocumentId) <> ''")
  
  #3.JOIN TABLES  
  billedConsDf = erchDf.join(dberchz1Df, erchDf.billingDocumentNumber == dberchz1Df.billingDocumentNumber, how="inner") \
                   .drop(dberchz1Df.billingDocumentNumber)

  billedConsDf = billedConsDf.join(dberchz2Df, (billedConsDf.billingDocumentNumber == dberchz2Df.billingDocumentNumber) \
                                 & (billedConsDf.billingDocumentLineItemId == dberchz2Df.billingDocumentLineItemId), how="inner") \
                    .drop(dberchz2Df.billingDocumentNumber) \
                    .drop(dberchz2Df.billingDocumentLineItemId)

  billedConsDf = billedConsDf.select("sourceSystemCode", "billingDocumentNumber", "businessPartnerNumber", "equipmentNumber", \
                    "startBillingPeriod", "endBillingPeriod", "validFromDate", "validToDate", \
                    "billingDocumentCreateDate", "documentNotReleasedIndicator", "reversalDate", \
                    "portionNumber","documentTypeCode","meterReadingUnit","billingTransactionCode", "contractID", \
                    "billingQuantityPlaceBeforeDecimalPoint") \
                  .groupby("sourceSystemCode", "billingDocumentNumber", "businessPartnerNumber", "equipmentNumber", \
                             "startBillingPeriod", "endBillingPeriod", "billingDocumentCreateDate", \
                             "documentNotReleasedIndicator", "reversalDate", \
                    "portionNumber","documentTypeCode","meterReadingUnit","billingTransactionCode", "contractID") \
                  .agg(min("validFromDate").alias("meterActiveStartDate") \
                      ,max("validToDate").alias("meterActiveEndDate") \
                      ,sum("billingQuantityPlaceBeforeDecimalPoint").alias("meteredWaterConsumption"))
  
  #4.UNION TABLES
  
  #5.SELECT / TRANSFORM
  billedConsDf = billedConsDf.selectExpr \
                  ( \
                     "sourceSystemCode" \
                    ,"billingDocumentNumber" \
                    ,"businessPartnerNumber" \
                    ,"equipmentNumber" \
                    ,"startBillingPeriod as billingPeriodStartDate" \
                    ,"endBillingPeriod as billingPeriodEndDate" \
                    ,"billingDocumentCreateDate as billCreatedDate" \
                    ,"reversalDate" \
                    ,"portionNumber" \
                    ,"documentTypeCode" \
                    ,"meterReadingUnit" \
                    ,"billingTransactionCode" \
                    ,"contractID" \
                    ,"case when reversalDate is null then 'N' else 'Y' end as isReversedFlag" \
                    ,"case when documentNotReleasedIndicator == 'X' then 'Y' else 'N' end as isOutsortedFlag" \
                    ,"meterActiveStartDate" \
                    ,"meterActiveEndDate" \
                    ,"cast(meteredWaterConsumption as decimal(18,6)) as meteredWaterConsumption" \
                  )
  
  return billedConsDf
