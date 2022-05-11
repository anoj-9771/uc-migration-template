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
                             case when ltrim('0', businessPartnerGroupNumber) is null then 'Unknown' else ltrim('0', businessPartnerGroupNumber) end as businessPartnerGroupNumber, \
                             case when startBillingPeriod is null then to_date('19000101', 'yyyymmdd') else startBillingPeriod end as startBillingPeriod, \
                             case when endBillingPeriod is null then to_date('19000101', 'yyyymmdd') else endBillingPeriod end as endBillingPeriod, \
                             billingDocumentCreateDate, documentNotReleasedIndicator, reversalDate, \
                             portionNumber, \
                             documentTypeCode, \
                             meterReadingUnit, \
                             billingTransactionCode as billingReasonCode, \
                             contractId, \
                             divisionCode, \
                             lastChangedDate, \
                             createdDate, \
                             billingDocumentCreateDate, \
                             erchcExistIndicator, \
                             billingDocumentWithoutInvoicingCode, \
                             newBillingDocumentNumberForReversedInvoicing \
                         from {ADS_DATABASE_CLEANSED}.isu_erch \
                         where trim(billingSimulationIndicator) = ''")

    dberchz1Df = spark.sql(f"select billingDocumentNumber, billingDocumentLineItemId \
                                ,lineItemTypeCode, lineItemType, billingLineItemBudgetBillingIndicator \
                                ,subtransactionForDocumentItem, industryText as industryCode \
                                ,billingClassCode, billingClass, rateTypeCode \
                                ,rateType, rateId, rateDescription \
                                ,statisticalAnalysisRateType as statisticalAnalysisRateTypeCode, statisticalAnalysisRateTypeDescription as statisticalAnalysisRateType \
                                ,validFromDate, validToDate \
                                ,billingQuantityPlaceBeforeDecimalPoint \
                             from {ADS_DATABASE_CLEANSED}.isu_dberchz1 \
                             where lineItemTypeCode in ('ZDQUAN', 'ZRQUAN') ")
  
    dberchz2Df = spark.sql(f"select billingDocumentNumber, billingDocumentLineItemId \
                                ,equipmentNumber \
                                ,logicalRegisterNumber, registerNumber \
                                ,suppressedMeterReadingDocumentId, meterReadingReasonCode \
                                ,previousMeterReadingReasonCode, meterReadingTypeCode \
                                ,previousMeterReadingTypeCode, maxMeterReadingDate \
                                ,maxMeterReadingTime, billingMeterReadingTime \
                                ,previousMeterReadingTime, meterReadingResultsSimulationIndicator \
                                ,registerRelationshipConsecutiveNumber, meterReadingAllocationDate \
                                ,logicalDeviceNumber, registerRelationshipSortHelpCode \
                                ,meterReaderNoteText, quantityDeterminationProcedureCode, meterReadingDocumentId \
                             from {ADS_DATABASE_CLEANSED}.isu_dberchz2 \
                             where trim(suppressedMeterReadingDocumentId) <> ''")
    
    erchcDf = spark.sql(f"select * from (select billingDocumentNumber, postingDate as invoicePostingDate \
                                            , documentNotReleasedIndicator as invoiceNotReleasedIndicator \
                                            , invoiceReversalPostingDate, sequenceNumber as invoiceMaxSequenceNumber \
                                            , row_number() over (partition by billingDocumentNumber order by sequenceNumber desc) rnk \
                                         from {ADS_DATABASE_CLEANSED}.isu_erchc \
                                       ) as erchc where erchc.rnk=1")
  
    #3.JOIN TABLES
    billedConsDf = erchDf.join(erchcDf, erchDf.billingDocumentNumber == erchcDf.billingDocumentNumber, how="left") \
                       .drop(erchcDf.billingDocumentNumber)
    
    billedConsDf = billedConsDf.join(dberchz1Df, erchDf.billingDocumentNumber == dberchz1Df.billingDocumentNumber, how="inner") \
                       .drop(dberchz1Df.billingDocumentNumber)

    billedConsDf = billedConsDf.join(dberchz2Df, (billedConsDf.billingDocumentNumber == dberchz2Df.billingDocumentNumber) \
                                 & (billedConsDf.billingDocumentLineItemId == dberchz2Df.billingDocumentLineItemId), how="inner") \
                    .drop(dberchz2Df.billingDocumentNumber) \
                    .drop(dberchz2Df.billingDocumentLineItemId)

#     billedConsDf = billedConsDf.select("sourceSystemCode", "billingDocumentNumber", "businessPartnerGroupNumber", "equipmentNumber", \
#                     "startBillingPeriod", "endBillingPeriod", "validFromDate", "validToDate", \
#                     "billingDocumentCreateDate", "documentNotReleasedIndicator", "reversalDate", \
#                     "portionNumber","documentTypeCode","meterReadingUnit","billingReasonCode", "contractId", \
#                     "billingQuantityPlaceBeforeDecimalPoint") \
#                   .groupby("sourceSystemCode", "billingDocumentNumber", "businessPartnerGroupNumber", "equipmentNumber", \
#                              "startBillingPeriod", "endBillingPeriod", "billingDocumentCreateDate", \
#                              "documentNotReleasedIndicator", "reversalDate", \
#                     "portionNumber","documentTypeCode","meterReadingUnit","billingReasonCode", "contractId") \
#                   .agg(min("validFromDate").alias("meterActiveStartDate") \
#                       ,max("validToDate").alias("meterActiveEndDate") \
#                       ,sum("billingQuantityPlaceBeforeDecimalPoint").alias("meteredWaterConsumption"))

  #4.UNION TABLES
  
    #5.SELECT / TRANSFORM
    billedConsDf = billedConsDf.selectExpr \
                  ( \
                     "sourceSystemCode" \
                    ,"billingDocumentNumber" \
                    ,"billingDocumentLineItemId" \
                    ,"businessPartnerGroupNumber" \
                    ,"equipmentNumber" \
                    ,"startBillingPeriod as billingPeriodStartDate" \
                    ,"endBillingPeriod as billingPeriodEndDate" \
                    ,"billingDocumentCreateDate as billCreatedDate" \
                    ,"reversalDate" \
                    ,"portionNumber" \
                    ,"documentTypeCode" \
                    ,"meterReadingUnit" \
                    ,"billingReasonCode" \
                    ,"contractId" \
                    ,"divisionCode" \
                    ,"lastChangedDate" \
                    ,"createdDate" \
                    ,"billingDocumentCreateDate" \
                    ,"erchcExistIndicator" \
                    ,"billingDocumentWithoutInvoicingCode" \
                    ,"newBillingDocumentNumberForReversedInvoicing" \
                    ,"invoicePostingDate" \
                    ,"invoiceNotReleasedIndicator" \
                    ,"invoiceReversalPostingDate" \
                    ,"invoiceMaxSequenceNumber" \
                    ,"case when (reversalDate is null  or reversalDate = '1900-01-01' or reversalDate = '9999-12-31') then 'N' else 'Y' end as isReversedFlag" \
                    ,"case when documentNotReleasedIndicator == 'X' then 'Y' else 'N' end as isOutsortedFlag" \
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
                    ,"cast(billingQuantityPlaceBeforeDecimalPoint as decimal(18,6)) as meteredWaterConsumption" \
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
                  )
  
    return billedConsDf

# COMMAND ----------


