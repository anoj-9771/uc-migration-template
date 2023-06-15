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
    erchDf = spark.sql(f"""select 'ISU' as sourceSystemCode, erch.billingDocumentNumber,
                             case when businessPartnerGroupNumber is null then 'Unknown' else erch.businessPartnerGroupNumber end as businessPartnerGroupNumber,
                             case when erch.startBillingPeriod is null then to_date('19000101', 'yyyymmdd') else erch.startBillingPeriod end as billingPeriodStartDate,
                             case when erch.endBillingPeriod is null then to_date('19000101', 'yyyymmdd') else erch.endBillingPeriod end as billingPeriodEndDate,
                             erch.billingDocumentCreateDate as billCreatedDate, erch.documentNotReleasedFlag as isOutsortedFlag,
                             erch.reversalDate,
                             case when (erch.reversalDate is null  or erch.reversalDate = '1900-01-01' or erch.reversalDate = '9999-12-31') then 'N' else 'Y' end as isReversedFlag,
                             erch.portionNumber,
                             erch.documentTypeCode,
                             erch.documentType,
                             erch.meterReadingUnit,
                             erch.billingTransactionCode as billingReasonCode,
                             erch.contractId,
                             conth.installationNumber,
                             erch.divisionCode,
                             erch.division,
                             erch.lastChangedDate,
                             erch.createdDate,
                             erch.erchcExistFlag,
                             erch.billingDocumentWithoutInvoicingCode,
                             erch.newBillingDocumentNumberForReversedInvoicing
                         from {ADS_DATABASE_CLEANSED}.isu.erch erch
                         left outer join {ADS_DATABASE_CLEANSED}.isu.0uccontracth_attr_2 conth on conth.contractid = erch.contractid
                                        and erch.endbillingperiod between conth.validFromDate and conth.validToDate and conth._recordDeleted = 0
                         where trim(billingSimulationIndicator) = '' """)

    dberchz1Df = spark.sql(f"""select billingDocumentNumber, billingDocumentLineItemId
                                ,lineItemTypeCode, lineItemType, billingLineItemBudgetBillingFlag
                                ,subTransactionCode, industryCode as inferiorPropertyTypeCode , infprty.inferiorPropertyType
                                ,billingClassCode, billingClass, rateTypeCode
                                ,rateType, rateId, rateDescription
                                ,statisticalAnalysisRateType as statisticalAnalysisRateTypeCode, statisticalAnalysisRateTypeDescription as statisticalAnalysisRateType
                                ,validFromDate as meterActiveStartDate, validToDate as meterActiveEndDate
                                ,billingQuantityPlaceBeforeDecimalPoint
                             from {ADS_DATABASE_CLEANSED}.isu.dberchz1 isu_dberchz1 left outer join cleansed.isu.zcd_tinfprty_tx infprty on infprty.inferiorPropertyTypeCode = isu_dberchz1.industryCode
                             where lineItemTypeCode in ('ZDQUAN', 'ZRQUAN') """)
  
    dberchz2Df = spark.sql(f"""select billingDocumentNumber, billingDocumentLineItemId
                                ,equipmentNumber as deviceNumber, logicalDeviceNumber
                                ,logicalRegisterNumber, cast(registerNumber as int) registerNumber
                                ,suppressedMeterReadingDocumentId, meterReadingReasonCode
                                ,previousMeterReadingReasonCode, meterReadingTypeCode
                                ,previousMeterReadingTypeCode, maxMeterReadingDate
                                ,maxMeterReadingTime, billingMeterReadingTime
                                ,previousMeterReadingTime, meterReadingResultsSimulationIndicator
                                ,registerRelationshipConsecutiveNumber, meterReadingAllocationDate
                                ,registerRelationshipSortHelpCode
                                ,meterReaderNoteText, quantityDeterminationProcedureCode, meterReadingDocumentId
                             from {ADS_DATABASE_CLEANSED}.isu.dberchz2
                             where trim(suppressedMeterReadingDocumentId) <> '' """)
    
    erchcDf = spark.sql(f"""select * from (select billingDocumentNumber, postingDate as invoicePostingDate
                                            , documentNotReleasedFlag as invoiceNotReleasedIndicator
                                            , invoiceReversalPostingDate, sequenceNumber as invoiceMaxSequenceNumber
                                            , row_number() over (partition by billingDocumentNumber order by sequenceNumber desc) rnk
                                         from {ADS_DATABASE_CLEANSED}.isu.erchc
                                       ) as erchc where erchc.rnk=1""")
  
    #3.JOIN TABLES
    billedConsDf = erchDf.join(erchcDf, erchDf.billingDocumentNumber == erchcDf.billingDocumentNumber, how="left") \
                       .drop(erchcDf.billingDocumentNumber)
    
    billedConsDf = billedConsDf.join(dberchz1Df, erchDf.billingDocumentNumber == dberchz1Df.billingDocumentNumber, how="inner") \
                       .drop(dberchz1Df.billingDocumentNumber)

    billedConsDf = billedConsDf.join(dberchz2Df, (billedConsDf.billingDocumentNumber == dberchz2Df.billingDocumentNumber) \
                                 & (billedConsDf.billingDocumentLineItemId == dberchz2Df.billingDocumentLineItemId), how="inner") \
                    .drop(dberchz2Df.billingDocumentNumber) \
                    .drop(dberchz2Df.billingDocumentLineItemId)

  #4.UNION TABLES
  
    #5.SELECT / TRANSFORM
    billedConsDf = billedConsDf.selectExpr \
                  ( \
                     "sourceSystemCode" \
                    ,"billingDocumentNumber" \
                    ,"billingDocumentLineItemId" \
                    ,"businessPartnerGroupNumber" \
                    ,"contractId" \
                    ,"installationNumber" \
                    ,"deviceNumber" \
                    ,"logicalDeviceNumber" \
                    ,"logicalRegisterNumber" \
                    ,"registerNumber" \
                    ,"divisionCode" \
                    ,"division" \
                    ,"documentTypeCode" \
                    ,"documentType" \
                    ,"lineItemTypeCode" \
                    ,"lineItemType" \
                    ,"isReversedFlag" \
                    ,"isOutsortedFlag" \
                    ,"billingLineItemBudgetBillingFlag" \
                    ,"portionNumber" \
                    ,"meterReadingUnit" \
                    ,"billingReasonCode" \
                    ,"erchcExistFlag" \
                    ,"billingDocumentWithoutInvoicingCode" \
                    ,"newBillingDocumentNumberForReversedInvoicing" \
                    ,"invoiceNotReleasedIndicator" \
                    ,"invoiceReversalPostingDate" \
                    ,"invoiceMaxSequenceNumber" \
                    ,"subTransactionCode" \
                    ,"inferiorPropertyTypeCode" \
                    ,"inferiorPropertyType" \
                    ,"billingClassCode" \
                    ,"billingClass" \
                    ,"rateTypeCode" \
                    ,"rateType" \
                    ,"rateId" \
                    ,"rateDescription" \
                    ,"statisticalAnalysisRateTypeCode" \
                    ,"statisticalAnalysisRateType" \
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
                    ,"registerRelationshipSortHelpCode" \
                    ,"meterReaderNoteText" \
                    ,"quantityDeterminationProcedureCode" \
                    ,"meterReadingDocumentId" \
                    ,"billingPeriodStartDate" \
                    ,"billingPeriodEndDate" \
                    ,"invoicePostingDate" \
                    ,"reversalDate" \
                    ,"billCreatedDate" \
                    ,"createdDate" \
                    ,"lastChangedDate" \
                    ,"meterActiveStartDate" \
                    ,"meterActiveEndDate" \
                    ,"billingQuantityPlaceBeforeDecimalPoint as meteredWaterConsumption" \
                  )
    return billedConsDf
