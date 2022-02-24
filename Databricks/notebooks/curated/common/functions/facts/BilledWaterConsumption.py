# Databricks notebook source
#%run ../../includes/util-common

# COMMAND ----------

#%run ../commonBilledWaterConsumptionIsu

# COMMAND ----------

#%run ../commonBilledWaterConsumptionAccess

# COMMAND ----------

# Run the above commands only when running this notebook independently, otherwise the curated master notebook would take care of calling the above notebooks

# COMMAND ----------

###########################################################################################################################
# Function: getBilledWaterConsumption
#  Merges both ISU and ACCESS BilledWaterConsumption FACT 
# Returns:
#  Dataframe of transformed BilledWaterConsumption
#############################################################################################################################
# Method
# 1.Create Function
# 2.Load Cleansed layer table data into dataframe and transform
# 3.JOIN TABLES
# 4.UNION TABLES
# 5.SELECT / TRANSFORM
#############################################################################################################################
#1.Create Function
def getBilledWaterConsumption():
    spark.udf.register("TidyCase", GeneralToTidyCase)  

    #FactBilledWaterConsumption

    #2.Load Cleansed layer tables into dataframe
    isuConsDf = getBilledWaterConsumptionIsu()
    accessConsDf = getBilledWaterConsumptionAccess()

    legacyConsDS = accessConsDf.select('propertyNumber', 'billingPeriodStartDate', 'billingPeriodEndDate') \
                           .subtract(isuConsDf.select('businessPartnerNumber', 'billingPeriodStartDate', 'billingPeriodEndDate'))

    accessConsDf = accessConsDf.join(legacyConsDS, (legacyConsDS.propertyNumber == accessConsDf.propertyNumber) \
                                           & ((legacyConsDS.billingPeriodStartDate == accessConsDf.billingPeriodStartDate) \
                                           & (legacyConsDS.billingPeriodEndDate == accessConsDf.billingPeriodEndDate)), how="inner" ) \
                           .select(accessConsDf['*'])

    #3.Union tables
    isuConsDf = isuConsDf.select("sourceSystemCode", "billingDocumentNumber", \
                                "businessPartnerNumber", "equipmentNumber", "contractId", \
                                "billingPeriodStartDate", "billingPeriodEndDate", \
                                "meteredWaterConsumption") \
                                .where((isuConsDf.isReversedFlag == 'N') & (isuConsDf.isOutsortedFlag == 'N'))

    accessConsDf = accessConsDf.selectExpr("sourceSystemCode", "-1 as billingDocumentNumber", \
                                "PropertyNumber", "propertyMeterNumber", "-1 as contractId", \
                                "billingPeriodStartDate", "billingPeriodEndDate", \
                                "meteredWaterConsumption") \

    billedConsDf = isuConsDf.union(accessConsDf)
    billedConsDf.display()

    #4.Load dimension tables into dataframe

    dimPropertyDf = spark.sql(f"select sourceSystemCode, dimPropertySK, propertyNumber \
                                from {ADS_DATABASE_CURATED}.dimProperty \
                                where _RecordCurrent = 1 and _RecordDeleted = 0")

    dimLocationDf = spark.sql(f"select dimLocationSK, locationId \
                                 from {ADS_DATABASE_CURATED}.dimLocation \
                                 where _RecordCurrent = 1 and _RecordDeleted = 0")

    dimMeterDf = spark.sql(f"select sourceSystemCode, dimMeterSK, meterNumber \
                                 from {ADS_DATABASE_CURATED}.dimMeter \
                                 where _RecordCurrent = 1 and _RecordDeleted = 0")
    dimBillDocDf = spark.sql(f"select dimBillingDocumentSK, sourceSystemCode, billingDocumentNumber \
                                from {ADS_DATABASE_CURATED}.dimBillingDocument \
                                where _RecordCurrent = 1 and _RecordDeleted = 0")

    dimBusinessPartnerGroupDf = spark.sql(f"select dimBusinessPartnerGroupSK, ltrim('0', businessPartnerGroupNumber) as businessPartnerGroupNumber \
                                from {ADS_DATABASE_CURATED}.dimBusinessPartnerGroup \
                                where _RecordCurrent = 1 and _RecordDeleted = 0")

    dimContractDf = spark.sql(f"select dimContractSK, contractId, validFromDate, validToDate \
                                from {ADS_DATABASE_CURATED}.dimContract \
                                where _RecordCurrent = 1 and _RecordDeleted = 0")


    dummyDimRecDf = spark.sql(f"select dimPropertySk as dummyDimSK, sourceSystemCode, 'dimProperty' as dimension from {ADS_DATABASE_CURATED}.dimProperty \
                                                                                                                                where propertyNumber in ('-1', '-2') \
                          union select dimLocationSk as dummyDimSK, 'null' as sourceSystemCode, 'dimLocation' as dimension from {ADS_DATABASE_CURATED}.dimLocation \
                                                                                                                                where LocationId = '-1' \
                          union select dimMeterSK as dummyDimSK, sourceSystemCode, 'dimMeter' as dimension from {ADS_DATABASE_CURATED}.dimMeter where meterNumber in ('-1','-2')\
                          union select dimBillingDocumentSK as dummyDimSK, sourceSystemCode, 'dimBillingDocument' as dimension from {ADS_DATABASE_CURATED}.dimBillingDocument \
                                                                                                                                where billingDocumentNumber = '-1' \
                          union select dimBusinessPartnerGroupSK as dummyDimSK, 'ISU' as sourceSystemCode, 'dimBusinessPartnerGroup' as dimension from \
                                                                                                                              {ADS_DATABASE_CURATED}.dimBusinessPartnerGroup \
                                                                                                                                  where BusinessPartnerGroupNumber ='-1' \
                          union select dimContractSK as dummyDimSK, 'ISU' as sourceSystemCode, 'dimContract' as dimension from {ADS_DATABASE_CURATED}.dimContract where contractId = '-1' \
                          ")


    #5.Joins to derive SKs for Fact load

    billedConsDf = billedConsDf.join(dimPropertyDf, (billedConsDf.businessPartnerNumber == dimPropertyDf.propertyNumber) \
                             & (billedConsDf.sourceSystemCode == dimPropertyDf.sourceSystemCode), how="left") \
                  .select(billedConsDf['*'], dimPropertyDf['dimPropertySK'])

    billedConsDf = billedConsDf.join(dimLocationDf, (billedConsDf.businessPartnerNumber == dimLocationDf.locationId), how="left") \
                  .select(billedConsDf['*'], dimLocationDf['dimLocationSK'])

    billedConsDf = billedConsDf.join(dimMeterDf, (billedConsDf.equipmentNumber == dimMeterDf.meterNumber) \
                             & (billedConsDf.sourceSystemCode == dimMeterDf.sourceSystemCode), how="left") \
                  .select(billedConsDf['*'], dimMeterDf['dimMeterSK'])

    billedConsDf = billedConsDf.join(dimBillDocDf, (billedConsDf.billingDocumentNumber == dimBillDocDf.billingDocumentNumber), how="left") \
                  .select(billedConsDf['*'], dimBillDocDf['dimBillingDocumentSK'])

    billedConsDf = billedConsDf.join(dimContractDf, (billedConsDf.contractId == dimContractDf.contractId) \
                             & (billedConsDf.billingPeriodEndDate >= dimContractDf.validFromDate) \
                             & (billedConsDf.billingPeriodEndDate <= dimContractDf.validToDate) , how="left") \
                  .select(billedConsDf['*'], dimContractDf['dimContractSK'])

    billedConsDf = billedConsDf.join(dimBusinessPartnerGroupDf, (billedConsDf.businessPartnerNumber == dimBusinessPartnerGroupDf.businessPartnerGroupNumber), how="left") \
                  .select(billedConsDf['*'], dimBusinessPartnerGroupDf['dimBusinessPartnerGroupSK'])

    #6.Joins to derive SKs of dummy dimension(-1) records, to be used when the lookup fails for dimensionSk

    billedConsDf = billedConsDf.join(dummyDimRecDf, (dummyDimRecDf.dimension == 'dimProperty') \
                             & (billedConsDf.sourceSystemCode == dummyDimRecDf.sourceSystemCode), how="left") \
                  .select(billedConsDf['*'], dummyDimRecDf['dummyDimSK'].alias('dummyPropertySK'))

    billedConsDf = billedConsDf.join(dummyDimRecDf, (dummyDimRecDf.dimension == 'dimLocation'), how="left") \
                  .select(billedConsDf['*'], dummyDimRecDf['dummyDimSK'].alias('dummyLocationSK'))

    billedConsDf = billedConsDf.join(dummyDimRecDf, (dummyDimRecDf.dimension == 'dimMeter') \
                             & (billedConsDf.sourceSystemCode == dummyDimRecDf.sourceSystemCode), how="left") \
                  .select(billedConsDf['*'], dummyDimRecDf['dummyDimSK'].alias('dummyMeterSK'))

    billedConsDf = billedConsDf.join(dummyDimRecDf, (dummyDimRecDf.dimension == 'dimBillingDocument'), how="left") \
                      .select(billedConsDf['*'], dummyDimRecDf['dummyDimSK'].alias('dummyBillingDocumentSK'))

    billedConsDf = billedConsDf.join(dummyDimRecDf, (dummyDimRecDf.dimension == 'dimContract'), how="left") \
                  .select(billedConsDf['*'], dummyDimRecDf['dummyDimSK'].alias('dummyContractSK'))

    billedConsDf = billedConsDf.join(dummyDimRecDf, (dummyDimRecDf.dimension == 'dimBusinessPartnerGroup'), how="left") \
                  .select(billedConsDf['*'], dummyDimRecDf['dummyDimSK'].alias('dummyBusinessPartnerGroupSK'))

    #7.SELECT / TRANSFORM
    #aggregating to address any duplicates due to failed SK lookups and dummy SKs being assigned in those cases
    billedConsDf = billedConsDf.selectExpr ( \
                                         "sourceSystemCode" \
                                        ,"coalesce(dimBillingDocumentSK, dummyBillingDocumentSK) as dimBillingDocumentSK" \
                                        ,"coalesce(dimPropertySK, dummyPropertySK) as dimPropertySK" \
                                        ,"coalesce(dimMeterSK, dummyMeterSK) as dimMeterSK" \
                                        ,"coalesce(dimLocationSk, dummyLocationSK) as dimLocationSK" \
                                        ,"-1 as dimWaterNetworkSK" \
                                        ,"coalesce(dimBusinessPartnerGroupSK, dummyBusinessPartnerGroupSK) as dimBusinessPartnerGroupSK" \
                                        ,"coalesce(dimContractSK, dummyContractSK) as dimContractSK" \
                                        ,"billingPeriodStartDate" \
                                        ,"billingPeriodEndDate" \
                                        ,"meteredWaterConsumption" \
                                       ) \
                          .groupby("sourceSystemCode", "dimBillingDocumentSK", "dimPropertySK", "dimMeterSK", \
                                   "dimLocationSK", "dimWaterNetworkSK", "billingPeriodStartDate", "dimBusinessPartnerGroupSK", "dimContractSK") \
                          .agg(max("billingPeriodEndDate").alias("billingPeriodEndDate") \
                              ,sum("meteredWaterConsumption").alias("meteredWaterConsumption"))

    return billedConsDf

# COMMAND ----------


