# Databricks notebook source
###########################################################################################################################
# Loads BILLEDWATERCONSUMPTION fact 
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

# MAGIC %run ../common/functions/commonBilledWaterConsumptionAccess

# COMMAND ----------

#-----------------------------------------------------------------------------------------------
# Note: BILLEDWATERCONSUMPTION fact requires the above two functions
#-----------------------------------------------------------------------------------------------

# COMMAND ----------

def getBilledWaterConsumption():

    #1.Load Cleansed layer tables into dataframe
    isuConsDf = getBilledWaterConsumptionIsu()
    accessConsDf = getBilledWaterConsumptionAccess()

    legacyConsDS = accessConsDf.select('propertyNumber', 'billingPeriodStartDate', 'billingPeriodEndDate') \
                           .subtract(isuConsDf.select('businessPartnerGroupNumber', 'billingPeriodStartDate', 'billingPeriodEndDate'))

    accessConsDf = accessConsDf.join(legacyConsDS, (legacyConsDS.propertyNumber == accessConsDf.propertyNumber) \
                                           & ((legacyConsDS.billingPeriodStartDate == accessConsDf.billingPeriodStartDate) \
                                           & (legacyConsDS.billingPeriodEndDate == accessConsDf.billingPeriodEndDate)), how="inner" ) \
                           .select(accessConsDf['*'])

    #2.Join tables
    #3.Union tables
    isuConsDf = isuConsDf.select("sourceSystemCode", "billingDocumentNumber", "billingDocumentLineItemId", \
                                "businessPartnerGroupNumber", "equipmentNumber", "contractId", \
                                "billingPeriodStartDate", "billingPeriodEndDate", \
                                "meteredWaterConsumption") \

    accessConsDf = accessConsDf.selectExpr("sourceSystemCode", "-4 as billingDocumentNumber", "-4 as billingDocumentLineItemId", \
                                "PropertyNumber", "meterNumber", "-4 as contractId", \
                                "billingPeriodStartDate", "billingPeriodEndDate", \
                                "meteredWaterConsumption") \

    billedConsDf = isuConsDf.union(accessConsDf)
#    billedConsDf.display()

    #4.Load dimension tables into dataframe

    dimPropertyDf = spark.sql(f"select sourceSystemCode, propertySK, propertyNumber \
                                from {ADS_DATABASE_CURATED}.dimProperty \
                                where _RecordCurrent = 1 and _RecordDeleted = 0")

    dimLocationDf = spark.sql(f"select locationSK, locationId \
                                 from {ADS_DATABASE_CURATED}.dimLocation \
                                 where _RecordCurrent = 1 and _RecordDeleted = 0")

    dimMeterDf = spark.sql(f"select sourceSystemCode, meterSK, meterNumber \
                                 from {ADS_DATABASE_CURATED}.dimMeter \
                                 where _RecordCurrent = 1 and _RecordDeleted = 0")
    
    dimBillDocDf = spark.sql(f"select meterConsumptionBillingDocumentSK, sourceSystemCode, billingDocumentNumber \
                                from {ADS_DATABASE_CURATED}.dimMeterConsumptionBillingDocument \
                                where _RecordCurrent = 1 and _RecordDeleted = 0")
    
    dimBillLineItemDf = spark.sql(f"select meterConsumptionBillingLineItemSK, billingDocumentNumber, billingDocumentLineItemId \
                                from {ADS_DATABASE_CURATED}.dimMeterConsumptionBillingLineItem \
                                where _RecordCurrent = 1 and _RecordDeleted = 0")

    dimBusinessPartnerGroupDf = spark.sql(f"select sourceSystemCode, businessPartnerGroupSK, ltrim('0', businessPartnerGroupNumber) as businessPartnerGroupNumber \
                                from {ADS_DATABASE_CURATED}.dimBusinessPartnerGroup \
                                where _RecordCurrent = 1 and _RecordDeleted = 0")

    dimContractDf = spark.sql(f"select sourceSystemCode, contractSK, contractId, validFromDate, validToDate \
                                from {ADS_DATABASE_CURATED}.dimContract \
                                where _RecordCurrent = 1 and _RecordDeleted = 0")


    dummyDimRecDf = spark.sql(f"select dimPropertySk as dummyDimSK, sourceSystemCode, 'dimProperty' as dimension from {ADS_DATABASE_CURATED}.dimProperty \
                                                                                                                                where propertyNumber in ('-1', '-2') \
                          union select dimLocationSk as dummyDimSK, 'null' as sourceSystemCode, 'dimLocation' as dimension from {ADS_DATABASE_CURATED}.dimLocation \
                                                                                                                                where LocationId = '-1' \
                          union select meterSK as dummyDimSK, sourceSystemCode, 'dimMeter' as dimension from {ADS_DATABASE_CURATED}.dimMeter where meterNumber in ('-1','-2')\
                          union select meterConsumptionBillingDocumentSK as dummyDimSK, sourceSystemCode, 'dimMeterConsumptionBillingDocument' as dimension \
                                                       from {ADS_DATABASE_CURATED}.dimMeterConsumptionBillingDocument where billingDocumentNumber in ('-1','-4') \
                          union select meterConsumptionBillingLineItemSK as dummyDimSK, sourceSystemCode, 'dimMeterConsumptionBillingLineItem' as dimension \
                                                       from {ADS_DATABASE_CURATED}.dimMeterConsumptionBillingLineItem where billingDocumentLineItemId in ('-1','-4') \
                          union select businessPartnerGroupSK as dummyDimSK, sourceSystemCode, 'dimBusinessPartnerGroup' as dimension from \
                                                                                                                              {ADS_DATABASE_CURATED}.dimBusinessPartnerGroup \
                                                                                                                                  where BusinessPartnerGroupNumber in ('-1','-4') \
                          union select contractSK as dummyDimSK, sourceSystemCode, 'dimContract' as dimension from {ADS_DATABASE_CURATED}.dimContract where contractId in ('-1','-4') \
                          ")


    #5.Joins to derive SKs for Fact load

    billedConsDf = billedConsDf.join(dimPropertyDf, (billedConsDf.businessPartnerGroupNumber == dimPropertyDf.propertyNumber) \
                             & (billedConsDf.sourceSystemCode == dimPropertyDf.sourceSystemCode), how="left") \
                  .select(billedConsDf['*'], dimPropertyDf['propertySK'])

    billedConsDf = billedConsDf.join(dimLocationDf, (billedConsDf.businessPartnerGroupNumber == dimLocationDf.locationId), how="left") \
                  .select(billedConsDf['*'], dimLocationDf['locationSK'])

    billedConsDf = billedConsDf.join(dimMeterDf, (billedConsDf.equipmentNumber == dimMeterDf.meterNumber) \
                             & (billedConsDf.sourceSystemCode == dimMeterDf.sourceSystemCode), how="left") \
                  .select(billedConsDf['*'], dimMeterDf['meterSK'])

    billedConsDf = billedConsDf.join(dimBillDocDf, (billedConsDf.billingDocumentNumber == dimBillDocDf.billingDocumentNumber), how="left") \
                  .select(billedConsDf['*'], dimBillDocDf['meterConsumptionBillingDocumentSK'])
    
    billedConsDf = billedConsDf.join(dimBillLineItemDf, (billedConsDf.billingDocumentNumber == dimBillLineItemDf.billingDocumentNumber) \
                             & (billedConsDf.billingDocumentLineItemId == dimBillLineItemDf.billingDocumentLineItemId), how="left") \
                  .select(billedConsDf['*'], dimBillLineItemDf['meterConsumptionBillingLineItemSK'])

    billedConsDf = billedConsDf.join(dimContractDf, (billedConsDf.contractId == dimContractDf.contractId) \
                             & (billedConsDf.billingPeriodStartDate >= dimContractDf.validFromDate) \
                             & (billedConsDf.billingPeriodStartDate <= dimContractDf.validToDate) , how="left") \
                  .select(billedConsDf['*'], dimContractDf['contractSK'])

    billedConsDf = billedConsDf.join(dimBusinessPartnerGroupDf, (billedConsDf.businessPartnerGroupNumber == dimBusinessPartnerGroupDf.businessPartnerGroupNumber) \
                             & (billedConsDf.sourceSystemCode == dimBusinessPartnerGroupDf.sourceSystemCode), how="left") \
                  .select(billedConsDf['*'], dimBusinessPartnerGroupDf['businessPartnerGroupSK'])

    #6.Joins to derive SKs of dummy dimension(-1) records, to be used when the lookup fails for dimensionSk

    billedConsDf = billedConsDf.join(dummyDimRecDf, (dummyDimRecDf.dimension == 'dimProperty') \
                             & (billedConsDf.sourceSystemCode == dummyDimRecDf.sourceSystemCode), how="left") \
                  .select(billedConsDf['*'], dummyDimRecDf['dummyDimSK'].alias('dummyPropertySK'))

    billedConsDf = billedConsDf.join(dummyDimRecDf, (dummyDimRecDf.dimension == 'dimLocation'), how="left") \
                  .select(billedConsDf['*'], dummyDimRecDf['dummyDimSK'].alias('dummyLocationSK'))

    billedConsDf = billedConsDf.join(dummyDimRecDf, (dummyDimRecDf.dimension == 'dimMeter') \
                             & (billedConsDf.sourceSystemCode == dummyDimRecDf.sourceSystemCode), how="left") \
                  .select(billedConsDf['*'], dummyDimRecDf['dummyDimSK'].alias('dummyMeterSK'))

    billedConsDf = billedConsDf.join(dummyDimRecDf, (dummyDimRecDf.dimension == 'dimMeterConsumptionBillingDocument') \
                             & (billedConsDf.sourceSystemCode == dummyDimRecDf.sourceSystemCode), how="left") \
                      .select(billedConsDf['*'], dummyDimRecDf['dummyDimSK'].alias('dummyMeterConsumptionBillingDocumentSK'))
    
    billedConsDf = billedConsDf.join(dummyDimRecDf, (dummyDimRecDf.dimension == 'dimMeterConsumptionBillingLineItem') \
                             & (billedConsDf.sourceSystemCode == dummyDimRecDf.sourceSystemCode), how="left") \
                      .select(billedConsDf['*'], dummyDimRecDf['dummyDimSK'].alias('dummyMeterConsumptionBillingLineItemSK'))

    billedConsDf = billedConsDf.join(dummyDimRecDf, (dummyDimRecDf.dimension == 'dimContract') \
                             & (billedConsDf.sourceSystemCode == dummyDimRecDf.sourceSystemCode), how="left") \
                  .select(billedConsDf['*'], dummyDimRecDf['dummyDimSK'].alias('dummyContractSK'))

    billedConsDf = billedConsDf.join(dummyDimRecDf, (dummyDimRecDf.dimension == 'dimBusinessPartnerGroup') \
                             & (billedConsDf.sourceSystemCode == dummyDimRecDf.sourceSystemCode), how="left") \
                  .select(billedConsDf['*'], dummyDimRecDf['dummyDimSK'].alias('dummyBusinessPartnerGroupSK'))

    #7.SELECT / TRANSFORM
    #aggregating to address any duplicates due to failed SK lookups and dummy SKs being assigned in those cases
    billedConsDf = billedConsDf.selectExpr ( \
                                         "sourceSystemCode" \
                                        ,"coalesce(meterConsumptionBillingDocumentSK, dummyMeterConsumptionBillingDocumentSK) as meterConsumptionBillingDocumentSK" \
                                        ,"coalesce(meterConsumptionBillingLineItemSK, dummyMeterConsumptionBillingLineItemSK) as meterConsumptionBillingLineItemSK" \
                                        ,"coalesce(propertySK, dummyPropertySK) as propertySK" \
                                        ,"coalesce(meterSK, dummyMeterSK) as meterSK" \
                                        ,"coalesce(dimLocationSk, dummyLocationSK) as locationSK" \
                                        ,"-1 as waterNetworkSK" \
                                        ,"coalesce(businessPartnerGroupSK, dummyBusinessPartnerGroupSK) as businessPartnerGroupSK" \
                                        ,"coalesce(contractSK, dummyContractSK) as contractSK" \
                                        ,"billingPeriodStartDate" \
                                        ,"billingPeriodEndDate" \
                                        ,"meteredWaterConsumption" \
                                       ) \
                          .groupby("sourceSystemCode", "meterConsumptionBillingDocumentSK", "meterConsumptionBillingLineItemSK", "propertySK", "meterSK", \
                                   "locationSK", "waterNetworkSK", "billingPeriodStartDate", "businessPartnerGroupSK", "contractSK") \
                          .agg(max("billingPeriodEndDate").alias("billingPeriodEndDate") \
                              ,sum("meteredWaterConsumption").alias("meteredWaterConsumption"))

    return billedConsDf

# COMMAND ----------

df = getBilledWaterConsumption()
TemplateEtl(df, entity="factBilledWaterConsumption", businessKey="sourceSystemCode,meterConsumptionBillingDocumentSK,meterConsumptionBillingLineItemSK,propertySK,meterSK,billingPeriodStartDate", schema=df.schema, AddSK=False)

# COMMAND ----------

dbutils.notebook.exit("1")
