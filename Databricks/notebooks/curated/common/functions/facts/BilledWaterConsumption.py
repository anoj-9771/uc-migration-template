# Databricks notebook source
#%run ../../includes/util-common

# COMMAND ----------

#%run ../commonBilledWaterConsumptionSapisu

# COMMAND ----------

#%run ../commonBilledWaterConsumptionAccess

# COMMAND ----------

# Run the above commands only when running this notebook independently, otherwise the curated master notebook would take care of calling the above notebooks

# COMMAND ----------

###########################################################################################################################
# Function: getBilledWaterConsumption
#  Merges both SAPISU and ACCESS BilledWaterConsumption FACT 
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
  sapisuConsDf = getBilledWaterConsumptionSapisu()
  accessConsDf = getBilledWaterConsumptionAccess()
  
  legacyConsDS = accessConsDf.select('propertyNumber', 'billingPeriodStartDate', 'billingPeriodEndDate') \
                             .subtract(sapisuConsDf.select('businessPartnerNumber', 'billingPeriodStartDate', 'billingPeriodEndDate'))
  
  accessConsDf = accessConsDf.join(legacyConsDS, (legacyConsDS.propertyNumber == accessConsDf.propertyNumber) \
                                             & ((legacyConsDS.billingPeriodStartDate == accessConsDf.billingPeriodStartDate) \
                                             & (legacyConsDS.billingPeriodEndDate == accessConsDf.billingPeriodEndDate)), how="inner" ) \
                             .select(accessConsDf['*'])

#3.Union tables
  sapisuConsDf = sapisuConsDf.select("sourceSystemCode", "billingDocumentNumber", \
                                  "businessPartnerNumber", "equipmentNumber", \
                                  "billingPeriodStartDate", "billingPeriodEndDate", \
                                  "meteredWaterConsumption") \
                                  .where((sapisuConsDf.isReversedFlag == 'N') & (sapisuConsDf.isOutsortedFlag == 'N'))

  accessConsDf = accessConsDf.selectExpr("sourceSystemCode", "-1 as billingDocumentNumber", \
                                  "PropertyNumber", "propertyMeterNumber", \
                                  "billingPeriodStartDate", "billingPeriodEndDate", \
                                  "meteredWaterConsumption") \

  billedConsDf = sapisuConsDf.union(accessConsDf)
  
  
#4.Load dimension tables into dataframe

  dimPropertyDf = spark.sql("select sourceSystemCode, dimPropertySK, propertyId, propertyStartDate, propertyEndDate \
                                  from curated.dimProperty \
                                  where _RecordCurrent = 1 and _RecordDeleted = 0")

  dimLocationDf = spark.sql("select dimLocationSK, locationId \
                                   from curated.dimLocation \
                                   where _RecordCurrent = 1 and _RecordDeleted = 0")

  dimMeterDf = spark.sql("select sourceSystemCode, dimMeterSK, meterId \
                                   from curated.dimMeter \
                                   where _RecordCurrent = 1 and _RecordDeleted = 0")

  dimBillDocDf = spark.sql("select dimBillingDocumentSK, sourceSystemCode, billingDocumentNumber \
                                  from curated.dimBillingDocument \
                                  where _RecordCurrent = 1 and _RecordDeleted = 0")

  dimDateDf = spark.sql("select dimDateSK, calendarDate \
                                  from curated.dimDate \
                                  where _RecordCurrent = 1 and _RecordDeleted = 0")

  dummyDimRecDf = spark.sql("select dimPropertySk as dummyDimSK, sourceSystemCode, 'dimProperty' as dimension from curated.dimProperty where propertyId = '-1' \
                            union select dimLocationSk as dummyDimSK, 'null' as sourceSystemCode, 'dimLocation' as dimension from curated.dimLocation where LocationId = '-1' \
                            union select dimMeterSK as dummyDimSK, sourceSystemCode, 'dimMeter' as dimension from curated.dimMeter where meterId = '-1' \
                            union select dimBillingDocumentSK as dummyDimSK, sourceSystemCode, 'dimBillingDocument' as dimension from curated.dimBillingDocument \
                                                                                                                                  where billingDocumentNumber in ('-1', '-2') \
                            union select dimDateSK as dummyDimSK, 'null' as sourceSystemCode, 'dimDate' as dimension from curated.dimDate \
                                                                                                                                  where calendarDate = ('1900-01-01') \
                            ")

  
#5.Joins to derive SKs for Fact load

  billedConsDf = billedConsDf.join(dimPropertyDf, (billedConsDf.businessPartnerNumber == dimPropertyDf.propertyId) \
                               & (billedConsDf.sourceSystemCode == dimPropertyDf.sourceSystemCode) \
                               & (billedConsDf.billingPeriodStartDate >= dimPropertyDf.propertyStartDate) \
                               & (billedConsDf.billingPeriodEndDate <= dimPropertyDf.propertyEndDate), how="left") \
                    .select(billedConsDf['*'], dimPropertyDf['dimPropertySK'])

  billedConsDf = billedConsDf.join(dimLocationDf, (billedConsDf.businessPartnerNumber == dimLocationDf.locationId), how="left") \
                    .select(billedConsDf['*'], dimLocationDf['dimLocationSK'])
  
  billedConsDf = billedConsDf.join(dimMeterDf, (billedConsDf.equipmentNumber == dimMeterDf.meterId) \
                               & (billedConsDf.sourceSystemCode == dimMeterDf.sourceSystemCode), how="left") \
                    .select(billedConsDf['*'], dimMeterDf['dimMeterSK'])

  billedConsDf = billedConsDf.join(dimBillDocDf, (billedConsDf.billingDocumentNumber == dimBillDocDf.billingDocumentNumber) \
                               & (billedConsDf.sourceSystemCode == dimBillDocDf.sourceSystemCode), how="left") \
                    .select(billedConsDf['*'], dimBillDocDf['dimBillingDocumentSK'])

  billedConsDf = billedConsDf.join(dimDateDf, billedConsDf.billingPeriodStartDate == dimDateDf.calendarDate, how="left") \
                    .select(billedConsDf['*'], dimDateDf['dimDateSK'].alias('billingPeriodStartDateSK'))

  billedConsDf = billedConsDf.join(dimDateDf, billedConsDf.billingPeriodEndDate == dimDateDf.calendarDate, how="left") \
                    .select(billedConsDf['*'], dimDateDf['dimDateSK'].alias('billingPeriodEndDateSK'))

  
#6.Joins to derive SKs of dummy dimension(-1) records, to be used when the lookup fails for dimensionSk
  
  billedConsDf = billedConsDf.join(dummyDimRecDf, (dummyDimRecDf.dimension == 'dimProperty') \
                               & (billedConsDf.sourceSystemCode == dummyDimRecDf.sourceSystemCode), how="left") \
                    .select(billedConsDf['*'], dummyDimRecDf['dummyDimSK'].alias('dummyPropertySK'))
  
  billedConsDf = billedConsDf.join(dummyDimRecDf, (dummyDimRecDf.dimension == 'dimLocation'), how="left") \
                    .select(billedConsDf['*'], dummyDimRecDf['dummyDimSK'].alias('dummyLocationSK'))

  billedConsDf = billedConsDf.join(dummyDimRecDf, (dummyDimRecDf.dimension == 'dimMeter') \
                               & (billedConsDf.sourceSystemCode == dummyDimRecDf.sourceSystemCode), how="left") \
                    .select(billedConsDf['*'], dummyDimRecDf['dummyDimSK'].alias('dummyMeterSK'))

  billedConsDf = billedConsDf.join(dummyDimRecDf, (dummyDimRecDf.dimension == 'dimBillingDocument') \
                                 & (billedConsDf.sourceSystemCode == dummyDimRecDf.sourceSystemCode), how="left") \
                    .select(billedConsDf['*'], dummyDimRecDf['dummyDimSK'].alias('dummyBillingDocumentSK'))
  
  billedConsDf = billedConsDf.join(dummyDimRecDf, (dummyDimRecDf.dimension == 'dimDate'), how="left") \
                    .select(billedConsDf['*'], dummyDimRecDf['dummyDimSK'].alias('dummyBillingPeriodStartSK'))

    
#6.SELECT / TRANSFORM
  #aggregating to address any duplicates due to failed SK lookups and dummy SKs being assigned in those cases
  billedConsDf = billedConsDf.selectExpr ( \
                                           "sourceSystemCode" \
                                          ,"coalesce(dimBillingDocumentSK, dummyBillingDocumentSK) as dimBillingDocumentSK" \
                                          ,"coalesce(dimPropertySK, dummyPropertySK) as dimPropertySK" \
                                          ,"coalesce(dimMeterSK, dummyMeterSK) as dimMeterSK" \
                                          ,"coalesce(dimLocationSk, dummyLocationSK) as dimLocationSK" \
                                          ,"-1 as dimWaterNetworkSK" \
                                          ,"coalesce(billingPeriodStartDateSK, dummyBillingPeriodStartSK) as billingPeriodStartDateSK" \
                                          ,"billingPeriodEndDateSK" \
                                          ,"meteredWaterConsumption" \
                                         ) \
                            .groupby("sourceSystemCode", "dimBillingDocumentSK", "dimPropertySK", "dimMeterSK", \
                                     "dimLocationSK", "dimWaterNetworkSK", "billingPeriodStartDateSK") \
                            .agg(max("billingPeriodEndDateSK").alias("billingPeriodEndDateSK") \
                                ,sum("meteredWaterConsumption").alias("meteredWaterConsumption"))
  
  return billedConsDf

