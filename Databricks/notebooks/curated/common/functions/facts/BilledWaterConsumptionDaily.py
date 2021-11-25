# Databricks notebook source
#%run ../../includes/util-common

# COMMAND ----------

#%run ../commonBilledWaterConsumptionIsu

# COMMAND ----------

#%run ../commonBilledWaterConsumptionAccess

# COMMAND ----------

# Run the above commands only when running this notebook independently, otherwise the curated master notebook would take care of calling the above notebooks

# COMMAND ----------

import pyspark.sql.functions as F

# COMMAND ----------

###########################################################################################################################
# Function: getBilledWaterConsumptiondaily
#  GETS daily apportioned BilledWaterConsumption FACT 
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
def getBilledWaterConsumptionDaily():
  
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

#3.Union Access and isu billed consumption datasets
  isuConsDf = isuConsDf.select("sourceSystemCode", "billingDocumentNumber", \
                                  "businessPartnerNumber", "equipmentNumber", \
                                  "meterActiveStartDate", "meterActiveEndDate", \
                                  (datediff("meterActiveEndDate", "meterActiveStartDate") + 1).alias("totalMeterActiveDays"), \
                                  "meteredWaterConsumption") \
                                  .where((isuConsDf.isReversedFlag == 'N') & (isuConsDf.isOutsortedFlag == 'N'))

  accessConsDf = accessConsDf.selectExpr("sourceSystemCode", "-1 as billingDocumentNumber", \
                                  "PropertyNumber", "propertyMeterNumber", \
                                  "billingPeriodStartDate", "billingPeriodEndDate", \
                                  "billingPeriodDays", \
                                  "meteredWaterConsumption") \
  
  billedConsDf = isuConsDf.union(accessConsDf)
  
  billedConsDf = billedConsDf.withColumn("avgMeteredWaterConsumption", F.col("meteredWaterConsumption")/F.col("totalMeterActiveDays"))

#4.Load Dmension tables into dataframe
  dimDateDf = spark.sql(f"select dimDateSK, calendarDate \
                                  from {ADS_DATABASE_CURATED}.dimDate \
                                  where _RecordCurrent = 1 and _RecordDeleted = 0")

  dimPropertyDf = spark.sql(f"select sourceSystemCode, dimPropertySK, propertyId, propertyStartDate, propertyEndDate \
                                  from {ADS_DATABASE_CURATED}.dimProperty \
                                  where _RecordCurrent = 1 and _RecordDeleted = 0")

  dimLocationDf = spark.sql(f"select dimLocationSK, locationID \
                                   from {ADS_DATABASE_CURATED}.dimLocation \
                                   where _RecordCurrent = 1 and _RecordDeleted = 0")

  dimMeterDf = spark.sql(f"select sourceSystemCode, dimMeterSK, meterID \
                                   from {ADS_DATABASE_CURATED}.dimMeter \
                                   where _RecordCurrent = 1 and _RecordDeleted = 0")

  dimBillDocDf = spark.sql(f"select dimBillingDocumentSK, sourceSystemCode, billingDocumentNumber \
                                  from {ADS_DATABASE_CURATED}.dimBillingDocument \
                                  where _RecordCurrent = 1 and _RecordDeleted = 0")
    
  dummyDimRecDf = spark.sql(f"select dimPropertySk as dummyDimSK, sourceSystemCode, 'dimProperty' as dimension from {ADS_DATABASE_CURATED}.dimProperty where propertyId = '-1' \
                            union select dimLocationSk as dummyDimSK, 'null' as sourceSystemCode, 'dimLocation' as dimension from {ADS_DATABASE_CURATED}.dimLocation where LocationId = '-1' \
                            union select dimMeterSK as dummyDimSK, sourceSystemCode, 'dimMeter' as dimension from {ADS_DATABASE_CURATED}.dimMeter where meterId = '-1' \
                            union select dimBillingDocumentSK as dummyDimSK, sourceSystemCode, 'dimBillingDocument' as dimension from {ADS_DATABASE_CURATED}.dimBillingDocument \
                                                                                                                                  where billingDocumentNumber in ('-1', '-2') \
                            union select dimDateSK as dummyDimSK, 'null' as sourceSystemCode, 'dimDate' as dimension from {ADS_DATABASE_CURATED}.dimDate \
                                                                                                                                  where calendarDate = ('1900-01-01') \
                            ")    

#5.JOIN TABLES
  billedConsDf = billedConsDf.join(dimPropertyDf, (billedConsDf.businessPartnerNumber == dimPropertyDf.propertyId) \
                               & (billedConsDf.sourceSystemCode == dimPropertyDf.sourceSystemCode) \
                               & (billedConsDf.meterActiveEndDate >= dimPropertyDf.propertyStartDate) \
                               & (billedConsDf.meterActiveEndDate <= dimPropertyDf.propertyEndDate), how="left") \
                    .select(billedConsDf['*'], dimPropertyDf['dimPropertySK'])

  billedConsDf = billedConsDf.join(dimLocationDf, (billedConsDf.businessPartnerNumber == dimLocationDf.locationID), how="left") \
                    .select(billedConsDf['*'], dimLocationDf['dimLocationSK'])

  billedConsDf = billedConsDf.join(dimMeterDf, (billedConsDf.equipmentNumber == dimMeterDf.meterID) \
                               & (billedConsDf.sourceSystemCode == dimMeterDf.sourceSystemCode), how="left") \
                    .select(billedConsDf['*'], dimMeterDf['dimMeterSK'])

  billedConsDf = billedConsDf.join(dimBillDocDf, (billedConsDf.billingDocumentNumber == dimBillDocDf.billingDocumentNumber) \
                               & (billedConsDf.sourceSystemCode == dimBillDocDf.sourceSystemCode), how="left") \
                    .select(billedConsDf['*'], dimBillDocDf['dimBillingDocumentSK'])

  billedConsDf = billedConsDf.join(dimDateDf, (billedConsDf.meterActiveStartDate <= dimDateDf.calendarDate) \
                               & (billedConsDf.meterActiveEndDate >= dimDateDf.calendarDate), how="left") \
                    .select(billedConsDf['*'], dimDateDf['dimDateSK'], dimDateDf['calendarDate'])

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
                    .select(billedConsDf['*'], dummyDimRecDf['dummyDimSK'].alias('dummyDateSK'))
  
#7.SELECT / TRANSFORM
  billedConsDf = billedConsDf.selectExpr \
                              ( \
                               "sourceSystemCode" \
                              ,"coalesce(dimDateSK, dummyDateSK) as consumptionDateSK"
                              ,"coalesce(dimBillingDocumentSK, dummyBillingDocumentSK) as dimBillingDocumentSK" \
                              ,"coalesce(dimPropertySK, dummyPropertySK) as dimPropertySK" \
                              ,"coalesce(dimMeterSK, dummyMeterSK) as dimMeterSK" \
                              ,"coalesce(dimLocationSk, dummyLocationSK) as dimLocationSK" \
                              ,"-1 as dimWaterNetworkSK" \
                              ,"cast(avgMeteredWaterConsumption as decimal(18,6)) as dailyApportionedConsumption" \
                              )

  return billedConsDf
