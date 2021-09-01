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

  dimLocationDf = spark.sql("select dimLocationSK, locationID \
                                   from curated.dimLocation \
                                   where _RecordCurrent = 1 and _RecordDeleted = 0")

  dimMeterDf = spark.sql("select sourceSystemCode, dimMeterSK, meterID \
                                   from curated.dimMeter \
                                   where _RecordCurrent = 1 and _RecordDeleted = 0")

  dimBillDocDf = spark.sql("select dimBillingDocumentSK, sourceSystemCode, billingDocumentNumber \
                                  from curated.dimBillingDocument \
                                  where _RecordCurrent = 1 and _RecordDeleted = 0")

  dimDateDf = spark.sql("select dimDateSK, calendarDate \
                                  from curated.dimDate \
                                  where _RecordCurrent = 1 and _RecordDeleted = 0")

#5.Joins to derive SKs for Fact load

  billedConsDf = billedConsDf.join(dimPropertyDf, (billedConsDf.businessPartnerNumber == dimPropertyDf.propertyId) \
                               & (billedConsDf.sourceSystemCode == dimPropertyDf.sourceSystemCode) \
                               & (billedConsDf.billingPeriodStartDate >= dimPropertyDf.propertyStartDate) \
                               & (billedConsDf.billingPeriodEndDate <= dimPropertyDf.propertyEndDate), how="left") \
                    .select(billedConsDf['*'], dimPropertyDf['dimPropertySK'])

  billedConsDf = billedConsDf.join(dimLocationDf, (billedConsDf.businessPartnerNumber == dimLocationDf.locationID), how="left") \
                    .select(billedConsDf['*'], dimLocationDf['dimLocationSK'])

  billedConsDf = billedConsDf.join(dimMeterDf, (billedConsDf.equipmentNumber == dimMeterDf.meterID) \
                               & (billedConsDf.sourceSystemCode == dimMeterDf.sourceSystemCode), how="left") \
                    .select(billedConsDf['*'], dimMeterDf['dimMeterSK'])

  billedConsDf = billedConsDf.join(dimBillDocDf, (billedConsDf.billingDocumentNumber == dimBillDocDf.billingDocumentNumber) \
                               & (billedConsDf.sourceSystemCode == dimBillDocDf.sourceSystemCode), how="left") \
                    .select(billedConsDf['*'], dimBillDocDf['dimBillingDocumentSK'])

  billedConsDf = billedConsDf.join(dimDateDf, billedConsDf.billingPeriodStartDate == dimDateDf.calendarDate, how="left") \
                    .select(billedConsDf['*'], dimDateDf['dimDateSK'].alias('billingPeriodStartDateSK'))

  billedConsDf = billedConsDf.join(dimDateDf, billedConsDf.billingPeriodEndDate == dimDateDf.calendarDate, how="left") \
                    .select(billedConsDf['*'], dimDateDf['dimDateSK'].alias('billingPeriodEndDateSK'))

#6.SELECT / TRANSFORM
  billedConsDf = billedConsDf.selectExpr ( \
                                           "sourceSystemCode" \
                                          ,"dimBillingDocumentSK" \
                                          ,"dimPropertySK" \
                                          ,"dimMeterSK" \
                                          ,"dimLocationSK" \
                                          ,"-1 as dimWaterNetworkSK" \
                                          ,"billingPeriodStartDateSK" \
                                          ,"billingPeriodEndDateSK" \
                                          ,"meteredWaterConsumption" \
                                         )
  return billedConsDf

