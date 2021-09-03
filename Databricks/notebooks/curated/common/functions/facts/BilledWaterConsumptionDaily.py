# Databricks notebook source
#%run ../../includes/util-common

# COMMAND ----------

#%run ../commonBilledWaterConsumptionSapisu

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
  sapisuConsDf = getBilledWaterConsumptionSapisu()
  accessConsDf = getBilledWaterConsumptionAccess()

  legacyConsDS = accessConsDf.select('propertyNumber', 'billingPeriodStartDate', 'billingPeriodEndDate') \
                             .subtract(sapisuConsDf.select('businessPartnerNumber', 'billingPeriodStartDate', 'billingPeriodEndDate'))
  
  accessConsDf = accessConsDf.join(legacyConsDS, (legacyConsDS.propertyNumber == accessConsDf.propertyNumber) \
                                             & ((legacyConsDS.billingPeriodStartDate == accessConsDf.billingPeriodStartDate) \
                                             & (legacyConsDS.billingPeriodEndDate == accessConsDf.billingPeriodEndDate)), how="inner" ) \
                             .select(accessConsDf['*'])

#3.Union Access and Sapisu billed consumption datasets
  sapisuConsDf = sapisuConsDf.select("sourceSystemCode", "billingDocumentNumber", \
                                  "businessPartnerNumber", "equipmentNumber", \
                                  "meterActiveStartDate", "meterActiveEndDate", \
                                  (datediff("meterActiveEndDate", "meterActiveStartDate") + 1).alias("totalMeterActiveDays"), \
                                  "meteredWaterConsumption") \
                                  .where((sapisuConsDf.isReversedFlag == 'N') & (sapisuConsDf.isOutsortedFlag == 'N'))

  accessConsDf = accessConsDf.selectExpr("sourceSystemCode", "-1 as billingDocumentNumber", \
                                  "PropertyNumber", "propertyMeterNumber", \
                                  "billingPeriodStartDate", "billingPeriodEndDate", \
                                  "billingPeriodDays", \
                                  "meteredWaterConsumption") \
  
  billedConsDf = sapisuConsDf.union(accessConsDf)
  
  billedConsDf = billedConsDf.withColumn("avgMeteredWaterConsumption", F.col("meteredWaterConsumption")/F.col("totalMeterActiveDays"))

#4.Load Dmension tables into dataframe
  dimDateDf = spark.sql("select dimDateSK, calendarDate \
                                  from curated.dimDate \
                                  where _RecordCurrent = 1 and _RecordDeleted = 0")

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

#4.JOIN TABLES
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

 #5.UNION TABLES
  
#6.SELECT / TRANSFORM
  billedConsDf = billedConsDf.selectExpr \
                              ( \
                               "sourceSystemCode" \
                              ,"dimDateSK as consumptionDateSK"
                              ,"dimBillingDocumentSK" \
                              ,"dimPropertySK" \
                              ,"dimMeterSK" \
                              ,"dimLocationSK" \
                              ,"-1 as dimWaterNetworkSK" \
                              ,"cast(avgMeteredWaterConsumption as decimal(18,6)) as dailyApportionedConsumption" \
                              )

  return billedConsDf
