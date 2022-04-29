# Databricks notebook source
###########################################################################################################################
# Loads DAILYAPPORTIONEDCONSUMPTION fact 
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
# Note: DAILYAPPORTIONEDCONSUMPTION fact requires the above two functions
#-----------------------------------------------------------------------------------------------

# COMMAND ----------

import pyspark.sql.functions as F

# COMMAND ----------

def getBilledWaterConsumptionDaily():

    #1.Load Cleansed layer tables into dataframe
    isuConsDf = getBilledWaterConsumptionIsu()
    accessConsDf = getBilledWaterConsumptionAccess()

    legacyConsDS = accessConsDf.select('propertyNumber', 'billingPeriodStartDate', 'billingPeriodEndDate') \
                             .subtract(isuConsDf.select('businessPartnerGroupNumber', 'billingPeriodStartDate', 'billingPeriodEndDate'))

    accessConsDf = accessConsDf.join(legacyConsDS, (legacyConsDS.propertyNumber == accessConsDf.propertyNumber) \
                                             & ((legacyConsDS.billingPeriodStartDate == accessConsDf.billingPeriodStartDate) \
                                             & (legacyConsDS.billingPeriodEndDate == accessConsDf.billingPeriodEndDate)), how="inner" ) \
                             .select(accessConsDf['*'])

    #2.Join Tables
    #3.Union Access and isu billed consumption datasets
    isuConsDf = isuConsDf.select("sourceSystemCode", "billingDocumentNumber", \
                                  "businessPartnerGroupNumber", "equipmentNumber", "contractID", \
                                  "billingPeriodStartDate", "billingPeriodEndDate", \
                                  "meterActiveStartDate", "meterActiveEndDate", \
                                  (datediff("meterActiveEndDate", "meterActiveStartDate") + 1).alias("totalMeterActiveDays"), \
                                  "meteredWaterConsumption") \

    accessConsDf = accessConsDf.selectExpr("sourceSystemCode", "-4 as billingDocumentNumber", \
                                  "PropertyNumber", "meterNumber", "-4 as contractID", \
                                  "billingPeriodStartDate", "billingPeriodEndDate", \
                                  "billingPeriodStartDate as meterActiveStartDate", "billingPeriodEndDate as meterActiveEndDate", \
                                  "billingPeriodDays", \
                                  "meteredWaterConsumption") \

    billedConsDf = isuConsDf.union(accessConsDf)

    billedConsDf = billedConsDf.withColumn("avgMeteredWaterConsumption", F.col("meteredWaterConsumption")/F.col("totalMeterActiveDays"))
    #billedConsDf = billedConsDf.withColumn("avgMeteredWaterConsumption",col("avgMeteredWaterConsumption").cast("decimal(18,6)"))
    #4.Load Dmension tables into dataframe
    
    dimPropertyDf = spark.sql(f"select sourceSystemCode, dimPropertySK, propertyNumber \
                                from {ADS_DATABASE_CURATED}.dimProperty \
                                where _RecordCurrent = 1 and _RecordDeleted = 0")

    dimLocationDf = spark.sql(f"select dimLocationSK, locationID \
                                 from {ADS_DATABASE_CURATED}.dimLocation \
                                 where _RecordCurrent = 1 and _RecordDeleted = 0")

    dimMeterDf = spark.sql(f"select sourceSystemCode, dimMeterSK, meterNumber \
                                 from {ADS_DATABASE_CURATED}.dimMeter \
                                 where _RecordCurrent = 1 and _RecordDeleted = 0")
    dimBillDocDf = spark.sql(f"select dimBillingDocumentSK, sourceSystemCode, billingDocumentNumber \
                                from {ADS_DATABASE_CURATED}.dimBillingDocument \
                                where _RecordCurrent = 1 and _RecordDeleted = 0")

    dimDateDf = spark.sql(f"select dimDateSK, calendarDate \
                                from {ADS_DATABASE_CURATED}.dimDate \
                                where _RecordCurrent = 1 and _RecordDeleted = 0")

    dimBusinessPartnerGroupDf = spark.sql(f"select sourceSystemCode, dimBusinessPartnerGroupSK, ltrim('0', businessPartnerGroupNumber) as businessPartnerGroupNumber \
                                from {ADS_DATABASE_CURATED}.dimBusinessPartnerGroup \
                                where _RecordCurrent = 1 and _RecordDeleted = 0")

    dimContractDf = spark.sql(f"select sourceSystemCode, dimContractSK, contractId, validFromDate, validToDate \
                                from {ADS_DATABASE_CURATED}.dimContract \
                                where _RecordCurrent = 1 and _RecordDeleted = 0")

    dummyDimRecDf = spark.sql(f"select dimPropertySk as dummyDimSK, sourceSystemCode, 'dimProperty' as dimension from {ADS_DATABASE_CURATED}.dimProperty \
                                                                                                                                where propertyNumber in ('-1', '-2') \
                          union select dimLocationSk as dummyDimSK, 'null' as sourceSystemCode, 'dimLocation' as dimension from {ADS_DATABASE_CURATED}.dimLocation \
                                                                                                                                where LocationId = '-1' \
                          union select dimMeterSK as dummyDimSK, sourceSystemCode, 'dimMeter' as dimension from {ADS_DATABASE_CURATED}.dimMeter where meterNumber in ('-1','-2')\
                          union select dimBillingDocumentSK as dummyDimSK, sourceSystemCode, 'dimBillingDocument' as dimension from {ADS_DATABASE_CURATED}.dimBillingDocument \
                                                                                                                                where billingDocumentNumber in ('-1','-4') \
                          union select dimBusinessPartnerGroupSK as dummyDimSK, sourceSystemCode, 'dimBusinessPartnerGroup' as dimension from \
                                                                                                                              {ADS_DATABASE_CURATED}.dimBusinessPartnerGroup \
                                                                                                                                  where BusinessPartnerGroupNumber in ('-1','-4') \
                          union select dimContractSK as dummyDimSK, sourceSystemCode, 'dimContract' as dimension from {ADS_DATABASE_CURATED}.dimContract where contractId in ('-1','-4') \
                          ")


    #5.JOIN TABLES
    billedConsDf = billedConsDf.join(dimPropertyDf, (billedConsDf.businessPartnerGroupNumber == dimPropertyDf.propertyNumber) \
                               & (billedConsDf.sourceSystemCode == dimPropertyDf.sourceSystemCode), how="left") \
                    .select(billedConsDf['*'], dimPropertyDf['dimPropertySK'])

    billedConsDf = billedConsDf.join(dimLocationDf, (billedConsDf.businessPartnerGroupNumber == dimLocationDf.locationID), how="left") \
                    .select(billedConsDf['*'], dimLocationDf['dimLocationSK'])

    billedConsDf = billedConsDf.join(dimMeterDf, (billedConsDf.equipmentNumber == dimMeterDf.meterNumber) \
                               & (billedConsDf.sourceSystemCode == dimMeterDf.sourceSystemCode), how="left") \
                    .select(billedConsDf['*'], dimMeterDf['dimMeterSK'])

    billedConsDf = billedConsDf.join(dimBillDocDf, (billedConsDf.billingDocumentNumber == dimBillDocDf.billingDocumentNumber), how="left") \
                    .select(billedConsDf['*'], dimBillDocDf['dimBillingDocumentSK'])

    billedConsDf = billedConsDf.join(dimDateDf, (billedConsDf.meterActiveStartDate <= dimDateDf.calendarDate) \
                               & (billedConsDf.meterActiveEndDate >= dimDateDf.calendarDate), how="left") \
                    .select(billedConsDf['*'], dimDateDf['calendarDate'].alias('consumptionDate'))
					
    billedConsDf = billedConsDf.join(dimContractDf, (billedConsDf.contractID == dimContractDf.contractId) \
                             & (billedConsDf.billingPeriodStartDate >= dimContractDf.validFromDate) \
                             & (billedConsDf.billingPeriodStartDate <= dimContractDf.validToDate), how="left") \
                  .select(billedConsDf['*'], dimContractDf['dimContractSK'])

    billedConsDf = billedConsDf.join(dimBusinessPartnerGroupDf, (billedConsDf.businessPartnerGroupNumber == dimBusinessPartnerGroupDf.businessPartnerGroupNumber) \
                             & (billedConsDf.sourceSystemCode == dimBusinessPartnerGroupDf.sourceSystemCode), how="left") \
                  .select(billedConsDf['*'], dimBusinessPartnerGroupDf['dimBusinessPartnerGroupSK'])

#    billedConsDf = billedConsDf.join(meterTimesliceDf, (billedConsDf.equipmentNumber == meterTimesliceDf.equipmentNumber), how="left") \
#                  .select(billedConsDf['*'], meterTimesliceDf['logicalDeviceNumber'])
#
#    billedConsDf = billedConsDf.join(meterInstallationDf, (billedConsDf.logicalDeviceNumber == meterInstallationDf.logicalDeviceNumber), how="left") \
#                  .select(billedConsDf['*'], meterInstallationDf['installationId']) \
#                  .drop(billedConsDf.logicalDeviceNumber)
#
#    billedConsDf = billedConsDf.join(dimInstallationDf, (billedConsDf.installationId == dimInstallationDf.installationId), how="left") \
#                  .select(billedConsDf['*'], dimInstallationDf['dimInstallationSK']) \
#                  .drop(dimInstallationDf.installationId)


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

    billedConsDf = billedConsDf.join(dummyDimRecDf, (dummyDimRecDf.dimension == 'dimContract') \
                               & (billedConsDf.sourceSystemCode == dummyDimRecDf.sourceSystemCode), how="left") \
                  .select(billedConsDf['*'], dummyDimRecDf['dummyDimSK'].alias('dummyContractSK'))


    billedConsDf = billedConsDf.join(dummyDimRecDf, (dummyDimRecDf.dimension == 'dimBusinessPartnerGroup') \
                               & (billedConsDf.sourceSystemCode == dummyDimRecDf.sourceSystemCode), how="left") \
                  .select(billedConsDf['*'], dummyDimRecDf['dummyDimSK'].alias('dummyBusinessPartnerGroupSK'))


#    billedConsDf = billedConsDf.join(dummyDimRecDf, (dummyDimRecDf.dimension == 'dimInstallation'), how="left") \
#                  .select(billedConsDf['*'], dummyDimRecDf['dummyDimSK'].alias('dummyInstallationSK'))

    #7.SELECT / TRANSFORM
    billedConsDf = billedConsDf.selectExpr \
                              ( \
                               "sourceSystemCode" \
                              ,"consumptionDate" \
                              ,"coalesce(dimBillingDocumentSK, dummyBillingDocumentSK) as dimBillingDocumentSK" \
                              ,"coalesce(dimPropertySK, dummyPropertySK) as dimPropertySK" \
                              ,"coalesce(dimMeterSK, dummyMeterSK) as dimMeterSK" \
                              ,"coalesce(dimLocationSk, dummyLocationSK) as dimLocationSK" \
                              ,"coalesce(dimBusinessPartnerGroupSk, dummyBusinessPartnerGroupSK) as dimBusinessPartnerGroupSK" \
                              ,"-1 as dimWaterNetworkSK" \
                              ,"coalesce(dimContractSK, dummyContractSK) as dimContractSK" \
                              ,"cast(avgMeteredWaterConsumption as decimal(18,6))" \
                              ) \
                          .groupby("sourceSystemCode", "consumptionDate", "dimBillingDocumentSK", "dimPropertySK", "dimMeterSK", \
                                   "dimLocationSK", "dimBusinessPartnerGroupSK", "dimWaterNetworkSK", "dimContractSK") \
                          .agg(sum("avgMeteredWaterConsumption").alias("dailyApportionedConsumption"))  
    
    return billedConsDf

# COMMAND ----------

df = getBilledWaterConsumptionDaily()
TemplateEtl(df, entity="factDailyApportionedConsumption", businessKey="sourceSystemCode,consumptionDate,dimBillingDocumentSK,dimPropertySK,dimMeterSK", schema=df.schema, AddSK=False)

# COMMAND ----------

dbutils.notebook.exit("1")
