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
    isuConsDf = isuConsDf.select("sourceSystemCode", "billingDocumentNumber", "billingDocumentLineItemId", \
                                  "businessPartnerGroupNumber", "equipmentNumber", "contractID", \
                                  "billingPeriodStartDate", "billingPeriodEndDate", \
                                  "validFromDate", "validToDate", \
                                  (datediff("validToDate", "validFromDate") + 1).alias("totalMeterActiveDays"), \
                                  "meteredWaterConsumption")  \
                            .withColumnRenamed("validFromDate", "meterActiveStartDate") \
                            .withColumnRenamed("validToDate", "meterActiveEndDate")

    accessConsDf = accessConsDf.selectExpr("sourceSystemCode", "-4 as billingDocumentNumber", "-4 as billingDocumentLineItemId", \
                                  "PropertyNumber", "meterNumber", "-4 as contractID", \
                                  "billingPeriodStartDate", "billingPeriodEndDate", \
                                  "billingPeriodStartDate as meterActiveStartDate", "billingPeriodEndDate as meterActiveEndDate", \
                                  "billingPeriodDays", \
                                  "meteredWaterConsumption") \

    billedConsDf = isuConsDf.union(accessConsDf)

    billedConsDf = billedConsDf.withColumn("avgMeteredWaterConsumption", F.col("meteredWaterConsumption")/F.col("totalMeterActiveDays"))
    #billedConsDf = billedConsDf.withColumn("avgMeteredWaterConsumption",col("avgMeteredWaterConsumption").cast("decimal(18,6)"))
    #4.Load Dmension tables into dataframe
    
    dimPropertyDf = spark.sql(f"select sourceSystemCode, propertySK, propertyNumber \
                                from {ADS_DATABASE_CURATED}.dimProperty \
                                where _RecordCurrent = 1 and _RecordDeleted = 0")

    dimLocationDf = spark.sql(f"select locationSK, locationID \
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

    dimDateDf = spark.sql(f"select dateSK, calendarDate \
                                from {ADS_DATABASE_CURATED}.dimDate \
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


    #5.JOIN TABLES
    billedConsDf = billedConsDf.join(dimPropertyDf, (billedConsDf.businessPartnerGroupNumber == dimPropertyDf.propertyNumber) \
                               & (billedConsDf.sourceSystemCode == dimPropertyDf.sourceSystemCode), how="left") \
                    .select(billedConsDf['*'], dimPropertyDf['propertySK'])

    billedConsDf = billedConsDf.join(dimLocationDf, (billedConsDf.businessPartnerGroupNumber == dimLocationDf.locationID), how="left") \
                    .select(billedConsDf['*'], dimLocationDf['locationSK'])

    billedConsDf = billedConsDf.join(dimMeterDf, (billedConsDf.equipmentNumber == dimMeterDf.meterNumber) \
                               & (billedConsDf.sourceSystemCode == dimMeterDf.sourceSystemCode), how="left") \
                    .select(billedConsDf['*'], dimMeterDf['meterSK'])

    billedConsDf = billedConsDf.join(dimBillDocDf, (billedConsDf.billingDocumentNumber == dimBillDocDf.billingDocumentNumber), how="left") \
                    .select(billedConsDf['*'], dimBillDocDf['meterConsumptionBillingDocumentSK'])
    
    billedConsDf = billedConsDf.join(dimBillLineItemDf, (billedConsDf.billingDocumentNumber == dimBillLineItemDf.billingDocumentNumber) \
                             & (billedConsDf.billingDocumentLineItemId == dimBillLineItemDf.billingDocumentLineItemId), how="left") \
                  .select(billedConsDf['*'], dimBillLineItemDf['meterConsumptionBillingLineItemSK'])

    billedConsDf = billedConsDf.join(dimDateDf, (billedConsDf.meterActiveStartDate <= dimDateDf.calendarDate) \
                               & (billedConsDf.meterActiveEndDate >= dimDateDf.calendarDate), how="left") \
                    .select(billedConsDf['*'], dimDateDf['calendarDate'].alias('consumptionDate'))
					
    billedConsDf = billedConsDf.join(dimContractDf, (billedConsDf.contractID == dimContractDf.contractId) \
                             & (billedConsDf.billingPeriodStartDate >= dimContractDf.validFromDate) \
                             & (billedConsDf.billingPeriodStartDate <= dimContractDf.validToDate), how="left") \
                  .select(billedConsDf['*'], dimContractDf['contractSK'])

    billedConsDf = billedConsDf.join(dimBusinessPartnerGroupDf, (billedConsDf.businessPartnerGroupNumber == dimBusinessPartnerGroupDf.businessPartnerGroupNumber) \
                             & (billedConsDf.sourceSystemCode == dimBusinessPartnerGroupDf.sourceSystemCode), how="left") \
                  .select(billedConsDf['*'], dimBusinessPartnerGroupDf['businessPartnerGroupSK'])

#    billedConsDf = billedConsDf.join(meterTimesliceDf, (billedConsDf.equipmentNumber == meterTimesliceDf.equipmentNumber), how="left") \
#                  .select(billedConsDf['*'], meterTimesliceDf['logicalDeviceNumber'])
#
#    billedConsDf = billedConsDf.join(meterInstallationDf, (billedConsDf.logicalDeviceNumber == meterInstallationDf.logicalDeviceNumber), how="left") \
#                  .select(billedConsDf['*'], meterInstallationDf['installationId']) \
#                  .drop(billedConsDf.logicalDeviceNumber)
#
#    billedConsDf = billedConsDf.join(dimInstallationDf, (billedConsDf.installationId == dimInstallationDf.installationId), how="left") \
#                  .select(billedConsDf['*'], dimInstallationDf['installationSK']) \
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

    billedConsDf = billedConsDf.join(dummyDimRecDf, (dummyDimRecDf.dimension == 'dimMeterConsumptionBillingDocument') \
                               & (billedConsDf.sourceSystemCode == dummyDimRecDf.sourceSystemCode), how="left") \
                    .select(billedConsDf['*'], dummyDimRecDf['dummyDimSK'].alias('dummyMeterConsumptionBillingDocumentSK'))
    
    billedConsDf = billedConsDf.join(dummyDimRecDf, (dummyDimRecDf.dimension == 'dimMeterConsumptionBillingLineItem') \
                             & (billedConsDf.sourceSystemCode == dummyDimRecDf.sourceSystemCode), how="left") \
                      .select(billedConsDf['*'], dummyDimRecDf['dummyDimSK'].alias('dummyMeterConsumptionBillingLineItemSK'))

#     billedConsDf = billedConsDf.join(dummyDimRecDf, (dummyDimRecDf.dimension == 'dimDate'), how="left") \
#                     .select(billedConsDf['*'], dummyDimRecDf['dummyDimSK'].alias('dummyDateSK'))

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
                              ,"coalesce(meterConsumptionBillingDocumentSK, dummyMeterConsumptionBillingDocumentSK) as meterConsumptionBillingDocumentSK" \
                              ,"coalesce(meterConsumptionBillingLineItemSK, dummyMeterConsumptionBillingLineItemSK) as meterConsumptionBillingLineItemSK" \
                              ,"coalesce(propertySK, dummyPropertySK) as propertySK" \
                              ,"coalesce(meterSK, dummyMeterSK) as meterSK" \
                              ,"coalesce(dimLocationSk, dummyLocationSK) as locationSK" \
                              ,"coalesce(dimBusinessPartnerGroupSk, dummyBusinessPartnerGroupSK) as businessPartnerGroupSK" \
                              ,"-1 as waterNetworkSK" \
                              ,"coalesce(contractSK, dummyContractSK) as contractSK" \
                              ,"cast(avgMeteredWaterConsumption as decimal(18,6))" \
                              ) \
                          .groupby("sourceSystemCode", "consumptionDate", "meterConsumptionBillingDocumentSK", "meterConsumptionBillingLineItemSK", \
                                   "propertySK", "meterSK", \
                                   "locationSK", "businessPartnerGroupSK", "waterNetworkSK", "contractSK") \
                          .agg(sum("avgMeteredWaterConsumption").alias("dailyApportionedConsumption"))  
    
    return billedConsDf

# COMMAND ----------

df = getBilledWaterConsumptionDaily()
TemplateEtl(df, entity="factDailyApportionedConsumption", businessKey="sourceSystemCode,consumptionDate,meterConsumptionBillingDocumentSK,meterConsumptionBillingLineItemSK,propertySK,meterSK", schema=df.schema, AddSK=False)

# COMMAND ----------

# MAGIC %sql
# MAGIC --Create or replace view curated.viewDailyApportionedConsumption as
# MAGIC select * from (
# MAGIC select * ,RANK() OVER   (PARTITION BY sourceSystemCode,consumptionDate,meterConsumptionBillingDocumentSK,meterConsumptionBillingLineItemSK,propertySK,meterSK ORDER BY propertyTypeValidToDate desc,propertyTypeValidFromDate desc) as flag  from 
# MAGIC (select prop.propertyNumber,
# MAGIC prophist.inferiorPropertyTypeCode,
# MAGIC prophist.inferiorPropertyType,
# MAGIC prophist.superiorPropertyTypeCode,
# MAGIC prophist.superiorPropertyType,
# MAGIC prophist.validFromDate propertyTypeValidFromDate,
# MAGIC prophist.validToDate propertyTypeValidToDate,
# MAGIC fact.*
# MAGIC from curated.factDailyApportionedConsumption fact
# MAGIC left outer join curated.dimproperty prop
# MAGIC on fact.propertySK = prop.propertySK
# MAGIC left outer join cleansed.isu_zcd_tpropty_hist prophist
# MAGIC on (prop.propertyNumber = prophist.propertyNumber  and prophist.validFromDate <= fact.consumptionDate and prophist.validToDate >= fact.consumptionDate)
# MAGIC )
# MAGIC )
# MAGIC where flag = 1
# MAGIC ;

# COMMAND ----------

dbutils.notebook.exit("1")
