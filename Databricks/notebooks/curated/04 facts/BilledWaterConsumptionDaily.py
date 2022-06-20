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

dbutils.widgets.text("Source System", "ISU")

# COMMAND ----------

source_system = dbutils.widgets.get("Source System").strip().upper()
source_system = "ISU & ACCESS" if not source_system else source_system

print(f"Source System = {source_system}")

loadISUConsumption = True if source_system == 'ISU' else False
loadAccessConsumption = True if source_system == 'ACCESS' else False
loadConsumption = True if source_system == 'ISU & ACCESS' else False

print(f"Load ISU only = {loadISUConsumption}")
print(f"Load Access only = {loadAccessConsumption}")
print(f"Load both Sources = {loadConsumption}")

# COMMAND ----------

import pyspark.sql.functions as F

# COMMAND ----------

def isuConsumption():
    isuConsDf = getBilledWaterConsumptionIsu()
    isuConsDf = isuConsDf.select("sourceSystemCode", "billingDocumentNumber", "billingDocumentLineItemId", \
                                  "businessPartnerGroupNumber", "equipmentNumber", "contractID", \
                                  "billingPeriodStartDate", "billingPeriodEndDate", \
                                  "validFromDate", "validToDate", \
                                  (datediff("validToDate", "validFromDate") + 1).alias("totalMeterActiveDays"), \
                                  "meteredWaterConsumption")  \
                            .withColumnRenamed("validFromDate", "meterActiveStartDate") \
                            .withColumnRenamed("validToDate", "meterActiveEndDate")
    return isuConsDf

# COMMAND ----------

def accessConsumption():
    accessConsDf = getBilledWaterConsumptionAccess()
    isuConsDf = isuConsumption()
        
    legacyConsDf = accessConsDf.select('propertyNumber', 'billingPeriodStartDate', 'billingPeriodEndDate') \
                           .subtract(isuConsDf.select('businessPartnerGroupNumber', 'billingPeriodStartDate', 'billingPeriodEndDate'))

    accessConsDf = accessConsDf.join(legacyConsDf, (legacyConsDf.propertyNumber == accessConsDf.propertyNumber) \
                                           & ((legacyConsDf.billingPeriodStartDate == accessConsDf.billingPeriodStartDate) \
                                           & (legacyConsDf.billingPeriodEndDate == accessConsDf.billingPeriodEndDate)), how="inner" ) \
                           .select(accessConsDf['*'])
    
    accessConsDf = accessConsDf.selectExpr("sourceSystemCode", "-1 as billingDocumentNumber", "-1 as billingDocumentLineItemId", \
                                  "PropertyNumber as businessPartnerGroupNumber", "meterNumber as equipmentNumber", "-1 as contractID", \
                                  "billingPeriodStartDate", "billingPeriodEndDate", \
                                  "billingPeriodStartDate as meterActiveStartDate", "billingPeriodEndDate as meterActiveEndDate", \
                                  "billingPeriodDays as totalMeterActiveDays", \
                                  "meteredWaterConsumption")
    return accessConsDf

# COMMAND ----------

def getBilledWaterConsumptionDaily():

    #1.Load Cleansed layer tables into dataframe
    if loadISUConsumption :
        billedConsDf = isuConsumption()

    if loadAccessConsumption :
        billedConsDf = accessConsumption()

    if loadConsumption :
        isuConsDf = isuConsumption()
        accessConsDf = accessConsumption()
        billedConsDf = isuConsDf.union(accessConsDf)

    #2.Join Tables
    
    #3.Union Access and isu billed consumption datasets

    billedConsDf = billedConsDf.withColumn("avgMeteredWaterConsumption", F.col("meteredWaterConsumption")/F.col("totalMeterActiveDays"))
    #billedConsDf = billedConsDf.withColumn("avgMeteredWaterConsumption",col("avgMeteredWaterConsumption").cast("decimal(18,6)"))
    
    #4.Load Dmension tables into dataframe    
    dimPropertyDf = spark.sql(f"select sourceSystemCode, propertySK, waterNetworkSK_drinkingWater, waterNetworkSK_recycledWater, propertyNumber \
                                from {ADS_DATABASE_CURATED}.dimProperty \
                                where _RecordCurrent = 1 and _RecordDeleted = 0")

    dimLocationDf = spark.sql(f"select locationSK, locationID \
                                 from {ADS_DATABASE_CURATED}.dimLocation \
                                 where _RecordCurrent = 1 and _RecordDeleted = 0")

    dimMeterDf = spark.sql(f"select sourceSystemCode, meterSK, meterNumber, waterType \
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

    dimWaterNetworkDf = spark.sql(f"select waterNetworkSK, deliverySystem, distributionSystem, supplyZone, pressureArea \
                                from {ADS_DATABASE_CURATED}.dimWaterNetwork \
                                where _RecordCurrent = 1 and _RecordDeleted = 0")
    
    dummyDimRecDf = spark.sql(f"select PropertySk as dummyDimSK, 'dimProperty' as dimension from {ADS_DATABASE_CURATED}.dimProperty where propertyNumber = '-1' \
                          union select LocationSk as dummyDimSK, 'dimLocation' as dimension from {ADS_DATABASE_CURATED}.dimLocation where LocationId = '-1' \
                          union select meterSK as dummyDimSK, 'dimMeter' as dimension from {ADS_DATABASE_CURATED}.dimMeter where meterNumber = '-1'\
                          union select meterConsumptionBillingDocumentSK as dummyDimSK, 'dimMeterConsumptionBillingDocument' as dimension \
                                                       from {ADS_DATABASE_CURATED}.dimMeterConsumptionBillingDocument where billingDocumentNumber = '-1' \
                          union select meterConsumptionBillingLineItemSK as dummyDimSK, 'dimMeterConsumptionBillingLineItem' as dimension \
                                                       from {ADS_DATABASE_CURATED}.dimMeterConsumptionBillingLineItem where billingDocumentLineItemId = '-1' \
                          union select businessPartnerGroupSK as dummyDimSK, 'dimBusinessPartnerGroup' as dimension from {ADS_DATABASE_CURATED}.dimBusinessPartnerGroup where BusinessPartnerGroupNumber = '-1' \
                          union select contractSK as dummyDimSK, 'dimContract' as dimension from {ADS_DATABASE_CURATED}.dimContract where contractId = '-1' \
                          union select waterNetworkSK as dummyDimSK, 'dimWaterNetwork_drinkingWater' as dimension from {ADS_DATABASE_CURATED}.dimWaterNetwork where pressureArea = '-1' \
                                                and isPotableWaterNetwork='Y' and isRecycledWaterNetwork='N' \
                          union select waterNetworkSK as dummyDimSK, 'dimWaterNetwork_recycledWater' as dimension from {ADS_DATABASE_CURATED}.dimWaterNetwork where supplyZone = '-1' \
                                                and isPotableWaterNetwork='N' and isRecycledWaterNetwork='Y' \
                          ")

    #5.JOIN TABLES
    billedConsDf = billedConsDf.join(dimPropertyDf, (billedConsDf.businessPartnerGroupNumber == dimPropertyDf.propertyNumber), how="left") \
                    .select(billedConsDf['*'], dimPropertyDf['propertySK'], dimPropertyDf['waterNetworkSK_drinkingWater'], dimPropertyDf['waterNetworkSK_recycledWater'])

    billedConsDf = billedConsDf.join(dimLocationDf, (billedConsDf.businessPartnerGroupNumber == dimLocationDf.locationID), how="left") \
                    .select(billedConsDf['*'], dimLocationDf['locationSK'])

    billedConsDf = billedConsDf.join(dimMeterDf, (billedConsDf.equipmentNumber == dimMeterDf.meterNumber), how="left") \
                    .select(billedConsDf['*'], dimMeterDf['meterSK'], dimMeterDf['waterType'])

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

    billedConsDf = billedConsDf.join(dimBusinessPartnerGroupDf, (billedConsDf.businessPartnerGroupNumber == dimBusinessPartnerGroupDf.businessPartnerGroupNumber), how="left") \
                  .select(billedConsDf['*'], dimBusinessPartnerGroupDf['businessPartnerGroupSK'])


    #6.Joins to derive SKs of dummy dimension(-1) records, to be used when the lookup fails for dimensionSk

    billedConsDf = billedConsDf.join(dummyDimRecDf, (dummyDimRecDf.dimension == 'dimProperty'), how="left") \
                    .select(billedConsDf['*'], dummyDimRecDf['dummyDimSK'].alias('dummyPropertySK'))

    billedConsDf = billedConsDf.join(dummyDimRecDf, (dummyDimRecDf.dimension == 'dimLocation'), how="left") \
                    .select(billedConsDf['*'], dummyDimRecDf['dummyDimSK'].alias('dummyLocationSK'))

    billedConsDf = billedConsDf.join(dummyDimRecDf, (dummyDimRecDf.dimension == 'dimMeter'), how="left") \
                    .select(billedConsDf['*'], dummyDimRecDf['dummyDimSK'].alias('dummyMeterSK'))

    billedConsDf = billedConsDf.join(dummyDimRecDf, (dummyDimRecDf.dimension == 'dimMeterConsumptionBillingDocument'), how="left") \
                    .select(billedConsDf['*'], dummyDimRecDf['dummyDimSK'].alias('dummyMeterConsumptionBillingDocumentSK'))
    
    billedConsDf = billedConsDf.join(dummyDimRecDf, (dummyDimRecDf.dimension == 'dimMeterConsumptionBillingLineItem'), how="left") \
                      .select(billedConsDf['*'], dummyDimRecDf['dummyDimSK'].alias('dummyMeterConsumptionBillingLineItemSK'))

    billedConsDf = billedConsDf.join(dummyDimRecDf, (dummyDimRecDf.dimension == 'dimContract'), how="left") \
                  .select(billedConsDf['*'], dummyDimRecDf['dummyDimSK'].alias('dummyContractSK'))

    billedConsDf = billedConsDf.join(dummyDimRecDf, (dummyDimRecDf.dimension == 'dimBusinessPartnerGroup'), how="left") \
                  .select(billedConsDf['*'], dummyDimRecDf['dummyDimSK'].alias('dummyBusinessPartnerGroupSK'))

    billedConsDf = billedConsDf.join(dummyDimRecDf, (dummyDimRecDf.dimension == 'dimWaterNetwork_drinkingWater'), how="left") \
                  .select(billedConsDf['*'], dummyDimRecDf['dummyDimSK'].alias('dummyWaterNetworkSK_drinkingWater'))
    
    billedConsDf = billedConsDf.join(dummyDimRecDf, (dummyDimRecDf.dimension == 'dimWaterNetwork_recycledWater'), how="left") \
                  .select(billedConsDf['*'], dummyDimRecDf['dummyDimSK'].alias('dummyWaterNetworkSK_recycledWater'))


    #7.SELECT / TRANSFORM
    billedConsDf = billedConsDf.selectExpr \
                              ( \
                               "sourceSystemCode" \
                              ,"consumptionDate" \
                              ,"coalesce(meterConsumptionBillingDocumentSK, dummyMeterConsumptionBillingDocumentSK) as meterConsumptionBillingDocumentSK" \
                              ,"coalesce(meterConsumptionBillingLineItemSK, dummyMeterConsumptionBillingLineItemSK) as meterConsumptionBillingLineItemSK" \
                              ,"coalesce(propertySK, dummyPropertySK) as propertySK" \
                              ,"coalesce(meterSK, dummyMeterSK) as meterSK" \
                              ,"coalesce(locationSK, dummyLocationSK) as locationSK" \
                              ,"coalesce( \
                                          (case when waterType = 'Drinking Water' then waterNetworkSK_drinkingWater \
                                                when waterType = 'Recycled Water' then waterNetworkSK_recycledWater else null end) \
                                                , (case when waterType = 'Drinking Water' then dummyWaterNetworkSK_drinkingWater \
                                                         when waterType = 'Recycled Water' then dummyWaterNetworkSK_recycledWater else dummyWaterNetworkSK_drinkingWater end)) as waterNetworkSK" \
                              ,"coalesce(BusinessPartnerGroupSk, dummyBusinessPartnerGroupSK) as businessPartnerGroupSK" \
                              ,"coalesce(contractSK, dummyContractSK) as contractSK" \
                              ,"cast(avgMeteredWaterConsumption as decimal(18,6))" \
                              ) \
                          .groupby("sourceSystemCode", "consumptionDate", "meterConsumptionBillingDocumentSK", "meterConsumptionBillingLineItemSK", \
                                   "propertySK", "meterSK", \
                                   "locationSK", "waterNetworkSK", "businessPartnerGroupSK", "contractSK") \
                          .agg(sum("avgMeteredWaterConsumption").alias("dailyApportionedConsumption"))  
    
    #8.Apply schema definition
    schema = StructType([
                            StructField("sourceSystemCode", StringType(), False),
                            StructField("consumptionDate", DateType(), False),
                            StructField("meterConsumptionBillingDocumentSK", StringType(), False),
                            StructField("meterConsumptionBillingLineItemSK", StringType(), False),
                            StructField("propertySK", StringType(), False),
                            StructField("meterSK", StringType(), False),
                            StructField("locationSK", StringType(), False),
                            StructField("waterNetworkSK", StringType(), False),
                            StructField("businessPartnerGroupSK", StringType(), False),
                            StructField("contractSK", StringType(), False),
                            StructField("dailyApportionedConsumption", FloatType(), True)
                        ])

    return billedConsDf, schema

# COMMAND ----------

df, schema = getBilledWaterConsumptionDaily()
# TemplateEtl(df, entity="factDailyApportionedConsumption", businessKey="consumptionDate,meterConsumptionBillingDocumentSK,meterConsumptionBillingLineItemSK,propertySK,meterSK,locationSK,waterNetworkSK,businessPartnerGroupSK,contractSK", schema=schema, writeMode=ADS_WRITE_MODE_MERGE, AddSK=False)

# COMMAND ----------

df = df.withColumn("_DLCuratedZoneTimeStamp",current_timestamp().cast("timestamp")).withColumn("_RecordStart",col('_DLCuratedZoneTimeStamp').cast("timestamp")).withColumn("_RecordEnd",lit('9999-12-31 00:00:00').cast("timestamp")).withColumn("_RecordDeleted",lit(0).cast("int")).withColumn("_RecordCurrent",lit(1).cast("int"))

if loadConsumption:
    dfAccess = df.filter("sourceSystemCode='ACCESS'")
    dfAccess.write \
      .format("delta") \
      .mode("overwrite") \
      .option("replaceWhere", "sourceSystemCode = 'ACCESS'") \
      .option("overwriteSchema","true").saveAsTable("curated.factDailyApportionedConsumption")
    
    dfISU = df.filter("sourceSystemCode='ISU'")
    dfISU.write \
      .format("delta") \
      .mode("overwrite") \
      .option("replaceWhere", "sourceSystemCode = 'ISU'") \
      .option("overwriteSchema","true").saveAsTable("curated.factDailyApportionedConsumption")
else:
    df.write \
      .format("delta") \
      .mode("overwrite") \
      .option("replaceWhere", f"sourceSystemCode = '{source_system}'") \
      .option("overwriteSchema","true").saveAsTable("curated.factDailyApportionedConsumption")
    
verifyTableSchema(f"curated.factDailyApportionedConsumption", schema)

# COMMAND ----------

# MAGIC %sql
# MAGIC Create or replace view curated.viewDailyApportionedConsumption as
# MAGIC select 
# MAGIC propertyNumber,
# MAGIC inferiorPropertyTypeCode,
# MAGIC inferiorPropertyType,
# MAGIC superiorPropertyTypeCode,
# MAGIC superiorPropertyType,
# MAGIC propertyTypeValidFromDate,
# MAGIC propertyTypeValidToDate,
# MAGIC sourceSystemCode,
# MAGIC consumptionDate,
# MAGIC meterConsumptionBillingDocumentSK,
# MAGIC meterConsumptionBillingLineItemSK,
# MAGIC propertySK,
# MAGIC meterSK,
# MAGIC locationSK,
# MAGIC waterNetworkSK,
# MAGIC businessPartnerGroupSK,
# MAGIC contractSK,
# MAGIC dailyApportionedConsumption from (
# MAGIC select * ,RANK() OVER   (PARTITION BY consumptionDate,meterConsumptionBillingDocumentSK,meterConsumptionBillingLineItemSK,propertySK,meterSK,locationSK,waterNetworkSK,businessPartnerGroupSK,contractSK ORDER BY propertyTypeValidToDate desc,propertyTypeValidFromDate desc) as flag  from 
# MAGIC (select prop.propertyNumber,
# MAGIC prophist.inferiorPropertyTypeCode,
# MAGIC prophist.inferiorPropertyType,
# MAGIC prophist.superiorPropertyTypeCode,
# MAGIC prophist.superiorPropertyType,
# MAGIC prophist.validFromDate as propertyTypeValidFromDate,
# MAGIC prophist.validToDate as propertyTypeValidToDate,
# MAGIC fact.sourceSystemCode,
# MAGIC fact.consumptionDate,
# MAGIC fact.meterConsumptionBillingDocumentSK,
# MAGIC fact.meterConsumptionBillingLineItemSK,
# MAGIC fact.propertySK,
# MAGIC fact.meterSK,
# MAGIC fact.locationSK,
# MAGIC fact.waterNetworkSK,
# MAGIC fact.businessPartnerGroupSK,
# MAGIC fact.contractSK,
# MAGIC fact.dailyApportionedConsumption
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
