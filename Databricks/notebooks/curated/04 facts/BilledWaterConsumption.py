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

def isuConsumption():
    isuConsDf = getBilledWaterConsumptionIsu()
    isuConsDf = isuConsDf.select("sourceSystemCode", "billingDocumentNumber", "billingDocumentLineItemId", \
                                "businessPartnerGroupNumber", "equipmentNumber", "contractId", \
                                "billingPeriodStartDate", "billingPeriodEndDate", \
                                "meteredWaterConsumption")
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
                                "PropertyNumber as businessPartnerGroupNumber", "meterNumber as equipmentNumber", "-1 as contractId", \
                                "billingPeriodStartDate", "billingPeriodEndDate", \
                                "meteredWaterConsumption")
    return accessConsDf

# COMMAND ----------

def getBilledWaterConsumption():

    #1.Load Cleansed layer tables into dataframe
    if loadISUConsumption :
        billedConsDf = isuConsumption()

    if loadAccessConsumption :
        billedConsDf = accessConsumption()

    if loadConsumption :
        isuConsDf = isuConsumption()
        accessConsDf = accessConsumption()
        billedConsDf = isuConsDf.union(accessConsDf)

#    billedConsDf.display()

    #2.Join tables
    
    #3.Union tables

    #4.Load dimension tables into dataframe
    dimPropertyDf = spark.sql(f"select sourceSystemCode, propertySK, waterNetworkSK_drinkingWater, waterNetworkSK_recycledWater, propertyNumber \
                                from {ADS_DATABASE_CURATED}.dimProperty \
                                where _RecordCurrent = 1 and _RecordDeleted = 0")

    dimLocationDf = spark.sql(f"select locationSK, locationId \
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


    #5.Joins to derive SKs for Fact load

    billedConsDf = billedConsDf.join(dimPropertyDf, (billedConsDf.businessPartnerGroupNumber == dimPropertyDf.propertyNumber), how="left") \
                  .select(billedConsDf['*'], dimPropertyDf['propertySK'], dimPropertyDf['waterNetworkSK_drinkingWater'], dimPropertyDf['waterNetworkSK_recycledWater'])

    billedConsDf = billedConsDf.join(dimLocationDf, (billedConsDf.businessPartnerGroupNumber == dimLocationDf.locationId), how="left") \
                  .select(billedConsDf['*'], dimLocationDf['locationSK'])

    billedConsDf = billedConsDf.join(dimMeterDf, (billedConsDf.equipmentNumber == dimMeterDf.meterNumber), how="left") \
                  .select(billedConsDf['*'], dimMeterDf['meterSK'], dimMeterDf['waterType'])

    billedConsDf = billedConsDf.join(dimBillDocDf, (billedConsDf.billingDocumentNumber == dimBillDocDf.billingDocumentNumber), how="left") \
                  .select(billedConsDf['*'], dimBillDocDf['meterConsumptionBillingDocumentSK'])
    
    billedConsDf = billedConsDf.join(dimBillLineItemDf, (billedConsDf.billingDocumentNumber == dimBillLineItemDf.billingDocumentNumber) \
                             & (billedConsDf.billingDocumentLineItemId == dimBillLineItemDf.billingDocumentLineItemId), how="left") \
                  .select(billedConsDf['*'], dimBillLineItemDf['meterConsumptionBillingLineItemSK'])

    billedConsDf = billedConsDf.join(dimContractDf, (billedConsDf.contractId == dimContractDf.contractId) \
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
    #aggregating to address any duplicates due to failed SK lookups and dummy SKs being assigned in those cases
    billedConsDf = billedConsDf.selectExpr ( \
                                         "sourceSystemCode" \
                                        ,"coalesce(meterConsumptionBillingDocumentSK, dummyMeterConsumptionBillingDocumentSK) as meterConsumptionBillingDocumentSK" \
                                        ,"coalesce(meterConsumptionBillingLineItemSK, dummyMeterConsumptionBillingLineItemSK) as meterConsumptionBillingLineItemSK" \
                                        ,"coalesce(propertySK, dummyPropertySK) as propertySK" \
                                        ,"coalesce(meterSK, dummyMeterSK) as meterSK" \
                                        ,"coalesce(locationSk, dummyLocationSK) as locationSK" \
                                        ,"coalesce( \
                                                    (case when waterType = 'Drinking Water' then waterNetworkSK_drinkingWater \
                                                         when waterType = 'Recycled Water' then waterNetworkSK_recycledWater else null end) \
                                                         , (case when waterType = 'Drinking Water' then dummyWaterNetworkSK_drinkingWater \
                                                         when waterType = 'Recycled Water' then dummyWaterNetworkSK_recycledWater else dummyWaterNetworkSK_drinkingWater end)) as waterNetworkSK" \
                                        ,"coalesce(businessPartnerGroupSK, dummyBusinessPartnerGroupSK) as businessPartnerGroupSK" \
                                        ,"coalesce(contractSK, dummyContractSK) as contractSK" \
                                        ,"billingPeriodStartDate" \
                                        ,"billingPeriodEndDate" \
                                        ,"meteredWaterConsumption" \
                                       ) \
                          .groupby("sourceSystemCode", "meterConsumptionBillingDocumentSK", "meterConsumptionBillingLineItemSK", "propertySK", "meterSK", \
                                   "locationSK", "waterNetworkSK", "businessPartnerGroupSK", "contractSK", "billingPeriodStartDate") \
                          .agg(max("billingPeriodEndDate").alias("billingPeriodEndDate") \
                              ,sum("meteredWaterConsumption").alias("meteredWaterConsumption"))

    #8.Apply schema definition
    schema = StructType([
                            StructField("sourceSystemCode", StringType(), False),
                            StructField("meterConsumptionBillingDocumentSK", LongType(), False),
                            StructField("meterConsumptionBillingLineItemSK", LongType(), False),
                            StructField("propertySK", LongType(), False),
                            StructField("meterSK", LongType(), False),
                            StructField("locationSK", LongType(), False),
                            StructField("waterNetworkSK", LongType(), False),
                            StructField("businessPartnerGroupSK", LongType(), False),
                            StructField("contractSK", LongType(), False),
                            StructField("billingPeriodStartDate", DateType(), False),
                            StructField("billingPeriodEndDate", DateType(), False),
                            StructField("meteredWaterConsumption", DecimalType(18,6), True)
                        ])

    return billedConsDf, schema

# COMMAND ----------

df, schema = getBilledWaterConsumption()
TemplateEtl(df, entity="factBilledWaterConsumption", businessKey="sourceSystemCode,meterConsumptionBillingDocumentSK,meterConsumptionBillingLineItemSK", schema=schema, writeMode=ADS_WRITE_MODE_MERGE, AddSK=False)

# COMMAND ----------

# MAGIC %sql
# MAGIC --View to get property history for Billed Water Consumption.
# MAGIC Create or replace view curated.viewBilledWaterConsumption as
# MAGIC select propertyNumber,
# MAGIC inferiorPropertyTypeCode,
# MAGIC inferiorPropertyType,
# MAGIC superiorPropertyTypeCode,
# MAGIC superiorPropertyType,
# MAGIC propertyTypeValidFromDate,
# MAGIC propertyTypeValidToDate,
# MAGIC sourceSystemCode,
# MAGIC meterConsumptionBillingDocumentSK,
# MAGIC meterConsumptionBillingLineItemSK,
# MAGIC propertySK,
# MAGIC meterSK,
# MAGIC locationSK,
# MAGIC waterNetworkSK,
# MAGIC businessPartnerGroupSK,
# MAGIC contractSK,
# MAGIC billingPeriodStartDate,
# MAGIC billingPeriodEndDate,
# MAGIC meteredWaterConsumption from (
# MAGIC select * ,RANK() OVER (PARTITION BY sourceSystemCode,meterConsumptionBillingDocumentSK,meterConsumptionBillingLineItemSK ORDER BY propertyTypeValidToDate desc,propertyTypeValidFromDate desc) as flag  from 
# MAGIC (select prop.propertyNumber,
# MAGIC prophist.inferiorPropertyTypeCode,
# MAGIC prophist.inferiorPropertyType,
# MAGIC prophist.superiorPropertyTypeCode,
# MAGIC prophist.superiorPropertyType,
# MAGIC prophist.validFromDate as propertyTypeValidFromDate,
# MAGIC prophist.validToDate as propertyTypeValidToDate,
# MAGIC fact.sourceSystemCode,
# MAGIC fact.meterConsumptionBillingDocumentSK,
# MAGIC fact.meterConsumptionBillingLineItemSK,
# MAGIC fact.propertySK,
# MAGIC fact.meterSK,
# MAGIC fact.locationSK,
# MAGIC fact.waterNetworkSK,
# MAGIC fact.businessPartnerGroupSK,
# MAGIC fact.contractSK,
# MAGIC fact.billingPeriodStartDate,
# MAGIC fact.billingPeriodEndDate,
# MAGIC fact.meteredWaterConsumption
# MAGIC from curated.factbilledwaterconsumption fact
# MAGIC left outer join curated.dimproperty prop
# MAGIC on fact.propertySK = prop.propertySK
# MAGIC left outer join cleansed.isu_zcd_tpropty_hist prophist
# MAGIC on (prop.propertyNumber = prophist.propertyNumber  and prophist.validFromDate <= fact.billingPeriodEndDate and prophist.validToDate >= fact.billingPeriodEndDate)
# MAGIC )
# MAGIC )
# MAGIC where flag = 1

# COMMAND ----------

dbutils.notebook.exit("1")

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace TABLE curated.factbilledwaterconsumption_access 
# MAGIC     as select * from curated.factbilledwaterconsumption where sourcesystemcode = 'ACCESS'

# COMMAND ----------

# MAGIC %sql
# MAGIC --drop table `curated`.`factbilledwaterconsumption`;
# MAGIC CREATE TABLE `curated`.`factbilledwaterconsumption` (
# MAGIC   `sourceSystemCode` STRING NOT NULL,
# MAGIC   `meterConsumptionBillingDocumentSK` BIGINT NOT NULL,
# MAGIC   `meterConsumptionBillingLineItemSK` BIGINT NOT NULL,
# MAGIC   `propertySK` BIGINT NOT NULL,
# MAGIC   `meterSK` BIGINT NOT NULL,
# MAGIC   `locationSK` BIGINT NOT NULL,
# MAGIC   `waterNetworkSK` BIGINT NOT NULL,
# MAGIC   `businessPartnerGroupSK` BIGINT NOT NULL,
# MAGIC   `contractSK` BIGINT NOT NULL,
# MAGIC   `billingPeriodStartDate` DATE NOT NULL,
# MAGIC   `billingPeriodEndDate` DATE NOT NULL,
# MAGIC   `meteredWaterConsumption` DECIMAL(28,6),
# MAGIC   `_DLCuratedZoneTimeStamp` TIMESTAMP NOT NULL,
# MAGIC   `_RecordStart` TIMESTAMP NOT NULL,
# MAGIC   `_RecordEnd` TIMESTAMP NOT NULL,
# MAGIC   `_RecordDeleted` INT NOT NULL,
# MAGIC   `_RecordCurrent` INT NOT NULL)
# MAGIC USING delta
# MAGIC partitioned by (sourceSystemCode)
# MAGIC LOCATION "dbfs:/mnt/datalake-curated/factbilledwaterconsumption/delta"

# COMMAND ----------

# MAGIC %sql
# MAGIC --select sourcesystemcode, count(1) from curated.factbilledwaterconsumption group by sourcesystemcode
# MAGIC --select count(1) from curated.factbilledwaterconsumption_isu
# MAGIC --show create table curated.factbilledwaterconsumption
# MAGIC insert into curated.factbilledwaterconsumption (select * from curated.factbilledwaterconsumption_access)

# COMMAND ----------

# MAGIC %sql
# MAGIC select sourcesystemcode, max(_DLCuratedZoneTimeStamp) from curated.factbilledwaterconsumption
# MAGIC group by sourcesystemcode

# COMMAND ----------

# MAGIC %sql
# MAGIC select sourcesystemcode, max(_DLCuratedZoneTimeStamp) from curated.factbilledwaterconsumption
# MAGIC group by sourcesystemcode

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended curated.factbilledwaterconsumption

# COMMAND ----------

# def getBilledWaterConsumption():

#     #1.Load Cleansed layer tables into dataframe
#     isuConsDf = getBilledWaterConsumptionIsu()
#     accessConsDf = getBilledWaterConsumptionAccess()

#     legacyConsDf = accessConsDf.select('propertyNumber', 'billingPeriodStartDate', 'billingPeriodEndDate') \
#                            .subtract(isuConsDf.select('businessPartnerGroupNumber', 'billingPeriodStartDate', 'billingPeriodEndDate'))

#     accessConsDf = accessConsDf.join(legacyConsDf, (legacyConsDf.propertyNumber == accessConsDf.propertyNumber) \
#                                            & ((legacyConsDf.billingPeriodStartDate == accessConsDf.billingPeriodStartDate) \
#                                            & (legacyConsDf.billingPeriodEndDate == accessConsDf.billingPeriodEndDate)), how="inner" ) \
#                            .select(accessConsDf['*'])

    
#     #2.Join tables
    
#     #3.Union tables
#     isuConsDf = isuConsDf.select("sourceSystemCode", "billingDocumentNumber", "billingDocumentLineItemId", \
#                                 "businessPartnerGroupNumber", "equipmentNumber", "contractId", \
#                                 "billingPeriodStartDate", "billingPeriodEndDate", \
#                                 "meteredWaterConsumption") \

#     accessConsDf = accessConsDf.selectExpr("sourceSystemCode", "-1 as billingDocumentNumber", "-1 as billingDocumentLineItemId", \
#                                 "PropertyNumber", "meterNumber", "-1 as contractId", \
#                                 "billingPeriodStartDate", "billingPeriodEndDate", \
#                                 "meteredWaterConsumption") \

#     billedConsDf = isuConsDf.union(accessConsDf)
# #    billedConsDf.display()

#     #4.Load dimension tables into dataframe
#     dimPropertyDf = spark.sql(f"select sourceSystemCode, propertySK, waterNetworkSK_drinkingWater, waterNetworkSK_recycledWater, propertyNumber \
#                                 from {ADS_DATABASE_CURATED}.dimProperty \
#                                 where _RecordCurrent = 1 and _RecordDeleted = 0")

#     dimLocationDf = spark.sql(f"select locationSK, locationId \
#                                  from {ADS_DATABASE_CURATED}.dimLocation \
#                                  where _RecordCurrent = 1 and _RecordDeleted = 0")

#     dimMeterDf = spark.sql(f"select sourceSystemCode, meterSK, meterNumber, waterType \
#                                  from {ADS_DATABASE_CURATED}.dimMeter \
#                                  where _RecordCurrent = 1 and _RecordDeleted = 0")
    
#     dimBillDocDf = spark.sql(f"select meterConsumptionBillingDocumentSK, sourceSystemCode, billingDocumentNumber \
#                                 from {ADS_DATABASE_CURATED}.dimMeterConsumptionBillingDocument \
#                                 where _RecordCurrent = 1 and _RecordDeleted = 0")
    
#     dimBillLineItemDf = spark.sql(f"select meterConsumptionBillingLineItemSK, billingDocumentNumber, billingDocumentLineItemId \
#                                 from {ADS_DATABASE_CURATED}.dimMeterConsumptionBillingLineItem \
#                                 where _RecordCurrent = 1 and _RecordDeleted = 0")

#     dimBusinessPartnerGroupDf = spark.sql(f"select sourceSystemCode, businessPartnerGroupSK, ltrim('0', businessPartnerGroupNumber) as businessPartnerGroupNumber \
#                                 from {ADS_DATABASE_CURATED}.dimBusinessPartnerGroup \
#                                 where _RecordCurrent = 1 and _RecordDeleted = 0")

#     dimContractDf = spark.sql(f"select sourceSystemCode, contractSK, contractId, validFromDate, validToDate \
#                                 from {ADS_DATABASE_CURATED}.dimContract \
#                                 where _RecordCurrent = 1 and _RecordDeleted = 0")

#     dimWaterNetworkDf = spark.sql(f"select waterNetworkSK, deliverySystem, distributionSystem, supplyZone, pressureArea \
#                                 from {ADS_DATABASE_CURATED}.dimWaterNetwork \
#                                 where _RecordCurrent = 1 and _RecordDeleted = 0")

#     dummyDimRecDf = spark.sql(f"select PropertySk as dummyDimSK, 'dimProperty' as dimension from {ADS_DATABASE_CURATED}.dimProperty where propertyNumber = '-1' \
#                           union select LocationSk as dummyDimSK, 'dimLocation' as dimension from {ADS_DATABASE_CURATED}.dimLocation where LocationId = '-1' \
#                           union select meterSK as dummyDimSK, 'dimMeter' as dimension from {ADS_DATABASE_CURATED}.dimMeter where meterNumber = '-1'\
#                           union select meterConsumptionBillingDocumentSK as dummyDimSK, 'dimMeterConsumptionBillingDocument' as dimension \
#                                                        from {ADS_DATABASE_CURATED}.dimMeterConsumptionBillingDocument where billingDocumentNumber = '-1' \
#                           union select meterConsumptionBillingLineItemSK as dummyDimSK, 'dimMeterConsumptionBillingLineItem' as dimension \
#                                                        from {ADS_DATABASE_CURATED}.dimMeterConsumptionBillingLineItem where billingDocumentLineItemId = '-1' \
#                           union select businessPartnerGroupSK as dummyDimSK, 'dimBusinessPartnerGroup' as dimension from {ADS_DATABASE_CURATED}.dimBusinessPartnerGroup where BusinessPartnerGroupNumber = '-1' \
#                           union select contractSK as dummyDimSK, 'dimContract' as dimension from {ADS_DATABASE_CURATED}.dimContract where contractId = '-1' \
#                           union select waterNetworkSK as dummyDimSK, 'dimWaterNetwork_drinkingWater' as dimension from {ADS_DATABASE_CURATED}.dimWaterNetwork where pressureArea = '-1' \
#                                                 and isPotableWaterNetwork='Y' and isRecycledWaterNetwork='N' \
#                           union select waterNetworkSK as dummyDimSK, 'dimWaterNetwork_recycledWater' as dimension from {ADS_DATABASE_CURATED}.dimWaterNetwork where supplyZone = '-1' \
#                                                 and isPotableWaterNetwork='N' and isRecycledWaterNetwork='Y' \
#                           ")


#     #5.Joins to derive SKs for Fact load

#     billedConsDf = billedConsDf.join(dimPropertyDf, (billedConsDf.businessPartnerGroupNumber == dimPropertyDf.propertyNumber), how="left") \
#                   .select(billedConsDf['*'], dimPropertyDf['propertySK'], dimPropertyDf['waterNetworkSK_drinkingWater'], dimPropertyDf['waterNetworkSK_recycledWater'])

#     billedConsDf = billedConsDf.join(dimLocationDf, (billedConsDf.businessPartnerGroupNumber == dimLocationDf.locationId), how="left") \
#                   .select(billedConsDf['*'], dimLocationDf['locationSK'])

#     billedConsDf = billedConsDf.join(dimMeterDf, (billedConsDf.equipmentNumber == dimMeterDf.meterNumber), how="left") \
#                   .select(billedConsDf['*'], dimMeterDf['meterSK'], dimMeterDf['waterType'])

#     billedConsDf = billedConsDf.join(dimBillDocDf, (billedConsDf.billingDocumentNumber == dimBillDocDf.billingDocumentNumber), how="left") \
#                   .select(billedConsDf['*'], dimBillDocDf['meterConsumptionBillingDocumentSK'])
    
#     billedConsDf = billedConsDf.join(dimBillLineItemDf, (billedConsDf.billingDocumentNumber == dimBillLineItemDf.billingDocumentNumber) \
#                              & (billedConsDf.billingDocumentLineItemId == dimBillLineItemDf.billingDocumentLineItemId), how="left") \
#                   .select(billedConsDf['*'], dimBillLineItemDf['meterConsumptionBillingLineItemSK'])

#     billedConsDf = billedConsDf.join(dimContractDf, (billedConsDf.contractId == dimContractDf.contractId) \
#                              & (billedConsDf.billingPeriodStartDate >= dimContractDf.validFromDate) \
#                              & (billedConsDf.billingPeriodStartDate <= dimContractDf.validToDate), how="left") \
#                   .select(billedConsDf['*'], dimContractDf['contractSK'])

#     billedConsDf = billedConsDf.join(dimBusinessPartnerGroupDf, (billedConsDf.businessPartnerGroupNumber == dimBusinessPartnerGroupDf.businessPartnerGroupNumber), how="left") \
#                   .select(billedConsDf['*'], dimBusinessPartnerGroupDf['businessPartnerGroupSK'])
    
# #     billedConsDf = billedConsDf.join(lotParcelDf, (billedConsDf.businessPartnerGroupNumber == lotParcelDf.propertyNumber), how="left") \
# #                   .select(billedConsDf['*'], lotParcelDf['deliverySystem'], lotParcelDf['distributionSystem'], lotParcelDf['supplyZone'], lotParcelDf['pressureArea'])
    
# #     billedConsDf = billedConsDf.join(dimWaterNetworkDf, (billedConsDf.deliverySystem == dimWaterNetworkDf.deliverySystem) \
# #                              & (billedConsDf.distributionSystem == dimWaterNetworkDf.distributionSystem) \
# #                              & (billedConsDf.supplyZone == dimWaterNetworkDf.supplyZone) \
# #                              & (billedConsDf.pressureArea == dimWaterNetworkDf.pressureArea), how="left") \
# #                   .select(billedConsDf['*'], dimWaterNetworkDf['waterNetworkSK'])

#     #6.Joins to derive SKs of dummy dimension(-1) records, to be used when the lookup fails for dimensionSk

#     billedConsDf = billedConsDf.join(dummyDimRecDf, (dummyDimRecDf.dimension == 'dimProperty'), how="left") \
#                   .select(billedConsDf['*'], dummyDimRecDf['dummyDimSK'].alias('dummyPropertySK'))

#     billedConsDf = billedConsDf.join(dummyDimRecDf, (dummyDimRecDf.dimension == 'dimLocation'), how="left") \
#                   .select(billedConsDf['*'], dummyDimRecDf['dummyDimSK'].alias('dummyLocationSK'))

#     billedConsDf = billedConsDf.join(dummyDimRecDf, (dummyDimRecDf.dimension == 'dimMeter'), how="left") \
#                   .select(billedConsDf['*'], dummyDimRecDf['dummyDimSK'].alias('dummyMeterSK'))

#     billedConsDf = billedConsDf.join(dummyDimRecDf, (dummyDimRecDf.dimension == 'dimMeterConsumptionBillingDocument'), how="left") \
#                       .select(billedConsDf['*'], dummyDimRecDf['dummyDimSK'].alias('dummyMeterConsumptionBillingDocumentSK'))
    
#     billedConsDf = billedConsDf.join(dummyDimRecDf, (dummyDimRecDf.dimension == 'dimMeterConsumptionBillingLineItem'), how="left") \
#                       .select(billedConsDf['*'], dummyDimRecDf['dummyDimSK'].alias('dummyMeterConsumptionBillingLineItemSK'))

#     billedConsDf = billedConsDf.join(dummyDimRecDf, (dummyDimRecDf.dimension == 'dimContract'), how="left") \
#                   .select(billedConsDf['*'], dummyDimRecDf['dummyDimSK'].alias('dummyContractSK'))

#     billedConsDf = billedConsDf.join(dummyDimRecDf, (dummyDimRecDf.dimension == 'dimBusinessPartnerGroup'), how="left") \
#                   .select(billedConsDf['*'], dummyDimRecDf['dummyDimSK'].alias('dummyBusinessPartnerGroupSK'))

#     billedConsDf = billedConsDf.join(dummyDimRecDf, (dummyDimRecDf.dimension == 'dimWaterNetwork_drinkingWater'), how="left") \
#                   .select(billedConsDf['*'], dummyDimRecDf['dummyDimSK'].alias('dummyWaterNetworkSK_drinkingWater'))
    
#     billedConsDf = billedConsDf.join(dummyDimRecDf, (dummyDimRecDf.dimension == 'dimWaterNetwork_recycledWater'), how="left") \
#                   .select(billedConsDf['*'], dummyDimRecDf['dummyDimSK'].alias('dummyWaterNetworkSK_recycledWater'))
    
#     #7.SELECT / TRANSFORM
#     #aggregating to address any duplicates due to failed SK lookups and dummy SKs being assigned in those cases
#     billedConsDf = billedConsDf.selectExpr ( \
#                                          "sourceSystemCode" \
#                                         ,"coalesce(meterConsumptionBillingDocumentSK, dummyMeterConsumptionBillingDocumentSK) as meterConsumptionBillingDocumentSK" \
#                                         ,"coalesce(meterConsumptionBillingLineItemSK, dummyMeterConsumptionBillingLineItemSK) as meterConsumptionBillingLineItemSK" \
#                                         ,"coalesce(propertySK, dummyPropertySK) as propertySK" \
#                                         ,"coalesce(meterSK, dummyMeterSK) as meterSK" \
#                                         ,"coalesce(locationSk, dummyLocationSK) as locationSK" \
#                                         ,"coalesce( \
#                                                     (case when waterType = 'Drinking Water' then waterNetworkSK_drinkingWater \
#                                                          when waterType = 'Recycled Water' then waterNetworkSK_recycledWater else null end) \
#                                                          , (case when waterType = 'Drinking Water' then dummyWaterNetworkSK_drinkingWater \
#                                                          when waterType = 'Recycled Water' then dummyWaterNetworkSK_recycledWater else dummyWaterNetworkSK_drinkingWater end)) as waterNetworkSK" \
#                                         ,"coalesce(businessPartnerGroupSK, dummyBusinessPartnerGroupSK) as businessPartnerGroupSK" \
#                                         ,"coalesce(contractSK, dummyContractSK) as contractSK" \
#                                         ,"billingPeriodStartDate" \
#                                         ,"billingPeriodEndDate" \
#                                         ,"meteredWaterConsumption" \
#                                        ) \
#                           .groupby("sourceSystemCode", "meterConsumptionBillingDocumentSK", "meterConsumptionBillingLineItemSK", "propertySK", "meterSK", \
#                                    "locationSK", "waterNetworkSK", "businessPartnerGroupSK", "contractSK", "billingPeriodStartDate") \
#                           .agg(max("billingPeriodEndDate").alias("billingPeriodEndDate") \
#                               ,sum("meteredWaterConsumption").alias("meteredWaterConsumption"))

#     #8.Apply schema definition
#     schema = StructType([
#                             StructField("sourceSystemCode", StringType(), False),
#                             StructField("meterConsumptionBillingDocumentSK", LongType(), False),
#                             StructField("meterConsumptionBillingLineItemSK", LongType(), False),
#                             StructField("propertySK", LongType(), False),
#                             StructField("meterSK", LongType(), False),
#                             StructField("locationSK", LongType(), False),
#                             StructField("waterNetworkSK", LongType(), False),
#                             StructField("businessPartnerGroupSK", LongType(), False),
#                             StructField("contractSK", LongType(), False),
#                             StructField("billingPeriodStartDate", DateType(), False),
#                             StructField("billingPeriodEndDate", DateType(), False),
#                             StructField("meteredWaterConsumption", DecimalType(18,6), True)
#                         ])

#     return billedConsDf, schema
