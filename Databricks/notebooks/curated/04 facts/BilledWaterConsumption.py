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
    dimPropertyDf = spark.sql(f"select sourceSystemCode, propertySK, propertyNumber \
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

    dummyDimRecDf = spark.sql(f"select PropertySk as dummyDimSK, 'dimProperty' as dimension from {ADS_DATABASE_CURATED}.dimProperty where propertyNumber = '-1' \
                          union select LocationSk as dummyDimSK, 'dimLocation' as dimension from {ADS_DATABASE_CURATED}.dimLocation where LocationId = '-1' \
                          union select meterSK as dummyDimSK, 'dimMeter' as dimension from {ADS_DATABASE_CURATED}.dimMeter where meterNumber = '-1'\
                          union select meterConsumptionBillingDocumentSK as dummyDimSK, 'dimMeterConsumptionBillingDocument' as dimension \
                                                       from {ADS_DATABASE_CURATED}.dimMeterConsumptionBillingDocument where billingDocumentNumber = '-1' \
                          union select meterConsumptionBillingLineItemSK as dummyDimSK, 'dimMeterConsumptionBillingLineItem' as dimension \
                                                       from {ADS_DATABASE_CURATED}.dimMeterConsumptionBillingLineItem where billingDocumentLineItemId = '-1' \
                          union select businessPartnerGroupSK as dummyDimSK, 'dimBusinessPartnerGroup' as dimension from {ADS_DATABASE_CURATED}.dimBusinessPartnerGroup where BusinessPartnerGroupNumber = '-1' \
                          union select contractSK as dummyDimSK, 'dimContract' as dimension from {ADS_DATABASE_CURATED}.dimContract where contractId = '-1' \
                          ")


    #5.Joins to derive SKs for Fact load

    billedConsDf = billedConsDf.join(dimPropertyDf, (billedConsDf.businessPartnerGroupNumber == dimPropertyDf.propertyNumber), how="left") \
                  .select(billedConsDf['*'], dimPropertyDf['propertySK'])

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

    #7.SELECT / TRANSFORM
    #aggregating to address any duplicates due to failed SK lookups and dummy SKs being assigned in those cases
    billedConsDf = billedConsDf.selectExpr ( \
                                         "sourceSystemCode" \
                                        ,"coalesce(meterConsumptionBillingDocumentSK, dummyMeterConsumptionBillingDocumentSK) as meterConsumptionBillingDocumentSK" \
                                        ,"coalesce(meterConsumptionBillingLineItemSK, dummyMeterConsumptionBillingLineItemSK) as meterConsumptionBillingLineItemSK" \
                                        ,"coalesce(propertySK, dummyPropertySK) as propertySK" \
                                        ,"coalesce(meterSK, dummyMeterSK) as meterSK" \
                                        ,"coalesce(locationSk, dummyLocationSK) as locationSK" \
                                        ,"coalesce(businessPartnerGroupSK, dummyBusinessPartnerGroupSK) as businessPartnerGroupSK" \
                                        ,"coalesce(contractSK, dummyContractSK) as contractSK" \
                                        ,"billingPeriodStartDate" \
                                        ,"billingPeriodEndDate" \
                                        ,"meteredWaterConsumption" \
                                       ) \
                          .groupby("sourceSystemCode", "meterConsumptionBillingDocumentSK", "meterConsumptionBillingLineItemSK", "propertySK", "meterSK", \
                                   "locationSK", "businessPartnerGroupSK", "contractSK", "billingPeriodStartDate") \
                          .agg(max("billingPeriodEndDate").alias("billingPeriodEndDate") \
                              ,sum("meteredWaterConsumption").alias("meteredWaterConsumption"))

    #8.Apply schema definition
    schema = StructType([
                            StructField("sourceSystemCode", StringType(), False),
                            StructField("meterConsumptionBillingDocumentSK", StringType(), False),
                            StructField("meterConsumptionBillingLineItemSK", StringType(), False),
                            StructField("propertySK", StringType(), False),
                            StructField("meterSK", StringType(), False),
                            StructField("locationSK", StringType(), False),
                            StructField("businessPartnerGroupSK", StringType(), False),
                            StructField("contractSK", StringType(), False),
                            StructField("billingPeriodStartDate", DateType(), False),
                            StructField("billingPeriodEndDate", DateType(), False),
                            StructField("meteredWaterConsumption", DecimalType(18,6), True)
                        ])

    return billedConsDf, schema

# COMMAND ----------

df, schema = getBilledWaterConsumption()
# TemplateEtl(df, entity="factBilledWaterConsumption", businessKey="meterConsumptionBillingDocumentSK,meterConsumptionBillingLineItemSK,propertySK,meterSK,locationSK,businessPartnerGroupSK,contractSK,billingPeriodStartDate", schema=schema, writeMode=ADS_WRITE_MODE_MERGE, AddSK=False)

# COMMAND ----------

df = df.withColumn("_DLCuratedZoneTimeStamp",current_timestamp().cast("timestamp")).withColumn("_RecordStart",col('_DLCuratedZoneTimeStamp').cast("timestamp")).withColumn("_RecordEnd",lit('9999-12-31 00:00:00').cast("timestamp")).withColumn("_RecordDeleted",lit(0).cast("int")).withColumn("_RecordCurrent",lit(1).cast("int"))

if loadConsumption:
    dfAccess = df.filter("sourceSystemCode='ACCESS'")
    dfAccess.write \
      .format("delta") \
      .mode("overwrite") \
      .option("replaceWhere", "sourceSystemCode = 'ACCESS'") \
      .option("overwriteSchema","true").saveAsTable("curated.factBilledWaterConsumption")
    
    dfISU = df.filter("sourceSystemCode='ISU'")
    dfISU.write \
      .format("delta") \
      .mode("overwrite") \
      .option("replaceWhere", "sourceSystemCode = 'ISU'") \
      .option("overwriteSchema","true").saveAsTable("curated.factBilledWaterConsumption")
else:
    df.write \
      .format("delta") \
      .mode("overwrite") \
      .option("replaceWhere", f"sourceSystemCode = '{source_system}'") \
      .option("overwriteSchema","true").saveAsTable("curated.factBilledWaterConsumption")
    
verifyTableSchema(f"curated.factBilledWaterConsumption", schema)

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
# MAGIC businessPartnerGroupSK,
# MAGIC contractSK,
# MAGIC billingPeriodStartDate,
# MAGIC billingPeriodEndDate,
# MAGIC meteredWaterConsumption from (
# MAGIC select * ,RANK() OVER (PARTITION BY meterConsumptionBillingDocumentSK,meterConsumptionBillingLineItemSK,propertySK,meterSK,locationSK,businessPartnerGroupSK,contractSK,billingPeriodStartDate ORDER BY propertyTypeValidToDate desc,propertyTypeValidFromDate desc) as flag  from 
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
