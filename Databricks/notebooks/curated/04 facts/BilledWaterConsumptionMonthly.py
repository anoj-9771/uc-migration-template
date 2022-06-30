# Databricks notebook source
###########################################################################################################################
# Loads MONTHLYAPPORTIONEDCONSUMPTION fact 
#############################################################################################################################
# Method
# 1.Load Cleansed layer table data into dataframe and transform
# 2.JOIN TABLES
# 3.UNION TABLES
# 4.SELECT / TRANSFORM
# 5.SCHEMA DEFINITION
#############################################################################################################################

# COMMAND ----------

# FOLLOWING TABLE TO BE CREATED MANUALLY FIRST TIME LOADING AFTER THE TABLE CLEANUP
# CREATE TABLE `curated`.`factMonthlyApportionedConsumption` (
#   `sourceSystemCode` STRING NOT NULL,
#   `consumptionYear` INT NOT NULL,
#   `consumptionMonth` INT NOT NULL,
#   `billingPeriodStartDate` DATE NOT NULL,
#   `billingPeriodEndDate` DATE NOT NULL,
#   `meterActiveStartDate` DATE NOT NULL,
#   `meterActiveEndDate` DATE NOT NULL,
#   `firstDayOfMeterActiveMonth` DATE NOT NULL,
#   `lastDayOfMeterActiveMonth` DATE NOT NULL,
#   `meterActiveMonthStartDate` DATE NOT NULL,
#   `meterActiveMonthEndDate` DATE NOT NULL,
#   `meterConsumptionBillingDocumentSK` STRING NOT NULL,
#   `meterConsumptionBillingLineItemSK` STRING NOT NULL,
#   `propertySK` STRING NOT NULL,
#   `meterSK` STRING NOT NULL,
#   `locationSK` STRING NOT NULL,
#   `businessPartnerGroupSK` STRING NOT NULL,
#   `contractSK` STRING NOT NULL,
#   `totalMeterActiveDaysPerMonth` INT NOT NULL,
#   `monthlyApportionedConsumption` DECIMAL(24,12),
#   `_DLCuratedZoneTimeStamp` TIMESTAMP NOT NULL,
#   `_RecordStart` TIMESTAMP NOT NULL,
#   `_RecordEnd` TIMESTAMP NOT NULL,
#   `_RecordDeleted` INT NOT NULL,
#   `_RecordCurrent` INT NOT NULL)
# USING delta
# PARTITIONED BY (sourceSystemCode)
# LOCATION 'dbfs:/mnt/datalake-curated/factmonthlyapportionedconsumption/delta'

# COMMAND ----------

# FOLLOWING COMMAND TO BE RUN MANUALLY FIRST TIME LOADING AFTER THE TABLE CLEANUP. THIS COMMAND WILL CREATE A VIEW stage.access_property_hist
# %run ../common/functions/commonAccessPropertyHistory

# COMMAND ----------

# MAGIC %run ../common/common-curated-includeMain

# COMMAND ----------

# MAGIC %run ../common/functions/commonBilledWaterConsumptionIsu

# COMMAND ----------

# MAGIC %run ../common/functions/commonBilledWaterConsumptionAccess

# COMMAND ----------

#-----------------------------------------------------------------------------------------------
# Note: MONTHLYAPPORTIONEDCONSUMPTION fact requires the above two functions
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

def getBilledWaterConsumptionMonthly():

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
    dimPropertyDf = spark.sql(f"select sourceSystemCode, propertySK, propertyNumber \
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

    dimDateDf = spark.sql(f"select dateSK, calendarDate, calendarYear, monthOfYear, monthStartDate, monthEndDate \
                                from {ADS_DATABASE_CURATED}.dimDate \
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

    #5.JOIN TABLES
    billedConsDf = billedConsDf.join(dimPropertyDf, (billedConsDf.businessPartnerGroupNumber == dimPropertyDf.propertyNumber), how="left") \
                    .select(billedConsDf['*'], dimPropertyDf['propertySK'])

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
                    .select(billedConsDf['*'], dimDateDf['calendarYear'].alias('consumptionYear').cast("int"), dimDateDf['monthOfYear'].alias('consumptionMonth').cast("int"), dimDateDf['monthStartDate'].alias('firstDayOfMeterActiveMonth'), dimDateDf['monthEndDate'].alias('lastDayOfMeterActiveMonth')).dropDuplicates()
    
    billedConsDf = billedConsDf.withColumn("meterActiveMonthStartDate", when((col("meterActiveStartDate") >= col("firstDayOfMeterActiveMonth")) & (col("meterActiveStartDate") <= col("lastDayOfMeterActiveMonth")), col("meterActiveStartDate")).otherwise(col("firstDayOfMeterActiveMonth"))) \
                    .withColumn("meterActiveMonthEndDate", when((col("meterActiveEndDate") >= col("firstDayOfMeterActiveMonth")) & (col("meterActiveEndDate") <= col("lastDayOfMeterActiveMonth")), col("meterActiveEndDate")).otherwise(col("lastDayOfMeterActiveMonth"))) \
                    .withColumn("totalMeterActiveDaysPerMonth", (datediff("meterActiveMonthEndDate", "meterActiveMonthStartDate") + 1)) \
                    .withColumn("avgMeteredWaterConsumptionPerMonth", (col("avgMeteredWaterConsumption")*col("totalMeterActiveDaysPerMonth")))
					
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

    #7.SELECT / TRANSFORM
    billedConsDf = billedConsDf.selectExpr \
                              ( \
                               "sourceSystemCode" \
                              ,"consumptionYear" \
                              ,"consumptionMonth" \
                              ,"billingPeriodStartDate" \
                              ,"billingPeriodEndDate" \
                              ,"meterActiveStartDate" \
                              ,"meterActiveEndDate" \
                              ,"firstDayOfMeterActiveMonth" \
                              ,"lastDayOfMeterActiveMonth" \
                              ,"meterActiveMonthStartDate" \
                              ,"meterActiveMonthEndDate" \
                              ,"coalesce(meterConsumptionBillingDocumentSK, dummyMeterConsumptionBillingDocumentSK) as meterConsumptionBillingDocumentSK" \
                              ,"coalesce(meterConsumptionBillingLineItemSK, dummyMeterConsumptionBillingLineItemSK) as meterConsumptionBillingLineItemSK" \
                              ,"coalesce(propertySK, dummyPropertySK) as propertySK" \
                              ,"coalesce(meterSK, dummyMeterSK) as meterSK" \
                              ,"coalesce(locationSK, dummyLocationSK) as locationSK" \
                              ,"coalesce(BusinessPartnerGroupSk, dummyBusinessPartnerGroupSK) as businessPartnerGroupSK" \
                              ,"coalesce(contractSK, dummyContractSK) as contractSK" \
                              ,"totalMeterActiveDaysPerMonth" \
                              ,"cast(avgMeteredWaterConsumptionPerMonth as decimal(24,12)) as monthlyApportionedConsumption" \
                              ) \
                          .groupby("sourceSystemCode", "firstDayOfMeterActiveMonth", "meterConsumptionBillingDocumentSK", "meterConsumptionBillingLineItemSK", "propertySK", "meterSK") \
                          .agg(max("consumptionYear").alias("consumptionYear"), max("consumptionMonth").alias("consumptionMonth"), \
                               max("billingPeriodStartDate").alias("billingPeriodStartDate"), max("billingPeriodEndDate").alias("billingPeriodEndDate"), \
                               max("meterActiveStartDate").alias("meterActiveStartDate"), max("meterActiveEndDate").alias("meterActiveEndDate"), \
                               max("lastDayOfMeterActiveMonth").alias("lastDayOfMeterActiveMonth"), \
                               max("meterActiveMonthStartDate").alias("meterActiveMonthStartDate"), max("meterActiveMonthEndDate").alias("meterActiveMonthEndDate"), \
                               max("locationSK").alias("locationSK"),max("businessPartnerGroupSK").alias("businessPartnerGroupSK"), \
                               max("contractSK").alias("contractSK"), max("totalMeterActiveDaysPerMonth").alias("totalMeterActiveDaysPerMonth"), sum("monthlyApportionedConsumption").alias("monthlyApportionedConsumption")) 
                          .selectExpr \
                                  ( \
                                   "sourceSystemCode" \
                                  ,"consumptionYear" \
                                  ,"consumptionMonth" \
                                  ,"billingPeriodStartDate" \
                                  ,"billingPeriodEndDate" \
                                  ,"meterActiveStartDate" \
                                  ,"meterActiveEndDate" \
                                  ,"firstDayOfMeterActiveMonth" \
                                  ,"lastDayOfMeterActiveMonth" \
                                  ,"meterActiveMonthStartDate" \
                                  ,"meterActiveMonthEndDate" \
                                  ,"meterConsumptionBillingDocumentSK" \
                                  ,"meterConsumptionBillingLineItemSK" \
                                  ,"propertySK" \
                                  ,"meterSK" \
                                  ,"locationSK" \
                                  ,"businessPartnerGroupSK" \
                                  ,"contractSK" \
                                  ,"totalMeterActiveDaysPerMonth" \
                                  ,"monthlyApportionedConsumption" \
                                  )
    
    #8.Apply schema definition
    schema = StructType([
                            StructField("sourceSystemCode", StringType(), False),
                            StructField("consumptionYear", IntegerType(), False),
                            StructField("consumptionMonth", IntegerType(), False),
                            StructField("billingPeriodStartDate", DateType(), False),
                            StructField("billingPeriodEndDate", DateType(), False),
                            StructField("meterActiveStartDate", DateType(), False),
                            StructField("meterActiveEndDate", DateType(), False),
                            StructField("firstDayOfMeterActiveMonth", DateType(), False),
                            StructField("lastDayOfMeterActiveMonth", DateType(), False),
                            StructField("meterActiveMonthStartDate", DateType(), False),
                            StructField("meterActiveMonthEndDate", DateType(), False),
                            StructField("meterConsumptionBillingDocumentSK", StringType(), False),
                            StructField("meterConsumptionBillingLineItemSK", StringType(), False),
                            StructField("propertySK", StringType(), False),
                            StructField("meterSK", StringType(), False),
                            StructField("locationSK", StringType(), False),
                            StructField("businessPartnerGroupSK", StringType(), False),
                            StructField("contractSK", StringType(), False),
                            StructField("totalMeterActiveDaysPerMonth", IntegerType(), True),
                            StructField("monthlyApportionedConsumption", FloatType(), True)
                        ])

    return billedConsDf, schema

# COMMAND ----------

df, schema = getBilledWaterConsumptionMonthly()
# TemplateEtl(df, entity="factMonthlyApportionedConsumption", businessKey="sourceSystemCode,firstDayOfMeterActiveMonth,meterConsumptionBillingDocumentSK,meterConsumptionBillingLineItemSK,propertySK,meterSK, schema=schema, writeMode=ADS_WRITE_MODE_MERGE, AddSK=False)

# COMMAND ----------

df = df.withColumn("_DLCuratedZoneTimeStamp",current_timestamp().cast("timestamp")).withColumn("_RecordStart",col('_DLCuratedZoneTimeStamp').cast("timestamp")).withColumn("_RecordEnd",lit('9999-12-31 00:00:00').cast("timestamp")).withColumn("_RecordDeleted",lit(0).cast("int")).withColumn("_RecordCurrent",lit(1).cast("int"))

if loadConsumption:
    dfAccess = df.filter("sourceSystemCode='ACCESS'")
    dfAccess.write \
      .format("delta") \
      .mode("overwrite") \
      .option("replaceWhere", "sourceSystemCode = 'ACCESS'") \
      .option("overwriteSchema","true").saveAsTable("curated.factMonthlyApportionedConsumption")
    
    dfISU = df.filter("sourceSystemCode='ISU'")
    dfISU.write \
      .format("delta") \
      .mode("overwrite") \
      .option("replaceWhere", "sourceSystemCode = 'ISU'") \
      .option("overwriteSchema","true").saveAsTable("curated.factMonthlyApportionedConsumption")
else:
    df.write \
      .format("delta") \
      .mode("overwrite") \
      .option("replaceWhere", f"sourceSystemCode = '{source_system}'") \
      .option("overwriteSchema","true").saveAsTable("curated.factMonthlyApportionedConsumption")
    
verifyTableSchema(f"curated.factMonthlyApportionedConsumption", schema)

# COMMAND ----------

# THIS IS COMMENTED AND TO BE UNCOMMENTED TO RUN ONLY WHEN ACCESS DATA LOADING USING THIS NOTEBOOK.
# %sql
# OPTIMIZE curated.factMonthlyApportionedConsumption
# WHERE sourceSystemCode = 'ACCESS'

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE curated.factMonthlyApportionedConsumption
# MAGIC WHERE sourceSystemCode = 'ISU'

# COMMAND ----------

# MAGIC %sql
# MAGIC set spark.databricks.delta.retentionDurationCheck.enabled=false;
# MAGIC VACUUM curated.factMonthlyApportionedConsumption RETAIN 0 HOURS;
# MAGIC set spark.databricks.delta.retentionDurationCheck.enabled=true;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE curated.viewMonthlyApportionedConsumption 
# MAGIC LOCATION 'dbfs:/mnt/datalake-curated/viewmonthlyapportionedconsumption'
# MAGIC as with prophist as (
# MAGIC           select propertyNumber,inferiorPropertyTypeCode,inferiorPropertyType,superiorPropertyTypeCode,superiorPropertyType,validFromDate,validToDate from cleansed.isu_zcd_tpropty_hist
# MAGIC           union  
# MAGIC           select propertyNumber,inferiorPropertyTypeCode,inferiorPropertyType,superiorPropertyTypeCode,superiorPropertyType,validFromDate,validToDate from stage.access_property_hist
# MAGIC         )
# MAGIC select 
# MAGIC propertyNumber,
# MAGIC inferiorPropertyTypeCode,
# MAGIC inferiorPropertyType,
# MAGIC superiorPropertyTypeCode,
# MAGIC superiorPropertyType,
# MAGIC propertyTypeValidFromDate,
# MAGIC propertyTypeValidToDate,
# MAGIC sourceSystemCode,
# MAGIC consumptionYear,
# MAGIC consumptionMonth,
# MAGIC billingPeriodStartDate,
# MAGIC billingPeriodEndDate,
# MAGIC meterActiveStartDate,
# MAGIC meterActiveEndDate,
# MAGIC firstDayOfMeterActiveMonth,
# MAGIC lastDayOfMeterActiveMonth,
# MAGIC meterActiveMonthStartDate,
# MAGIC meterActiveMonthEndDate
# MAGIC meterConsumptionBillingDocumentSK,
# MAGIC meterConsumptionBillingLineItemSK,
# MAGIC propertySK,
# MAGIC meterSK,
# MAGIC locationSK,
# MAGIC businessPartnerGroupSK,
# MAGIC contractSK,
# MAGIC totalMeterActiveDaysPerMonth,
# MAGIC monthlyApportionedConsumption from (
# MAGIC select *, row_number() OVER   (PARTITION BY consumptionYear,consumptionMonth,billingPeriodStartDate,billingPeriodEndDate,meterActiveStartDate,meterActiveEndDate,meterActiveMonthStartDate,meterActiveMonthEndDate,meterConsumptionBillingDocumentSK,meterConsumptionBillingLineItemSK,propertySK,meterSK,locationSK,businessPartnerGroupSK,contractSK ORDER BY propertyTypeValidToDate desc,propertyTypeValidFromDate desc) as flag  from 
# MAGIC (select prop.propertyNumber,
# MAGIC prophist.inferiorPropertyTypeCode,
# MAGIC prophist.inferiorPropertyType,
# MAGIC prophist.superiorPropertyTypeCode,
# MAGIC prophist.superiorPropertyType,
# MAGIC prophist.validFromDate as propertyTypeValidFromDate,
# MAGIC prophist.validToDate as propertyTypeValidToDate,
# MAGIC fact.sourceSystemCode,
# MAGIC fact.consumptionYear,
# MAGIC fact.consumptionMonth,
# MAGIC fact.billingPeriodStartDate,
# MAGIC fact.billingPeriodEndDate,
# MAGIC fact.meterActiveStartDate,
# MAGIC fact.meterActiveEndDate,
# MAGIC fact.firstDayOfMeterActiveMonth,
# MAGIC fact.lastDayOfMeterActiveMonth,
# MAGIC fact.meterActiveMonthStartDate,
# MAGIC fact.meterActiveMonthEndDate,
# MAGIC fact.meterConsumptionBillingDocumentSK,
# MAGIC fact.meterConsumptionBillingLineItemSK,
# MAGIC fact.propertySK,
# MAGIC fact.meterSK,
# MAGIC fact.locationSK,
# MAGIC fact.businessPartnerGroupSK,
# MAGIC fact.contractSK,
# MAGIC fact.totalMeterActiveDaysPerMonth,
# MAGIC fact.monthlyApportionedConsumption
# MAGIC from curated.factMonthlyApportionedConsumption fact
# MAGIC left outer join curated.dimproperty prop
# MAGIC on fact.propertySK = prop.propertySK
# MAGIC left outer join prophist
# MAGIC on (prop.propertyNumber = prophist.propertyNumber  and prophist.validFromDate <= fact.meterActiveMonthStartDate and prophist.validToDate >= fact.meterActiveMonthStartDate)
# MAGIC )
# MAGIC )
# MAGIC where flag = 1
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC set spark.databricks.delta.retentionDurationCheck.enabled=false;
# MAGIC VACUUM curated.viewMonthlyApportionedConsumption RETAIN 0 HOURS;
# MAGIC set spark.databricks.delta.retentionDurationCheck.enabled=true;

# COMMAND ----------

dbutils.notebook.exit("1")
