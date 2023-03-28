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

# %sql
#FOLLOWING TABLE TO BE CREATED MANUALLY FIRST TIME LOADING AFTER THE TABLE CLEANUP
# CREATE TABLE `curated_v2`.`factMonthlyApportionedConsumption` (
#   `sourceSystemCode` STRING NOT NULL,
#   `consumptionYear` INT NOT NULL,
#   `consumptionMonth` INT NOT NULL,
#   `billingPeriodStartDate` DATE NOT NULL,
#   `billingPeriodEndDate` DATE NOT NULL,
#   `firstDayOfMeterActiveMonth` DATE NOT NULL,
#   `lastDayOfMeterActiveMonth` DATE NOT NULL,
#   `meterActiveMonthStartDate` DATE NOT NULL,
#   `meterActiveMonthEndDate` DATE NOT NULL,
#   `meterConsumptionBillingDocumentSK` STRING NOT NULL,
#   `propertySK` STRING NOT NULL,
#   `deviceSK` STRING NOT NULL,
#   `locationSK` STRING NOT NULL,
#   `businessPartnerGroupSK` STRING NOT NULL,
#   `contractSK` STRING NOT NULL,
#   `totalMeteredWaterConsumption` DECIMAL(24,12),
#   `totalMeterActiveDays` INT,
#   `avgMeteredWaterConsumption` DECIMAL(24,12),
#   `totalMeterActiveDaysPerMonth` INT,
#   `monthlyApportionedConsumption` DECIMAL(24,12),
#   `_DLCuratedZoneTimeStamp` TIMESTAMP NOT NULL,
#   `_RecordStart` TIMESTAMP NOT NULL,
#   `_RecordEnd` TIMESTAMP NOT NULL,
#   `_RecordDeleted` INT NOT NULL,
#   `_RecordCurrent` INT NOT NULL)
# USING delta
# PARTITIONED BY (sourceSystemCode)
# LOCATION 'dbfs:/mnt/datalake-curated-v2/factmonthlyapportionedconsumption/delta'



# COMMAND ----------

# FOLLOWING COMMAND TO BE RUN MANUALLY FIRST TIME LOADING AFTER THE TABLE CLEANUP. THIS COMMAND WILL CREATE A VIEW stage.access_property_hist
# %run ../common/functions/commonAccessPropertyHistory

# COMMAND ----------

# MAGIC %run ../common/common-curated-includeMain

# COMMAND ----------

# MAGIC %run ../common/functions/commonBilledWaterConsumptionIsu

# COMMAND ----------

# %run ../common/functions/commonBilledWaterConsumptionAccess

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
import pyspark.sql.window

# COMMAND ----------

def isuConsumption():
    isuConsDf = getBilledWaterConsumptionIsu()
    isuConsDf = isuConsDf.select("sourceSystemCode", "billingDocumentNumber", \
                                  "businessPartnerGroupNumber", "equipmentNumber", "contractID", \
                                  "billingPeriodStartDate", "billingPeriodEndDate", \
                                  "validFromDate", "validToDate", "billingLineItemBudgetBillingFlag", \
                                  "meteredWaterConsumption") \
                         .withColumnRenamed("validFromDate", "meterActiveStartDate") \
                         .withColumnRenamed("validToDate", "meterActiveEndDate") \
                         .sort("billingDocumentNumber", "billingPeriodStartDate", "billingPeriodEndDate", "meterActiveStartDate", "meterActiveEndDate") \
                         .filter("billingLineItemBudgetBillingFlag='N'")
    return isuConsDf

# COMMAND ----------

# isuConsDf = isuConsumption()
# isuConsDf.write.mode("Overwrite").saveAsTable("tempBilledCons")
# df = spark.sql("""select * from tempBilledCons""")
df = isuConsumption()
partition = Window.partitionBy("billingDocumentNumber", "businessPartnerGroupNumber", "equipmentNumber", "billingperiodStartDate", "billingPeriodEndDate").orderBy("meterActiveStartDate", "meterActiveEndDate").rowsBetween(Window.unboundedPreceding, -1)
df = df.withColumn("maxEndDate", max("meterActiveEndDate").over(partition))
df = df.selectExpr("*", "case when meterActiveStartDate > maxEndDate then 1 else 0 end as groupStarts")
partition = Window.partitionBy("billingDocumentNumber", "businessPartnerGroupNumber", "equipmentNumber", "billingperiodStartDate", "billingPeriodEndDate").orderBy("meterActiveStartDate", "meterActiveEndDate")
df = df.withColumn("group", sum("groupStarts").over(partition))
df = df.groupBy("sourceSystemCode", "billingDocumentNumber", "businessPartnerGroupNumber", "equipmentNumber", "contractId", "billingperiodStartDate", "billingPeriodEndDate", "group") \
                .agg(min("meterActiveStartDate").alias("meterActiveStartDate"), \
                     max("meterActiveEndDate").alias("meterActiveEndDate"), \
                     sum("meteredWaterConsumption").alias("meteredWaterConsumption"))
df = df.selectExpr("sourceSystemCode", "billingDocumentNumber", \
                                  "businessPartnerGroupNumber", "equipmentNumber", "contractId", \
                                  "billingPeriodStartDate", "billingPeriodEndDate", \
                                  "meterActiveStartDate", "meterActiveEndDate", \
                                  "meteredWaterConsumption")
partition = Window.partitionBy("sourceSystemCode", "billingDocumentNumber", "businessPartnerGroupNumber", "equipmentNumber", "contractId", "billingperiodStartDate", "billingPeriodEndDate")
df = df.withColumn("totalMeteredWaterConsumption", sum("meteredWaterConsumption").over(partition))
df.createOrReplaceTempView("billedConsDf")

# COMMAND ----------

# def accessConsumption():
#     accessConsDf = getBilledWaterConsumptionAccess()
#     isuConsDf = isuConsumption()
        
#     legacyConsDf = accessConsDf.select('propertyNumber', 'billingPeriodStartDate', 'billingPeriodEndDate') \
#                            .subtract(isuConsDf.select('businessPartnerGroupNumber', 'billingPeriodStartDate', 'billingPeriodEndDate'))

#     accessConsDf = accessConsDf.join(legacyConsDf, (legacyConsDf.propertyNumber == accessConsDf.propertyNumber) \
#                                            & ((legacyConsDf.billingPeriodStartDate == accessConsDf.billingPeriodStartDate) \
#                                            & (legacyConsDf.billingPeriodEndDate == accessConsDf.billingPeriodEndDate)), how="inner" ) \
#                            .select(accessConsDf['*'])
    
#     accessConsDf = accessConsDf.selectExpr("sourceSystemCode", "-1 as billingDocumentNumber", "-1 as billingDocumentLineItemId", \
#                                   "PropertyNumber as businessPartnerGroupNumber", "meterNumber as equipmentNumber", "-1 as contractID", \
#                                   "billingPeriodStartDate", "billingPeriodEndDate", \
#                                   "billingPeriodStartDate as meterActiveStartDate", "billingPeriodEndDate as meterActiveEndDate", \
#                                   "billingPeriodDays as totalMeterActiveDays", \
#                                   "meteredWaterConsumption") \
#                                .withColumnRenamed("meteredWaterConsumption", "totalMeteredWaterConsumption")
#     return accessConsDf

# COMMAND ----------

def getBilledWaterConsumptionMonthly():

    #1.Load Cleansed layer tables into dataframe
    if loadISUConsumption :
#         billedConsDf = isuConsumption()
        billedConsDf = spark.sql("""select * from billedConsDf""")

#     if loadAccessConsumption :
#         billedConsDf = accessConsumption()

    if loadConsumption :
#         isuConsDf = isuConsumption()
        billedConsDf = spark.sql("""select * from billedConsDf""")
        accessConsDf = accessConsumption()
#         billedConsDf = isuConsDf.unionByName(accessConsDf)
        billedConsDf = billedConsDf.unionByName(accessConsDf)

    #2.Join Tables
    
    #3.Union Access and isu billed consumption datasets

    #billedConsDf = billedConsDf.withColumn("avgMeteredWaterConsumption", F.col("totalMeteredWaterConsumption")/F.col("totalMeterActiveDays"))
    #billedConsDf = billedConsDf.withColumn("avgMeteredWaterConsumption",col("avgMeteredWaterConsumption").cast("decimal(18,6)"))

    #4.Load Dmension tables into dataframe    
    dimPropertyDf = spark.sql(f"""
        SELECT sourceSystemCode, propertySK, propertyNumber, _RecordStart, _RecordEnd
        FROM {ADS_DATABASE_CURATED_V2}.dimProperty
        """
     )

    dimLocationDf = spark.sql(f"""
        SELECT sourceSystemCode, locationSK, locationId, _RecordStart, _RecordEnd 
        FROM {ADS_DATABASE_CURATED_V2}.dimLocation
        """
    )

    dimDeviceDf = spark.sql(f"""
        SELECT sourceSystemCode, deviceSK, deviceNumber, _RecordStart, _RecordEnd 
        FROM {ADS_DATABASE_CURATED_V2}.dimDevice
        """
    )
    
    dimBillDocDf = spark.sql(f"""
        SELECT sourceSystemCode, meterConsumptionBillingDocumentSK, billingDocumentNumber, _RecordStart, _RecordEnd
        FROM {ADS_DATABASE_CURATED_V2}.dimMeterConsumptionBillingDocument
        """
    )
    
    dimBillLineItemDf = spark.sql(f"""
        SELECT sourceSystemCode, meterConsumptionBillingLineItemSK, billingDocumentNumber, billingDocumentLineItemId, _RecordStart, _RecordEnd
        FROM {ADS_DATABASE_CURATED_V2}.dimMeterConsumptionBillingLineItem
        """
    )

    dimBusinessPartnerGroupDf = spark.sql(f"""
        SELECT sourceSystemCode, businessPartnerGroupSK, ltrim('0', businessPartnerGroupNumber) as businessPartnerGroupNumber, _RecordStart, _RecordEnd 
        FROM {ADS_DATABASE_CURATED_V2}.dimBusinessPartnerGroup
        """
    )

    dimContractDf = spark.sql(f"""
        SELECT sourceSystemCode, contractSK, contractId, _RecordStart, _RecordEnd 
        FROM {ADS_DATABASE_CURATED_V2}.dimContract 
        """
    )
    
    dimDateDf = spark.sql(f"""
        SELECT calendarYear, monthOfYear, monthStartDate, monthEndDate
        FROM {ADS_DATABASE_CURATED_V2}.dimDate 
        """
    ).dropDuplicates()
    
    dummyDimRecDf = spark.sql(f"""
    /* Union All Dimensions 'dummy' Records */
    SELECT PropertySk as dummyDimSK, 'dimProperty' as dimension 
    FROM {ADS_DATABASE_CURATED_V2}.dimProperty 
    WHERE propertyNumber = '-1'
    UNION
    SELECT LocationSk as dummyDimSK, 'dimLocation' as dimension 
    FROM {ADS_DATABASE_CURATED_V2}.dimLocation 
    WHERE LocationId = '-1'
    UNION 
    SELECT deviceSK as dummyDimSK, 'dimDevice' as dimension 
    FROM {ADS_DATABASE_CURATED_V2}.dimDevice 
    WHERE deviceNumber = '-1'
    UNION
    SELECT meterConsumptionBillingDocumentSK as dummyDimSK, 'dimMeterConsumptionBillingDocument' as dimension 
    FROM {ADS_DATABASE_CURATED_V2}.dimMeterConsumptionBillingDocument 
    WHERE billingDocumentNumber = '-1'
    UNION
    SELECT meterConsumptionBillingLineItemSK as dummyDimSK, 'dimMeterConsumptionBillingLineItem' as dimension 
    FROM {ADS_DATABASE_CURATED_V2}.dimMeterConsumptionBillingLineItem 
    WHERE billingDocumentLineItemId = '-1'
    UNION 
    SELECT businessPartnerGroupSK as dummyDimSK, 'dimBusinessPartnerGroup' as dimension 
    FROM {ADS_DATABASE_CURATED_V2}.dimBusinessPartnerGroup 
    WHERE BusinessPartnerGroupNumber = '-1'
    UNION
    SELECT contractSK as dummyDimSK, 'dimContract' as dimension 
    FROM {ADS_DATABASE_CURATED_V2}.dimContract 
    WHERE contractId = '-1' 
    """
    )
    
    #5.JOIN TABLES
    # --- dimProperty --- #
    billedConsDf = (
        billedConsDf
        .join(
            dimPropertyDf, 
            (   # join conditions 
                (billedConsDf.businessPartnerGroupNumber == dimPropertyDf.propertyNumber) 
                & (dimPropertyDf._RecordStart  <= billedConsDf.billingPeriodEndDate)
                & (dimPropertyDf._RecordEnd >= billedConsDf.billingPeriodEndDate)
            ), 
            how="left"
        )
        .select(billedConsDf['*'], dimPropertyDf['propertySK'])
    )
    
    # --- dimLocation --- #
    billedConsDf = (
        billedConsDf
            .join(
                dimLocationDf, 
                (   # join conditions
                    (billedConsDf.businessPartnerGroupNumber == dimLocationDf.locationId) 
                    & (dimLocationDf._RecordStart  <= billedConsDf.billingPeriodEndDate)
                    & (dimLocationDf._RecordEnd >= billedConsDf.billingPeriodEndDate)
                ), 
                how="left"
            ) 
            .select(billedConsDf['*'], dimLocationDf['locationSK'])
   )
    
    # --- dimDevice --- #
    billedConsDf = (
        billedConsDf
        .join(
            dimDeviceDf, 
            (   # join conditions
                (billedConsDf.equipmentNumber == dimDeviceDf.deviceNumber) 
                & (dimDeviceDf._RecordStart  <= billedConsDf.billingPeriodEndDate)
                & (dimDeviceDf._RecordEnd >= billedConsDf.billingPeriodEndDate)
            ), 
            how="left"
        ) 
        .select(billedConsDf['*'], dimDeviceDf['deviceSK'])
    )

    # --- dimBillDoc --- #
    billedConsDf = (
        billedConsDf
        .join(
            dimBillDocDf, 
            (   # join conditions
                (billedConsDf.billingDocumentNumber == dimBillDocDf.billingDocumentNumber)
                & (dimBillDocDf._RecordStart  <= billedConsDf.billingPeriodEndDate)
                & (dimBillDocDf._RecordEnd >= billedConsDf.billingPeriodEndDate)
            ), 
            how="left"
        ) 
        .select(billedConsDf['*'], dimBillDocDf['meterConsumptionBillingDocumentSK'])
    )
    
    # --- dimContract ---#
    billedConsDf = (
        billedConsDf
        .join(
            dimContractDf, 
            (   # join conditions
                (billedConsDf.contractId == dimContractDf.contractId)
                & (dimContractDf._RecordStart  <= billedConsDf.billingPeriodEndDate)
                & (dimContractDf._RecordEnd >= billedConsDf.billingPeriodEndDate)
            ), 
            how="left"
        ) 
        .select(billedConsDf['*'], dimContractDf['contractSK'])
    )
    
    # --- dimBusinessPartnerGroup --- #
    billedConsDf = (
        billedConsDf
        .join(
            dimBusinessPartnerGroupDf, 
            (
                (billedConsDf.businessPartnerGroupNumber == dimBusinessPartnerGroupDf.businessPartnerGroupNumber)
                & (dimBusinessPartnerGroupDf._RecordStart  <= billedConsDf.billingPeriodEndDate)
                & (dimBusinessPartnerGroupDf._RecordEnd >= billedConsDf.billingPeriodEndDate)
            ), 
            how="left"
        ) 
        .select(billedConsDf['*'], dimBusinessPartnerGroupDf['businessPartnerGroupSK'])
    )
    
    # --- dimDate --- #
    billedConsDf = (
        billedConsDf
        .join(
            dimDateDf,
            (   # join conditions
                (billedConsDf.meterActiveEndDate >= dimDateDf.monthStartDate)
                & (dimDateDf.monthEndDate >= billedConsDf.meterActiveStartDate)
            ),
            how="left"
        ) 
        .select(billedConsDf['*'], dimDateDf['calendarYear'].alias('consumptionYear').cast("int"), dimDateDf['monthOfYear'].alias('consumptionMonth').cast("int"), dimDateDf['monthStartDate'].alias('firstDayOfMeterActiveMonth'), dimDateDf['monthEndDate'].alias('lastDayOfMeterActiveMonth'))
    )


    billedConsDf = billedConsDf.withColumn("meterActiveMonthStartDate", when((col("meterActiveStartDate") >= col("firstDayOfMeterActiveMonth")) & (col("meterActiveStartDate") <= col("lastDayOfMeterActiveMonth")), col("meterActiveStartDate")).otherwise(col("firstDayOfMeterActiveMonth"))) \
                    .withColumn("meterActiveMonthEndDate", when((col("meterActiveEndDate") >= col("firstDayOfMeterActiveMonth")) & (col("meterActiveEndDate") <= col("lastDayOfMeterActiveMonth")), col("meterActiveEndDate")).otherwise(col("lastDayOfMeterActiveMonth"))) \
                    .withColumn("meterActiveDaysPerMonth", (datediff("meterActiveMonthEndDate", "meterActiveMonthStartDate") + 1).cast("int")) \
    
    billedConsDf = billedConsDf.groupBy("sourceSystemCode", "billingDocumentNumber", "businessPartnerGroupNumber", "equipmentNumber", "contractId", "billingperiodStartDate", "billingPeriodEndDate", "consumptionYear", "consumptionMonth", "firstDayOfMeterActiveMonth", "lastDayOfMeterActiveMonth", "totalMeteredWaterConsumption", "meterConsumptionBillingDocumentSK", "propertySK", "deviceSK", "locationSK", "businessPartnerGroupSK", "contractSK") \
                                .agg(min("meterActiveMonthStartDate").alias("meterActiveMonthStartDate"), \
                                     max("meterActiveMonthEndDate").alias("meterActiveMonthEndDate"), \
                                     sum("meterActiveDaysPerMonth").alias("totalMeterActiveDaysPerMonth"))
    
    partition = Window.partitionBy("sourceSystemCode", "billingDocumentNumber", "businessPartnerGroupNumber", "equipmentNumber", "contractId", "billingperiodStartDate", "billingPeriodEndDate")
    billedConsDf = billedConsDf.withColumn("totalMeterActiveDays", sum("totalMeterActiveDaysPerMonth").over(partition))
    
    billedConsDf = billedConsDf.withColumn("avgMeteredWaterConsumption", F.col("totalMeteredWaterConsumption")/F.col("totalMeterActiveDays")) \
                               .withColumn("avgMeteredWaterConsumptionPerMonth", (col("avgMeteredWaterConsumption")*col("totalMeterActiveDaysPerMonth")))

    #6.Joins to derive SKs of dummy dimension(-1) records, to be used when the lookup fails for dimensionSk
    
    # --- dimProperty --- #
    billedConsDf = (
        billedConsDf
        .join(
            dummyDimRecDf, 
            (dummyDimRecDf.dimension == 'dimProperty'), how="left"
        )
        .select(billedConsDf['*'], dummyDimRecDf['dummyDimSK'].alias('dummyPropertySK'))
    )

    # --- dimLocation --- #
    billedConsDf = (
        billedConsDf
        .join(
            dummyDimRecDf, 
            (dummyDimRecDf.dimension == 'dimLocation'), how="left"
        ) 
        .select(billedConsDf['*'], dummyDimRecDf['dummyDimSK'].alias('dummyLocationSK'))
    )

    # --- dimDevice --- #
    billedConsDf = (
        billedConsDf
        .join(
            dummyDimRecDf, 
            (dummyDimRecDf.dimension == 'dimDevice'), how="left"
        ) 
        .select(billedConsDf['*'], dummyDimRecDf['dummyDimSK'].alias('dummyDeviceSK'))
    )

    # --- dimMeterConsumtpionBillingDocument --- #
    billedConsDf = (
        billedConsDf
        .join(
            dummyDimRecDf, 
            (dummyDimRecDf.dimension == 'dimMeterConsumptionBillingDocument'), how="left"
        ) 
        .select(billedConsDf['*'], dummyDimRecDf['dummyDimSK'].alias('dummyMeterConsumptionBillingDocumentSK'))
    )
    
    # --- dimContract --- #
    billedConsDf = (
        billedConsDf
        .join(
            dummyDimRecDf, 
            (dummyDimRecDf.dimension == 'dimContract'), how="left"
        )
        .select(billedConsDf['*'], dummyDimRecDf['dummyDimSK'].alias('dummyContractSK'))
    )

    # --- dimBusinessPartnerGroup --- #
    billedConsDf = (
        billedConsDf
        .join(
            dummyDimRecDf, 
            (dummyDimRecDf.dimension == 'dimBusinessPartnerGroup'), how="left"
        )
        .select(billedConsDf['*'], dummyDimRecDf['dummyDimSK'].alias('dummyBusinessPartnerGroupSK'))
    )

    #7.SELECT / TRANSFORM
    billedConsDf = billedConsDf.selectExpr \
                                  ( \
                                   "sourceSystemCode" \
                                  ,"consumptionYear" \
                                  ,"consumptionMonth" \
                                  ,"billingPeriodStartDate" \
                                  ,"billingPeriodEndDate" \
                                  ,"firstDayOfMeterActiveMonth" \
                                  ,"lastDayOfMeterActiveMonth" \
                                  ,"meterActiveMonthStartDate" \
                                  ,"meterActiveMonthEndDate" \
                                  ,"coalesce(meterConsumptionBillingDocumentSK, dummyMeterConsumptionBillingDocumentSK) as meterConsumptionBillingDocumentSK" \
                                  ,"coalesce(propertySK, dummyPropertySK) as propertySK" \
                                  ,"coalesce(deviceSK, dummyDeviceSK) as deviceSK" \
                                  ,"coalesce(locationSK, dummyLocationSK) as locationSK" \
                                  ,"coalesce(BusinessPartnerGroupSk, dummyBusinessPartnerGroupSK) as businessPartnerGroupSK" \
                                  ,"coalesce(contractSK, dummyContractSK) as contractSK" \
                                  ,"cast(totalMeteredWaterConsumption as decimal(24,12)) as totalMeteredWaterConsumption" \
                                  ,"cast(totalMeterActiveDays as int) as totalMeterActiveDays"  \
                                  ,"cast(avgMeteredWaterConsumption as decimal(24,12)) as avgMeteredWaterConsumption" \
                                  ,"cast(totalMeterActiveDaysPerMonth as int) as totalMeterActiveDaysPerMonth" \
                                  ,"cast(avgMeteredWaterConsumptionPerMonth as decimal(24,12)) as monthlyApportionedConsumption" \
                                  )
    
    #8.Apply schema definition
    schema = StructType([
                            StructField("sourceSystemCode", StringType(), False),
                            StructField("consumptionYear", IntegerType(), False),
                            StructField("consumptionMonth", IntegerType(), False),
                            StructField("billingPeriodStartDate", DateType(), False),
                            StructField("billingPeriodEndDate", DateType(), False),
                            StructField("firstDayOfMeterActiveMonth", DateType(), False),
                            StructField("lastDayOfMeterActiveMonth", DateType(), False),
                            StructField("meterActiveMonthStartDate", DateType(), False),
                            StructField("meterActiveMonthEndDate", DateType(), False),
                            StructField("meterConsumptionBillingDocumentSK", StringType(), False),
                            StructField("propertySK", StringType(), False),
                            StructField("deviceSK", StringType(), False),
                            StructField("locationSK", StringType(), False),
                            StructField("businessPartnerGroupSK", StringType(), False),
                            StructField("contractSK", StringType(), False),
                            StructField("totalMeteredWaterConsumption", DecimalType(24,12), True),
                            StructField("totalMeterActiveDays", IntegerType(), True),
                            StructField("avgMeteredWaterConsumption", DecimalType(24,12), True),
                            StructField("totalMeterActiveDaysPerMonth", IntegerType(), True),
                            StructField("monthlyApportionedConsumption", DecimalType(24,12), True)
                        ])

    return billedConsDf, schema

# COMMAND ----------

df, schema = getBilledWaterConsumptionMonthly()
# TemplateEtl(df, entity="factMonthlyApportionedConsumption", businessKey="sourceSystemCode,firstDayOfMeterActiveMonth,meterConsumptionBillingDocumentSK,meterConsumptionBillingLineItemSK,propertySK,deviceSK, schema=schema, writeMode=ADS_WRITE_MODE_MERGE, AddSK=False)

# COMMAND ----------

df = df.withColumn("_DLCuratedZoneTimeStamp",current_timestamp().cast("timestamp")).withColumn("_RecordStart",col('_DLCuratedZoneTimeStamp').cast("timestamp")).withColumn("_RecordEnd",lit('9999-12-31 00:00:00').cast("timestamp")).withColumn("_RecordDeleted",lit(0).cast("int")).withColumn("_RecordCurrent",lit(1).cast("int"))

if loadConsumption:
    dfAccess = df.filter("sourceSystemCode='ACCESS'")
    dfAccess.write \
      .format("delta") \
      .mode("overwrite") \
      .option("replaceWhere", "sourceSystemCode = 'ACCESS'") \
      .option("overwriteSchema","true").saveAsTable("curated_v2.factMonthlyApportionedConsumption")
    
    dfISU = df.filter("sourceSystemCode='ISU'")
    dfISU.write \
      .format("delta") \
      .mode("overwrite") \
      .option("replaceWhere", "sourceSystemCode = 'ISU'") \
      .option("overwriteSchema","true").saveAsTable("curated_v2.factMonthlyApportionedConsumption")
else:
    df.write \
      .format("delta") \
      .mode("overwrite") \
      .option("replaceWhere", f"sourceSystemCode = '{source_system}'") \
      .option("overwriteSchema","true").saveAsTable("curated_v2.factMonthlyApportionedConsumption")
    
verifyTableSchema(f"curated_v2.factMonthlyApportionedConsumption", schema)

# COMMAND ----------

# %sql
# --THIS IS COMMENTED AND TO BE UNCOMMENTED TO RUN ONLY WHEN ACCESS DATA LOADING USING THIS NOTEBOOK.
# OPTIMIZE curated_v2.factMonthlyApportionedConsumption
# WHERE sourceSystemCode = 'ACCESS'

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE curated_v2.factMonthlyApportionedConsumption
# MAGIC WHERE sourceSystemCode = 'ISU'

# COMMAND ----------

# MAGIC %sql
# MAGIC set spark.databricks.delta.retentionDurationCheck.enabled=false;
# MAGIC VACUUM curated_v2.factMonthlyApportionedConsumption RETAIN 0 HOURS;
# MAGIC set spark.databricks.delta.retentionDurationCheck.enabled=true;

# COMMAND ----------

dbutils.notebook.exit("1")
