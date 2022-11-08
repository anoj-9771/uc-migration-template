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

# FOLLOWING TABLE TO BE CREATED MANUALLY FIRST TIME LOADING AFTER THE TABLE CLEANUP
# CREATE TABLE `curated_v2`.`factBilledWaterConsumption` (
#   `sourceSystemCode` STRING NOT NULL,
#   `meterConsumptionBillingDocumentSK` STRING NOT NULL,
#   `meterConsumptionBillingLineItemSK` STRING NOT NULL,
#   `propertySK` STRING NOT NULL,
#   `deviceSK` STRING NOT NULL,
#   `locationSK` STRING NOT NULL,
#   `businessPartnerGroupSK` STRING NOT NULL,
#   `contractSK` STRING NOT NULL,
#   `billingPeriodStartDate` DATE NOT NULL,
#   `billingPeriodEndDate` DATE NOT NULL,
#   `meteredWaterConsumption` DECIMAL(24,12),
#   `_DLCuratedZoneTimeStamp` TIMESTAMP NOT NULL,
#   `_RecordStart` TIMESTAMP NOT NULL,
#   `_RecordEnd` TIMESTAMP NOT NULL,
#   `_RecordDeleted` INT NOT NULL,
#   `_RecordCurrent` INT NOT NULL)
# USING delta
# PARTITIONED BY (sourceSystemCode)
# LOCATION 'dbfs:/mnt/datalake-curated-v2/factbilledwaterconsumption/delta'

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
    isuConsDf = (
        isuConsDf
        .select(
            "sourceSystemCode", 
            "billingDocumentNumber", 
            "billingDocumentLineItemId",
            "businessPartnerGroupNumber", 
            "equipmentNumber", 
            "contractId", 
            "billingPeriodStartDate", 
            "billingPeriodEndDate", 
            "meteredWaterConsumption"
        )
    )
    return isuConsDf

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
#                                 "PropertyNumber as businessPartnerGroupNumber", "meterNumber as equipmentNumber", "-1 as contractId", \
#                                 "billingPeriodStartDate", "billingPeriodEndDate", \
#                                 "meteredWaterConsumption")
#     return accessConsDf

# COMMAND ----------

def getBilledWaterConsumption():

    #1.Load Cleansed layer tables into dataframe
    if loadISUConsumption :
        billedConsDf = isuConsumption()

#     if loadAccessConsumption :
#         billedConsDf = accessConsumption()

    if loadConsumption :
        isuConsDf = isuConsumption()
        accessConsDf = accessConsumption()
        billedConsDf = isuConsDf.union(accessConsDf)

    #2.Join tables
    
    #3.Union tables

    #4.Load dimension tables into dataframe
    dimPropertyDf = spark.sql(f"""
        SELECT sourceSystemCode, propertySK, propertyNumber, _RecordStart, _RecordEnd
        FROM {ADS_DATABASE_CURATED}.dimProperty
        """
     )

    dimLocationDf = spark.sql(f"""
        SELECT sourceSystemCode, locationSK, locationId, _RecordStart, _RecordEnd 
        FROM {ADS_DATABASE_CURATED}.dimLocation
        """
    )

    dimDeviceDf = spark.sql(f"""
        SELECT sourceSystemCode, deviceSK, deviceNumber, _RecordStart, _RecordEnd 
        FROM {ADS_DATABASE_CURATED}.dimDevice
        """
    )
    
    dimBillDocDf = spark.sql(f"""
        SELECT sourceSystemCode, meterConsumptionBillingDocumentSK, billingDocumentNumber, _RecordStart, _RecordEnd \
        FROM {ADS_DATABASE_CURATED}.dimMeterConsumptionBillingDocument
        """
    )
    
    dimBillLineItemDf = spark.sql(f"""
        SELECT sourceSystemCode, meterConsumptionBillingLineItemSK, billingDocumentNumber, billingDocumentLineItemId, _RecordStart, _RecordEnd \
        FROM {ADS_DATABASE_CURATED}.dimMeterConsumptionBillingLineItem
        """
    )

    dimBusinessPartnerGroupDf = spark.sql(f"""
        SELECT sourceSystemCode, businessPartnerGroupSK, ltrim('0', businessPartnerGroupNumber) as businessPartnerGroupNumber, _RecordStart, _RecordEnd 
        FROM {ADS_DATABASE_CURATED}.dimBusinessPartnerGroup
        """
    )

    dimContractDf = spark.sql(f"""
        SELECT sourceSystemCode, contractSK, contractId, _RecordStart, _RecordEnd 
        FROM {ADS_DATABASE_CURATED}.dimContract 
        WHERE _RecordCurrent = 1
        """
    )

    dummyDimRecDf = spark.sql(f"""
    /* Union All Dimensions 'dummy' Records */
    SELECT PropertySk as dummyDimSK, 'dimProperty' as dimension 
    FROM {ADS_DATABASE_CURATED}.dimProperty 
    WHERE propertyNumber = '-1' UNION
     
    SELECT LocationSk as dummyDimSK, 'dimLocation' as dimension 
    FROM {ADS_DATABASE_CURATED}.dimLocation 
    WHERE LocationId = '-1' UNION
     
    SELECT deviceSK as dummyDimSK, 'dimDevice' as dimension 
    FROM {ADS_DATABASE_CURATED}.dimDevice 
    WHERE deviceNumber = '-1' UNION
     
    SELECT meterConsumptionBillingDocumentSK as dummyDimSK, 'dimMeterConsumptionBillingDocument' as dimension 
    FROM {ADS_DATABASE_CURATED}.dimMeterConsumptionBillingDocument 
    WHERE billingDocumentNumber = '-1' UNION
     
    SELECT meterConsumptionBillingLineItemSK as dummyDimSK, 'dimMeterConsumptionBillingLineItem' as dimension 
    FROM {ADS_DATABASE_CURATED}.dimMeterConsumptionBillingLineItem 
    WHERE billingDocumentLineItemId = '-1' UNION
     
    SELECT businessPartnerGroupSK as dummyDimSK, 'dimBusinessPartnerGroup' as dimension 
    FROM {ADS_DATABASE_CURATED}.dimBusinessPartnerGroup 
    WHERE BusinessPartnerGroupNumber = '-1' UNION
     
    SELECT contractSK as dummyDimSK, 'dimContract' as dimension 
    FROM {ADS_DATABASE_CURATED}.dimContract 
    WHERE contractId = '-1' 
    """
    )


    #5.Joins to derive SKs for Fact load

    # --- dimProperty --- #
    billedConsDf = (
        billedConsDf
        .join(
            dimPropertyDf, 
            (   # join conditions 
                billedConsDf.businessPartnerGroupNumber == dimPropertyDf.propertyNumber 
                & dimPropertyDf._RecordStart  <= billedConsDf.billingPeriodEndDate 
                & dimPropertyDf._RecordEnd >= billedConsDf.billingPeriodEndDate
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
                    billedConsDf.businessPartnerGroupNumber == dimLocationDf.locationId 
                    & dimLocationDf._RecordStart  <= billedConsDf.billingPeriodEndDate 
                    & dimLocationDf._RecordEnd >= billedConsDf.billingPeriodEndDate
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
                billedConsDf.equipmentNumber == dimDeviceDf.deviceNumber 
                & dimDeviceDf._RecordStart  <= billedConsDf.billingPeriodEndDate 
                & dimDeviceDf._RecordEnd >= billedConsDf.billingPeriodEndDate
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
                billedConsDf.billingDocumentNumber == dimBillDocDf.billingDocumentNumber 
                & dimBillDocDf._RecordStart  <= billedConsDf.billingPeriodEndDate 
                & dimBillDocDf._RecordEnd >= billedConsDf.billingPeriodEndDate
            ), 
            how="left"
        ) 
        .select(billedConsDf['*'], dimBillDocDf['meterConsumptionBillingDocumentSK'])
    )
    
    # --- dimBillLineItem --- #
    billedConsDf = (
        billedConsDf
        .join(
            dimBillLineItemDf,
            (   # join conditions
                billedConsDf.billingDocumentNumber == dimBillLineItemDf.billingDocumentNumber 
                & billedConsDf.billingDocumentLineItemId == dimBillLineItemDf.billingDocumentLineItemId  
                & dimBillLineItemDf._RecordStart  <= billedConsDf.billingPeriodEndDate 
                & dimBillLineItemDf._RecordEnd >= billedConsDf.billingPeriodEndDate, 
            ),
            how="left"
        ) 
        .select(billedConsDf['*'], dimBillLineItemDf['meterConsumptionBillingLineItemSK'])
    )

    # --- dimContract ---#
    billedConsDf = (
        billedConsDf
        .join(
            dimContractDf, 
            (   # join conditions
                billedConsDf.contractId == dimContractDf.contractId 
                & dimContractDf._RecordStart  <= billedConsDf.billingPeriodEndDate 
                & dimContractDf._RecordEnd >= billedConsDf.billingPeriodEndDate
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
                billedConsDf.businessPartnerGroupNumber == dimBusinessPartnerGroupDf.businessPartnerGroupNumber \
                & dimBusinessPartnerGroupDf._RecordStart  <= billedConsDf.billingPeriodEndDate \
                & dimBusinessPartnerGroupDf._RecordEnd >= billedConsDf.billingPeriodEndDate
            ), 
            how="left"
        ) 
        .select(billedConsDf['*'], dimBusinessPartnerGroupDf['businessPartnerGroupSK'])
    )
    

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
    
    # --- dimMeterConsumptionBillingLineItem --- #
    billedConsDf = (
        billedConsDf
        .join(
            dummyDimRecDf, 
            (dummyDimRecDf.dimension == 'dimMeterConsumptionBillingLineItem'), how="left" 
        )
        .select(billedConsDf['*'], dummyDimRecDf['dummyDimSK'].alias('dummyMeterConsumptionBillingLineItemSK'))
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
    #aggregating to address any duplicates due to failed SK lookups and dummy SKs being assigned in those cases
    billedConsDf = (
        billedConsDf
        .selectExpr(
            "sourceSystemCode",
            "coalesce(meterConsumptionBillingDocumentSK, dummyMeterConsumptionBillingDocumentSK) as meterConsumptionBillingDocumentSK",
            "coalesce(meterConsumptionBillingLineItemSK, dummyMeterConsumptionBillingLineItemSK) as meterConsumptionBillingLineItemSK",
            "coalesce(propertySK, dummyPropertySK) as propertySK",
            "coalesce(deviceSK, dummyDeviceSK) as deviceSK",
            "coalesce(locationSk, dummyLocationSK) as locationSK" ,
            "coalesce(businessPartnerGroupSK, dummyBusinessPartnerGroupSK) as businessPartnerGroupSK",
            "coalesce(contractSK, dummyContractSK) as contractSK",
            "billingPeriodStartDate",
            "billingPeriodEndDate" ,
            "meteredWaterConsumption" ,
        ) 
        .groupby(
            "sourceSystemCode", 
            "meterConsumptionBillingDocumentSK", 
            "meterConsumptionBillingLineItemSK", 
            "propertySK", 
            "meterSK", 
            "locationSK",
            "businessPartnerGroupSK",
            "contractSK", 
            "billingPeriodStartDate"
        ) 
        .agg(
            max("billingPeriodEndDate").alias("billingPeriodEndDate"), 
            sum("meteredWaterConsumption").alias("meteredWaterConsumption")
        ) 
        .selectExpr(
            "sourceSystemCode", 
            "meterConsumptionBillingDocumentSK", 
            "meterConsumptionBillingLineItemSK", 
            "propertySK", 
            "deviceSK", 
            "locationSK",
            "businessPartnerGroupSK", 
            "contractSK", 
            "billingPeriodStartDate", 
            "billingPeriodEndDate", 
            "cast(meteredWaterConsumption as decimal(24,12)) as meteredWaterConsumption" 
        )
    )

    #8.Apply schema definition
    schema = StructType([
        StructField("sourceSystemCode", StringType(), False),
        StructField("meterConsumptionBillingDocumentSK", StringType(), False),
        StructField("meterConsumptionBillingLineItemSK", StringType(), False),
        StructField("propertySK", StringType(), False),
        StructField("deviceSK", StringType(), False),
        StructField("locationSK", StringType(), False),
        StructField("businessPartnerGroupSK", StringType(), False),
        StructField("contractSK", StringType(), False),
        StructField("billingPeriodStartDate", DateType(), False),
        StructField("billingPeriodEndDate", DateType(), False),
        StructField("meteredWaterConsumption", DecimalType(24,12), True)
    ])

    return billedConsDf, schema

# COMMAND ----------

df, schema = getBilledWaterConsumption()
# TemplateEtl(df, entity="factBilledWaterConsumption", businessKey="meterConsumptionBillingDocumentSK,meterConsumptionBillingLineItemSK,propertySK,meterSK,billingPeriodStartDate", schema=schema, writeMode=ADS_WRITE_MODE_MERGE, AddSK=False)

# COMMAND ----------

df = (
    df
    .withColumn("_DLCuratedZoneTimeStamp",current_timestamp().cast("timestamp"))
    .withColumn("_RecordStart",col('_DLCuratedZoneTimeStamp').cast("timestamp"))
    .withColumn("_RecordEnd",lit('9999-12-31 00:00:00').cast("timestamp"))
    .withColumn("_RecordDeleted",lit(0).cast("int"))
    .withColumn("_RecordCurrent",lit(1).cast("int"))
)

if loadConsumption:
    dfAccess = df.filter("sourceSystemCode='ACCESS'")
    dfAccess.write \
      .format("delta") \
      .mode("overwrite") \
      .option("replaceWhere", "sourceSystemCode = 'ACCESS'") \
      .option("overwriteSchema","true").saveAsTable("curated_v2.factBilledWaterConsumption")
    
    dfISU = df.filter("sourceSystemCode='ISU'")
    dfISU.write \
      .format("delta") \
      .mode("overwrite") \
      .option("replaceWhere", "sourceSystemCode = 'ISU'") \
      .option("overwriteSchema","true").saveAsTable("curated_v2.factBilledWaterConsumption")
else:
    df.write \
      .format("delta") \
      .mode("overwrite") \
      .option("replaceWhere", f"sourceSystemCode = '{source_system}'") \
      .option("overwriteSchema","true").saveAsTable("curated_v2.factBilledWaterConsumption")
    
verifyTableSchema(f"curated_v2.factBilledWaterConsumption", schema)

# COMMAND ----------

# %sql
# --THIS IS COMMENTED AND TO BE UNCOMMENTED TO RUN ONLY WHEN ACCESS DATA LOADING USING THIS NOTEBOOK.
# OPTIMIZE curated_v2.factBilledWaterConsumption
# WHERE sourceSystemCode = 'ACCESS'

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE curated_v2.factBilledWaterConsumption
# MAGIC WHERE sourceSystemCode = 'ISU'

# COMMAND ----------

# MAGIC %sql
# MAGIC set spark.databricks.delta.retentionDurationCheck.enabled=false;
# MAGIC VACUUM curated_v2.factBilledWaterConsumption RETAIN 0 HOURS;
# MAGIC set spark.databricks.delta.retentionDurationCheck.enabled=true;

# COMMAND ----------

dbutils.notebook.exit("1")
