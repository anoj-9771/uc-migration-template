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

table_name_fq  = f"{ADS_DATABASE_CURATED}.fact.billedWaterConsumption"

# COMMAND ----------

# %sql
# FOLLOWING TABLE TO BE CREATED MANUALLY FIRST TIME LOADING AFTER THE TABLE CLEANUP
# CREATE TABLE `curated`.`fact`.`BilledWaterConsumption` (
#   `sourceSystemCode` STRING NOT NULL,
#   `meterConsumptionBillingDocumentSK` STRING NOT NULL,
#   `billingDocumentNumber` STRING NOT NULL,
#   `meterConsumptionBillingLineItemSK` STRING NOT NULL,
#   `billingDocumentLineItemId` STRING NOT NULL,
#   `propertySK` STRING NOT NULL,
#   `propertyNumber` STRING NOT NULL,
#   `locationSK` STRING NOT NULL,
#   `locationId` STRING NOT NULL,
#   `deviceSK` STRING NOT NULL,
#   `deviceNumber` STRING NOT NULL,
#   `logicalDeviceNumber` STRING NOT NULL,
#   `logicalRegisterNumber` STRING NOT NULL,
#   `registerNumber` INT NOT NULL,
#   `installationSK` STRING NOT NULL,
#   `installationNumber` STRING NOT NULL,
#   `businessPartnerGroupSK` STRING NOT NULL,
#   `businessPartnerGroupNumber` STRING NOT NULL,
#   `contractSK` STRING NOT NULL,
#   `contractId` STRING NOT NULL,
#   `divisionCode` STRING NOT NULL,
#   `division` STRING NOT NULL,
#   `isReversedFlag` STRING NOT NULL,
#   `isOutsortedFlag` STRING NOT NULL,
#   `billingLineItemBudgetBillingFlag` STRING NOT NULL,
#   `billingPeriodStartDate` DATE NOT NULL,
#   `billingPeriodEndDate` DATE NOT NULL,
#   `meterActiveStartDate` DATE NOT NULL,
#   `meterActiveEndDate` DATE NOT NULL,
#   `meteredWaterConsumption` DECIMAL(24,12),
#   `_DLCuratedZoneTimeStamp` TIMESTAMP NOT NULL,
#   `_RecordStart` TIMESTAMP NOT NULL,
#   `_RecordEnd` TIMESTAMP NOT NULL,
#   `_RecordDeleted` INT NOT NULL,
#   `_RecordCurrent` INT NOT NULL)
# USING delta
# PARTITIONED BY (sourceSystemCode)

# COMMAND ----------

# FOLLOWING COMMAND TO BE RUN MANUALLY FIRST TIME LOADING AFTER THE TABLE CLEANUP. THIS COMMAND WILL CREATE A VIEW stage.access_property_hist
# %run ../common/functions/commonAccessPropertyHistory

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
            "isReversedFlag",
            "isOutsortedFlag",
            "billingLineItemBudgetBillingFlag",
            "divisionCode",
            "division",
            "businessPartnerGroupNumber", 
            "deviceNumber", 
            "logicalDeviceNumber",
            "logicalRegisterNumber",
            "registerNumber",
            "installationNumber",
            "contractId", 
            "billingPeriodStartDate", 
            "billingPeriodEndDate", 
            "meterActiveStartDate",
            "meterActiveEndDate",
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
        FROM {ADS_DATABASE_CURATED}.dim.property
        """
    )

    dimLocationDf = spark.sql(f"""
        SELECT sourceSystemCode, locationSK, locationId, _RecordStart, _RecordEnd 
        FROM {ADS_DATABASE_CURATED}.dim.location
        """
    )

    dimDeviceDf = spark.sql(f"""
        SELECT sourceSystemCode, deviceSK, deviceNumber, _RecordStart, _RecordEnd 
        FROM {ADS_DATABASE_CURATED}.dim.device
        """
    )
    
    dimBillDocDf = spark.sql(f"""
        SELECT sourceSystemCode, meterConsumptionBillingDocumentSK, billingDocumentNumber, _RecordStart, _RecordEnd
        FROM {ADS_DATABASE_CURATED}.dim.meterConsumptionBillingDocument
        """
    )
    
    dimBillLineItemDf = spark.sql(f"""
        SELECT sourceSystemCode, meterConsumptionBillingLineItemSK, billingDocumentNumber, billingDocumentLineItemId, _RecordStart, _RecordEnd
        FROM {ADS_DATABASE_CURATED}.dim.meterConsumptionBillingLineItem
        """
    )

    dimBusinessPartnerGroupDf = spark.sql(f"""
        SELECT sourceSystemCode, businessPartnerGroupSK, businessPartnerGroupNumber, _RecordStart, _RecordEnd 
        FROM {ADS_DATABASE_CURATED}.dim.businessPartnerGroup
        """
    )

    dimContractDf = spark.sql(f"""
        SELECT sourceSystemCode, contractSK, contractId, _RecordStart, _RecordEnd 
        FROM {ADS_DATABASE_CURATED}.dim.contract 
        """
    )

    dimInstallationDf = spark.sql(f"""
        SELECT sourceSystemCode, installationSK, installationNumber, _RecordStart, _RecordEnd
        FROM {ADS_DATABASE_CURATED}.dim.installation
        """
    )
    
    dummyDimRecDf = spark.sql(f"""
    /* Union All Dimensions 'dummy' Records */
    SELECT PropertySk as dummyDimSK, PropertyNumber as dummyDimNumber, 'dimProperty' as dimension 
    FROM {ADS_DATABASE_CURATED}.dim.property 
    WHERE propertyNumber = '-1' UNION
     
    SELECT LocationSk as dummyDimSK, LocationId as dummyDimNumber, 'dimLocation' as dimension 
    FROM {ADS_DATABASE_CURATED}.dim.location 
    WHERE LocationId = '-1' UNION
     
    SELECT deviceSK as dummyDimSK, deviceNumber as dummyDimNumber, 'dimDevice' as dimension 
    FROM {ADS_DATABASE_CURATED}.dim.device 
    WHERE deviceNumber = '-1' UNION
     
    SELECT meterConsumptionBillingDocumentSK as dummyDimSK, billingDocumentNumber as dummyDimNumber, 'dimMeterConsumptionBillingDocument' as dimension 
    FROM {ADS_DATABASE_CURATED}.dim.meterConsumptionBillingDocument 
    WHERE billingDocumentNumber = '-1' UNION
     
    SELECT meterConsumptionBillingLineItemSK as dummyDimSK, billingDocumentLineItemId as dummyDimNumber, 'dimMeterConsumptionBillingLineItem' as dimension 
    FROM {ADS_DATABASE_CURATED}.dim.meterConsumptionBillingLineItem 
    WHERE billingDocumentLineItemId = '-1' UNION
     
    SELECT businessPartnerGroupSK as dummyDimSK, businessPartnerGroupNumber as dummyDimNumber, 'dimBusinessPartnerGroup' as dimension 
    FROM {ADS_DATABASE_CURATED}.dim.businessPartnerGroup 
    WHERE BusinessPartnerGroupNumber = '-1' UNION
     
    SELECT contractSK as dummyDimSK, contractId as dummyDimNumber, 'dimContract' as dimension 
    FROM {ADS_DATABASE_CURATED}.dim.contract 
    WHERE contractId = '-1'  UNION

    SELECT installationSK as dummyDimSK, installationNumber as dummyDimNumber, 'dimInstallation' as dimension 
    FROM {ADS_DATABASE_CURATED}.dim.installation 
    WHERE installationNumber = '-1'
    """
    )


    #5.Joins to derive SKs for Fact load

    # --- dimProperty --- #
    billedConsDf = (
        billedConsDf
        .join(
            dimPropertyDf, 
            (   # join conditions 
                (billedConsDf.businessPartnerGroupNumber == dimPropertyDf.propertyNumber) 
                & (dimPropertyDf._RecordStart  <= billedConsDf.meterActiveEndDate)
                & (dimPropertyDf._RecordEnd >= billedConsDf.meterActiveEndDate)
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
                    & (dimLocationDf._RecordStart  <= billedConsDf.meterActiveEndDate)
                    & (dimLocationDf._RecordEnd >= billedConsDf.meterActiveEndDate)
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
                (billedConsDf.deviceNumber == dimDeviceDf.deviceNumber) 
                & (dimDeviceDf._RecordStart  <= billedConsDf.meterActiveEndDate)
                & (dimDeviceDf._RecordEnd >= billedConsDf.meterActiveEndDate)
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
    
    # --- dimBillLineItem --- #
    billedConsDf = (
        billedConsDf
        .join(
            dimBillLineItemDf,
            (   # join conditions
                (billedConsDf.billingDocumentNumber == dimBillLineItemDf.billingDocumentNumber)
                & (billedConsDf.billingDocumentLineItemId == dimBillLineItemDf.billingDocumentLineItemId) 
                & (dimBillLineItemDf._RecordStart  <= billedConsDf.billingPeriodEndDate) 
                & (dimBillLineItemDf._RecordEnd >= billedConsDf.billingPeriodEndDate) 
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
                (billedConsDf.contractId == dimContractDf.contractId)
                & (dimContractDf._RecordStart  <= billedConsDf.meterActiveEndDate)
                & (dimContractDf._RecordEnd >= billedConsDf.meterActiveEndDate)
            ), 
            how="left"
        ) 
        .select(billedConsDf['*'], dimContractDf['contractSK'])
    )

    # --- dimInstallation ---#
    billedConsDf = (
        billedConsDf
        .join(
            dimInstallationDf, 
            (   # join conditions
                (billedConsDf.installationNumber == dimInstallationDf.installationNumber)
                & (dimInstallationDf._RecordStart  <= billedConsDf.meterActiveEndDate)
                & (dimInstallationDf._RecordEnd >= billedConsDf.meterActiveEndDate)
            ), 
            how="left"
        ) 
        .select(billedConsDf['*'], dimInstallationDf['installationSK'])
    )

    # --- dimBusinessPartnerGroup --- #
    billedConsDf = (
        billedConsDf
        .join(
            dimBusinessPartnerGroupDf, 
            (
                (billedConsDf.businessPartnerGroupNumber == dimBusinessPartnerGroupDf.businessPartnerGroupNumber)
                & (dimBusinessPartnerGroupDf._RecordStart  <= billedConsDf.meterActiveEndDate)
                & (dimBusinessPartnerGroupDf._RecordEnd >= billedConsDf.meterActiveEndDate)
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
        .select(billedConsDf['*'], dummyDimRecDf['dummyDimSK'].alias('dummyPropertySK'), dummyDimRecDf['dummyDimNumber'].alias('dummyPropertyNumber'))
    )

    # --- dimLocation --- #
    billedConsDf = (
        billedConsDf
        .join(
            dummyDimRecDf, 
            (dummyDimRecDf.dimension == 'dimLocation'), how="left"
        ) 
        .select(billedConsDf['*'], dummyDimRecDf['dummyDimSK'].alias('dummyLocationSK'), dummyDimRecDf['dummyDimNumber'].alias('dummyLocationId'))
    )

    # --- dimDevice --- #
    billedConsDf = (
        billedConsDf
        .join(
            dummyDimRecDf, 
            (dummyDimRecDf.dimension == 'dimDevice'), how="left"
        ) 
        .select(billedConsDf['*'], dummyDimRecDf['dummyDimSK'].alias('dummyDeviceSK'), dummyDimRecDf['dummyDimNumber'].alias('dummyDeviceNumber'))
    )

    # --- dimMeterConsumtpionBillingDocument --- #
    billedConsDf = (
        billedConsDf
        .join(
            dummyDimRecDf, 
            (dummyDimRecDf.dimension == 'dimMeterConsumptionBillingDocument'), how="left"
        ) 
        .select(billedConsDf['*'], dummyDimRecDf['dummyDimSK'].alias('dummyMeterConsumptionBillingDocumentSK'), dummyDimRecDf['dummyDimNumber'].alias('dummyBillingDocumentNumber'))
    )
    
    # --- dimMeterConsumptionBillingLineItem --- #
    billedConsDf = (
        billedConsDf
        .join(
            dummyDimRecDf, 
            (dummyDimRecDf.dimension == 'dimMeterConsumptionBillingLineItem'), how="left" 
        )
        .select(billedConsDf['*'], dummyDimRecDf['dummyDimSK'].alias('dummyMeterConsumptionBillingLineItemSK'), dummyDimRecDf['dummyDimNumber'].alias('dummyBillingDocumentLineItemId'))
    )

    # --- dimContract --- #
    billedConsDf = (
        billedConsDf
        .join(
            dummyDimRecDf, 
            (dummyDimRecDf.dimension == 'dimContract'), how="left"
        )
        .select(billedConsDf['*'], dummyDimRecDf['dummyDimSK'].alias('dummyContractSK'), dummyDimRecDf['dummyDimNumber'].alias('dummyContractId'))
    )

    # --- dimInstallation --- #
    billedConsDf = (
        billedConsDf
        .join(
            dummyDimRecDf, 
            (dummyDimRecDf.dimension == 'dimInstallation'), how="left"
        )
        .select(billedConsDf['*'], dummyDimRecDf['dummyDimSK'].alias('dummyInstallationSK'), dummyDimRecDf['dummyDimNumber'].alias('dummyInstallationNumber'))
    )

    # --- dimBusinessPartnerGroup --- #
    billedConsDf = (
        billedConsDf
        .join(
            dummyDimRecDf, 
            (dummyDimRecDf.dimension == 'dimBusinessPartnerGroup'), how="left"
        )
        .select(billedConsDf['*'], dummyDimRecDf['dummyDimSK'].alias('dummyBusinessPartnerGroupSK'), dummyDimRecDf['dummyDimNumber'].alias('dummyBusinessPartnerGroupNumber'))
    )

    #7.SELECT / TRANSFORM
    #aggregating to address any duplicates due to failed SK lookups and dummy SKs being assigned in those cases
    billedConsDf = (
        billedConsDf
        .selectExpr(
            "sourceSystemCode",
            "coalesce(meterConsumptionBillingDocumentSK, dummyMeterConsumptionBillingDocumentSK) as meterConsumptionBillingDocumentSK",
            "coalesce(billingDocumentNumber, dummyBillingDocumentNumber) as billingDocumentNumber",
            "coalesce(meterConsumptionBillingLineItemSK, dummyMeterConsumptionBillingLineItemSK) as meterConsumptionBillingLineItemSK",
            "coalesce(billingDocumentLineItemId, dummyBillingDocumentLineItemId) as billingDocumentLineItemId",
            "coalesce(propertySK, dummyPropertySK) as propertySK",
            "coalesce(businessPartnerGroupNumber, dummyPropertyNumber) as propertyNumber",
            "coalesce(locationSK, dummyLocationSK) as locationSK" ,
            "coalesce(businessPartnerGroupNumber, dummyLocationId) as locationId",
            "coalesce(deviceSK, dummyDeviceSK) as deviceSK",
            "coalesce(deviceNumber, dummyDeviceNumber) as deviceNumber",
            "logicalDeviceNumber",
            "logicalRegisterNumber",
            "registerNumber",
            "coalesce(installationSK, dummyinstallationSK) as installationSK",
            "coalesce(installationNumber, dummyInstallationNumber) as installationNumber",
            "coalesce(businessPartnerGroupSK, dummyBusinessPartnerGroupSK) as businessPartnerGroupSK",
            "coalesce(businessPartnerGroupNumber, dummybusinessPartnerGroupNumber) as businessPartnerGroupNumber",
            "coalesce(contractSK, dummyContractSK) as contractSK",
            "coalesce(contractId, dummyContractId) as contractId",
            "divisionCode",
            "division",
            "isReversedFlag",
            "isOutsortedFlag",
            "billingLineItemBudgetBillingFlag",
            "billingPeriodStartDate",
            "billingPeriodEndDate",
            "meterActiveStartDate",
            "meterActiveEndDate",
            "meteredWaterConsumption"
        ) 
        .groupby(
            "sourceSystemCode", 
            "meterConsumptionBillingDocumentSK", 
            "billingDocumentNumber",
            "meterConsumptionBillingLineItemSK", 
            "billingDocumentLineItemId",
            "propertySK", 
            "propertyNumber",
            "locationSK",
            "locationId",
            "deviceSK", 
            "deviceNumber", 
            "logicalDeviceNumber",
            "logicalRegisterNumber",
            "registerNumber",
            "installationSK",
            "installationNumber",
            "businessPartnerGroupSK",
            "businessPartnerGroupNumber",
            "contractSK",
            "contractId",
            "divisionCode",
            "division",
            "isReversedFlag",
            "isOutsortedFlag",
            "billingLineItemBudgetBillingFlag",
            "billingPeriodStartDate",
            "meterActiveStartDate",
            "meterActiveEndDate"
        ) 
        .agg(
            max("billingPeriodEndDate").alias("billingPeriodEndDate"), 
            sum("meteredWaterConsumption").alias("meteredWaterConsumption")
        ) 
        .selectExpr(
            "sourceSystemCode", 
            "meterConsumptionBillingDocumentSK", 
            "billingDocumentNumber", 
            "meterConsumptionBillingLineItemSK", 
            "billingDocumentLineItemId",
            "propertySK", 
            "propertyNumber", 
            "locationSK",
            "locationId",
            "deviceSK", 
            "deviceNumber", 
            "logicalDeviceNumber",
            "logicalRegisterNumber",
            "registerNumber",
            "installationSK",
            "installationNumber",
            "businessPartnerGroupSK", 
            "businessPartnerGroupNumber",
            "contractSK", 
            "contractId", 
            "divisionCode",
            "division",
            "isReversedFlag",
            "isOutsortedFlag",
            "billingLineItemBudgetBillingFlag",
            "billingPeriodStartDate", 
            "billingPeriodEndDate", 
            "meterActiveStartDate",
            "meterActiveEndDate",
            "cast(meteredWaterConsumption as decimal(24,12)) as meteredWaterConsumption" 
        )
    )


    #8.Apply schema definition
    schema = StructType([
        StructField("sourceSystemCode", StringType(), False),
        StructField("meterConsumptionBillingDocumentSK", StringType(), False),
        StructField("billingDocumentNumber", StringType(), False),
        StructField("meterConsumptionBillingLineItemSK", StringType(), False),
        StructField("billingDocumentLineItemId", StringType(), False),
        StructField("propertySK", StringType(), False),
        StructField("propertyNumber", StringType(), False),
        StructField("locationSK", StringType(), False),
        StructField("locationId", StringType(), False),
        StructField("deviceSK", StringType(), False),
        StructField("deviceNumber", StringType(), False),
        StructField("logicalDeviceNumber",StringType(), False),
        StructField("logicalRegisterNumber",StringType(), False),
        StructField("registerNumber",IntegerType(), False),
        StructField("installationSK", StringType(), False),
        StructField("installationNumber", StringType(), False),
        StructField("businessPartnerGroupSK", StringType(), False),
        StructField("businessPartnerGroupNumber", StringType(), False),
        StructField("contractSK", StringType(), False),
        StructField("contractId", StringType(), False),
        StructField("divisionCode",StringType(), False),
        StructField("division",StringType(), False),
        StructField("isReversedFlag", StringType(), False),
        StructField("isOutsortedFlag", StringType(), False),
        StructField("billingLineItemBudgetBillingFlag",StringType(), False),
        StructField("billingPeriodStartDate", DateType(), False),
        StructField("billingPeriodEndDate", DateType(), False),
        StructField("meterActiveStartDate", DateType(), False),
        StructField("meterActiveEndDate", DateType(), False),
        StructField("meteredWaterConsumption", DecimalType(24,12), True)
    ])

    return billedConsDf, schema

# COMMAND ----------

df, schema = getBilledWaterConsumption()
# TemplateEtl(df, entity="fact.BilledWaterConsumption", businessKey="meterConsumptionBillingDocumentSK,meterConsumptionBillingLineItemSK,propertySK,meterSK,billingPeriodStartDate", schema=schema, writeMode=ADS_WRITE_MODE_MERGE, AddSK=False)

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
      .option("overwriteSchema","true").saveAsTable(table_name_fq)
    
    dfISU = df.filter("sourceSystemCode='ISU'")
    dfISU.write \
      .format("delta") \
      .mode("overwrite") \
      .option("replaceWhere", "sourceSystemCode = 'ISU'") \
      .option("overwriteSchema","true").saveAsTable(table_name_fq)
else:
    df.write \
      .format("delta") \
      .mode("overwrite") \
      .option("replaceWhere", f"sourceSystemCode = '{source_system}'") \
      .option("overwriteSchema","true").saveAsTable(table_name_fq)
    
verifyTableSchema(table_name_fq, schema)

# COMMAND ----------

spark.conf.set("c.table_name", table_name_fq)

# COMMAND ----------

# %sql
# --THIS IS COMMENTED AND TO BE UNCOMMENTED TO RUN ONLY WHEN ACCESS DATA LOADING USING THIS NOTEBOOK.
# OPTIMIZE ${c.table_name}
# WHERE sourceSystemCode = 'ACCESS'

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE ${c.table_name} WHERE sourceSystemCode = 'ISU'

# COMMAND ----------

# MAGIC %sql
# MAGIC set spark.databricks.delta.retentionDurationCheck.enabled=false;
# MAGIC VACUUM ${c.table_name} RETAIN 0 HOURS;
# MAGIC set spark.databricks.delta.retentionDurationCheck.enabled=true;

# COMMAND ----------

dbutils.notebook.exit("1")
