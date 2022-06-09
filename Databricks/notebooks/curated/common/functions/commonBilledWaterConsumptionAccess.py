# Databricks notebook source
#%run ../includes/util-common

# COMMAND ----------

###########################################################################################################################
# Function: getBilledWaterConsumptionAccess
#  GETS Access Billed Water Consumption from cleansed layer
# Returns:
#  Dataframe of transformed Access Billed Water Consumption
#############################################################################################################################
# Method
# 1.Create Function
# 2.Load Cleansed layer table data into dataframe and transform
# 3.JOIN TABLES
# 4.UNION TABLES
# 5.SELECT / TRANSFORM
#############################################################################################################################
#1.Create Function
def getBilledWaterConsumptionAccess():

    spark.udf.register("TidyCase", GeneralToTidyCase)  

    #reusable query to derive the base billed consumption from Access Meter Reading dataset
    #2.Load Cleansed layer table data into dataframe
    #dm.sourceSystemCode is null is there for the -1 case as that dummy row has a null sourcesystem code
    billedConsDf = spark.sql(f"select 'ACCESS' as sourceSystemCode, mr.propertyNumber, coalesce(dm.meterNumber,'-1') as meterNumber, \
                                   mr.readingFromDate, mr.readingToDate, mr.meterReadingDays, \
                                   mr.meterReadingConsumption, coalesce(mts.meterMakerNumber,'Unknowm') as meterMakerNumber, \
                                   row_number() over (partition by mr.propertyNumber, mr.propertyMeterNumber, mr.readingFromDate \
                                                       order by mr.meterReadingNumber desc) meterReadRecNumFrom, \
                                   row_number() over (partition by mr.propertyNumber, mr.propertyMeterNumber, mr.readingToDate \
                                                       order by mr.meterReadingNumber desc) meterReadRecNumTo \
                              from {ADS_DATABASE_CLEANSED}.access_z309_tmeterreading mr \
                                   left outer join {ADS_DATABASE_CURATED}.metertimesliceaccess mts on mts.propertyNumber = mr.propertyNumber \
                                                                                     and mts.propertyMeterNumber = mr.propertyMeterNumber \
                                                                                     and mr.readingToDate between mts.validFrom and mts.validTo \
                                                                                     and mr.readingToDate != mts.validFrom \
                                   left outer join {ADS_DATABASE_CURATED}.dimMeter dm on dm.meterSerialNumber = coalesce(mts.meterMakerNumber,'-1') \
                              where mr.meterReadingStatusCode IN ('A','B','P','V') \
                                    and mr.meterReadingDays > 0 \
                                    and mr.meterReadingConsumption > 0 \
                                    and (not mts.isCheckMeter or mts.isCheckMeter is null) \
                                    and mr._RecordCurrent = 1 \
                                    and mr._RecordDeleted = 0 \
                                    and not exists (select 1 \
                                                    from {ADS_DATABASE_CLEANSED}.access_z309_tdebit dr \
                                                    where mr.propertyNumber = dr.propertyNumber \
                                                    and dr.debitTypeCode = '10' \
                                                    and dr.debitReasonCode IN ('360','367')) \
                                   ")

    billedConsDf = billedConsDf.where("meterReadRecNumFrom = 1 and meterReadRecNumTo = 1")

    #3.JOIN TABLES  

    #4.UNION TABLES

    #5.SELECT / TRANSFORM
    billedConsDf = billedConsDf.selectExpr \
                              ( \
                                 "sourceSystemCode" \
                                ,"propertyNumber" \
                                ,"meterNumber" \
                                ,"readingFromDate as billingPeriodStartDate" \
                                ,"readingToDate as billingPeriodEndDate" \
                                ,"meterReadingDays as billingPeriodDays" \
                                ,"meterReadingConsumption as meteredWaterConsumption" \
                              )

    return billedConsDf

# COMMAND ----------

# ADS_DATABASE_CLEANSED = 'cleansed'
# ADS_DATABASE_CURATED = 'curated'
# df = getBilledWaterConsumptionAccess()
# df.createOrReplaceTempView('accs')
