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
    billedConsDf = spark.sql(f"select 'ACCESS' as sourceSystemCode, mr.propertyNumber, dm.meterNumber, \
                                   mr.readingFromDate, mr.readingToDate, mr.meterReadingDays, \
                                   mr.meterReadingConsumption, \
                                   row_number() over (partition by mr.propertyNumber, mr.propertyMeterNumber, mr.readingFromDate \
                                                       order by mr.meterReadingNumber desc) meterReadRecNumFrom, \
                                   row_number() over (partition by mr.propertyNumber, mr.propertyMeterNumber, mr.readingToDate \
                                                       order by mr.meterReadingNumber desc) meterReadRecNumTo \
                              from {ADS_DATABASE_CLEANSED}.access_z309_tmeterreading mr \
                                   inner join {ADS_DATABASE_CLEANSED}.access_z309_tpropmeter pm on pm.propertyNumber = mr.propertyNumber \
                                                                                     and pm.propertyMeterNumber = mr.propertyMeterNumber \
                                   inner join {ADS_DATABASE_CURATED}.dimMeter dm on dm.meterSerialNumber = pm.meterMakerNumber and dm.sourceSystemCode = 'ACCESS' \
                              where mr.meterReadingStatusCode IN ('A','B','P','V') \
                                    and mr.meterReadingDays > 0 \
                                    and not(pm.isCheckMeter) \
                                    and mr._RecordCurrent = 1 and mr._RecordDeleted = 0 \
                                    and pm._RecordCurrent = 1 and pm._RecordDeleted = 0 \
                                    and not exists (select 1 \
                                                    from {ADS_DATABASE_CLEANSED}.access_z309_tdebit dr \
                                                    where mr.propertyNumber = dr.propertyNumber \
                                                    and dr.debitTypeCode = '10' \
                                                    and dr.debitReasonCode IN ('360','367')) \
                                                    --and mr.propertyNumber = '3692184' \
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


