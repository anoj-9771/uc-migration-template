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
    billedConsDf = spark.sql(f"select 'ACCESS' as sourceSystemCode, mr.propertyNumber, mr.meterReadingNumber, mr.propertyMeterNumber, \
                                   coalesce(dm.meterNumber,'-1') as meterNumber, \
                                   mr.readingFromDate as billingPeriodStartDate, mr.readingToDate as billingPeriodEndDate, mr.meterReadingDays as billingPeriodDays, \
                                   mr.meterReadingConsumption as meteredWaterConsumption, coalesce(mts.meterMakerNumber,'Unknown') as meterMakerNumber, \
                                   row_number() over (partition by mr.propertyNumber, mr.propertyMeterNumber, mr.readingFromDate \
                                                       order by mr.meterReadingNumber desc, mr.meterReadingUpdatedDate desc, dm.meterNumber asc) meterReadRecNumFrom, \
                                   row_number() over (partition by mr.propertyNumber, mr.propertyMeterNumber, mr.readingToDate \
                                                       order by mr.meterReadingNumber desc, mr.meterReadingUpdatedDate desc, dm.meterNumber asc) meterReadRecNumTo \
                              from {ADS_DATABASE_CLEANSED}.access_z309_tmeterreading mr \
                                   left outer join {ADS_DATABASE_CURATED}.metertimesliceaccess mts on mts.propertyNumber = mr.propertyNumber \
                                                                                     and mts.propertyMeterNumber = mr.propertyMeterNumber \
                                                                                     and mr.readingToDate between mts.validFrom and mts.validTo \
                                                                                     and mr.readingToDate != mts.validFrom \
                                   left outer join {ADS_DATABASE_CURATED}.dimMeter dm on dm.meterSerialNumber = coalesce(mts.meterMakerNumber,'-1') \
                                   left outer join {ADS_DATABASE_CLEANSED}.access_z309_tdebit dr on mr.propertyNumber = dr.propertyNumber \
                                                   and dr.debitTypeCode = '10' and dr.debitReasonCode IN ('360','367') \
                              where mr.meterReadingStatusCode IN ('A','B','P','V') \
                                    and mr.meterReadingDays > 0 \
                                    and mr.meterReadingConsumption > 0 \
                                    and mr.readingFromDate >= '1990-07-01' \
                                    and mr.readingToDate >= '1991-01-01' \
                                    and (not mts.isCheckMeter or mts.isCheckMeter is null) \
                                    and mr._RecordCurrent = 1 \
                                    and mr._RecordDeleted = 0 \
                                    and dr.propertyNumber is null \
                                   ")

    billedConsDf = billedConsDf.where("meterReadRecNumFrom = 1 and meterReadRecNumTo = 1")
    billedConsDf.createOrReplaceTempView('accs') #leave this here for testing
    
    #3.JOIN TABLES  

    #4.UNION TABLES

    #5.SELECT / TRANSFORM
    billedConsDf = billedConsDf.selectExpr \
                              ( \
                                 "sourceSystemCode" \
                                ,"propertyNumber" \
                                ,"meterNumber" \
                                ,"billingPeriodStartDate" \
                                ,"billingPeriodEndDate" \
                                ,"billingPeriodDays" \
                                ,"meteredWaterConsumption" \
                              )

    return billedConsDf

# COMMAND ----------

# DBTITLE 1,Test Execution only
# ADS_DATABASE_CLEANSED = 'cleansed'
# ADS_DATABASE_CURATED = 'curated'
# df = getBilledWaterConsumptionAccess()

# COMMAND ----------

# DBTITLE 1,Verify no consumption records finds more than one meter (must return no results)
# %sql
# select propertyNumber, billingPeriodstartDate, billingPeriodEndDate, meterMakerNumber, count(*) from accs
# group by propertyNumber, billingPeriodstartDate, billingPeriodEndDate, meterMakerNumber
# having count(*) > 1

# COMMAND ----------

# DBTITLE 1,Check meters without known meter maker number (should be based on result in CreateMeterTimeSliceAccess and a very low number)
# %sql
# select * from accs
# where meterMakerNumber = 'Unknown'


# COMMAND ----------

# %sql
# select *
# from cleansed.access_z309_tpropmeter
# where propertyNumber = 5591530

# COMMAND ----------

# %sql
# select *
# from curated.metertimesliceaccess
# where propertyNumber = 3654983

# COMMAND ----------

# %sql
# select * from accs
# where propertyNumber = 5327438


# COMMAND ----------


