# Databricks notebook source
#%run ../includes/util-common

# COMMAND ----------

# DBTITLE 1,ACCESS Meter Time Slice
# MAGIC %sql
# MAGIC create or replace view vw_ACCESS_MeterTimeSlice as 
# MAGIC with t1 as(
# MAGIC         --grab just the last row from history for a particular date
# MAGIC         SELECT propertyNumber, propertyMeterNumber, rowSupersededDate, meterSize, meterFittedDate, meterRemovedDate, meterMakerNumber, meterClass, meterCategory, 
# MAGIC                 coalesce(meterGroup,  'Normal Reading') as meterGroup, isCheckMeter,
# MAGIC                 row_number() over (partition by propertyNumber, propertyMeterNumber, metermakernumber, meterfitteddate, rowSupersededDate order by rowSupersededTime desc) as rn 
# MAGIC         FROM cleansed.access_Z309_THPROPMETER),
# MAGIC         --merge history with current, group by identical attributes
# MAGIC      t2 as(
# MAGIC         SELECT propertyNumber, propertyMeterNumber, meterMakerNumber, propertyMeterUpdatedDate as validFrom, to_date('99991231','yyyyMMdd') as validTo, meterSize, 
# MAGIC                 meterFittedDate, meterRemovedDate,  meterClass, meterCategory, meterGroup, isCheckMeter
# MAGIC         FROM cleansed.access_Z309_TPROPMETER
# MAGIC         union all
# MAGIC         SELECT propertyNumber, propertyMeterNumber, meterMakerNumber, null as validFrom, min(rowSupersededDate) as validto, meterSize,
# MAGIC                 meterFittedDate, meterRemovedDate, meterClass, meterCategory, meterGroup, isCheckMeter 
# MAGIC         FROM t1
# MAGIC         where rn = 1
# MAGIC         group by propertyNumber, propertyMeterNumber, meterSize, meterFittedDate, meterRemovedDate, meterMakerNumber, meterClass, meterCategory, meterGroup, isCheckMeter),
# MAGIC         --group by identical attributes, consider meterFittedDate as validFrom (will correct after) 
# MAGIC      t3 as(
# MAGIC         select propertyNumber, propertyMeterNumber, meterMakerNumber, min(validFrom) as validFrom, max(validto) as validto, meterSize, min(meterFittedDate) as meterFittedDate, 
# MAGIC                 max(meterRemovedDate) as meterRemovedDate, meterClass, meterCategory, meterGroup, isCheckMeter from t2
# MAGIC         group by propertyNumber,propertyMeterNumber, meterMakerNumber, meterFittedDate, meterSize, meterClass, meterCategory, meterGroup, isCheckMeter),
# MAGIC         --change validTo the earliest of validTo and meterRemovedDate
# MAGIC      t4 as(
# MAGIC         select propertyNumber, propertyMeterNumber, meterMakerNumber, 
# MAGIC                 coalesce(validFrom, lag(validTo,1) over (partition by propertyNumber, propertyMeterNumber, meterMakerNumber order by validTo)) as validFrom, 
# MAGIC                 coalesce(meterRemovedDate,validto) as validto, meterSize, meterFittedDate, meterRemovedDate, 
# MAGIC                 meterClass, meterCategory, meterGroup, isCheckMeter, row_number() over (partition by propertyNumber, propertyMeterNumber, meterMakerNumber order by validTo) as rn
# MAGIC         from t3),
# MAGIC         -- from date for the first row in a set is the minimum of meter fit and validFrom date
# MAGIC      t5 as(
# MAGIC         select propertyNumber, propertyMeterNumber, meterMakerNumber, least(validFrom, meterFittedDate) as validFrom, validTo, meterSize, meterFittedDate, meterRemovedDate, 
# MAGIC                 meterClass, meterCategory, meterGroup, isCheckMeter
# MAGIC         from t4
# MAGIC         where rn = 1
# MAGIC         union all
# MAGIC         select propertyNumber, propertyMeterNumber, meterMakerNumber, validFrom, validTo, meterSize, meterFittedDate, meterRemovedDate, 
# MAGIC                 meterClass, meterCategory, meterGroup, isCheckMeter
# MAGIC         from t4
# MAGIC         where rn > 1)
# MAGIC select * 
# MAGIC from t5
# MAGIC order by propertymeterNumber, validTo, meterMakernumber

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
                                   mr.meterReadingConsumption, coalesce(mts.meterMakerNumber,'0') as meterMakerNumber, \
                                   row_number() over (partition by mr.propertyNumber, mr.propertyMeterNumber, mr.readingFromDate \
                                                       order by mr.meterReadingNumber desc) meterReadRecNumFrom, \
                                   row_number() over (partition by mr.propertyNumber, mr.propertyMeterNumber, mr.readingToDate \
                                                       order by mr.meterReadingNumber desc) meterReadRecNumTo \
                              from {ADS_DATABASE_CLEANSED}.access_z309_tmeterreading mr \
                                   left outer join vw_ACCESS_MeterTimeSlice mts on mts.propertyNumber = mr.propertyNumber \
                                                                                     and mts.propertyMeterNumber = mr.propertyMeterNumber \
                                                                                     and mr.readingToDate between mts.validFrom and mts.validTo \
                                                                                     and mr.readingToDate != mts.validFrom \
                                   left outer join {ADS_DATABASE_CURATED}.dimMeter dm on dm.meterSerialNumber = coalesce(mts.meterMakerNumber,'0') and dm.sourceSystemCode = 'ACCESS' \
                              where mr.meterReadingStatusCode IN ('A','B','P','V') \
                                    and mr.meterReadingDays > 0 \
                                    and mr.meterReadingConsumption > 0 \
                                    and (not mts.isCheckMeter or isCheckMeter ) \
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


