# Databricks notebook source
def getBilledWaterConsumptionAccess():

    spark.udf.register("TidyCase", GeneralToTidyCase)  

    #reusable query to derive the base billed consumption from Access Meter Reading dataset
    #2.Load Cleansed layer table data into dataframe
    #dm.sourceSystemCode is null is there for the -1 case as that dummy row has a null sourcesystem code
    billedConsDf = spark.sql(f"""
                             select 
                              'ACCESS' as sourceSystemCode, 
                              mr.propertyNumber, 
                              mr.meterReadingNumber, 
                              mr.propertyMeterNumber, 
                              concat(mr.propertyNumber,'-',mr.propertyMeterNumber) as deviceNumber, 
                              mr.readingFromDate as billingPeriodStartDate, 
                              mr.readingToDate as billingPeriodEndDate, 
                              mr.meterReadingDays as billingPeriodDays,
                              mr.meterReadingConsumption as meteredWaterConsumption, 
                              row_number() over (partition by mr.propertyNumber, mr.propertyMeterNumber, mr.readingFromDate 
                                order by mr.meterReadingNumber desc, mr.meterReadingUpdatedDate desc) meterReadRecNumFrom,
                              row_number() over (partition by mr.propertyNumber, mr.propertyMeterNumber, mr.readingToDate 
                                order by mr.meterReadingNumber desc, mr.meterReadingUpdatedDate desc) meterReadRecNumTo 
                              from {ADS_DATABASE_CLEANSED}.access_z309_tmeterreading mr 
                                left join {ADS_DATABASE_CLEANSED}.access_meterTimeslice mts 
                                  on mts.propertyNumber = mr.propertyNumber 
                                  and mts.propertyMeterNumber = mr.propertyMeterNumber 
                                  and mr.readingToDate between mts.validFrom and mts.validTo  
                                  and mr.readingToDate != mts.validFrom   
                                left outer join {ADS_DATABASE_CLEANSED}.access_z309_tdebit dr 
                                  on mr.propertyNumber = dr.propertyNumber 
                                  and dr.debitTypeCode = '10' 
                                  and dr.debitReasonCode IN ('360','367')
                                where mr.meterReadingStatusCode IN ('A','B','P','V')  --147,272,783
                                                                and mr.meterReadingDays > 0 
                                                                and mr.meterReadingConsumption > 0 
                                                                and mr.readingFromDate >= '1990-07-01' 
                                                                and mr.readingToDate >= '1991-01-01' 
                                                                and (not mts.isCheckMeter or mts.isCheckMeter is null) 
                                                                and mr._RecordCurrent = 1 
                                                                and mr._RecordDeleted = 0 
                                                                and dr.propertyNumber is null 
                                   """)

    billedConsDf = billedConsDf.where("meterReadRecNumFrom = 1 and meterReadRecNumTo = 1")
    billedConsDf.createOrReplaceTempView('accs') #leave this here for testing
    
    #3.JOIN TABLES  

    #4.UNION TABLES

    #5.SELECT / TRANSFORM
    billedConsDf = billedConsDf.selectExpr \
                              ( \
                                 "sourceSystemCode" \
                                ,"propertyNumber" \
                                ,"deviceNumber" \
                                ,"billingPeriodStartDate" \
                                ,"billingPeriodEndDate" \
                                ,"billingPeriodDays" \
                                ,"meteredWaterConsumption" \
                              )

    return billedConsDf

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC select 
# MAGIC   'ACCESS' as sourceSystemCode, 
# MAGIC   mr.propertyNumber, 
# MAGIC   mr.meterReadingNumber, 
# MAGIC   mr.propertyMeterNumber, 
# MAGIC   coalesce(dm.deviceNumber,'-1') as deviceNumber, 
# MAGIC   mr.readingFromDate as billingPeriodStartDate, 
# MAGIC   mr.readingToDate as billingPeriodEndDate, 
# MAGIC   mr.meterReadingDays as billingPeriodDays,
# MAGIC   mr.meterReadingConsumption as meteredWaterConsumption, 
# MAGIC   coalesce(mts.meterMakerNumber,'Unknown') as meterMakerNumber,
# MAGIC   row_number() over (partition by mr.propertyNumber, mr.propertyMeterNumber, mr.readingFromDate 
# MAGIC     order by mr.meterReadingNumber desc, mr.meterReadingUpdatedDate desc, dm.deviceNumber asc) meterReadRecNumFrom,
# MAGIC   row_number() over (partition by mr.propertyNumber, mr.propertyMeterNumber, mr.readingToDate 
# MAGIC     order by mr.meterReadingNumber desc, mr.meterReadingUpdatedDate desc, dm.deviceNumber asc) meterReadRecNumTo 
# MAGIC   from cleansed.access_z309_tmeterreading mr --156365233
# MAGIC     left join cleansed.access_meterTimeslice mts --156,379,127
# MAGIC       on mts.propertyNumber = mr.propertyNumber 
# MAGIC       and mts.propertyMeterNumber = mr.propertyMeterNumber 
# MAGIC       and mr.readingToDate between mts.validFrom and mts.validTo  --??
# MAGIC       and mr.readingToDate != mts.validFrom   --??
# MAGIC     left join curated_v2.dimDevice dm  --?? -- 165,541,437
# MAGIC       on dm.deviceId = coalesce(mts.meterMakerNumber,'-1') and dm.sourceSystemCode = 'ACCESS'
# MAGIC     left outer join cleansed.access_z309_tdebit dr --165,615,246
# MAGIC       on mr.propertyNumber = dr.propertyNumber 
# MAGIC       and dr.debitTypeCode = '10' 
# MAGIC       and dr.debitReasonCode IN ('360','367')
# MAGIC     where mr.meterReadingStatusCode IN ('A','B','P','V')  --147,272,783
# MAGIC                                     and mr.meterReadingDays > 0 
# MAGIC                                     and mr.meterReadingConsumption > 0 
# MAGIC                                     and mr.readingFromDate >= '1990-07-01' 
# MAGIC                                     and mr.readingToDate >= '1991-01-01' 
# MAGIC                                     and (not mts.isCheckMeter or mts.isCheckMeter is null) 
# MAGIC                                     and mr._RecordCurrent = 1 
# MAGIC                                     and mr._RecordDeleted = 0 
# MAGIC                                     and dr.propertyNumber is null 
# MAGIC   
