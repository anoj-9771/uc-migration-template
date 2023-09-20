# Databricks notebook source
# MAGIC %md
# MAGIC <b>Note that this notebook should be run via the /cleansed/ACCESS Utils/Create Missing Meters notebook</b>

# COMMAND ----------

# MAGIC %run ../../includes/include-all-util

# COMMAND ----------

def getmeterTimesliceAccess():
    
    #1.Load current Cleansed layer table data into dataframe
        #t1: take history rows from the cleansed table, set removed date to removed date from the current table, if present. 
        #t2: join history and current together. Only take the last row for a date for history
        #t3: assign row number to meterFittedDate so we can get the eariest one, set validfrom date
        #t4: adjust validFrom to meterFittedDate if required
        #t5: calculate the validTo date based on date updated of the next record. Set end date to meter removed date or infinite
        #t6: remove the rows where the validFrom date is greater than or equal to the validTo date (happens when row updated more than once on the same day or meter was incorrectly fitted)
        #t7: collapse adjoining date ranges
        #at the end perform the meter change code change from ACCESS values to ISU values. The code tables have already been verified to carry the same descriptions
#         where exists (select 1 \
#                                     from {ADS_DATABASE_CLEANSED}.access.z309_tpropmeter_cleansed pm \
#                                     where pm.propertyNumber = hpm.propertyNumber and pm.propertyMeterNumber = hpm.propertyMeterNumber and pm.meterMakerNumber = hpm.meterMakerNumber) \
    df = spark.sql(" \
            with history as( \
                      SELECT 'HISTORY' as src, hpm.propertyNumber, hpm.propertyMeterNumber, hpm.meterSizeCode, hpm.meterSize, hpm.meterReadingFrequencyCode, coalesce(pm.meterFittedDate,hpm.meterFittedDate) as meterFittedDate, \
                              if(pm.meterFittedDate is not null and pm.meterRemovedDate is null, null, coalesce(hpm.meterRemovedDate,pm.meterRemovedDate)) as meterRemovedDate, \
                              hpm.meterMakerNumber, hpm.meterChangeReasonCode, hpm.meterExchangeReason, hpm.meterClassCode, hpm.meterClass, hpm.meterCategoryCode, hpm.meterCategory, hpm.meterGroupCode, hpm.meterGroup, \
                              hpm.isMasterMeter, hpm.isCheckMeter, hpm.allowAlso, hpm.waterMeterType, hpm.propertyMeterUpdatedDate, hpm.rowSupersededDate, \
                              row_number() over (partition by hpm.propertyNumber, hpm.propertyMeterNumber, hpm.meterSize, hpm.meterFittedDate, hpm.meterMakerNumber, hpm.meterChangeReasonCode, hpm.meterExchangeReason, \
                              hpm.meterClass, hpm.meterCategory, hpm.meterGroup, hpm.isMasterMeter, hpm.isCheckMeter, hpm.waterMeterType, hpm.rowSupersededDate order by hpm.rowSupersededTime desc) as rn_latestUpdate \
                      FROM {ADS_DATABASE_CLEANSED}.access.Z309_thpropmeter hpm \
                           left outer join {ADS_DATABASE_CLEANSED}.access.z309_tpropmeter_cleansed pm on hpm.propertyNumber = pm.propertyNumber and hpm.propertyMeterNumber = pm.propertyMeterNumber and hpm.meterMakerNumber = pm.meterMakerNumber \
                      ), \
                 nextMeterFitted as( \
                     select distinct propertyNumber, propertyMeterNumber, meterMakerNumber, meterfittedDate, \
                            max(meterFittedDate) over (partition by propertyNumber, propertyMeterNumber order by meterFittedDate rows between 1 following and 1 following) as nextMeterFitted \
                     from   (select propertyNumber, propertyMeterNumber, meterMakerNumber, meterFittedDate \
                             from   {ADS_DATABASE_CLEANSED}.access.Z309_thpropmeter \
                             union \
                             select propertyNumber, propertyMeterNumber, meterMakerNumber, meterFittedDate \
                             from   {ADS_DATABASE_CLEANSED}.access.Z309_tpropmeter \
                             ) as t \
                     ), \
                 bestRemovalDates as( \
                      select a.propertyNumber, a.propertyMeterNumber, a.meterMakerNumber, coalesce(a.meterRemovedDate, \
                                                                                                   mf.nextMeterFitted, \
                                                                                                   (select max(rowSupersededDate) \
                                                                                                    from   history b \
                                                                                                    where  b.propertyNumber = a.propertyNumber \
                                                                                                    and    b.propertyMeterNumber = a.propertyMeterNumber \
                                                                                                    and    b.meterMakerNumber = a.meterMakerNumber)) as newMeterRemovedDate \
                      from   history a, nextMeterFitted mf \
                      where  not exists (select 1 \
                                         from   {ADS_DATABASE_CLEANSED}.access.z309_tpropmeter_cleansed pm \
                                         where  pm.propertyNumber = a.propertyNumber \
                                         and    pm.propertyMeterNumber = a.propertyMeterNumber \
                                         and    pm.meterMakerNumber = a.meterMakerNumber) \
                      and    mf.propertyNumber = a.propertyNumber \
                      and    mf.propertyMeterNumber = a.propertyMeterNumber \
                      and    mf.meterMakerNumber = a.meterMakerNumber \
                      ), \
                 t2 as( \
                      select distinct dataSource as src, pm.propertyNumber, pm.propertyMeterNumber, meterSizeCode, meterSize, pm.meterReadingFrequencyCode, pm.meterFittedDate, \
                             if(dataSource = 'BI',coalesce(meterRemovedDate,mf.nextMeterFitted,to_date('2018-06-15')),meterRemovedDate) as meterRemovedDate, \
                             pm.meterMakerNumber, pm.meterChangeReasonCode, pm.meterExchangeReason, meterClassCode, meterClass, meterCategoryCode, meterCategory, meterGroupCode, \
                             meterGroup, isMasterMeter, isCheckMeter, allowAlso, waterMeterType, propertyMeterUpdatedDate, \
                             row_number() over (partition by pm.propertyNumber, pm.propertyMeterNumber, pm.meterFittedDate order by dataSource) as rn_source \
                      from   {ADS_DATABASE_CLEANSED}.access.z309_tpropmeter pm, \
                             nextMeterFitted mf \
                      where  mf.propertyNumber = pm.propertyNumber \
                      and    mf.propertyMeterNumber = pm.propertyMeterNumber \
                      and    mf.meterMakerNumber = pm.meterMakerNumber \
                      union all \
                      select src, t1.propertyNumber, t1.propertyMeterNumber, t1.meterSizeCode, t1.meterSize, t1.meterReadingFrequencyCode, t1.meterFittedDate, \
                             coalesce(t1.meterRemovedDate,t1a.newMeterRemovedDate) as meterRemovedDate, t1.meterMakerNumber, t1.meterChangeReasonCode, t1.meterExchangeReason, \
                             t1.meterClassCode, t1.meterClass, t1.meterCategoryCode, t1.meterCategory, t1.meterGroupCode, t1.meterGroup, t1.isMasterMeter, t1.isCheckMeter, t1.allowAlso, t1.waterMeterType, t1.propertyMeterUpdatedDate, 1 \
                      from history t1 left outer join bestRemovalDates t1a on t1a.propertyNumber = t1.propertyNumber \
                                                 and t1a.propertyMeterNumber = t1.propertyMeterNumber \
                                                 and t1a.meterMakerNumber = t1.meterMakerNumber \
                      where  rn_latestUpdate = 1 \
                      ), \
                 t3 as( \
                      select distinct src, propertyNumber, propertyMeterNumber, meterSizeCode, meterSize, meterReadingFrequencyCode, meterFittedDate, meterRemovedDate, meterMakerNumber, meterChangeReasonCode, meterExchangeReason, \
                             meterClassCode, meterClass, meterCategoryCode, meterCategory, meterGroupCode, meterGroup, isMasterMeter, isCheckMeter, allowAlso, waterMeterType, propertyMeterUpdatedDate, \
                             case when propertyMeterUpdatedDate > meterFittedDate then propertyMeterUpdatedDate else meterFittedDate end as validFrom, \
                             row_number() over (partition by propertyNumber, propertyMeterNumber, meterMakerNumber order by meterFittedDate, propertyMeterUpdatedDate) as rn_updateOrder \
                      from t2 \
                      where rn_source = 1 \
                      and   src != 'BI' \
                      or    (src = 'BI' \
                      and    not exists (select 1 \
                                         from   t2 t2a \
                                         where  t2a.propertyNumber = t2.propertyNumber \
                                         and    t2a.propertyMeterNumber = t2.propertyMeterNumber \
                                         and    t2a.src = 'ACCESS' \
                                         and    t2a.meterFittedDate <= t2.meterFittedDate \
                                         and    (t2a.meterRemovedDate is null \
                                         or      t2a.meterRemovedDate > t2.meterRemovedDate))) \
                      ), \
                 t4 as( \
                      select t3.propertyNumber, t3.propertyMeterNumber, t3.meterSizeCode, t3.meterSize, t3.meterReadingFrequencyCode, t3.meterFittedDate, \
                             meterRemovedDate, t3.meterMakerNumber, t3.meterChangeReasonCode, t3.meterExchangeReason, \
                             meterClassCode, meterClass, meterCategoryCode, meterCategory, meterGroupCode, meterGroup, isMasterMeter, isCheckMeter, allowAlso, waterMeterType, rn_updateOrder, \
                             mf.meterFittedDate as validFrom \
                      from   t3, nextMeterFitted mf \
                      where  mf.propertyNumber = t3.propertyNumber \
                      and    mf.propertyMeterNumber = t3.propertyMeterNumber \
                      and    mf.meterMakerNumber = t3.meterMakerNumber \
                      and    rn_updateOrder = 1 \
                      union all \
                      select propertyNumber, propertyMeterNumber, meterSizeCode, meterSize, meterReadingFrequencyCode, meterFittedDate, \
                             meterRemovedDate, meterMakerNumber, meterChangeReasonCode, meterExchangeReason, \
                             meterClassCode, meterClass, meterCategoryCode, meterCategory, meterGroupCode, meterGroup, isMasterMeter, isCheckMeter, allowAlso, waterMeterType, rn_updateOrder, validFrom \
                      from   t3 \
                      where  rn_updateOrder > 1 \
                      ), \
                 t5 as( \
                      select distinct propertyNumber, propertyMeterNumber, meterSizeCode, meterSize, meterReadingFrequencyCode, meterFittedDate, meterRemovedDate, meterMakerNumber, meterChangeReasonCode, meterExchangeReason, \
                             meterClassCode, meterClass, \
                             meterCategoryCode, meterCategory, meterGroupCode, meterGroup, isMasterMeter, isCheckMeter, allowAlso, waterMeterType, validFrom, \
                             least(coalesce( \
                                      date_add( \
                                         lag(validFrom,1) over (partition by propertyNumber, propertyMeterNumber, meterMakerNumber order by validFrom desc),-1), \
                                      least(meterRemovedDate,to_date('99991231','yyyyMMdd'))), meterRemovedDate) as validTo \
                      from t4 \
                      ), \
                 t6 as( \
                      select propertyNumber, propertyMeterNumber, meterSizeCode, meterSize, meterReadingFrequencyCode, meterFittedDate, meterRemovedDate, meterMakerNumber, meterChangeReasonCode, meterExchangeReason, \
                             meterClassCode, meterClass, \
                             meterCategoryCode, meterCategory, meterGroupCode, meterGroup, isMasterMeter, isCheckMeter, allowAlso, waterMeterType, validFrom, validTo \
                      from   t5 \
                      where  validFrom < validTo \
                      ), \
                 t7 as( \
                      SELECT \
                          propertyNumber, propertyMeterNumber, meterSizeCode, meterSize, meterReadingFrequencyCode, meterFittedDate, meterRemovedDate, meterMakerNumber, \
                          meterChangeReasonCode, meterExchangeReason, meterClassCode, meterClass, meterCategoryCode, meterCategory, meterGroupCode, meterGroup, isMasterMeter, isCheckMeter, allowAlso, waterMeterType, \
                          MIN(validFrom) as validFrom, \
                          max(validTo) as validTo \
                      FROM ( \
                          SELECT \
                              propertyNumber, propertyMeterNumber, meterSizeCode, meterSize, meterReadingFrequencyCode, meterFittedDate, meterRemovedDate, meterMakerNumber, \
                              meterChangeReasonCode, meterExchangeReason, meterClassCode, meterClass, \
                              meterCategoryCode, meterCategory, meterGroupCode, meterGroup, isMasterMeter, isCheckMeter, allowAlso, waterMeterType, validFrom, validTo, \
                              DATEADD( \
                                  DAY, \
                                  -COALESCE( \
                                      SUM(DATEDIFF(DAY, validFrom, validTo) +1) OVER (PARTITION BY propertyNumber, propertyMeterNumber, meterSize, meterFittedDate, \
                                      meterRemovedDate, meterMakerNumber, meterClass, meterCategory, \
                                        meterGroup, isMasterMeter, isCheckMeter, waterMeterType ORDER BY validTo ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), \
                                      0 \
                                  ), \
                                  validFrom \
                          ) as grp \
                          FROM t6 \
                      ) withGroup \
                      GROUP BY propertyNumber, propertyMeterNumber, meterSizeCode, meterSize, meterReadingFrequencyCode, meterFittedDate, meterRemovedDate, meterMakerNumber, \
                             meterChangeReasonCode, meterExchangeReason, meterClassCode, meterClass, \
                             meterCategoryCode, meterCategory, meterGroupCode, meterGroup, isMasterMeter, isCheckMeter, allowAlso, waterMeterType, grp \
                      ORDER BY propertyNumber, propertyMeterNumber, meterSizeCode, meterSize, meterReadingFrequencyCode, meterFittedDate, meterRemovedDate, meterMakerNumber, \
                             meterChangeReasonCode, meterExchangeReason, meterClassCode, meterClass, \
                             meterCategoryCode, meterCategory, meterGroupCode, meterGroup, isMasterMeter, isCheckMeter, allowAlso, waterMeterType, validFrom \
                      ) \
            select propertyNumber, propertyMeterNumber, meterSizeCode, meterSize, meterReadingFrequencyCode, meterFittedDate, meterRemovedDate, meterMakerNumber, \
                   case when meterChangeReasonCode = '11' then '09' \
                        when meterChangeReasonCode = '13' then '10' \
                        when meterChangeReasonCode = '14' then '05' \
                        when meterChangeReasonCode = '15' then '11' \
                        when meterChangeReasonCode = '17' then '13' \
                        when meterChangeReasonCode = '19' then '14' \
                        when meterChangeReasonCode = '20' then '15' \
                        when meterChangeReasonCode = '22' then '16' \
                        when meterChangeReasonCode = '26' then '19' \
                        when meterChangeReasonCode = '27' then '20' \
                        when meterChangeReasonCode = '28' then '21' \
                        when meterChangeReasonCode = '29' then '22' \
                             else meterChangeReasonCode \
                   end as meterChangeReasonCode, \
                   meterExchangeReason, meterClassCode, meterClass, \
                   meterCategoryCode, meterCategory, meterGroupCode, meterGroup, isMasterMeter, isCheckMeter, allowAlso, waterMeterType, MIN(validFrom) as validFrom, max(validTo) as validTo \
            from   t7 \
            group by propertyNumber, propertyMeterNumber, meterSizeCode, meterSize, meterReadingFrequencyCode, meterFittedDate, meterRemovedDate, meterMakerNumber, meterChangeReasonCode, meterExchangeReason, meterClassCode, meterClass, \
                             meterCategoryCode, meterCategory, meterGroupCode, meterGroup, isMasterMeter, isCheckMeter, allowAlso, waterMeterType \
            except \
            select propertyNumber, propertyMeterNumber, meterSizeCode, meterSize, meterReadingFrequencyCode, meterFittedDate, meterRemovedDate, meterMakerNumber, meterChangeReasonCode, meterExchangeReason, meterClassCode, meterClass, \
                             meterCategoryCode, meterCategory, meterGroupCode, meterGroup, isMasterMeter, isCheckMeter, allowAlso, waterMeterType, min(validFrom) as validFrom, max(validTo) as validTo \
            from t7 \
            where exists(select 1 \
                         from   t7 t7a \
                         where  t7a.propertyNumber = t7.propertyNumber \
                         and    t7a.propertyMeterNumber = t7.propertyMeterNumber \
                         and    t7a.meterMakerNumber != t7.meterMakerNumber \
                         and    t7.meterFittedDate between t7a.meterFittedDate and coalesce(t7a.meterRemovedDate,'9999-12-31') \
                         and    t7.meterRemovedDate between t7a.meterFittedDate and coalesce(t7a.meterRemovedDate,'9999-12-31')) \
            GROUP BY propertyNumber, propertyMeterNumber, meterSizeCode, meterSize, meterReadingFrequencyCode, meterFittedDate, meterRemovedDate, meterMakerNumber, meterChangeReasonCode, meterExchangeReason, meterClassCode, meterClass, \
                             meterCategoryCode, meterCategory, meterGroupCode, meterGroup, isMasterMeter, isCheckMeter, allowAlso, waterMeterType \
            order  by propertyNumber, propertyMeterNumber, validFrom \
     ")
    #2.SELECT / TRANSFORM
    df = df.selectExpr( \
                        'propertyNumber' \
                        ,'propertyMeterNumber' \
                        ,'meterMakerNumber' \
                        ,'meterFittedDate' \
                        ,'meterRemovedDate' \
                        ,'validFrom' \
                        ,'validTo' \
                        ,'meterChangeReasonCode' \
                        ,'meterExchangeReason' \
                        ,'meterSizeCode' \
                        ,'meterSize' \
                        ,'meterReadingFrequencyCode' \
                        ,'meterClassCode' \
                        ,'meterClass' \
                        ,'meterCategoryCode' \
                        ,'meterCategory' \
                        ,'meterGroupCode' \
                        ,'meterGroup' \
                        ,'isMasterMeter' \
                        ,'isCheckMeter' \
                        ,'allowAlso' \
                        ,'waterMeterType' \
                )

    #5.Apply schema definition
    schema = StructType([
                            StructField('propertyNumber', StringType(), False),
                            StructField("propertyMeterNumber", StringType(), False),
                            StructField("meterMakerNumber", StringType(), True),
                            StructField("meterFittedDate", DateType(), True),
                            StructField("meterRemovedDate", DateType(), True),
                            StructField("validFrom", DateType(), False),
                            StructField("validTo", DateType(), True),
                            StructField("meterChangeReasonCode", StringType(), True),
                            StructField("meterExchangeReason", StringType(), True),
                            StructField("meterSizeCode", StringType(), True),
                            StructField("meterSize", StringType(), True),
                            StructField("meterReadingFrequencyCode", StringType(), True),
                            StructField("meterClassCode", StringType(), True),
                            StructField("meterClass", StringType(), True),
                            StructField("meterCategoryCode", StringType(), True),
                            StructField("meterCategory", StringType(), True),
                            StructField("meterGroupCode", StringType(), True),
                            StructField("meterGroup", StringType(), True),
                            StructField("isMasterMeter", StringType(), True),
                            StructField("isCheckMeter", StringType(), True),
                            StructField("allowAlso", StringType(), True),
                            StructField("waterMeterType", StringType(), True)
    ])

    return df, schema

# COMMAND ----------

df, schema = getmeterTimesliceAccess()
df.createOrReplaceTempView('ts')
#TemplateEtl(df, entity="meterTimesliceAccess", businessKey="propertyNumber,propertymeterNumber,validFrom", schema=schema, writeMode=ADS_WRITE_MODE_OVERWRITE, AddSK=False)
DeltaSaveDataframeDirect(df, 'accessts', 'access_meterTimeslice', ADS_DATABASE_CLEANSED, ADS_CONTAINER_CLEANSED, "overwrite", schema, "")

# COMMAND ----------

dbutils.notebook.exit("1")
