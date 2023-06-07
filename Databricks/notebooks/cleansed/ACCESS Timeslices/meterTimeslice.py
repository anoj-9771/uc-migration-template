# Databricks notebook source
# MAGIC %md
# MAGIC <b>Note that this notebook should be run via the /cleansed/ACCESS Utils/Create Missing Meters notebook</b>

# COMMAND ----------

# MAGIC %run ../../includes/include-all-util

# COMMAND ----------

spark.conf.set("c.catalog_name", ADS_DATABASE_CLEANSED)

# COMMAND ----------

# df = spark.sql("select distinct 'C' as src, propertyNumber, propertyMeterNumber, meterSize, meterFittedDate, meterRemovedDate, meterMakerNumber, \
#                              meterClass, meterCategory, coalesce(meterGroup,  'Normal Reading') as meterGroup, isCheckMeter, waterMeterType, propertyMeterUpdatedDate, \
#                              row_number() over (partition by propertyNumber, propertyMeterNumber, metermakernumber order by meterRemovedDate nulls first) as rn \
#                       from   {ADS_DATABASE_CLEANSED}.access.z309_tpropmeter_cleansed pm1 \
#                       where  (meterRemovedDate is null \
#                       or     meterRemovedDate > meterFittedDate) \
#                       and    (meterRemovedDate is null \
#                       or     not exists (select 1 from {ADS_DATABASE_CLEANSED}.access.z309_tpropmeter_cleansed pm2 where pm1.propertyNumber = pm2.propertyNumber and pm1.propertyMeterNumber = pm2.propertyMeterNumber and pm1.meterfittedDate = pm2.meterFittedDate and pm1.meterMakerNumber != pm2.meterMakerNumber and pm2.meterRemovedDate is null)) and propertyNumber = 3100208")
# display(df)

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
                      FROM {ADS_DATABASE_CLEANSED}.access.Z309_thpropmeter_cleansed hpm \
                           left outer join {ADS_DATABASE_CLEANSED}.access.z309_tpropmeter_cleansed pm on hpm.propertyNumber = pm.propertyNumber and hpm.propertyMeterNumber = pm.propertyMeterNumber and hpm.meterMakerNumber = pm.meterMakerNumber \
                      ), \
                 nextMeterFitted as( \
                     select distinct propertyNumber, propertyMeterNumber, meterMakerNumber, meterfittedDate, \
                            max(meterFittedDate) over (partition by propertyNumber, propertyMeterNumber order by meterFittedDate rows between 1 following and 1 following) as nextMeterFitted \
                     from   (select propertyNumber, propertyMeterNumber, meterMakerNumber, meterFittedDate \
                             from   {ADS_DATABASE_CLEANSED}.access.Z309_thpropmeter_cleansed \
                             union \
                             select propertyNumber, propertyMeterNumber, meterMakerNumber, meterFittedDate \
                             from   {ADS_DATABASE_CLEANSED}.access.Z309_tpropmeter_cleansed \
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
                      from   {ADS_DATABASE_CLEANSED}.access.z309_tpropmeter_cleansed pm, \
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

# %sql
# drop table cleansed.meterTimeSliceACCESS

# COMMAND ----------

df, schema = getmeterTimesliceAccess()
df.createOrReplaceTempView('ts')
#TemplateEtl(df, entity="meterTimesliceAccess", businessKey="propertyNumber,propertymeterNumber,validFrom", schema=schema, writeMode=ADS_WRITE_MODE_OVERWRITE, AddSK=False)
DeltaSaveDataframeDirect(df, 'accessts', 'access_meterTimeslice', ADS_DATABASE_CLEANSED, ADS_CONTAINER_CLEANSED, "overwrite", schema, "")

# COMMAND ----------

dbutils.notebook.exit('0')

# COMMAND ----------

# MAGIC %sql
# MAGIC --> new code 14/10/22 <--
# MAGIC             with t0 as (select cast(3130313 as int) as propertyNumber, cast(1 as int) as propertyMeterNumber),
# MAGIC                   history as( 
# MAGIC                       SELECT 'HISTORY' as src, hpm.propertyNumber, hpm.propertyMeterNumber, hpm.meterSize, coalesce(pm.meterFittedDate,hpm.meterFittedDate) as meterFittedDate, 
# MAGIC                               if(pm.meterFittedDate is not null and pm.meterRemovedDate is null, null, coalesce(hpm.meterRemovedDate,pm.meterRemovedDate)) as meterRemovedDate, 
# MAGIC                               hpm.meterMakerNumber, hpm.meterClass, hpm.meterCategory, hpm.meterGroup, hpm.isMasterMeter, hpm.isCheckMeter, hpm.waterMeterType, hpm.propertyMeterUpdatedDate, hpm.rowSupersededDate,
# MAGIC                               row_number() over (partition by hpm.propertyNumber, hpm.propertyMeterNumber, hpm.meterSize, hpm.meterFittedDate, hpm.meterMakerNumber, 
# MAGIC                               hpm.meterClass, hpm.meterCategory, hpm.meterGroup, hpm.isMasterMeter, hpm.isCheckMeter, hpm.waterMeterType, hpm.rowSupersededDate order by hpm.rowSupersededTime desc) as rn_latestUpdate 
# MAGIC                       FROM ${c.catalog_name}.access.z309_thpropmeter_cleansed hpm, t0 
# MAGIC                            left outer join ${c.catalog_name}.access.z309_tpropmeter_cleansed pm on hpm.propertyNumber = pm.propertyNumber and hpm.propertyMeterNumber = pm.propertyMeterNumber and hpm.meterMakerNumber = pm.meterMakerNumber 
# MAGIC                                            --and pm.meterRemovedDate is not null 
# MAGIC --                       where exists (select 1 
# MAGIC --                                     from ${c.catalog_name}.access.z309_tpropmeter_cleansed pm 
# MAGIC --                                     where pm.propertyNumber = hpm.propertyNumber and pm.propertyMeterNumber = hpm.propertyMeterNumber and pm.meterMakerNumber = hpm.meterMakerNumber) 
# MAGIC                       where hpm.propertyNumber = t0.propertyNumber
# MAGIC                       and   hpm.propertyMeterNumber = t0.propertyMeterNumber
# MAGIC                       ),
# MAGIC --                  metersFitted as(
# MAGIC --                      select propertyNumber, propertyMeterNumber, meterMakerNumber, meterFittedDate
# MAGIC --                      from   (select distinct propertyNumber, propertyMeterNumber, meterMakerNumber, meterFittedDate, 1 as rnk
# MAGIC --                              from   ${c.catalog_name}.access.Z309_tpropmeter_cleansed
# MAGIC --                              union all
# MAGIC --                              select distinct propertyNumber, propertyMeterNumber, meterMakerNumber, meterFittedDate, 2 as rnk
# MAGIC --                              from   ${c.catalog_name}.access.Z309_tpropmeter_cleansed
# MAGIC --                              ) as t
# MAGIC --                      ),
# MAGIC                  nextMeterFitted as(
# MAGIC                      select distinct propertyNumber, propertyMeterNumber, meterMakerNumber, meterFittedDate,
# MAGIC                             max(meterFittedDate) over (partition by propertyNumber, propertyMeterNumber order by meterFittedDate rows between 1 following and 1 following) as nextMeterFitted
# MAGIC                      from   (select propertyNumber, propertyMeterNumber, meterMakerNumber, meterFittedDate
# MAGIC                              from   ${c.catalog_name}.access.Z309_thpropmeter_cleansed
# MAGIC                              union all
# MAGIC                              select propertyNumber, propertyMeterNumber, meterMakerNumber, meterFittedDate
# MAGIC                              from   ${c.catalog_name}.access.Z309_tpropmeter_cleansed
# MAGIC                              ) as t
# MAGIC                      ),
# MAGIC                  bestRemovalDates as(
# MAGIC                       select a.propertyNumber, a.propertyMeterNumber, a.meterMakerNumber, coalesce(a.meterRemovedDate, 
# MAGIC                                                                                              mf.nextMeterFitted,
# MAGIC                                                                                              (select max(rowSupersededDate) 
# MAGIC                                                                                               from history b 
# MAGIC                                                                                               where b.propertyNumber = a.propertyNumber
# MAGIC                                                                                               and   b.propertyMeterNumber = a.propertyMeterNumber
# MAGIC                                                                                               and   b.meterMakerNumber = a.meterMakerNumber)) as newMeterRemovedDate
# MAGIC                       from   history a, nextMeterFitted mf
# MAGIC                       where  not exists (select 1 
# MAGIC                                          from   ${c.catalog_name}.access.z309_tpropmeter_cleansed pm
# MAGIC                                          where  pm.propertyNumber = a.propertyNumber
# MAGIC                                          and    pm.propertyMeterNumber = a.propertyMeterNumber
# MAGIC                                          and    pm.meterMakerNumber = a.meterMakerNumber)
# MAGIC                       and    mf.propertyNumber = a.propertyNumber
# MAGIC                       and    mf.propertyMeterNumber = a.propertyMeterNumber
# MAGIC                       and    mf.meterMakerNumber = a.meterMakerNumber
# MAGIC                       ),
# MAGIC                  t2 as( 
# MAGIC                       select distinct dataSource as src, pm.propertyNumber, pm.propertyMeterNumber, meterSize, pm.meterFittedDate, 
# MAGIC                              if(dataSource = 'BI',coalesce(meterRemovedDate,mf.nextMeterFitted,to_date('2018-06-15')),meterRemovedDate) as meterRemovedDate, 
# MAGIC                              pm.meterMakerNumber, meterClass, meterCategory, meterGroup, isMasterMeter, isCheckMeter, waterMeterType, propertyMeterUpdatedDate,
# MAGIC                              row_number() over (partition by pm.propertyNumber, pm.propertyMeterNumber, pm.meterFittedDate order by dataSource) as rn_source
# MAGIC                       from   ${c.catalog_name}.access.z309_tpropmeter_cleansed pm,
# MAGIC                              nextMeterFitted mf, t0
# MAGIC                       where  pm.propertyNumber = t0.propertyNumber
# MAGIC                       and    pm.propertyMeterNumber = t0.propertyMeterNumber
# MAGIC                       and    mf.propertyNumber = pm.propertyNumber
# MAGIC                       and    mf.propertyMeterNumber = pm.propertyMeterNumber
# MAGIC                       and    mf.meterMakerNumber = pm.meterMakerNumber
# MAGIC                       union all 
# MAGIC                       select src, t1.propertyNumber, t1.propertyMeterNumber, t1.meterSize, t1.meterFittedDate, coalesce(t1.meterRemovedDate,t1a.newMeterRemovedDate) as meterRemovedDate, t1.meterMakerNumber, 
# MAGIC                              t1.meterClass, t1.meterCategory, t1.meterGroup, t1.isMasterMeter, t1.isCheckMeter, t1.waterMeterType, t1.propertyMeterUpdatedDate, 1
# MAGIC                       from history t1 left outer join bestRemovalDates t1a on t1a.propertyNumber = t1.propertyNumber
# MAGIC                                                  and t1a.propertyMeterNumber = t1.propertyMeterNumber
# MAGIC                                                  and t1a.meterMakerNumber = t1.meterMakerNumber
# MAGIC                       where  rn_latestUpdate = 1 
# MAGIC                       ), 
# MAGIC                  t3 as( 
# MAGIC                       select distinct src, propertyNumber, propertyMeterNumber, meterSize, meterFittedDate, meterRemovedDate, meterMakerNumber, 
# MAGIC                              meterClass, meterCategory, meterGroup, isMasterMeter, isCheckMeter, waterMeterType, propertyMeterUpdatedDate, 
# MAGIC                              case when propertyMeterUpdatedDate > meterFittedDate then propertyMeterUpdatedDate else meterFittedDate end as validFrom, 
# MAGIC                              row_number() over (partition by propertyNumber, propertyMeterNumber, meterMakerNumber order by meterFittedDate, propertyMeterUpdatedDate desc) as rn_updateOrder 
# MAGIC                       from t2 
# MAGIC                       where rn_source = 1
# MAGIC                       and   src != 'BI'
# MAGIC                       or    (src = 'BI'
# MAGIC                       and    not exists (select 1 
# MAGIC                                          from   t2 t2a 
# MAGIC                                          where  t2a.propertyNumber = t2.propertyNumber 
# MAGIC                                          and    t2a.propertyMeterNumber = t2.propertyMeterNumber 
# MAGIC                                          and    t2a.src = 'ACCESS' 
# MAGIC                                          and    t2a.meterFittedDate <= t2.meterFittedDate 
# MAGIC                                          and    (t2a.meterRemovedDate is null
# MAGIC                                          or      t2a.meterRemovedDate > t2.meterRemovedDate)))
# MAGIC                       ),
# MAGIC --                  t3a as(
# MAGIC --                       select propertyNumber, propertyMeterNumber, meterSize, meterFittedDate,)
# MAGIC                  t4 as( 
# MAGIC                       select t3.propertyNumber, t3.propertyMeterNumber, t3.meterSize, t3.meterFittedDate, 
# MAGIC --                              case when meterRemovedDate is null
# MAGIC --                                        then lead(t3.meterfittedDate,1) over (partition by t3.propertyNumber, t3.propertyMeterNumber, t3.meterMakerNumber order by t3.meterFittedDate) 
# MAGIC --                                        else meterRemovedDate 
# MAGIC                              --end as 
# MAGIC                              meterRemovedDate, t3.meterMakerNumber, 
# MAGIC                              meterClass, meterCategory, meterGroup, isMasterMeter, isCheckMeter, waterMeterType, rn_updateOrder,
# MAGIC                              mf.meterFittedDate as validFrom 
# MAGIC                       from   t3, nextMeterFitted mf
# MAGIC                       where  mf.propertyNumber = t3.propertyNumber
# MAGIC                       and    mf.propertyMeterNumber = t3.propertyMeterNumber
# MAGIC                       and    mf.meterMakerNumber = t3.meterMakerNumber 
# MAGIC                       and    rn_updateOrder = 1
# MAGIC                       union all
# MAGIC                       select propertyNumber, propertyMeterNumber, meterSize, meterFittedDate, 
# MAGIC --                              case when meterRemovedDate is null
# MAGIC --                                        then lead(meterfittedDate,1) over (partition by propertyNumber, propertyMeterNumber, meterMakerNumber order by meterFittedDate) 
# MAGIC --                                        else meterRemovedDate 
# MAGIC --                              end as 
# MAGIC                              meterRemovedDate, meterMakerNumber, 
# MAGIC                              meterClass, meterCategory, meterGroup, isMasterMeter, isCheckMeter, waterMeterType, rn_updateOrder, validFrom 
# MAGIC                       from   t3 
# MAGIC                       where  rn_updateOrder > 1
# MAGIC                       ), 
# MAGIC                  t5 as( 
# MAGIC                       select distinct propertyNumber, propertyMeterNumber, meterSize, meterFittedDate, meterRemovedDate, meterMakerNumber, meterClass, meterCategory, 
# MAGIC                              meterGroup, isMasterMeter, isCheckMeter, waterMeterType, validFrom, 
# MAGIC                              least(coalesce( 
# MAGIC                                       date_add( 
# MAGIC                                          lag(validFrom,1) over (partition by propertyNumber, propertyMeterNumber, meterMakerNumber order by validFrom desc),-1), 
# MAGIC                                       least(meterRemovedDate,to_date('99991231','yyyyMMdd'))), meterRemovedDate) as validTo 
# MAGIC                       from t4 
# MAGIC                       ), 
# MAGIC                  t6 as( 
# MAGIC                       select propertyNumber, propertyMeterNumber, meterSize, meterFittedDate, meterRemovedDate, meterMakerNumber, meterClass, meterCategory, 
# MAGIC                                         meterGroup, isMasterMeter, isCheckMeter, waterMeterType, validFrom, validTo 
# MAGIC                       from   t5 
# MAGIC                       where  validFrom < validTo 
# MAGIC                       ), 
# MAGIC                  t7 as( 
# MAGIC                       SELECT 
# MAGIC                           propertyNumber, propertyMeterNumber, meterSize, meterFittedDate, meterRemovedDate, meterMakerNumber, meterClass, meterCategory, 
# MAGIC                                         meterGroup, isMasterMeter, isCheckMeter, waterMeterType, 
# MAGIC                           MIN(validFrom) as validFrom, 
# MAGIC                           max(validTo) as validTo 
# MAGIC                       FROM ( 
# MAGIC                           SELECT 
# MAGIC                               propertyNumber, propertyMeterNumber, meterSize, meterFittedDate, meterRemovedDate, meterMakerNumber, meterClass, meterCategory, 
# MAGIC                                         meterGroup, isMasterMeter, isCheckMeter, waterMeterType, validFrom, validTo, 
# MAGIC                               DATEADD( 
# MAGIC                                   DAY, 
# MAGIC                                   -COALESCE( 
# MAGIC                                       SUM(DATEDIFF(DAY, validFrom, validTo) +1) OVER (PARTITION BY propertyNumber, propertyMeterNumber, meterSize, meterFittedDate, 
# MAGIC                                       meterRemovedDate, meterMakerNumber, meterClass, meterCategory, 
# MAGIC                                         meterGroup, isMasterMeter, isCheckMeter, waterMeterType ORDER BY validTo ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 
# MAGIC                                       0 
# MAGIC                                   ), 
# MAGIC                                   validFrom 
# MAGIC                           ) as grp 
# MAGIC                           FROM t6 
# MAGIC                       ) withGroup
# MAGIC                       GROUP BY propertyNumber, propertyMeterNumber, meterSize, meterFittedDate, meterRemovedDate, meterMakerNumber, meterClass, meterCategory, 
# MAGIC                                meterGroup, isMasterMeter, isCheckMeter, waterMeterType, grp 
# MAGIC                       ORDER BY propertyNumber, propertyMeterNumber, meterSize, meterFittedDate, meterRemovedDate, meterMakerNumber, meterClass, meterCategory, 
# MAGIC                                meterGroup, isMasterMeter, isCheckMeter, waterMeterType, validFrom 
# MAGIC                       ) 
# MAGIC                       select propertyNumber, propertyMeterNumber, meterSize, meterFittedDate, meterRemovedDate, meterMakerNumber, meterClass, meterCategory, 
# MAGIC                                meterGroup, isMasterMeter, isCheckMeter, waterMeterType, min(validFrom) as validFrom, max(validTo) as validTo
# MAGIC                       from t7
# MAGIC                       GROUP BY propertyNumber, propertyMeterNumber, meterSize, meterFittedDate, meterRemovedDate, meterMakerNumber, meterClass, meterCategory, 
# MAGIC                                meterGroup, isMasterMeter, isCheckMeter, waterMeterType
# MAGIC                       except
# MAGIC                       select propertyNumber, propertyMeterNumber, meterSize, meterFittedDate, meterRemovedDate, meterMakerNumber, meterClass, meterCategory, 
# MAGIC                                meterGroup, isMasterMeter, isCheckMeter, waterMeterType, min(validFrom) as validFrom, max(validTo) as validTo
# MAGIC                       from t7
# MAGIC                       where exists(select 1
# MAGIC                                    from   t7 t7a
# MAGIC                                    where  t7a.propertyNumber = t7.propertyNumber
# MAGIC                                    and    t7a.propertyMeterNumber = t7.propertyMeterNumber
# MAGIC                                    and    t7a.meterMakerNumber != t7.meterMakerNumber
# MAGIC                                    and    t7.meterFittedDate between t7a.meterFittedDate and coalesce(t7a.meterRemovedDate,'9999-12-31')
# MAGIC                                    and    t7.meterRemovedDate between t7a.meterFittedDate and coalesce(t7a.meterRemovedDate,'9999-12-31'))
# MAGIC                       GROUP BY propertyNumber, propertyMeterNumber, meterSize, meterFittedDate, meterRemovedDate, meterMakerNumber, meterClass, meterCategory, 
# MAGIC                                meterGroup, isMasterMeter, isCheckMeter, waterMeterType
# MAGIC             order  by propertyNumber, propertyMeterNumber, validFrom

# COMMAND ----------

# MAGIC %sql
# MAGIC select meterMakerNumber, dataSource, meterFittedDate, meterRemovedDate, lead(meterfittedDate,1) over (partition by propertyNumber, propertyMeterNumber order by meterFittedDate) as leads --meterMakerNumber, meterfittedDate, meterRemovedDate, propertyMeterUpdatedDate 
# MAGIC from   ${c.catalog_name}.access.z309_tpropmeter
# MAGIC where  propertyNumber = 3230020
# MAGIC and    propertyMeterNumber = 1

# COMMAND ----------

#old code (pre 14/10/22)
with t1 as( 
                     SELECT 'H' as src, hpm.propertyNumber, hpm.propertyMeterNumber, hpm.meterSize, hpm.meterFittedDate, coalesce(hpm.meterRemovedDate,pm.meterRemovedDate) as meterRemovedDate, 
                              trim(translate(hpm.meterMakerNumber,chr(26),' ')) as meterMakerNumber, 
                              hpm.meterClass, hpm.meterCategory, coalesce(hpm.meterGroup,  'Normal Reading') as meterGroup, hpm.isCheckMeter, mc.waterMeterType, hpm.propertyMeterUpdatedDate, 
                              row_number() over (partition by hpm.propertyNumber, hpm.propertyMeterNumber, hpm.meterSize, hpm.meterFittedDate, hpm.metermakernumber, 
                              hpm.meterClass, hpm.meterCategory, hpm.meterGroup, hpm.isCheckMeter, mc.waterMeterType, hpm.rowSupersededDate order by hpm.rowSupersededTime desc) as rn 
                      FROM {ADS_DATABASE_CLEANSED}.access.Z309_thpropmeter_cleansed hpm left outer join CLEANSED.access_Z309_TMeterClass mc on mc.meterClassCode = hpm.meterClassCode 
                                                                left outer join {ADS_DATABASE_CLEANSED}.access.z309_tpropmeter_cleansed pm on hpm.propertyNumber = pm.propertyNumber and hpm.propertyMeterNumber = pm.propertyMeterNumber and hpm.meterMakerNumber = pm.meterMakerNumber and pm.meterRemovedDate is not null 
                      where hpm.meterRemovedDate is null
                      and   exists (select 1 from {ADS_DATABASE_CLEANSED}.access.z309_tpropmeter_cleansed pm where pm.propertyNumber = hpm.propertyNumber and pm.propertyMeterNumber = hpm.propertyMeterNumber and pm.meterMakerNumber = hpm.meterMakerNumber)
                      and   hpm.propertyNumber = 5568937
                      and   hpm.propertyMeterNumber = 1
                      ), 
                 t2 as( 
                      select distinct 'C' as src, pm1.propertyNumber, pm1.propertyMeterNumber, pm1.meterSize, pm1.meterFittedDate, coalesce(pm1.meterRemovedDate,pm2.meterRemovedDate) as meterRemovedDate, 
                             trim(translate(pm1.meterMakerNumber,chr(26),' ')) as meterMakerNumber, 
                             pm1.meterClass, pm1.meterCategory, coalesce(pm1.meterGroup,  'Normal Reading') as meterGroup, pm1.isCheckMeter, pm1.waterMeterType, pm1.propertyMeterUpdatedDate,
                             row_number() over (partition by pm1.propertyNumber, pm1.propertyMeterNumber, translate(pm1.meterMakerNumber,chr(26),' ') order by coalesce(pm1.meterRemovedDate,pm2.meterRemovedDate)) as rn 
                      from   {ADS_DATABASE_CLEANSED}.access.z309_tpropmeter_cleansed pm1 left outer join {ADS_DATABASE_CLEANSED}.access.z309_tpropmeter_cleansed pm2 on pm1.propertyNumber = pm2.propertyNumber and pm1.propertyMeterNumber = pm2.propertyMeterNumber and pm1.meterfittedDate = pm2.meterFittedDate and pm1.meterMakerNumber = pm2.meterMakerNumber and pm2.meterRemovedDate is not null
                      where  (pm1.meterRemovedDate is null 
                      or     pm1.meterRemovedDate > pm1.meterFittedDate )
                      and    pm1.propertyNumber = 5568937
                      and    pm1.propertyMeterNumber = 1
                      union all 
                      select src, t1.propertyNumber, t1.propertyMeterNumber, t1.meterSize, t1.meterFittedDate, t1.meterRemovedDate, t1.meterMakerNumber, 
                             t1.meterClass, t1.meterCategory, t1.meterGroup, t1.isCheckMeter, t1.waterMeterType, t1.propertyMeterUpdatedDate, t1.rn 
                      from t1 
                      where  rn = 1 
                      and not exists (select 1 from {ADS_DATABASE_CLEANSED}.access.z309_tpropmeter_cleansed pm where pm.propertyNumber = t1.propertyNumber and pm.propertyMeterNumber = t1.propertyMeterNumber and ((pm.meterFittedDate = t1.meterFittedDate and pm.meterMakerNumber != t1.meterMakerNumber) or (pm.meterFittedDate <> t1.meterFittedDate and pm.meterMakerNumber = t1.meterMakerNumber)))
                      ), 
                 t3 as( 
                      select src, propertyNumber, propertyMeterNumber, meterSize, meterFittedDate, meterRemovedDate, meterMakerNumber, 
                             meterClass, meterCategory, meterGroup, isCheckMeter, case when propertyMeterUpdatedDate > meterFittedDate then propertyMeterUpdatedDate else meterFittedDate end as validFrom, 
                             row_number() over (partition by propertyNumber, propertyMeterNumber, meterMakerNumber order by meterFittedDate, propertyMeterUpdatedDate) as rn
                      from t2 
                      where  rn = 1
                      and not exists (select 1 from {ADS_DATABASE_CLEANSED}.access.z309_tpropmeter_cleansed pm where pm.propertyNumber = t2.propertyNumber and pm.propertyMeterNumber = t2.propertyMeterNumber and pm.meterFittedDate = t2.meterFittedDate and pm.meterMakerNumber != t2.meterMakerNumber and ((t2.meterRemovedDate is not null and pm.meterRemovedDate is null) or t2.meterRemovedDate < pm.meterRemovedDate))
                      ), 
                 t4 as( 
                      select propertyNumber, propertyMeterNumber, meterSize, meterFittedDate, meterRemovedDate, meterMakerNumber, meterClass, meterCategory, 
                             meterGroup, isCheckMeter, least(validFrom,meterFittedDate) as validFrom 
                      from   t3 
                      where  rn = 1 
                      union all 
                      select propertyNumber, propertyMeterNumber, meterSize, meterFittedDate, meterRemovedDate, meterMakerNumber, meterClass, meterCategory, 
                             meterGroup, isCheckMeter, validFrom 
                      from   t3 
                      where  rn > 1 
                      ), 
                 t5 as( 
                      select distinct propertyNumber, propertyMeterNumber, meterSize, meterFittedDate, meterRemovedDate, meterMakerNumber, meterClass, meterCategory, 
                             meterGroup, isCheckMeter, validFrom, 
                             coalesce( 
                                      date_add( 
                                         lag(validFrom,1) over (partition by propertyNumber, propertyMeterNumber, meterMakerNumber order by validFrom desc),-1), 
                                      least(meterRemovedDate,to_date('99991231','yyyyMMdd'))) as validTo 
                      from t4 
                      ), 
                 t6 as( 
                      select propertyNumber, propertyMeterNumber, meterSize, meterFittedDate, meterRemovedDate, meterMakerNumber, meterClass, meterCategory, 
                                        meterGroup, isCheckMeter, validFrom, validTo 
                      from   t5 
                      where  validFrom < validTo 
                      and    (meterFittedDate != meterRemovedDate or meterRemovedDate is null)
                      ), 
                 t7 as( 
                      SELECT 
                          propertyNumber, propertyMeterNumber, meterSize, meterFittedDate, meterRemovedDate, meterMakerNumber, meterClass, meterCategory, 
                                        meterGroup, isCheckMeter, 
                          MIN(validFrom) as validFrom, 
                          max(validTo) as validTo 
                      FROM ( 
                          SELECT 
                              propertyNumber, propertyMeterNumber, meterSize, meterFittedDate, meterRemovedDate, meterMakerNumber, meterClass, meterCategory, 
                                        meterGroup, isCheckMeter, validFrom, validTo, 
                              DATEADD( 
                                  DAY, 
                                  -COALESCE( 
                                      SUM(DATEDIFF(DAY, validFrom, validTo) +1) OVER (PARTITION BY propertyNumber, propertyMeterNumber, meterSize, meterFittedDate, 
                                      meterRemovedDate, meterMakerNumber, meterClass, meterCategory, 
                                        meterGroup, isCheckMeter ORDER BY validTo ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 
                                      0 
                                  ), 
                                  validFrom 
                          ) as grp 
                          FROM t6 
                      ) withGroup 
                      GROUP BY propertyNumber, propertyMeterNumber, meterSize, meterFittedDate, meterRemovedDate, meterMakerNumber, meterClass, meterCategory, 
                               meterGroup, isCheckMeter, grp 
                      ORDER BY propertyNumber, propertyMeterNumber, meterSize, meterFittedDate, meterRemovedDate, meterMakerNumber, meterClass, meterCategory, 
                               meterGroup, isCheckMeter, validFrom 
                      ) 
            select *
            from   t3
            --order  by propertyNumber, propertyMeterNumber, src, validFrom, rn
-- select src, propertyNumber, propertyMeterNumber, meterSize, meterFittedDate, meterRemovedDate, meterMakerNumber, meterClass, meterCategory,  
--                   meterGroup, isCheckMeter, validFrom, validTo
-- from t3
-- order by validFrom
       
--, meterMakerNumber, meterClass, meterCategory, meterGroup, isCheckMeter

# COMMAND ----------

# MAGIC %sql
# MAGIC select mr.*, mts.meterMakerNumber, mts.meterFittedDate
# MAGIC from   ${c.catalog_name}.access.z309_tmeterreading mr left outer join cleansed.metertimesliceaccess mts on mts.propertyNumber = mr.propertyNumber 
# MAGIC                                                                                                and mts.propertyMeterNumber = mr.propertyMeterNumber
# MAGIC                                                                                                and mr.readingToDate between mts.validFrom and mts.validTo
# MAGIC                                                                                                and mr.readingToDate != mts.validFrom
# MAGIC where mr.meterReadingDays > 0 
# MAGIC and mr.meterReadingConsumption > 0  
# MAGIC and mr.propertyNumber = 3654983
# MAGIC and mr.propertyMeterNumber = 5

# COMMAND ----------

# MAGIC %sql
# MAGIC select * 
# MAGIC from ${c.catalog_name}.access.z309_tpropmeter_cleansed
# MAGIC where propertyNumber = 4995495

# COMMAND ----------

# MAGIC %sql
# MAGIC with t1 as(--latest update for a day
# MAGIC           SELECT 'H' as src, propertyNumber, propertyMeterNumber, meterSize, meterFittedDate, meterRemovedDate, meterMakerNumber, 
# MAGIC                   meterClass, meterCategory, coalesce(meterGroup,  'Normal Reading') as meterGroup, isCheckMeter, propertyMeterUpdatedDate, 
# MAGIC                   rank() over (partition by propertyNumber, propertyMeterNumber, meterSize, meterFittedDate, metermakernumber, 
# MAGIC                   meterClass, meterCategory, meterGroup, isCheckMeter, rowSupersededDate order by rowSupersededTime desc) as rnk 
# MAGIC           FROM ${c.catalog_name}.access.Z309_thpropmeter_cleansed
# MAGIC           where meterMakerNumber = 'DTED0028'
# MAGIC           ),
# MAGIC      t2 as(
# MAGIC           select 'C' as src, propertyNumber, propertyMeterNumber, meterSize, meterFittedDate, meterRemovedDate, meterMakerNumber, 
# MAGIC                   meterClass, meterCategory, coalesce(meterGroup,  'Normal Reading') as meterGroup, isCheckMeter, propertyMeterUpdatedDate
# MAGIC           from ${c.catalog_name}.access.z309_tpropmeter_cleansed 
# MAGIC           where meterMakerNumber = 'DTED0028'
# MAGIC           union all
# MAGIC           select src, propertyNumber, propertyMeterNumber, meterSize, meterFittedDate, meterRemovedDate, meterMakerNumber, 
# MAGIC                  meterClass, meterCategory, meterGroup, isCheckMeter, propertyMeterUpdatedDate 
# MAGIC           from t1
# MAGIC           where rnk = 1),
# MAGIC      t3 as(
# MAGIC           select src, propertyNumber, propertyMeterNumber, meterSize, meterFittedDate, meterRemovedDate, meterMakerNumber, 
# MAGIC                  meterClass, meterCategory, meterGroup, isCheckMeter, propertyMeterUpdatedDate as validFrom, 
# MAGIC                  coalesce(date_add(
# MAGIC                      lag(propertyMeterUpdatedDate,1) over (partition by propertyNumber, propertyMeterNumber order by propertyMeterUpdatedDate desc),-1),
# MAGIC                    least(meterRemovedDate,to_date('99991231','yyyyMMdd'))) as validTo
# MAGIC           from t2),
# MAGIC      t3a as(
# MAGIC           select propertyNumber, propertyMeterNumber, meterSize, meterFittedDate, meterRemovedDate, meterMakerNumber, meterClass, meterCategory,  
# MAGIC                             meterGroup, isCheckMeter, validFrom, validTo
# MAGIC           from   t3
# MAGIC           where  not validFrom > validTo --anomaliy if row updated more than once
# MAGIC           ),
# MAGIC      t4 as(
# MAGIC           SELECT 
# MAGIC               propertyNumber, propertyMeterNumber, meterSize, meterFittedDate, meterRemovedDate, meterMakerNumber, meterClass, meterCategory,  
# MAGIC                             meterGroup, isCheckMeter,
# MAGIC               MIN(validFrom) as validFrom, 
# MAGIC               max(validTo) as validTo
# MAGIC           FROM (
# MAGIC               SELECT 
# MAGIC                   propertyNumber, propertyMeterNumber, meterSize, meterFittedDate, meterRemovedDate, meterMakerNumber, meterClass, meterCategory,  
# MAGIC                             meterGroup, isCheckMeter, validFrom, validTo,
# MAGIC                   DATEADD(
# MAGIC                       DAY, 
# MAGIC                       -COALESCE(
# MAGIC                           SUM(DATEDIFF(DAY, validFrom, validTo) +1) OVER (PARTITION BY propertyNumber, propertyMeterNumber, meterSize, meterFittedDate, 
# MAGIC                           meterRemovedDate, meterMakerNumber, meterClass, meterCategory,  
# MAGIC                             meterGroup, isCheckMeter ORDER BY validTo ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 
# MAGIC                           0
# MAGIC                       ),
# MAGIC                       validFrom
# MAGIC                   ) as grp
# MAGIC               FROM t3a
# MAGIC           ) withGroup
# MAGIC           GROUP BY propertyNumber, propertyMeterNumber, meterSize, meterFittedDate, meterRemovedDate, meterMakerNumber, meterClass, meterCategory,  
# MAGIC                    meterGroup, isCheckMeter, grp
# MAGIC           ORDER BY propertyNumber, propertyMeterNumber, meterSize, meterFittedDate, meterRemovedDate, meterMakerNumber, meterClass, meterCategory,  
# MAGIC                    meterGroup, isCheckMeter, validFrom),
# MAGIC      t5 as(--now we may need to set the first validFrom date to the meter fit date     
# MAGIC           select propertyNumber, propertyMeterNumber, meterSize, meterFittedDate, meterRemovedDate, meterMakerNumber, meterClass, meterCategory,  
# MAGIC                             meterGroup, isCheckMeter, validFrom, validTo, 
# MAGIC                  row_number() over (partition by propertyNumber, propertyMeterNumber, meterMakerNumber order by validFrom) as rn
# MAGIC           from t4)
# MAGIC select propertyNumber, propertyMeterNumber, meterSize, meterFittedDate, meterRemovedDate, meterMakerNumber, meterClass, meterCategory,  
# MAGIC        meterGroup, isCheckMeter, least(validFrom,meterFittedDate) as validFrom, validTo 
# MAGIC from   t5
# MAGIC where  rn = 1
# MAGIC union all
# MAGIC select propertyNumber, propertyMeterNumber, meterSize, meterFittedDate, meterRemovedDate, meterMakerNumber, meterClass, meterCategory,  
# MAGIC        meterGroup, isCheckMeter, validFrom, validTo 
# MAGIC from   t5
# MAGIC where  rn > 1
# MAGIC order  by 1,2, validFrom

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from raw.access_z309_tpropmeter_cleansed_bi
# MAGIC where n_mete_make = 'DTED0028'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT propertyNumber, propertyMeterNumber, rowSupersededDate, meterSize, meterFittedDate, meterRemovedDate, meterMakerNumber, meterClass, meterCategory,  
# MAGIC                   coalesce(meterGroup,  'Normal Reading') as meterGroup, isCheckMeter, propertyMeterUpdatedDate 
# MAGIC            FROM ${c.catalog_name}.access.Z309_thpropmeter_cleansed
# MAGIC            where meterMakerNumber = 'DTED0028'
# MAGIC            order by propertyMeterUpdatedDate

# COMMAND ----------

# MAGIC %sql
# MAGIC with t1 as(--latest update for a day. this seems wrong
# MAGIC SELECT propertyNumber, propertyMeterNumber, rowSupersededDate, meterSize, meterFittedDate, meterRemovedDate, meterMakerNumber, meterClass, meterCategory,  
# MAGIC                   coalesce(meterGroup,  'Normal Reading') as meterGroup, isCheckMeter, propertyMeterUpdatedDate, 
# MAGIC                   --row_number() over (partition by propertyNumber, propertyMeterNumber, metermakernumber, meterfitteddate, meterClass, meterGroup, isCheckMeter, rowsupersededDate order by rowSupersededTime desc) as rn 
# MAGIC                   rank() over (partition by propertyNumber, propertyMeterNumber, metermakernumber, meterfitteddate, meterClass, meterGroup, isCheckMeter order by rowsupersededDate desc) as rnk 
# MAGIC            FROM ${c.catalog_name}.access.Z309_thpropmeter_cleansed
# MAGIC            where meterMakerNumber = 'DTED0028'
# MAGIC        )
# MAGIC        select * from t1
# MAGIC        where rnk = 1

# COMMAND ----------

dbutils.notebook.exit("1")

# COMMAND ----------

df = spark.sql(" \
            with t1 as( \
                      SELECT 'H' as src, propertyNumber, propertyMeterNumber, meterSize, meterFittedDate, meterRemovedDate, meterMakerNumber, \
                              meterClass, meterCategory, coalesce(meterGroup,  'Normal Reading') as meterGroup, isCheckMeter, propertyMeterUpdatedDate, \
                              row_number() over (partition by propertyNumber, propertyMeterNumber, meterSize, meterFittedDate, metermakernumber, \
                              meterClass, meterCategory, meterGroup, isCheckMeter, rowSupersededDate order by rowSupersededTime desc) as rn \
                      FROM {ADS_DATABASE_CLEANSED}.access.Z309_thpropmeter_cleansed \
                      where propertyNumber = 5000109 \
                      ), \
                 t2 as( \
                      select 'C' as src, propertyNumber, propertyMeterNumber, meterSize, meterFittedDate, meterRemovedDate, meterMakerNumber, \
                              meterClass, meterCategory, coalesce(meterGroup,  'Normal Reading') as meterGroup, isCheckMeter, propertyMeterUpdatedDate, \
                              row_number() over (partition by propertyNumber, propertyMeterNumber, metermakernumber order by meterRemovedDate desc) as rn \
                      from {ADS_DATABASE_CLEANSED}.access.z309_tpropmeter_cleansed \
                      where propertyNumber = 5000109 \
                      union all \
                      select src, propertyNumber, propertyMeterNumber, meterSize, meterFittedDate, meterRemovedDate, meterMakerNumber, \
                             meterClass, meterCategory, meterGroup, isCheckMeter, propertyMeterUpdatedDate, rn \
                      from t1 \
                      where rn = 1 \
                      ), \
                 t3 as( \
                      select src, propertyNumber, propertyMeterNumber, meterSize, meterFittedDate, meterRemovedDate, meterMakerNumber, \
                             meterClass, meterCategory, meterGroup, isCheckMeter, propertyMeterUpdatedDate as validFrom, \
                             row_number() over (partition by propertyNumber, propertyMeterNumber, meterMakerNumber order by propertyMeterUpdatedDate) as rn \
                      from t2 \
                      where rn = 1 \ 
                      ), \
                 t4 as( \
                      select propertyNumber, propertyMeterNumber, meterSize, meterFittedDate, meterRemovedDate, meterMakerNumber, meterClass, meterCategory, \
                             meterGroup, isCheckMeter, least(validFrom,meterFittedDate) as validFrom \
                      from   t3 \
                      where  rn = 1 \
                      union all \
                      select propertyNumber, propertyMeterNumber, meterSize, meterFittedDate, meterRemovedDate, meterMakerNumber, meterClass, meterCategory, \
                             meterGroup, isCheckMeter, validFrom \
                      from   t3 \
                      where  rn > 1 \
                      ), \
                 t5 as( \
                      select propertyNumber, propertyMeterNumber, meterSize, meterFittedDate, meterRemovedDate, meterMakerNumber, meterClass, meterCategory, \
                             meterGroup, isCheckMeter, validFrom, \
                             coalesce( \
                                      date_add( \
                                         lag(validFrom,1) over (partition by propertyNumber, propertyMeterNumber, meterMakerNumber order by validFrom desc),-1), \
                                      least(meterRemovedDate,to_date('99991231','yyyyMMdd'))) as validTo \
                      from t4 \
                      ) \
  select * from t5")
display(df)       

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from cleansed.metertimesliceaccess
# MAGIC where propertynumber = 3100208

# COMMAND ----------


