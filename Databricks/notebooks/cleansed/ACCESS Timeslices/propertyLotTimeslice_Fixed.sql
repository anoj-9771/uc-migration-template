-- Databricks notebook source
-- MAGIC %run ../../includes/include-all-util

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create the propertyLotTimeslice table by joining current and history tables for TLOT, TMASTRATAPLAN and TSTRATAUNITS, taking only the last update for a day, as that historical row is one that has been truly 'operational'. The validTo date is moved back one day to have continues timeslices rather than overlapping ones

-- COMMAND ----------

create or replace temp view tsPropertyLot as
with t0 as (select propertyNumber, min(validFrom) as firstCreatedDate
            from   cleansed.access_propertyTypeTimeslice
            group by propertyNumber), 
    t1 as
        (--current table
        select 'current' as fromTable, a.propertyNumber, planType, planNumber, lotNumber, sectionNumber, lotTypeCode, lotType, portionNumber, subdivisionNumber, lotAreaSize, lotAreaSizeUnit, 
               coalesce(lotDetailsUpdatedDate, t0.firstCreatedDate) as validFrom, 1 as rn
        from   cleansed.access_z309_tlot a,
               t0
        where  a.propertyNumber = t0.propertyNumber
        and    a.propertyNumber >= 3100000 --only take 'real' property numbers, exclude the suspense account one (3099999)
        ),
    t2 as
        (
        --history table, only take rows that are different from the current details, fix up data quality issue on effective date
        select 'history' as fromTable, a.propertyNumber, a.planType, a.planNumber, a.lotNumber, a.sectionNumber, a.lotTypeCode, a.lotType, a.portionNumber, a.subdivisionNumber, a.lotAreaSize, a.lotAreaSizeUnit, 
               coalesce(lotDetailsUpdatedDate, t0.firstCreatedDate) as validFrom, 
               row_number() over (partition by a.propertyNumber, a.planType, a.planNumber, a.lotNumber, a.sectionNumber, date(a.rowSupersededTimestamp) order by a.rowSupersededTimestamp desc) as rn,
               row_number() over (partition by a.propertyNumber order by a.rowSupersededTimestamp) as chronologyRN
        from   cleansed.access_z309_thlot a left outer join t1 on a.propertyNumber = t1.propertyNumber,
               t0
        where  a.lotDetailsUpdatedDate != t1.validFrom
        and    a.propertyNumber >= 3100000 --only take 'real' property numbers, exclude the suspense account one (3099999)
        and    a.propertyNumber = t0.propertyNumber),
    t3 as
        --join the above, only taking the latest row for a date (from history)
        (
        select fromTable, propertyNumber, planType, planNumber, lotNumber, sectionNumber, lotTypeCode, lotType, portionNumber, subdivisionNumber, lotAreaSize, lotAreaSizeUnit, validFrom, 99999 as chronologyRN
        from   t1
        union all
        select distinct fromTable, propertyNumber, planType, planNumber, lotNumber, sectionNumber, lotTypeCode, lotType, portionNumber, subdivisionNumber, lotAreaSize, lotAreaSizeUnit, validFrom, chronologyRN
        from   t2
        where  rn = 1
        ),
    t4 as
        --get distinct rows for a given set of data
        (
        select fromTable, propertyNumber, planType, planNumber, lotNumber, sectionNumber, lotTypeCode, lotType, portionNumber, subdivisionNumber, lotAreaSize, lotAreaSizeUnit, 1 as numberOfLots, 1 as totalUnitEntitlement, validFrom, min(chronologyRN) as chronologyRN
        from t3
        group by fromTable, propertyNumber, planType, planNumber, lotNumber, sectionNumber, lotTypeCode, lotType, portionNumber, subdivisionNumber, lotAreaSize, lotAreaSizeUnit, validFrom
        ),
    t5 as 
        --if there are multiple rows for a given validFrom date, take the latest
        (
        select propertyNumber, validFrom, max(chronologyRN) as chronologyRN
        from t4
        group by propertyNumber, validFrom
        )
    select t4.* 
    from t4, t5
    where t4.propertyNumber = t5.propertyNumber
    and   t4.chronologyRN = t5.chronologyRN
    order by 1,2,validFrom;

select count(*)
from   tsPropertyLot;

-- COMMAND ----------

create or replace temp view tsMasterStrata as
with t0 as (select propertyNumber, min(validFrom) as firstCreatedDate
            from   cleansed.access_propertyTypeTimeslice
            group by propertyNumber), 
    t1 as
        (--current table
        select 'current' as fromTable, a.propertyNumber, 'SP' as planType, a.planNumber, lotNumber, sectionNumber, lotTypeCode, lotType, portionNumber, subdivisionNumber, lotAreaSize, lotAreaSizeUnit, 
               coalesce(lotDetailsUpdatedDate, t0.firstCreatedDate) as validFrom, 1 as rn
        from   cleansed.access_z309_tlot a,
               t0
        where  a.propertyNumber = t0.propertyNumber
        and    a.propertyNumber >= 3100000 --only take 'real' property numbers, exclude the suspense account one (3099999)
        ),
    t2 as
        (
        --history table, only take rows that are different from the current details, fix up data quality issue on effective date
        select 'history' as fromTable, a.propertyNumber, a.planType, a.planNumber, a.lotNumber, a.sectionNumber, a.lotTypeCode, a.lotType, a.portionNumber, a.subdivisionNumber, a.lotAreaSize, a.lotAreaSizeUnit, 
               coalesce(lotDetailsUpdatedDate, t0.firstCreatedDate) as validFrom, 
               row_number() over (partition by a.propertyNumber, a.planType, a.planNumber, a.lotNumber, a.sectionNumber, date(a.rowSupersededTimestamp) order by a.rowSupersededTimestamp desc) as rn,
               row_number() over (partition by a.propertyNumber order by a.rowSupersededTimestamp) as chronologyRN
        from   cleansed.access_z309_thlot a left outer join t1 on a.propertyNumber = t1.propertyNumber,
               t0
        where  a.lotDetailsUpdatedDate != t1.validFrom
        and    a.propertyNumber >= 3100000 --only take 'real' property numbers, exclude the suspense account one (3099999)
        and    a.propertyNumber = t0.propertyNumber),
    t3 as
        --join the above, only taking the latest row for a date (from history)
        (
        select fromTable, propertyNumber, planType, planNumber, lotNumber, sectionNumber, lotTypeCode, lotType, portionNumber, subdivisionNumber, lotAreaSize, lotAreaSizeUnit, validFrom, 99999 as chronologyRN
        from   t1
        union all
        select distinct fromTable, propertyNumber, planType, planNumber, lotNumber, sectionNumber, lotTypeCode, lotType, portionNumber, subdivisionNumber, lotAreaSize, lotAreaSizeUnit, validFrom, chronologyRN
        from   t2
        where  rn = 1
        ),
    t4 as
        --get distinct rows for a given set of data
        (
        select fromTable, propertyNumber, planType, planNumber, lotNumber, sectionNumber, lotTypeCode, lotType, portionNumber, subdivisionNumber, lotAreaSize, lotAreaSizeUnit, validFrom, min(chronologyRN) as chronologyRN
        from t3
        group by fromTable, propertyNumber, planType, planNumber, lotNumber, sectionNumber, lotTypeCode, lotType, portionNumber, subdivisionNumber, lotAreaSize, lotAreaSizeUnit, validFrom
        ),
    t5 as 
        --if there are multiple rows for a given validFrom date, take the latest
        (
        select propertyNumber, validFrom, max(chronologyRN) as chronologyRN
        from t4
        group by propertyNumber, validFrom
        )
    select t4.* 
    from t4, t5
    where t4.propertyNumber = t5.propertyNumber
    and   t4.chronologyRN = t5.chronologyRN
    order by 1,2,validFrom;

select count(*)
from   tsPropertyLot;

-- COMMAND ----------

-- DBTITLE 1,Determine the validTo date based on the next row
create or replace temp view tsPropertyLotCleansed as
with t1 as(
          select propertyNumber, planType, planNumber, lotNumber, sectionNumber, lotTypeCode, lotType, portionNumber, subdivisionNumber, lotAreaSize, lotAreaSizeUnit, validFrom, 
                 coalesce((max(validFrom) over (partition by propertyNumber order by chronologyRN, validFrom rows between 1 following and 1 following)) -1, to_date('9999-12-31')) as validTo 
          from   (select propertyNumber, planType, planNumber, lotNumber, sectionNumber, lotTypeCode, lotType, portionNumber, subdivisionNumber, lotAreaSize, lotAreaSizeUnit, validFrom, chronologyRN
                  from   tsPropertyLot
                  where  fromTable = 'history'
                  union all
                  select propertyNumber, planType, planNumber, lotNumber, sectionNumber, lotTypeCode, lotType, portionNumber, subdivisionNumber, lotAreaSize, lotAreaSizeUnit, validFrom, chronologyRN
                  from   tsPropertyLot
                  where  fromTable = 'current')
          ),
     t2 as(
          select propertyNumber, planType, planNumber, lotNumber, sectionNumber, lotTypeCode, lotType, portionNumber, subdivisionNumber, lotAreaSize, lotAreaSizeUnit, min(validFrom) as validFrom, max(validTo) as validTo
          from   (
                 select propertyNumber, planType, planNumber, lotNumber, sectionNumber, lotTypeCode, lotType, portionNumber, subdivisionNumber, lotAreaSize, lotAreaSizeUnit, validFrom, validTo,
                        dateadd(
                                day,
                                -coalesce(
                                          sum(datediff(day, validFrom, validTo) + 1) over (partition by propertyNumber, planType, planNumber, lotNumber, sectionNumber, lotTypeCode, lotType, portionNumber, subdivisionNumber, lotAreaSize, lotAreaSizeUnit
                                                                                           order by validTo rows between unbounded preceding and 1 preceding),
                                          0
                                         ),
                                validFrom
                               ) as grp
                  from t1
                  ) withgroup
          group by propertyNumber, planType, planNumber, lotNumber, sectionNumber, lotTypeCode, lotType, portionNumber, subdivisionNumber, lotAreaSize, lotAreaSizeUnit, grp
          order by propertyNumber, validFrom
            )
select * 
from t2
where validFrom < validTo;

select count(*)
from   tsPropertyLotCleansed;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.sql("select * from tsPropertyLotCleansed")
-- MAGIC DeltaSaveDataframeDirect(df, 'accessts', 'access_propertyLotTimeslice', ADS_DATABASE_CLEANSED, ADS_CONTAINER_CLEANSED, "overwrite", df.schema, "")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.notebook.exit('0')
