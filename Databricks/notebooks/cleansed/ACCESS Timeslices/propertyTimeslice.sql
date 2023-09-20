-- Databricks notebook source
-- MAGIC %run ../../includes/include-all-util

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create the propertyTimeslice table by joining current and history tables, taking only the last update for a day, as that historical row is one that has been truly 'operational'. The validTo date is moved back one day to have continues timeslices rather than overlapping ones

-- COMMAND ----------

-- DBTITLE 1,Show count of rows that will be excluded because the end date is invalid
select count(*)
from cleansed.access_z309_thproperty
where date(rowSupersededTimeStamp) = '9999-12-31'
-- and propertyTypeeffectivefrom = '9999-12-31'


-- COMMAND ----------

create or replace temp view tsProperty as
with t1 as
        (--current table
        select 'current' as fromTable, propertyNumber, lotDescription, propertyType,  propertyArea, propertyAreaTypeCode, propertyTypeEffectiveFrom as validFrom, 1 as rn
        from   cleansed.access_z309_tproperty
        --where  propertyNumber = 5445615
        ),
    t2 as
        (
        --history table, only take rows that are different from the current details, fix up data quality issue on effective date
        select 'history' as fromTable, a.propertyNumber, a.lotDescription, a.propertyType, a.propertyArea, a.propertyAreaTypeCode, 
               if(a.propertyTypeEffectiveFrom = '9999-12-31',a.propertyUpdatedDate,a.propertyTypeEffectiveFrom) as validFrom, 
               row_number() over (partition by a.propertyNumber, a.lotDescription, a.rowSupersededDate order by a.rowSupersededDate, a.rowSupersededTime desc) as rn,
               row_number() over (partition by a.propertyNumber order by a.rowSupersededDate) as chronologyRN
        from   cleansed.access_z309_thproperty a left outer join t1 on a.propertyNumber = t1.propertyNumber
        where  a.rowSupersededDate != '9999-12-31'
        --and    a.propertyNumber = 5445615
        and    a.propertyTypeEffectiveFrom != '9999-12-31'
        and    a.propertyTypeEffectiveFrom != t1.validFrom),
    t3 as
        --join the above, only taking the latest row for a date (from history)
        (
        select fromTable, propertyNumber, lotDescription, propertyType,  propertyArea, propertyAreaTypeCode, validFrom, 99999 as chronologyRN
        from   t1
        union all
        select distinct fromTable, propertyNumber, lotDescription, propertyType,  propertyArea, propertyAreaTypeCode, validFrom, chronologyRN
        from   t2
        where  rn = 1
        ),
    t4 as
        --get distinct rows for a given set of data
        (
        select fromTable, propertyNumber, lotDescription, propertyType, propertyArea, propertyAreaTypeCode, validFrom, min(chronologyRN) as chronologyRN
        from t3
        group by fromTable, propertyNumber, lotDescription, propertyType, propertyArea, propertyAreaTypeCode, validFrom
        ),
    t5 as 
        --if there are multiple rows for a given validFrom date, take the latest
        (
        select propertyNumber, validFrom, max(chronologyRN) as chronologyRN
        from t4
        group by propertyNumber, validFrom
        )
        --select * from t5
    select t4.* 
    from t4, t5
    where t4.propertyNumber = t5.propertyNumber
    and   t4.chronologyRN = t5.chronologyRN
    order by 1,2,validFrom;

select count(*)
from   tsProperty;

-- COMMAND ----------

select *
from tsProperty
where propertyNumber = 5445615

-- COMMAND ----------

-- DBTITLE 1,Determine the validTo date based on the next row
create or replace temp view tsPropertyCleansed as
with t1 as(
          select propertyNumber, lotDescription, propertyType, propertyArea, propertyAreaTypeCode, validFrom, 
                 coalesce((max(validFrom) over (partition by propertyNumber order by chronologyRN, validFrom rows between 1 following and 1 following)) -1, to_date('9999-12-31')) as validTo 
          from   (select propertyNumber, lotDescription, propertyType, propertyArea, propertyAreaTypeCode, validFrom, chronologyRN
                  from   tsProperty
                  where  fromTable = 'history'
                  union all
                  select propertyNumber, lotDescription, propertyType, propertyArea, propertyAreaTypeCode, validFrom, chronologyRN
                  from   tsProperty
                  where  fromTable = 'current')
          ),
     t2 as(
          select propertyNumber, lotDescription, propertyType, propertyArea, propertyAreaTypeCode, min(validFrom) as validFrom, max(validTo) as validTo
          from   (
                 select propertyNumber, lotDescription, propertyType, propertyArea, propertyAreaTypeCode, validFrom, validTo,
                        dateadd(
                                day,
                                -coalesce(
                                          sum(datediff(day, validFrom, validTo) + 1) over (partition by propertyNumber, lotDescription, propertyType, propertyArea, propertyAreaTypeCode
                                                                                           order by validTo rows between unbounded preceding and 1 preceding),
                                          0
                                         ),
                                validFrom
                               ) as grp
                  from t1
                  ) withgroup
          group by propertyNumber, lotDescription, propertyType, propertyArea, propertyAreaTypeCode, grp
          order by propertyNumber, validFrom
            )
select * 
from t2
where validFrom < validTo;

select count(*)
from   tsPropertyCleansed;

-- COMMAND ----------

--select *
-- from tsProperty
-- group by propertyNumber, lotDescription
-- having count(*) > 6-- select *
-- from tsProperty
-- group by propertyNumber, lotDescription
-- having count(*) > 6

-- COMMAND ----------

select propertyNumber, lotDescription, propertyType, propertyArea, propertyAreaTypeCode, validFrom, validTo
from tsPropertyCleansed
where  propertyNumber = 5445615--3841953
--and    lotDescription = '13'    
order by validFrom

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.sql("select * from tsPropertyCleansed")
-- MAGIC DeltaSaveDataframeDirect(df, 'accessts', 'access_propertyTimeslice', ADS_DATABASE_CLEANSED, ADS_CONTAINER_CLEANSED, "overwrite", df.schema, "")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.notebook.exit('0')
