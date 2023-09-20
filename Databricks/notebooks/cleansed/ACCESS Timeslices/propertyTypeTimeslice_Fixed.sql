-- Databricks notebook source
-- MAGIC %run ../../includes/include-all-util

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create the propertyTypeTimeslice table by joining current and history tables, taking only the last update for a day, as that historical row is one that has been truly 'operational'. The validTo date is moved back one day to have continues timeslices rather than overlapping ones

-- COMMAND ----------

create or replace temp view tsPropertyType as
with t1 as
        (--current table
        select 'current' as fromTable, propertyNumber, propertyTypeCode, propertyType,  superiorPropertyTypeCode, superiorPropertyType, propertyTypeEffectiveFrom as validFrom, 1 as rn
        from   cleansed.access_z309_tproperty
        --where  propertyNumber = 5445615
        ),
    t2 as
        (
        --history table, only take rows that are different from the current details, fix up data quality issue on effective date
        select 'history' as fromTable, a.propertyNumber, a.propertyTypeCode, a.propertyType, a.superiorPropertyTypeCode, a.superiorPropertyType, 
               if(a.propertyTypeEffectiveFrom = '9999-12-31',a.propertyUpdatedDate,a.propertyTypeEffectiveFrom) as validFrom, 
               row_number() over (partition by a.propertyNumber, a.propertyTypeCode, a.rowSupersededDate order by a.rowSupersededDate, a.rowSupersededTime desc) as rn,
               row_number() over (partition by a.propertyNumber order by a.rowSupersededDate) as chronologyRN
        from   cleansed.access_z309_thproperty a left outer join t1 on a.propertyNumber = t1.propertyNumber
        where  a.rowSupersededDate != '9999-12-31'
        --and    a.propertyNumber = 5445615
        and    a.propertyTypeEffectiveFrom != '9999-12-31'
        and    a.propertyTypeEffectiveFrom != t1.validFrom),
    t3 as
        --join the above, only taking the latest row for a date (from history)
        (
        select fromTable, propertyNumber, propertyTypeCode, propertyType,  superiorPropertyTypeCode, superiorPropertyType, validFrom, 99999 as chronologyRN
        from   t1
        union all
        select distinct fromTable, propertyNumber, propertyTypeCode, propertyType,  superiorPropertyTypeCode, superiorPropertyType, validFrom, chronologyRN
        from   t2
        where  rn = 1
        ),
    t4 as
        --get distinct rows for a given set of data
        (
        select fromTable, propertyNumber, propertyTypeCode, propertyType, superiorPropertyTypeCode, superiorPropertyType, validFrom, min(chronologyRN) as chronologyRN
        from t3
        group by fromTable, propertyNumber, propertyTypeCode, propertyType, superiorPropertyTypeCode, superiorPropertyType, validFrom
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
from   tsPropertyType;

-- COMMAND ----------

-- DBTITLE 1,Determine the validTo date based on the next row
create or replace temp view tsPropertyTypeCleansed as
with t1 as(
          select propertyNumber, propertyTypeCode, propertyType, superiorPropertyTypeCode, superiorPropertyType, validFrom, 
                 coalesce((max(validFrom) over (partition by propertyNumber order by chronologyRN, validFrom rows between 1 following and 1 following)) -1, to_date('9999-12-31')) as validTo 
          from   (select propertyNumber, propertyTypeCode, propertyType, superiorPropertyTypeCode, superiorPropertyType, validFrom, chronologyRN
                  from   tsPropertyType
                  where  fromTable = 'history'
                  union all
                  select propertyNumber, propertyTypeCode, propertyType, superiorPropertyTypeCode, superiorPropertyType, validFrom, chronologyRN
                  from   tsPropertyType
                  where  fromTable = 'current')
          ),
     t2 as(
          select propertyNumber, propertyTypeCode, propertyType, superiorPropertyTypeCode, superiorPropertyType, min(validFrom) as validFrom, max(validTo) as validTo
          from   (
                 select propertyNumber, propertyTypeCode, propertyType, superiorPropertyTypeCode, superiorPropertyType, validFrom, validTo,
                        dateadd(
                                day,
                                -coalesce(
                                          sum(datediff(day, validFrom, validTo) + 1) over (partition by propertyNumber, propertyTypeCode, propertyType, superiorPropertyTypeCode, superiorPropertyType
                                                                                           order by validTo rows between unbounded preceding and 1 preceding),
                                          0
                                         ),
                                validFrom
                               ) as grp
                  from t1
                  ) withgroup
          group by propertyNumber, propertyTypeCode, propertyType, superiorPropertyTypeCode, superiorPropertyType, grp
          order by propertyNumber, validFrom
            )
select * 
from t2
where validFrom < validTo;

select count(*)
from   tsPropertyTypeCleansed;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.sql("select * from tspropertyTypeCleansed")
-- MAGIC DeltaSaveDataframeDirect(df, 'accessts', 'access_propertyTypeTimeslice', ADS_DATABASE_CLEANSED, ADS_CONTAINER_CLEANSED, "overwrite", df.schema, "")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.notebook.exit('0')
