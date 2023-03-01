-- Databricks notebook source
-- MAGIC %run ../../includes/include-all-util

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create the facilityTimeslice table by joining current and history tables, taking only the last update for a day, as that historical row is one that has been truly 'operational'. The validTo date is moved back one day to have continuous timeslices rather than overlapping ones

-- COMMAND ----------

create or replace temp view tsFacility as
with t1 as
        (--current table
        select propertyNumber, serviceTypeCode, serviceType, serviceConnectionDate, numberOfWaterClosets, numberOfUrinaryCisterns, serviceAvailableDate, serviceLiableDate, serviceLiablePercentage, 
               serviceDetailsUpdatedDate as validFrom, to_date('9999-12-31') as validTo, 1 as rn
        from   cleansed.access_z309_tpropservfaci
        where  serviceLiableDate != '9999-12-31'
        --join the history table
        union all
        select propertyNumber, serviceTypeCode, serviceType, serviceConnectionDate, numberOfWaterClosets, numberOfUrinaryCisterns, serviceAvailableDate, serviceLiableDate, serviceLiablePercentage, 
               serviceDetailsUpdatedDate as validFrom, date(rowSupersededTimestamp) - 1 as validTo,
               row_number() over (partition by propertyNumber, serviceTypeCode, date(rowSupersededTimestamp) order by rowSupersededTimestamp desc) as rn
        from   cleansed.access_z309_thpropservfaci
        where  serviceLiableDate != '9999-12-31'
        )
select propertyNumber, serviceTypeCode, serviceType, serviceConnectionDate, numberOfWaterClosets, numberOfUrinaryCisterns, serviceAvailableDate, serviceLiableDate, serviceLiablePercentage, 
       validFrom, validTo
from   t1
where  rn = 1
and    validFrom < validTo
order by 1,2,validFrom;

select count(*)
from   tsFacility;

-- COMMAND ----------

-- DBTITLE 1,Collapse adjoining identical rows into one
create or replace temp view tsFacilityCleansed as
select propertyNumber, serviceTypeCode, serviceType, serviceConnectionDate, numberOfWaterClosets, numberOfUrinaryCisterns, serviceAvailableDate, serviceLiableDate, serviceLiablePercentage,
       min(validFrom) as validFrom, max(validTo) as validTo 
from ( 
     select propertyNumber, serviceTypeCode, serviceType, serviceConnectionDate, numberOfWaterClosets, numberOfUrinaryCisterns, serviceAvailableDate, serviceLiableDate, serviceLiablePercentage, validFrom, validTo, 
        DATEADD( 
            DAY, 
            -COALESCE( 
                SUM(DATEDIFF(DAY, validFrom, validTo) +1) OVER (PARTITION BY propertyNumber, serviceTypeCode, serviceType, serviceConnectionDate, numberOfWaterClosets, numberOfUrinaryCisterns, 
                                                                              serviceAvailableDate, serviceLiableDate, serviceLiablePercentage ORDER BY validTo ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 
                0 
                ), 
                validFrom 
        ) as grp 
        FROM tsFacility
    ) withGroup
GROUP BY propertyNumber, serviceTypeCode, serviceType, serviceConnectionDate, numberOfWaterClosets, numberOfUrinaryCisterns, serviceAvailableDate, serviceLiableDate, serviceLiablePercentage, grp 
ORDER BY propertyNumber, serviceTypeCode, serviceType, serviceConnectionDate, numberOfWaterClosets, numberOfUrinaryCisterns, serviceAvailableDate, serviceLiableDate, serviceLiablePercentage, validFrom;

select count(*)
from   tsFacilityCleansed;

-- COMMAND ----------

-- select propertyNumber, servicetypecode, count(*)
-- from tsFacility
-- group by propertyNumber, servicetypecode
-- having count(*) > 6

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.sql("select * from tsFacilityCleansed")
-- MAGIC DeltaSaveDataframeDirect(df, 'accessts', 'access_facilityTimeslice', ADS_DATABASE_CLEANSED, ADS_CONTAINER_CLEANSED, "overwrite", df.schema, "")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.notebook.exit('0')
