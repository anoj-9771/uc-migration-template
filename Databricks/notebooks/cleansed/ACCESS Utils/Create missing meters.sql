-- Databricks notebook source
-- MAGIC %md
-- MAGIC <b>Before you run this notebook you first need to run the code that backfills consumption for 1998/1999 and 2012</b><br />
-- MAGIC <ol>
-- MAGIC   <li>Process input file Z309_TMETERREADING_BI and rerun Z309_TMETERREADING notebook</li>
-- MAGIC   <li>Process input file Z309_TPROPMETER_BI and rerun Z309_TPROPMETER notebook</li>
-- MAGIC </ol>
-- MAGIC This Notebook will generate meter records on ACCESS_Z309_TPROPMETER for meters that historically were removed from the property without having been written to history (and which could not be found in BI)<br />
-- MAGIC <br />

-- COMMAND ----------

-- DBTITLE 1,Start by taking working copies of the propmeter tables
--we want all of the current table, except where the meter is fitted and removed on the same day (obvious error)
--or where for the same meter fit date as a disconnected meter there is a connected or later disconnected one 
create or replace table cleansed.access_z309_tpropmeter_cleansed
as select propertyNumber, dataSource, propertyMeterNumber, meterSizeCode, meterSize, meterReadingFrequencyCode, meterFittedDate, meterRemovedDate, 
                   meterMakerNumber, meterClassCode, meterClass, meterCategoryCode, meterCategory, 
                   coalesce(meterGroupCode,'N') as meterGroupCode, coalesce(meterGroup,  'Normal Reading') as meterGroup, isMasterMeter, isCheckMeter, allowAlso, waterMeterType, propertyMeterUpdatedDate 
   from   cleansed.access_z309_tpropmeter pm1
   where  (meterRemovedDate is null 
   or      meterRemovedDate > meterFittedDate)
   and not exists (select 1 
                   from   cleansed.access_z309_tpropmeter pm2
                   where  pm1.propertyNumber = pm2.propertyNumber 
                   and    pm1.propertyMeterNumber = pm2.propertyMeterNumber 
                   and    pm1.meterFittedDate = pm2.meterFittedDate 
                   and    pm1.meterMakerNumber != pm2.meterMakerNumber
                   and    (pm1.meterRemovedDate < pm2.meterRemovedDate 
                   or      (pm1.meterRemovedDate is not null and pm2.meterRemovedDate is null)));

--we only want from the history table those rows that are NOT cancelled (because the cancelling should be the last thing that is done to a meter)
create or replace table cleansed.access_z309_thpropmeter_cleansed
as select distinct propertyNumber, propertyMeterNumber, meterSizeCode, meterSize, meterReadingFrequencyCode, meterFittedDate, meterRemovedDate, 
                   meterMakerNumber, meterClassCode, meterClass, meterCategoryCode, meterCategory, 
                   coalesce(meterGroupCode,'N') as meterGroupCode, coalesce(meterGroup,  'Normal Reading') as meterGroup, isMasterMeter, isCheckMeter, allowAlso, waterMeterType, propertyMeterUpdatedDate, rowSupersededDate, rowSupersededTime
   from   cleansed.access_z309_thpropmeter pm1
   where  meterSize is not null
   and    meterRemovedDate is null
   and not exists (select 1 
                   from   cleansed.access_z309_tpropmeter pm2
                   where  pm1.propertyNumber = pm2.propertyNumber 
                   and    pm1.propertyMeterNumber = pm2.propertyMeterNumber 
                   and    pm1.meterFittedDate = pm2.meterFittedDate 
                   and    pm1.meterMakerNumber != pm2.meterMakerNumber);
                   
--display totals
select 'Rows from TPropMeter', count(*) as no_of_rows
from   cleansed.access_z309_tpropmeter_cleansed
union all
select 'Rows from THPropMeter', count(*)
from   cleansed.access_z309_thpropmeter_cleansed

-- COMMAND ----------

-- DBTITLE 1,Rebuild metertimesliceaccess to obtain a baseline to validate
-- MAGIC %python
-- MAGIC #start by refreshing meterTimesliceAccess
-- MAGIC dbutils.notebook.run('../../curated/02 relationshipTables/meterTimesliceAccess',600)

-- COMMAND ----------

-- DBTITLE 1,List overlapping meter fit periods
with t1 as (
select propertyNumber, propertyMeterNumber, meterMakerNumber, meterFittedDate, meterRemovedDate, isMasterMeter, isCheckMeter, meterClass, meterGroup, meterSize, meterCategory, waterMetertype, validFrom, validto
from curated.meterTimeSliceAccess
),
t2 as (
select propertyNumber, propertyMeterNumber, meterMakerNumber, meterFittedDate
from t1
group by 1,2,3,4
),
t3 as (
select propertyNumber, propertyMeterNumber, meterMakerNumber, meterFittedDate,
max(meterFittedDate) over (partition by propertyNumber, propertyMeterNumber order by meterFittedDate rows between 1 following and 1 following) as nextMeterFittedDate
--, meterClass, isCheckMeter, meterGroup, meterSize, meterCategory, waterMetertype 
from t2
) 
select t1.propertyNumber, t1.propertyMeterNumber, t1.meterMakerNumber, t1.meterFittedDate, t1.meterRemovedDate, t3.nextMeterFittedDate, isMasterMeter, isCheckMeter, meterClass, meterGroup, meterSize, meterCategory, waterMetertype, validFrom, validto
from   t1, t3
where  t1.propertyNumber = t3.propertyNumber
and    t1.propertyMeterNumber = t3.propertyMeterNumber
and    t1.meterMakerNumber = t3.meterMakerNumber
and    t1.meterFittedDate = t3.meterFittedDate
and    (t1.meterRemovedDate > t3.nextMeterFittedDate
or      (t3.nextMeterFittedDate is not null and t1.meterRemovedDate is null))
order by 1,2,4

-- COMMAND ----------

-- DBTITLE 1,Update the overlapping meter fit period on meterTimeSliceAccess
with t1 as (
select propertyNumber, propertyMeterNumber, meterMakerNumber, meterFittedDate, meterRemovedDate, isMasterMeter, isCheckMeter, meterClass, meterGroup, meterSize, meterCategory, waterMetertype, validFrom, validto
from curated.meterTimeSliceAccess
),
t2 as (
select propertyNumber, propertyMeterNumber, meterMakerNumber, meterFittedDate
from t1
group by 1,2,3,4
),
t3 as (
select propertyNumber, propertyMeterNumber, meterMakerNumber, meterFittedDate,
max(meterFittedDate) over (partition by propertyNumber, propertyMeterNumber order by meterFittedDate rows between 1 following and 1 following) as nextMeterFittedDate
from t2
), 
t4 as (
select t1.propertyNumber, t1.propertyMeterNumber, t1.meterMakerNumber, t1.meterFittedDate, t1.meterRemovedDate, t3.nextMeterFittedDate, isMasterMeter, isCheckMeter, meterClass, meterGroup, meterSize, meterCategory, waterMetertype, validFrom, validto
from   t1, t3
where  t1.propertyNumber = t3.propertyNumber
and    t1.propertyMeterNumber = t3.propertyMeterNumber
and    t1.meterMakerNumber = t3.meterMakerNumber
and    t1.meterFittedDate = t3.meterFittedDate
and    (t1.meterRemovedDate > t3.nextMeterFittedDate
or      (t3.nextMeterFittedDate is not null and t1.meterRemovedDate is null))
)

merge into curated.meterTimeSliceAccess mts1
using t4
on    mts1.propertyNumber = t4.propertyNumber
and   mts1.propertyMeterNumber = t4.propertyMeterNumber
and   mts1.meterMakerNumber = t4.meterMakerNumber
and   mts1.meterFittedDate = t4.meterFittedDate
and   mts1.meterRemovedDate = t4.meterRemovedDate
and   mts1.validFrom = t4.validFrom
and   mts1.validTo = t4.validTo
and   mts1.meterClass = t4.meterClass
and   mts1.isMasterMeter = t4.isMasterMeter
and   mts1.isCheckMeter = t4.isCheckMeter
and   mts1.meterGroup = t4.meterGroup
and   mts1.meterSize = t4.meterSize
and   mts1.meterCategory = t4.meterCategory
and   mts1.waterMetertype = t4.waterMeterType
--and   mts1.propertyNumber >= 5568937 --and 5568937
when matched then update set meterRemovedDate = nextMeterFittedDate


-- COMMAND ----------

-- DBTITLE 1,Display total number of rows on meterTimeSliceAccess
select count(*)
from   curated.meterTimeSliceAccess

-- COMMAND ----------

-- DBTITLE 1,Update the validTo dates in the records
with t1 as 
        (select propertyNumber, propertyMeterNumber, validFrom, validTo, lead(validFrom) over (partition by propertyNumber, propertyMeterNumber order by validFrom) as newValidTo
         from   curated.meterTimesliceAccess
         where  validTo != '9999-12-31'
        ),
     t2 as
        (select *
         from   t1
         where  t1.newValidTo is not null
        )
merge into curated.meterTimeSliceAccess mts
using t2
on    mts.propertyNumber = t2.propertyNumber
and   mts.propertyMeterNumber = t2.propertyMeterNumber
and   mts.validFrom = t2.validFrom
and   mts.validTo = t2.validTo
when matched then update set validTo = newValidTo

-- COMMAND ----------

-- DBTITLE 1,Delete from the cleansed TPropMeter table all rows that didn't make it into the time slice
delete
from cleansed.access_z309_tpropmeter_cleansed pm
where not exists (select 1
                  from   curated.meterTimeSliceAccess mts
                  where  mts.propertyNumber = pm.propertyNumber
                  and    mts.propertyMeterNumber = pm.propertyMeterNumber
                  and    mts.meterMakerNumber = pm.meterMakerNumber)

-- COMMAND ----------

-- DBTITLE 1,Delete from the cleansed THPropMeter table all rows that didn't make it into the time slice
delete
from cleansed.access_z309_thpropmeter_cleansed pm
where not exists (select 1
                  from   curated.meterTimeSliceAccess mts
                  where  mts.propertyNumber = pm.propertyNumber
                  and    mts.propertyMeterNumber = pm.propertyMeterNumber
                  and    mts.meterMakerNumber = pm.meterMakerNumber)

-- COMMAND ----------

-- DBTITLE 1,In our cleansed tpropmeter, adjust the meterRemovedDate as per the meterTimeSliceAccess table
with t1 as (
    select distinct pm.*, mts.meterRemovedDate as newMeterRemovedDate
    from   cleansed.access_z309_tpropmeter_cleansed pm,
           curated.meterTimeSliceAccess mts
    where  mts.propertyNumber = pm.propertyNumber
    and    mts.propertyMeterNumber = pm.propertyMeterNumber
    and    mts.meterMakerNumber = pm.meterMakerNumber
    and    mts.meterFittedDate = pm.meterFittedDate
    and    (mts.meterRemovedDate != pm.meterRemovedDate
    or      (mts.meterRemovedDate is not null and pm.meterRemovedDate is null))
    )
merge into cleansed.access_z309_tpropmeter_cleansed pm
using t1
on    t1.propertyNumber = pm.propertyNumber
and   t1.propertyMeterNumber = pm.propertyMeterNumber
and   t1.meterMakerNumber = pm.meterMakerNumber
and   t1.meterSizeCode = pm.meterSizeCode
and   t1.isMasterMeter = pm.isMasterMeter
and   t1.isCheckMeter = pm.isCheckMeter
and   t1.allowAlso = pm.allowAlso
and   t1.meterReadingFrequencyCode = pm.meterReadingFrequencyCode
and   t1.meterFittedDate = pm.meterFittedDate
and   t1.meterClass = pm.meterClass
and   t1.meterCategoryCode = pm.meterCategoryCode
and   t1.meterGroup = pm.meterGroup
and   t1.waterMetertype = pm.waterMeterType
and   (pm.meterRemovedDate != t1.newMeterRemovedDate
or     (t1.meterRemovedDate is not null and pm.meterRemovedDate is null))
--and   mts1.propertyNumber >= 5568937 --and 5568937
when matched then update set pm.meterRemovedDate = t1.newMeterRemovedDate 

-- COMMAND ----------

-- DBTITLE 1,This one meter is a duplicate record that breaks something in time slice logic. Delete it.
delete
from   cleansed.access_z309_thpropmeter_cleansed hpm
where  propertyNumber = 3115082
and    propertyMeterNumber = 1
and    rowSupersededDate = '2017-04-13'
and    rowSupersededTime = '095931000'
and    meterFittedDate = '2011-02-15'

-- COMMAND ----------

-- DBTITLE 1,In our cleansed thpropmeter, adjust the meterFittedDate and meterRemovedDate as per the meterTimeSliceAccess table
with t1 as (
    select distinct hpm.propertyNumber, hpm.propertyMeterNumber, hpm.meterSizeCode, hpm.meterSize, hpm.meterReadingFrequencyCode, hpm.meterFittedDate, max(hpm.meterRemovedDate) as meterRemovedDate, 
                   hpm.meterMakerNumber, hpm.meterClassCode, hpm.meterClass, hpm.meterCategoryCode, hpm.meterCategory, 
                   hpm.meterGroupCode, hpm.meterGroup, hpm.isMasterMeter, hpm.isCheckMeter, hpm.allowAlso, hpm.waterMeterType, hpm.propertyMeterUpdatedDate, hpm.rowSupersededDate, hpm.rowSupersededTime, 
                   mts.meterFittedDate as newMeterFittedDate, max(mts.meterRemovedDate) as newMeterRemovedDate
    from   cleansed.access_z309_thpropmeter_cleansed hpm,
           curated.meterTimeSliceAccess mts
    where  mts.propertyNumber = hpm.propertyNumber
    and    mts.propertyMeterNumber = hpm.propertyMeterNumber
    and    mts.meterMakerNumber = hpm.meterMakerNumber
    and    (mts.meterFittedDate != hpm.meterFittedDate
    or      mts.meterRemovedDate != hpm.meterRemovedDate
    or      (mts.meterRemovedDate is not null and hpm.meterRemovedDate is null))
    group by hpm.propertyNumber, hpm.propertyMeterNumber, hpm.meterMakerNumber, hpm.meterSize, hpm.meterSizeCode, hpm.isMasterMeter, hpm.isCheckMeter, hpm.allowAlso, hpm.meterReadingFrequencyCode, hpm.meterClass, hpm.meterClassCode,
         hpm.meterCategory, hpm.meterCategoryCode, hpm.meterGroup, hpm.meterGroupCode, hpm.waterMeterType, hpm.propertyMeterUpdatedDate, hpm.rowSupersededDate, hpm.rowSupersededTime, hpm.meterFittedDate,
         mts.meterFittedDate
    )
    
merge into cleansed.access_z309_thpropmeter_cleansed hpm
using t1
on    t1.propertyNumber = hpm.propertyNumber
and   t1.propertyMeterNumber = hpm.propertyMeterNumber
and   t1.meterMakerNumber = hpm.meterMakerNumber
and   t1.meterSize = hpm.meterSize
and   t1.isMasterMeter = hpm.isMasterMeter
and   t1.isCheckMeter = hpm.isCheckMeter
and   t1.allowAlso = hpm.allowAlso
and   t1.meterReadingFrequencyCode = hpm.meterReadingFrequencyCode
and   t1.meterClass = hpm.meterClass
and   t1.meterCategoryCode = hpm.meterCategoryCode
and   t1.meterGroup = hpm.meterGroup
and   t1.propertyMeterUpdatedDate = hpm.propertyMeterUpdatedDate
and   t1.rowSupersededDate = hpm.rowSupersededDate
and   t1.rowSupersededTime = hpm.rowSupersededTime
and   (hpm.meterFittedDate != t1.newMeterFittedDate
or     hpm.meterRemovedDate != t1.newMeterRemovedDate
or     (t1.meterRemovedDate is not null and hpm.meterRemovedDate is null))
--and   mts1.propertyNumber >= 5568937 --and 5568937
when matched then update set hpm.meterFittedDate = t1.newMeterFittedDate, hpm.meterRemovedDate = t1.newMeterRemovedDate 

-- COMMAND ----------

-- DBTITLE 1,Rebuild meterTimeSliceACCESS with the adjusted dates in the source tables
-- MAGIC %python
-- MAGIC #start by refreshing meterTimesliceAccess
-- MAGIC dbutils.notebook.run('../../curated/02 relationshipTables/meterTimesliceAccess',600)

-- COMMAND ----------

-- DBTITLE 1,Build a complete list of billable meter readings with meters present at the time of meter reading
create or replace temp view allReadings as
select mr.*, mts.meterMakerNumber, mts.meterFittedDate
from   cleansed.access_z309_tmeterreading mr left outer join curated.metertimesliceaccess mts on mts.propertyNumber = mr.propertyNumber 
                                                                                               and mts.propertyMeterNumber = mr.propertyMeterNumber
                                                                                               and mr.readingToDate between mts.validFrom and mts.validTo
                                                                                               and mr.readingToDate != mts.validFrom
where mr.meterReadingDays > 0 
and mr.meterReadingConsumption > 0 
and mr.meterReadingStatusCode in ('A','P','V','B');

--Show unique count of missing meters
select count(distinct propertyNumber, propertyMeterNumber) as countOfMissingMeters
from   allReadings
where  meterMakerNumber is null;

-- COMMAND ----------

-- DBTITLE 1,Create dummy rows for meter readings prior to the first meter fit date or where there is no meter fitted at all
create or replace temp view newMeters as
with t1 as (select a.propertyNumber, a.propertyMeterNumber, min(a.readingFromDate - 1) as meterFittedDate, max(a.readingToDate) as meterRemovedDate, first(earliestMeterFit) as earliestMeterFit
            from   allreadings a left outer join (select propertyNumber, propertyMeterNumber, min(meterFittedDate) as earliestMeterFit
                                                  from   curated.meterTimeSliceAccess mts 
                                                  group by propertyNumber, propertyMeterNumber) ta
                                                  on     ta.propertyNumber = a.propertyNumber and ta.propertyMeterNumber = a.propertyMeterNumber 
            where  a.meterMakerNumber is null --and a.propertyNumber = 3100017
            and    (a.readingToDate <= earliestMeterFit
            or      earliestMeterFit is null)
            group by a.propertyNumber, a.propertyMeterNumber
            ),
     --meters with the same meter fit and removal dates are removed so subtract a day from the meterFittedDate when that happens
     t2 as (select t1.propertyNumber, 'ACCESS' as dataSource, t1.propertyMeterNumber, meterSizeCode, meterSize, meterReadingFrequencyCode, 
                   if(t1.meterFittedDate = t1.meterRemovedDate,date_add(t1.meterFittedDate,-1),t1.meterFittedDate) as meterFittedDate, t1.meterRemovedDate, 
                   'DUMMY-'||cast(t1.propertyNumber as string)||'-'||cast(t1.propertyMeterNumber as string)||'-1' as meterMakerNumber, meterClassCode, meterClass, meterCategoryCode, meterCategory, 
                   meterGroupCode, meterGroup, isMasterMeter, isCheckMeter, false as allowAlso, waterMeterType, earliestMeterFit as propertyMeterUpdatedDate
            from   t1, curated.meterTimeSliceAccess mts
            where  t1.earliestMeterFit is not null
            and    t1.propertyNumber = mts.propertyNumber
            and    t1.propertyMeterNumber = mts.propertyMeterNumber
            and    t1.earliestMeterFit = mts.meterFittedDate
            ),
     t3 as (select t1.propertyNumber, 'ACCESS' as dataSource, t1.propertyMeterNumber, '01' as meterSizeCode, '20 mm' as meterSize, '3M' as meterReadingFrequencyCode, 
                   if(t1.meterFittedDate = t1.meterRemovedDate,date_add(t1.meterFittedDate,-1),t1.meterFittedDate) as meterFittedDate, t1.meterRemovedDate, 
                    'DUMMY-'||cast(t1.propertyNumber as string)||'-'||cast(t1.propertyMeterNumber as string)||'-2' as meterMakerNumber, '01'  as meterClassCode, 'Potable' as meterClass, 'S' as meterCategoryCode, 
                    'Standard Meter' as meterCategory, 'N' as meterGroupCode, 'Normal Reading' as meterGroup, false as isMasterMeter, false as isCheckMeter, false as allowAlso, 'Potable' as waterMeterType, 
                    earliestMeterFit as propertyMeterUpdatedDate
            from   t1
            where  t1.earliestMeterFit is null
            )
select *
from   t2
union all
select * from t3;

select count(*) from newMeters

-- COMMAND ----------

-- DBTITLE 1,Insert rows for dummy meters into propmeter_cleansed
insert into cleansed.access_z309_tpropmeter_cleansed
select distinct * from newMeters

-- COMMAND ----------

-- DBTITLE 1,Create dummy rows for readings after the last meter was removed
create or replace temp view newMeters as
with t1 as (select a.propertyNumber, a.propertyMeterNumber, min(a.readingFromDate - 1) as meterFittedDate, max(a.readingToDate) as meterRemovedDate, first(latestRemoved) as latestRemoved
            from   allreadings a left outer join (select propertyNumber, propertyMeterNumber, max(meterRemovedDate) as latestRemoved
                                                  from   curated.meterTimeSliceAccess mts 
                                                  group by propertyNumber, propertyMeterNumber) ta
                                                  on     ta.propertyNumber = a.propertyNumber and ta.propertyMeterNumber = a.propertyMeterNumber 
            where  a.meterMakerNumber is null --and a.propertyNumber = 3100017
            and    a.readingToDate > latestRemoved
            group by a.propertyNumber, a.propertyMeterNumber
            ),
     --meters with the same meter fit and removal dates are removed so subtract a day from the meterFittedDate when that happens
     t2 as (select t1.propertyNumber, 'ACCESS' as dataSource, t1.propertyMeterNumber, meterSizeCode, meterSize, meterReadingFrequencyCode, 
                   if(t1.meterFittedDate = t1.meterRemovedDate,date_add(t1.meterFittedDate,-1),t1.meterFittedDate) as meterFittedDate, t1.meterRemovedDate, 
                   'DUMMY-'||cast(t1.propertyNumber as string)||'-'||cast(t1.propertyMeterNumber as string)||'-3' as meterMakerNumber, meterClassCode, meterClass, meterCategoryCode, meterCategory, 
                   meterGroupCode, meterGroup, isMasterMeter, isCheckMeter, false as allowAlso, waterMeterType, latestRemoved as propertyMeterUpdatedDate
            from   t1, curated.meterTimeSliceAccess mts
            where  t1.propertyNumber = mts.propertyNumber
            and    t1.propertyMeterNumber = mts.propertyMeterNumber
            and    t1.latestRemoved = mts.meterRemovedDate
            )
select *
from   t2;

select count(*) from newMeters

-- COMMAND ----------

-- DBTITLE 1,Insert rows for dummy meters into propmeter_cleansed
insert into cleansed.access_z309_tpropmeter_cleansed
select distinct * from newMeters

-- COMMAND ----------

-- DBTITLE 1,Rebuild metertimesliceaccess
-- MAGIC %python
-- MAGIC #run the timeslice table refresh
-- MAGIC dbutils.notebook.run('../../curated/02 relationshipTables/meterTimesliceAccess',600)

-- COMMAND ----------

-- DBTITLE 1,Display total number of rows on meterTimeSliceAccess
select count(*)
from   curated.meterTimeSliceAccess

-- COMMAND ----------

-- DBTITLE 1,Build a complete list of billable meter readings with meters present at the time of meter reading (to see what is left)
create or replace temp view allReadings as
select mr.*, mts.meterMakerNumber, mts.meterFittedDate
from   cleansed.access_z309_tmeterreading mr left outer join curated.metertimesliceaccess mts on mts.propertyNumber = mr.propertyNumber 
                                                                                               and mts.propertyMeterNumber = mr.propertyMeterNumber
                                                                                               and mr.readingToDate between mts.validFrom and mts.validTo
                                                                                               and mr.readingToDate != mts.validFrom
where mr.meterReadingDays > 0 
and mr.meterReadingConsumption > 0 
and mr.meterReadingStatusCode in ('A','P','V','B');

--show count of missing meters
select count(distinct propertyNumber, propertyMeterNumber) as countOfMissingMeters
from   allReadings
where  meterMakerNumber is null;

