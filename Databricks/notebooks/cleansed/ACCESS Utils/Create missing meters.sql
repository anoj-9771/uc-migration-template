-- Databricks notebook source
-- MAGIC %md
-- MAGIC <b>Before you run this notebook you first need to run the code that backfills consumption for 1998/1999 and 2012</b><br />
-- MAGIC <ol>
-- MAGIC   <li>Process input file Z309_TMETERREADING_BI and rerun Z309_TMETERREADING notebook</li>
-- MAGIC   <li>Process input file Z309_TPROPMETER_BI and rerun Z309_TPROPMETER notebook</li>
-- MAGIC </ol>
-- MAGIC This Notebook will generate meter records on ACCESS_Z309_TPROPMETER for meters that historically were removed from the property without having been written to history (and which could not be found in BI)<br />
-- MAGIC <br />
-- MAGIC Change the fact table query to include the date_add in the between clause:<br />
-- MAGIC <code>
-- MAGIC   and mr.readingToDate between mts.validFrom and date_add(mts.validTo,31)
-- MAGIC </code>

-- COMMAND ----------

-- DBTITLE 1,Delete all dummy records
delete from cleansed.access_z309_tpropmeter
where  meterMakerNumber like 'DUMMY-%'

-- COMMAND ----------

-- DBTITLE 1,Rebuild metertimesliceaccess
-- MAGIC %python
-- MAGIC #start by refreshing meterTimesliceAccess
-- MAGIC dbutils.notebook.run('../../curated/02 relationshipTables/meterTimesliceAccess',600)

-- COMMAND ----------

-- DBTITLE 1,Build a complete list of billable meter readings with meters present at the time, allow for manual reads added after an exchange (up to 31 days)
create or replace temp view allReadings as
select mr.*, mts.meterMakerNumber, mts.meterFittedDate
from   cleansed.access_z309_tmeterreading mr left outer join curated.metertimesliceaccess mts on mts.propertyNumber = mr.propertyNumber 
                                                                                               and mts.propertyMeterNumber = mr.propertyMeterNumber
                                                                                               and ((mr.readingToDate between mts.validFrom and mts.validTo
                                                                                                   and mr.readingToDate != mts.validFrom) 
                                                                                               or  (mr.readingFromDate between mts.validfrom and mts.validto
                                                                                                   and  mr.readingToDate between mts.validFrom and date_add(mts.validTo,31)))
                                                                                               -- either take the meter if the readingtoDate is between meter valid from and to
                                                                                               -- or if the reading from date is covered and the reading to data is less than a month out
where mr.meterReadingDays > 0 
and mr.meterReadingConsumption > 0                                                                                              

-- COMMAND ----------

-- DBTITLE 1,Show unique count of missing meters
select count(distinct propertyNumber, propertyMeterNumber) as countOfMissingMeters
from   allReadings
where  meterMakerNumber is null

-- COMMAND ----------

-- DBTITLE 1,Main query for insert of meters prior to first meter fitted or where no meters are present
create or replace temp view newMeters as
with t1 as (select propertyNumber, propertyMeterNumber, min(readingFromDate) as newMeterFittedDate, max(readingToDate) as lastReadDate
            from   allreadings
            where  meterMakerNumber is null --and propertyNumber = 3100078
            group by propertyNumber, propertyMeterNumber),
     t2 as (select pm.*, newMeterFittedDate, lastReadDate, row_number() over (partition by pm.propertyNumber, pm.propertyMeterNumber order by pm.meterFittedDate) as rn
            from   cleansed.access_z309_tpropmeter pm,
                   t1
            where  pm.propertyNumber = t1.propertyNumber
            and    pm.propertyMeterNumber = t1.propertyMeterNumber
            and    pm.meterFittedDate >= t1.newMeterFittedDate),
     t3 as (select propertyNumber, propertyMeterNumber, 'DUMMY-'||cast(propertyNumber as string)||'-'||cast(propertyMeterNumber as string)||'-1' as meterMakerNumber, meterSizeCode, meterSize, isMasterMeter, 
                   isCheckMeter, allowAlso, 
                   isMeterConnected, meterReadingFrequencyCode, meterClassCode, meterClass, waterMeterType, 
                   meterCategoryCode, meterCategory, meterGroupCode, meterGroup, meterReadingLocationCode, meterReadingRouteNumber, meterServes, meterGridLocationCode, readingInstructionCode1, readingInstructionCode2, 
                   hasAdditionalDescription, hasMeterRoutePreparation, hasMeterWarningNote, newMeterFittedDate as meterFittedDate, meterReadingSequenceNumber, meterChangeReasonCode, 
                   meterChangeAdviceNumber, least(date_add(meterFittedDate,-1),lastReadDate) as meterRemovedDate, 
                   least(date_add(meterFittedDate,-1),lastReadDate) as propertyMeterUpdatedDate, current_date as _RecordStart, current_date as _RecordEnd, 0 as _RecordDeleted, 1 as _RecordCurrent
            from   t2
            where  rn = 1),
     t4 as (select propertyNumber, propertyMeterNumber, 'DUMMY-'||cast(propertyNumber as string)||'-'||cast(propertyMeterNumber as string)||'-2' as meterMakerNumber, '01' as meterSizeCode, '20 mm' as meterSize, 
                   false as isMasterMeter, false as isCheckMeter, false as allowAlso, false as isMeterConnected, '3M' as meterReadingFrequencyCode, '01' as meterClassCode, 'Potable' as meterClass, 'Potable' as waterMeterType, 
                   'S' as meterCategoryCode, 'Standard Meter' as meterCategory, 'N' as meterGroupCode, 'Normal Reading' as meterGroup, ' ' as meterReadingLocationCode, null as meterReadingRouteNumber, ' ' as meterServes, 
                   ' ' as meterGridLocationCode, null as readingInstructionCode1, null as readingInstructionCode2, false as hasAdditionalDescription, false as hasMeterRoutePreparation, false as hasMeterWarningNote, 
                   newMeterFittedDate as meterFittedDate, 0 as meterReadingSequenceNumber, '01' as meterChangeReasonCode, null as meterChangeAdviceNumber, lastReadDate as meterRemovedDate, 
                   lastReadDate as propertyMeterUpdatedDate, current_date as _RecordStart, current_date as _RecordEnd, 0 as _RecordDeleted, 1 as _RecordCurrent
            from   t1
            where  not exists(select 1 
                              from cleansed.access_z309_tpropmeter pm 
                              where pm.propertyNumber = t1.propertyNumber
                              and   pm.propertyMeterNumber = t1.propertyMeterNumber
                              and   pm.meterFittedDate >= t1.newMeterFittedDate))
select * from t3
union all
select * from t4

-- COMMAND ----------

-- DBTITLE 1,Show count of meters to be inserted
select count(*) as toBeInserted
from newMeters

-- COMMAND ----------

-- DBTITLE 1,Insert all new meters
insert into cleansed.access_z309_tpropmeter
select * from newMeters

-- COMMAND ----------

-- DBTITLE 1,Rebuild metertimesliceaccess
-- MAGIC %python
-- MAGIC #run the timeslice table refresh
-- MAGIC dbutils.notebook.run('../../curated/02 relationshipTables/meterTimesliceAccess',600)

-- COMMAND ----------

-- DBTITLE 1,For the remaining readings, just insert a meter for each reading (no clues what else to do with historical junk)
insert into cleansed.access_z309_tpropmeter
        with t1 as (select mr.*, mts.meterMakerNumber, mts.meterFittedDate
        from   cleansed.access_z309_tmeterreading mr left outer join curated.metertimesliceaccess mts on mts.propertyNumber = mr.propertyNumber 
                                                                                                       and mts.propertyMeterNumber = mr.propertyMeterNumber
                                                                                                       and (mr.readingToDate between mts.validFrom and mts.validTo
                                                                                                       or   (mr.readingFromDate between mts.validfrom and mts.validto
                                                                                                       and  mr.readingToDate between mts.validFrom and date_add(mts.validTo,31)))
                                                                                                       -- either take the meter if the readingtoDate is between meter valid from and to
                                                                                                       -- or if the reading from date is covered and the reading to data is less than a month out
        where mr.meterReadingStatusCode IN ('A','B','P','V') 
        and mr.meterReadingDays > 0 
        and mr.meterReadingConsumption > 0     
        and meterMakerNumber is null
        )
        select propertyNumber, propertyMeterNumber, 'DUMMY-'||cast(propertyNumber as string)||'-'||cast(propertyMeterNumber as string)||'-3' as meterMakerNumber, '01' as meterSizeCode, '20 mm' as meterSize, 
                   false as isMasterMeter, false as isCheckMeter, false as allowAlso, false as isMeterConnected, '3M' as meterReadingFrequencyCode, '01' as meterClassCode, 'Potable' as meterClass, 'Potable' as waterMeterType, 
                   'S' as meterCategoryCode, 'Standard Meter' as meterCategory, 'N' as meterGroupCode, 'Normal Reading' as meterGroup, ' ' as meterReadingLocationCode, null as meterReadingRouteNumber, ' ' as meterServes, 
                   ' ' as meterGridLocationCode, null as readingInstructionCode1, null as readingInstructionCode2, false as hasAdditionalDescription, false as hasMeterRoutePreparation, false as hasMeterWarningNote, 
                   readingFromDate as meterFittedDate, 0 as meterReadingSequenceNumber, '01' as meterChangeReasonCode, null as meterChangeAdviceNumber, readingToDate as meterRemovedDate, 
                   readingToDate as propertyMeterUpdatedDate, current_date as _RecordStart, current_date as _RecordEnd, 0 as _RecordDeleted, 1 as _RecordCurrent
        from   t1

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #run the timeslice table refresh
-- MAGIC dbutils.notebook.run('../../curated/02 relationshipTables/meterTimesliceAccess',600)

-- COMMAND ----------

-- DBTITLE 1,Show count of meters still missing
--meters still missing
with t1 as (select mr.*, mts.meterMakerNumber, mts.meterFittedDate
from   cleansed.access_z309_tmeterreading mr left outer join curated.metertimesliceaccess mts on mts.propertyNumber = mr.propertyNumber 
                                                                                               and mts.propertyMeterNumber = mr.propertyMeterNumber
                                                                                               and (mr.readingToDate between mts.validFrom and mts.validTo
                                                                                               or   (mr.readingFromDate between mts.validfrom and mts.validto
                                                                                               and  mr.readingToDate between mts.validFrom and date_add(mts.validTo,31)))
                                                                                               -- either take the meter if the readingtoDate is between meter valid from and to
                                                                                               -- or if the reading from date is covered and the reading to data is less than a month out
where mr.meterReadingDays > 0 
and mr.meterReadingConsumption > 0     
and meterMakerNumber is null
)
select count(distinct propertyNumber, propertyMeterNumber) as countOfMissingMeters
from t1

-- COMMAND ----------

-- DBTITLE 1,Show total consumption for which no unique meter is identifiable
with t1 as (select mr.*, mts.meterMakerNumber, mts.meterFittedDate
from   cleansed.access_z309_tmeterreading mr left outer join curated.metertimesliceaccess mts on mts.propertyNumber = mr.propertyNumber 
                                                                                               and mts.propertyMeterNumber = mr.propertyMeterNumber
                                                                                               and (mr.readingToDate between mts.validFrom and mts.validTo
                                                                                               or   (mr.readingFromDate between mts.validfrom and mts.validto
                                                                                               and  mr.readingToDate between mts.validFrom and date_add(mts.validTo,31)))
                                                                                               -- either take the meter if the readingtoDate is between meter valid from and to
                                                                                               -- or if the reading from date is covered and the reading to data is less than a month out
where mr.meterReadingStatusCode IN ('A','B','P','V') 
and mr.meterReadingDays > 0 
and mr.meterReadingConsumption > 0    
and meterMakerNumber is null
)
select sum(meterReadingconsumption)
from t1
--where propertyNumber = 4582990

-- COMMAND ----------

-- MAGIC %md
-- MAGIC End of functional code. The rest is experimental/testing

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select * from curated.metertimesliceaccess
-- MAGIC where propertyNumber = 3100078

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select * from cleansed.access_z309_tmeterreading
-- MAGIC where propertyNumber = 3100078
-- MAGIC order by readingToDate

-- COMMAND ----------



-- COMMAND ----------

-- MERGE INTO curated.metertimesliceaccess mts
-- USING (select mr.*, mts.meterMakerNumber
--         from   cleansed.access_z309_tmeterreading mr left outer join curated.metertimesliceaccess mts on mts.propertyNumber = mr.propertyNumber 
--                                                                                                        and mts.propertyMeterNumber = mr.propertyMeterNumber
--                                                                                                        and (mr.readingToDate between mts.validFrom and mts.validTo
--                                                                                                        or   (mr.readingFromDate between mts.validfrom and mts.validto
--                                                                                                        and  mr.readingToDate between mts.validFrom and date_add(mts.validTo,31)))
--                                                                                                        -- either take the meter if the readingtoDate is between meter valid from and to
--                                                                                                        -- or if the reading from date is covered and the reading to data is less than a month out
--         where mr.meterReadingStatusCode IN ('A','B','P','V') 
--         and mr.meterReadingDays > 0 
--         and mr.meterReadingConsumption > 0    
--         and meterMakerNumber is null) as t1
-- ON (mts.propertyNumber = t1.propertyNumber
-- and    mts.propertyMeterNumber = t1.propertyMeterNumber
-- and    t1.readingFromDate between mts.validFrom and mts.validTo
-- and    mts.validTo < t1.readingToDate)
-- WHEN MATCHED THEN UPDATE SET mts.validTo = t1.readingToDate


-- COMMAND ----------

select * 
from allreadings
where propertyNumber = 3100190

-- COMMAND ----------

select * from curated.metertimesliceaccess
where propertyNumber = 3100190

-- COMMAND ----------

select * 
from cleansed.access_z309_tpropmeter
where propertyNumber = 4582990
and propertyMeterNumber = 1

-- COMMAND ----------

select * 
from curated.metertimesliceaccess
where propertyNumber = 3248093
--and propertyMeterNumber = 2

-- COMMAND ----------

select propertyNumber, propertyMeterNumber, min(readingFromDate) as newMeterFittedDate, max(readingToDate) as lastReadDate
            from   allreadings
            where propertyNumber = 4392889
            group by propertyNumber, propertyMeterNumber

-- COMMAND ----------

with t1 as (select propertyNumber, propertyMeterNumber, min(readingFromDate) as newMeterFittedDate, max(readingToDate) as lastReadDate
            from   allreadings
            where  meterMakerNumber is null
            and    propertyNumber = 4392889
            group by propertyNumber, propertyMeterNumber),
     t2 as (select pm.*, newMeterFittedDate, lastReadDate, row_number() over (partition by pm.propertyNumber, pm.propertyMeterNumber order by pm.meterFittedDate) as rn
            from   cleansed.access_z309_tpropmeter pm,
                   t1
            where  pm.propertyNumber = t1.propertyNumber
            and    pm.propertyMeterNumber = t1.propertyMeterNumber
            and    pm.meterFittedDate >= t1.newMeterFittedDate)
select * from t2            

-- COMMAND ----------

select *
from cleansed.access_z309_tpropmeter
where propertyNumber = 3117714

-- COMMAND ----------

--main query for insert of meters that were disconnected prior to the last reading
create or replace temp view newmeters as 
with t1 as (select propertyNumber, propertyMeterNumber, min(readingFromDate) as newMeterFittedDate, max(readingToDate) as lastReadDate
            from   allreadings
            where  meterMakerNumber is null
            group by propertyNumber, propertyMeterNumber),
     t2 as (select pm.*, newMeterFittedDate, lastReadDate, row_number() over (partition by pm.propertyNumber, pm.propertyMeterNumber order by pm.meterFittedDate) as rn
            from   cleansed.access_z309_tpropmeter pm,
                   t1
            where  pm.propertyNumber = t1.propertyNumber
            and    pm.propertyMeterNumber = t1.propertyMeterNumber),
     t3 as (select propertyNumber, propertyMeterNumber, 'DUMMY-'||cast(propertyNumber as string)||'-'||cast(propertyMeterNumber as string)||'-1' as meterMakerNumber, meterSizeCode, meterSize, isMasterMeter, 
                   isCheckMeter, allowAlso, 
                   isMeterConnected, meterReadingFrequencyCode, meterClassCode, meterClass, waterMeterType, 
                   meterCategoryCode, meterCategory, meterGroupCode, meterGroup, meterReadingLocationCode, meterReadingRouteNumber, meterServes, meterGridLocationCode, readingInstructionCode1, readingInstructionCode2, 
                   hasAdditionalDescription, hasMeterRoutePreparation, hasMeterWarningNote, newMeterFittedDate as meterFittedDate, meterReadingSequenceNumber, meterChangeReasonCode, 
                   meterChangeAdviceNumber, least(date_add(meterFittedDate,-1),lastReadDate) as meterRemovedDate, 
                   least(date_add(meterFittedDate,-1),lastReadDate) as propertyMeterUpdatedDate, newMeterFittedDate as _RecordStart, meterFittedDate as _RecordEnd, 0 as _RecordDeleted, 1 as _RecordCurrent
            from   t2
            where  rn = 1)
select * from t3

-- COMMAND ----------

--show unique count of missing meters
select count(distinct propertyNumber, propertyMeterNumber) as countOfMissingMeters
from   allReadings
where  meterMakerNumber is null

-- COMMAND ----------

--show unique missing meters
select * --distinct propertyNumber, propertyMeterNumber
from   allReadings a
where  meterMakerNumber is null
and exists (select 1 from cleansed.access_z309_tpropmeter b where a.propertyNumber = b.propertyNumber and a.propertyMeterNumber = b.propertyMeterNumber)
and propertyNumber = 3132982

-- COMMAND ----------

select * from cleansed.access_z309_tpropmeter
where propertyNumber = 3132982

-- COMMAND ----------

--main query for insert of meters prior to first meter fitted
with t1 as (select propertyNumber, propertyMeterNumber, min(readingFromDate) as newMeterFittedDate, max(readingToDate) as lastReadDate
            from   allreadings
            where  meterMakerNumber is null
            and propertyNumber = 3132982
            group by propertyNumber, propertyMeterNumber),
     t2 as (select pm.*, newMeterFittedDate, lastReadDate, row_number() over (partition by pm.propertyNumber, pm.propertyMeterNumber order by pm.meterFittedDate) as rn
            from   cleansed.access_z309_tpropmeter pm,
                   t1
            where  pm.propertyNumber = t1.propertyNumber
            and    pm.propertyMeterNumber = t1.propertyMeterNumber),
     t3 as (select propertyNumber, propertyMeterNumber, 'DUMMY-'||cast(propertyNumber as string)||'-'||cast(propertyMeterNumber as string)||'-2' as meterMakerNumber, meterSizeCode, meterSize, isMasterMeter, 
                   isCheckMeter, allowAlso, 
                   isMeterConnected, meterReadingFrequencyCode, meterClassCode, meterClass, waterMeterType, 
                   meterCategoryCode, meterCategory, meterGroupCode, meterGroup, meterReadingLocationCode, meterReadingRouteNumber, meterServes, meterGridLocationCode, readingInstructionCode1, readingInstructionCode2, 
                   hasAdditionalDescription, hasMeterRoutePreparation, hasMeterWarningNote, newMeterFittedDate as meterFittedDate, meterReadingSequenceNumber, meterChangeReasonCode, 
                   meterChangeAdviceNumber, least(date_add(meterFittedDate,-1),lastReadDate) as meterRemovedDate, 
                   least(date_add(meterFittedDate,-1),lastReadDate) as propertyMeterUpdatedDate, newMeterFittedDate as _RecordStart, meterFittedDate as _RecordEnd, 0 as _RecordDeleted, 1 as _RecordCurrent
            from   t2
            where  rn = 1)
select * from t3

-- COMMAND ----------



-- COMMAND ----------

with t1 as(select propertyNumber, propertyMeterNumber from allreadings where meterMakerNumber is null)
select pm.*
from a pm, t1
where t1.propertyNumber = pm.propertyNumber
and   t1.propertyMeterNumber = pm.propertyMeterNumber
and t1.propertyNumber = 3117714

-- COMMAND ----------

create or replace temp view newmeters as 
with t1 as (select propertyNumber, propertyMeterNumber, min(readingFromDate) as newMeterFittedDate, max(readingToDate) as lastReadDate
            from   allreadings
            where  meterMakerNumber is null
            group by propertyNumber, propertyMeterNumber),
     t2 as (select pm.*, newMeterFittedDate, lastReadDate, row_number() over (partition by pm.propertyNumber, pm.propertyMeterNumber order by pm.meterFittedDate) as rn
            from   cleansed.access_z309_tpropmeter pm,
                   t1
            where  pm.propertyNumber = t1.propertyNumber
            and    pm.propertyMeterNumber = t1.propertyMeterNumber),
     t3 as (select propertyNumber, propertyMeterNumber, 'DUMMY-'||cast(propertyNumber as string)||'-'||cast(propertyMeterNumber as string)||'-1' as meterMakerNumber, meterSizeCode, meterSize, isMasterMeter, 
                   isCheckMeter, allowAlso, 
                   isMeterConnected, meterReadingFrequencyCode, meterClassCode, meterClass, waterMeterType, 
                   meterCategoryCode, meterCategory, meterGroupCode, meterGroup, meterReadingLocationCode, meterReadingRouteNumber, meterServes, meterGridLocationCode, readingInstructionCode1, readingInstructionCode2, 
                   hasAdditionalDescription, hasMeterRoutePreparation, hasMeterWarningNote, newMeterFittedDate as meterFittedDate, meterReadingSequenceNumber, meterChangeReasonCode, 
                   meterChangeAdviceNumber, least(date_add(meterFittedDate,-1),lastReadDate) as meterRemovedDate, 
                   least(date_add(meterFittedDate,-1),lastReadDate) as propertyMeterUpdatedDate, newMeterFittedDate as _RecordStart, meterFittedDate as _RecordEnd, 0 as _RecordDeleted, 1 as _RecordCurrent
            from   t2
            where  rn = 1)
select * from t3

-- COMMAND ----------

select count(*) 
from newmeters

-- COMMAND ----------

insert into cleansed.access_z309_tpropmeter
select * from newMeters

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <b>At this point you have to rerun metertimesliceaccess</b><br />
-- MAGIC After this you can go back to the top of this notebook and run the first two queries again to see the difference you have made

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.notebook.run('../../curated/02 relationshipTables/meterTimesliceAccess',600)

-- COMMAND ----------

delete from cleansed.access_z309_tpropmeter
where  meterMakerNumber like 'DUMMY-%'

-- COMMAND ----------

select propertyNumber, propertyMeterNumber, readingToDate
from   allReadings
where  meterMakerNumber is null
limit 20

-- COMMAND ----------

select *
--from   cleansed.access_z309_tpropmeter
from curated.metertimesliceaccess
where propertyNumber = 3100017

-- COMMAND ----------

select *
from   cleansed.access_z309_tpropmeter
where  propertyNumber = 5547132

-- COMMAND ----------

select *
from curated.metertimesliceaccess
where propertyNumber = 5547132

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.sql(f"with t1 as (select mr.readingToDate, \
-- MAGIC                                    mr.meterReadingConsumption, \
-- MAGIC                                    row_number() over (partition by mr.propertyNumber, mr.propertyMeterNumber, mr.readingFromDate \
-- MAGIC                                                        order by mr.meterReadingNumber desc, mr.meterReadingUpdatedDate desc) meterReadRecNumFrom, \
-- MAGIC                                    row_number() over (partition by mr.propertyNumber, mr.propertyMeterNumber, mr.readingToDate \
-- MAGIC                                                        order by mr.meterReadingNumber desc, mr.meterReadingUpdatedDate desc) meterReadRecNumTo \
-- MAGIC                               from CLEANSED.access_z309_tmeterreading mr \
-- MAGIC                                    left outer join CURATED.metertimesliceaccess mts on mts.propertyNumber = mr.propertyNumber \
-- MAGIC                                                                                      and mts.propertyMeterNumber = mr.propertyMeterNumber \
-- MAGIC                                                                                      and mr.readingToDate between mts.validFrom and mts.validTo \
-- MAGIC                                                                                      and mr.readingToDate != mts.validFrom \
-- MAGIC                                    left outer join CURATED.dimMeter dm on dm.meterSerialNumber = coalesce(mts.meterMakerNumber,'-1') \
-- MAGIC                                    left outer join CLEANSED.access_z309_tdebit dr on mr.propertyNumber = dr.propertyNumber \
-- MAGIC                                                    and dr.debitTypeCode = '10' and dr.debitReasonCode IN ('360','367') \
-- MAGIC                               where mr.meterReadingStatusCode IN ('A','B','P','V') \
-- MAGIC                                     and mr.meterReadingDays > 0 \
-- MAGIC                                     and mr.meterReadingConsumption > 0 \
-- MAGIC                                     and (not mts.isCheckMeter or mts.isCheckMeter is null) \
-- MAGIC                                     and mr._RecordCurrent = 1 \
-- MAGIC                                     and mr._RecordDeleted = 0 \
-- MAGIC                                     and dr.propertyNumber is null) select year(readingToDate) as year, sum(meterReadingConsumption) from t1 group by year order by 1 \
-- MAGIC                                    ")
-- MAGIC display(df)

-- COMMAND ----------

--are there readings on properties without a meter?
with t1 as(
select mr.*, coalesce(pm.meterMakerNumber, hpm.meterMakerNumber) as meterMakerNumber, coalesce(pm.meterFittedDate, hpm.meterFittedDate) as meterFittedDate
from   cleansed.access_z309_tmeterreading mr left outer join cleansed.access_z309_tpropmeter pm on pm.propertyNumber = mr.propertyNumber 
                                                                                               and pm.propertyMeterNumber = mr.propertyMeterNumber
                                                                                               --and mr.readingFromDate >= pm.meterFittedDate
                                             left outer join cleansed.access_z309_thpropmeter hpm on hpm.propertyNumber = mr.propertyNumber 
                                                                                               and hpm.propertyMeterNumber = mr.propertyMeterNumber
                                                                                               --and mr.readingFromDate >= hpm.meterFittedDate
)                                                                                               
select count(*) 
from t1 
where meterMakerNumber is null                                                                                             

-- COMMAND ----------

--create view of earliest fitted meter for a property
create or replace temp view firstMeters as
select *, row_number() over (partition by propertyNumber, propertyMeterNumber order by meterFittedDate asc) as rownum
from   cleansed.access_z309_tpropmeter

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.sql("describe cleansed.access_z309_tpropmeter")
-- MAGIC listValues=df.select("col_name").rdd.flatMap(lambda x: x).collect()
-- MAGIC colString = ', '.join(listValues)
-- MAGIC colString

-- COMMAND ----------


