-- Databricks notebook source
-- MAGIC %md
-- MAGIC Fields to acquire for cancelled ratings:<br >
-- MAGIC <ul>
-- MAGIC   <li>dimLocation - LGA</li>
-- MAGIC   <li>dimLocation - latitude</li>
-- MAGIC   <li>dimLocation - longitude</li>
-- MAGIC   <li>dimProperty - waterNetworkSK_drinkingWater</li>
-- MAGIC   <li>dimProperty - waterNetworkSK_recycledWater</li>
-- MAGIC   <li>dimProperty - sewerNetworkSK</li>
-- MAGIC   <li>dimProperty - stormWaterNetworkSK</li>
-- MAGIC </ul>

-- COMMAND ----------

-- DBTITLE 1,Build View of all cancelled properties
create or replace temporary view vAllCancelledProps as
select propertyNumber, null as activeProperty
from   cleansed.access_z309_tproperty
where  propertyTypeCode = '998'

-- COMMAND ----------

select 'Total cancelled ratings' as text, count(*) as count
from   vAllCancelledProps

-- COMMAND ----------

-- DBTITLE 1,Build View of previous property
create or replace temporary view vPreviousProperty as
with t1 as (select pp.previousPropertyNumber as oldProp, pr1.propertyTypeCode as oldPropType, 
                   pp.propertyNumber as newProp, pr2.propertyTypeCode as newPropType,
                   row_number() over (partition by previousPropertyNumber order by pr2.propertyTypeCode, loc.latitude) as rn
            from   cleansed.access_z309_tprevproperty pp, 
                   cleansed.access_z309_tproperty pr1, 
                   cleansed.access_z309_tproperty pr2 left outer join curated.dimLocation loc on pp.propertyNumber = locationID and loc.latitude is not null
            where  pp.previousPropertyNumber = pr1.propertyNumber
            and    pp.propertyNumber = pr2.propertyNumber
            and    pp.propertyNumber != pp.previousPropertyNumber
            and    pr1.propertyTypeCode = '998'
            )
select oldProp, oldPropType, newProp, newPropType 
from t1 
where rn = 1
ORDER BY 1,3,4


-- COMMAND ----------

select count(distinct previousPropertyNumber) 
from  cleansed.access_z309_tprevproperty--propList

-- COMMAND ----------

-- MAGIC %python
-- MAGIC prop_df = spark.sql('select * from vPreviousProperty')
-- MAGIC propList = [row.asDict() for row in prop_df.collect()]

-- COMMAND ----------

-- DBTITLE 1,Create an index of old prop linked to new prop and new prop type
-- MAGIC %python
-- MAGIC propDict = {}
-- MAGIC for row in propList:
-- MAGIC     propDict[row['oldProp']] = [row['newProp'], row['newPropType']] 
-- MAGIC # propDict

-- COMMAND ----------

-- MAGIC %python
-- MAGIC def getActiveProp(cancelledProperty):
-- MAGIC     if propDict.get(cancelledProperty,[0,'x'])[1] == '998':
-- MAGIC         newProp = getActiveProp(propDict[cancelledProperty][0])
-- MAGIC     else:
-- MAGIC         newProp = propDict.get(cancelledProperty,[0,'x'])[0]
-- MAGIC
-- MAGIC     return newProp
-- MAGIC             

-- COMMAND ----------

-- DBTITLE 1,Loop the cancelled props until a new active one is found
-- MAGIC %python
-- MAGIC referenceList = []
-- MAGIC for row in propList:
-- MAGIC #     print(propDict.get(row['oldProp'],[0,'x'])[1])
-- MAGIC     referenceList.append([row['oldProp'],getActiveProp(row['oldProp'])])
-- MAGIC #     break
-- MAGIC cols = ['propertyNumber', 'activeProperty']
-- MAGIC dfProps = spark.createDataFrame(referenceList, cols)
-- MAGIC dfProps.createOrReplaceTempView('vResolvedFromPrevProps')

-- COMMAND ----------

select 'Total cancelled ratings' as text, count(*) as count
from   vAllCancelledProps
union all
select 'Resolved via previous property' as text, count(*) as count
from   vResolvedFromPrevProps
where  activeProperty > 0

-- COMMAND ----------

-- MAGIC %md
-- MAGIC At this point we have resolved cancelled props to an active one in the best possible way. For the ones we don't have yet, try an address match

-- COMMAND ----------

-- DBTITLE 1,Build new view of all resolved and unresolved cancelled properties
--this creates a complete list, including properties not on tpreviousproperty
create or replace temporary view vIntermediateResults as
--only take resolved properties if they have Hydra details
select propertyNumber, null as activeProperty
from   vAllCancelledProps cp
where propertyNumber not in (select propertyNumber 
                             from vResolvedFromPrevProps pp, curated.dimLocation loc 
                             where pp.activeProperty > 0
                             and   pp.activeProperty = loc.locationID
                             and   loc.latitude is not null)
union all
select cp.propertyNumber, pp.activeProperty
from   vAllCancelledProps cp, vResolvedFromPrevProps pp, curated.dimLocation loc  
where pp.propertyNumber = cp.propertyNumber 
and   pp.activeProperty > 0
and   pp.activeProperty = loc.locationID
and   loc.latitude is not null

-- COMMAND ----------

-- DBTITLE 1,For ACCESS get addresses of cancelled properties
create or replace temp view vACCESSCancelledAddr as
select pa.propertyNumber, cast(houseNumber1 as int) as houseNumber1, cast(houseNumber2 as int) as houseNumber2, lotNumber, streetName, streetType, suburb, postcode
from   cleansed.access_z309_tpropertyaddress pa,
       cleansed.access_z309_tproperty pr,
       cleansed.access_z309_tstreetguide sg,
       vIntermediateResults cp
where pa.propertyNumber = pr.propertyNumber
and   pr.propertyTypeCode = '998'
and   cp.propertyNumber = pr.propertyNumber
and   cp.activeProperty is null
and   pa.streetguideCode = sg.streetguideCode;

create or replace temp view vACCESSActiveAddr as
select pa.propertyNumber, cast(houseNumber1 as int) as houseNumber1, cast(houseNumber2 as int) as houseNumber2, lotNumber, sg.streetName, sg.streetType, sg.suburb, sg.postcode --, propertyTypecode
from   cleansed.access_z309_tpropertyaddress pa,
--       cleansed.access_z309_tproperty pr,
       cleansed.access_z309_tstreetguide sg,
       curated.dimLocation loc
-- where pa.propertyNumber = pr.propertyNumber
where pa.propertyNumber = loc.locationID
--and   pr.propertyTypeCode not in ('998','071')
and   pa.streetguideCode = sg.streetguideCode
and   loc.latitude is not null

-- COMMAND ----------

create or replace temp view vMatchHouseNumber as
      select can.propertyNumber as cancelled, act.propertyNumber as active, can.houseNumber1 as can_h1, can.houseNumber2 as can_h2, act.houseNumber1 as act_h1, act.houseNumber2 as act_h2, can.lotNumber, can.streetName, can.streetType, can.suburb,
              row_number() over (partition by can.propertyNumber order by act.propertyNumber desc) as rn --, propertyTypecode
      from   vACCESSCancelledAddr can left outer join vACCESSActiveAddr act on can.streetName = act.streetName and coalesce(can.streetType,'') = coalesce(act.streetType,'') and can.suburb = act.suburb and can.postcode = act.postcode
      where (can.houseNumber1 = act.houseNumber1 or can.houseNumber1 between act.houseNumber1 and act.houseNumber2 or can.houseNumber1 = act.houseNumber2 or (can.houseNumber2 > 0 and can.houseNumber2 = act.houseNumber1))
      and    can.houseNumber1 > 0 
      and    act.houseNumber1 > 0;
      
create or replace temp view vMatchLotNumber as      
      select can.propertyNumber as cancelled, act.propertyNumber as active, can.houseNumber1 as can_h1, can.houseNumber2 as can_h2, act.houseNumber1 as act_h1, act.houseNumber2 as act_h2, can.lotNumber, can.streetName, can.streetType, can.suburb,
              row_number() over (partition by can.propertyNumber order by act.propertyNumber desc) as rn --, propertyTypecode
      from   vACCESSCancelledAddr can left outer join vACCESSActiveAddr act on can.streetName = act.streetName and coalesce(can.streetType,'') = coalesce(act.streetType,'') and can.suburb = act.suburb and can.postcode = act.postcode
      where  can.lotNumber = act.lotNumber
      and    can.lotNumber is not null;
      
create or replace temp view vMatchLotToHouseNumber as            
      select can.propertyNumber as cancelled, act.propertyNumber as active, can.houseNumber1 as can_h1, can.houseNumber2 as can_h2, act.houseNumber1 as act_h1, act.houseNumber2 as act_h2, can.lotNumber, can.streetName, can.streetType, can.suburb,
              row_number() over (partition by can.propertyNumber order by act.propertyNumber desc) as rn --, propertyTypecode
      from   vACCESSCancelledAddr can left outer join vACCESSActiveAddr act on can.streetName = act.streetName and coalesce(can.streetType,'') = coalesce(act.streetType,'') and can.suburb = act.suburb and can.postcode = act.postcode
      where  ltrim(can.lotNumber) = string(act.houseNumber1)
      and    can.lotNumber is not null;      
      
create or replace temp view vMatchsideOfStreet5updown as       
      select can.propertyNumber as cancelled, act.propertyNumber as active, can.houseNumber1 as can_h1, can.houseNumber2 as can_h2, act.houseNumber1 as act_h1, act.houseNumber2 as act_h2, can.lotNumber, can.streetName, can.streetType, can.suburb,
              row_number() over (partition by can.propertyNumber order by act.propertyNumber desc) as rn --, propertyTypecode
      from   vACCESSCancelledAddr can left outer join vACCESSActiveAddr act on can.streetName = act.streetName and coalesce(can.streetType,'') = coalesce(act.streetType,'') and can.suburb = act.suburb and can.postcode = act.postcode
      where  mod(can.houseNumber1,2) = mod(act.houseNumber1,2) 
      and    can.houseNumber1 between act.houseNumber1 - 10 and act.houseNumber1 + 10
      and    can.houseNumber1 > 0 
      and    act.houseNumber1 > 0;      
      
create or replace temp view vMatchsideOfStreet as       
      select can.propertyNumber as cancelled, act.propertyNumber as active, can.houseNumber1 as can_h1, can.houseNumber2 as can_h2, act.houseNumber1 as act_h1, act.houseNumber2 as act_h2, can.lotNumber, can.streetName, can.streetType, can.suburb,
              row_number() over (partition by can.propertyNumber order by act.propertyNumber desc) as rn --, propertyTypecode
      from   vACCESSCancelledAddr can left outer join vACCESSActiveAddr act on can.streetName = act.streetName and coalesce(can.streetType,'') = coalesce(act.streetType,'') and can.suburb = act.suburb and can.postcode = act.postcode
      where  mod(can.houseNumber1,2) = mod(act.houseNumber1,2) 
      and    can.houseNumber1 > 0 
      and    act.houseNumber1 > 0;      
      
create or replace temp view vMatchStreet as       
      select can.propertyNumber as cancelled, act.propertyNumber as active, can.houseNumber1 as can_h1, can.houseNumber2 as can_h2, act.houseNumber1 as act_h1, act.houseNumber2 as act_h2, can.lotNumber, can.streetName, can.streetType, can.suburb,
              row_number() over (partition by can.propertyNumber order by act.propertyNumber desc) as rn --, propertyTypecode
      from   vACCESSCancelledAddr can left outer join vACCESSActiveAddr act on can.streetName = act.streetName and coalesce(can.streetType,'') = coalesce(act.streetType,'') and can.suburb = act.suburb and can.postcode = act.postcode;

create or replace temp view vMatchSuburb as       
      select can.propertyNumber as cancelled, act.propertyNumber as active, can.houseNumber1 as can_h1, can.houseNumber2 as can_h2, act.houseNumber1 as act_h1, act.houseNumber2 as act_h2, can.lotNumber, can.streetName, can.streetType, can.suburb,
              row_number() over (partition by can.propertyNumber order by act.propertyNumber desc) as rn --, propertyTypecode
      from   vACCESSCancelledAddr can left outer join vACCESSActiveAddr act on can.suburb = act.suburb and can.postcode = act.postcode    

-- COMMAND ----------

select can.propertyNumber as cancelled, act.propertyNumber as active, can.houseNumber1 as can_h1, can.houseNumber2 as can_h2, act.houseNumber1 as act_h1, act.houseNumber2 as act_h2, can.lotNumber, can.streetName, can.streetType, can.suburb,
              row_number() over (partition by can.propertyNumber order by act.propertyNumber desc) as rn --, propertyTypecode
      from   vACCESSCancelledAddr can left outer join vACCESSActiveAddr act on can.streetName = act.streetName and coalesce(can.streetType,'') = coalesce(act.streetType,'') and can.suburb = act.suburb and can.postcode = act.postcode
      where can.propertyNumber = 4293282

-- COMMAND ----------

with t1 as (select propertyNumber, null as activeProperty
from   vAllCancelledProps cp
where propertyNumber not in (select propertyNumber 
                             from vResolvedFromPrevProps pp, curated.dimLocation loc 
                             where pp.activeProperty > 0
                             and   pp.activeProperty = loc.locationID
                             and   loc.latitude is not null)
union all
select cp.propertyNumber, pp.activeProperty
from   vAllCancelledProps cp, vResolvedFromPrevProps pp, curated.dimLocation loc  
where pp.propertyNumber = cp.propertyNumber 
and   pp.activeProperty > 0
and   pp.activeProperty = loc.locationID
and   loc.latitude is not null)
select * from vACCESSCancelledAddr
where propertyNumber = 4293282

-- COMMAND ----------

select *
from   vallcancelledprops
where  propertyNumber = 4293282

-- COMMAND ----------

-- DBTITLE 1,Match house numbers, match lot numbers and, if even that fails, match the same side of the street and finally the street
-- MAGIC %sql
-- MAGIC create or replace temp view vmatchedProps as
-- MAGIC with t1 as(
-- MAGIC           select 1 as rank, *
-- MAGIC           from   vMatchHouseNumber
-- MAGIC           where  rn = 1),
-- MAGIC     t2 as (
-- MAGIC           select 2 as rank, *
-- MAGIC           from   vMatchLotNumber
-- MAGIC           where  rn = 1),
-- MAGIC     t3 as (
-- MAGIC           select 3 as rank, *
-- MAGIC           from   vMatchLotToHouseNumber
-- MAGIC           where  rn = 1),
-- MAGIC     t4 as (
-- MAGIC           select 4 as rank, *
-- MAGIC           from   vMatchsideOfStreet5updown
-- MAGIC           where  rn = 1),
-- MAGIC     t5 as (
-- MAGIC           select 5 as rank, *
-- MAGIC           from   vMatchsideOfStreet
-- MAGIC           where  rn = 1),
-- MAGIC     t6 as (
-- MAGIC           select 6 as rank, *
-- MAGIC           from   vMatchStreet
-- MAGIC           where  rn = 1),
-- MAGIC --     t6 as (
-- MAGIC --           select 6 as rank, *
-- MAGIC --           from   vMatchSuburb
-- MAGIC --           where  rn = 1),
-- MAGIC     t7 as (
-- MAGIC           select  * from t1
-- MAGIC           where  active is not null
-- MAGIC           union all
-- MAGIC           select * from t2
-- MAGIC           where  active is not null
-- MAGIC           union all
-- MAGIC           select * from t3
-- MAGIC           where  active is not null
-- MAGIC           union all
-- MAGIC           select * from t4
-- MAGIC           where  active is not null
-- MAGIC           union all
-- MAGIC           select * from t5
-- MAGIC           where  active is not null
-- MAGIC           union all
-- MAGIC           select * from t6
-- MAGIC --           where  active is not null
-- MAGIC --           union all
-- MAGIC --           select * from t6
-- MAGIC           where  active is not null),
-- MAGIC     t8 as (
-- MAGIC           select cancelled, min(rank) as rank
-- MAGIC           from   t7
-- MAGIC           where  active is not null
-- MAGIC           group  by cancelled)
-- MAGIC select t7.*
-- MAGIC from   t7 , t8
-- MAGIC where  t7.cancelled = t8.cancelled
-- MAGIC and    t7.rank = t8.rank

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select 'Total cancelled ratings' as text, count(*) as count
-- MAGIC from   vAllCancelledProps
-- MAGIC union all
-- MAGIC select 'Resolved via previous property' as text, count(*) as count
-- MAGIC from   vResolvedFromPrevProps
-- MAGIC where  activeProperty > 0
-- MAGIC union all
-- MAGIC select 'Address matching attempted' as query, count(*)
-- MAGIC from   vACCESSCancelledAddr
-- MAGIC union all
-- MAGIC select 'Matched on address - on house number' as query, (select count(*) from vmatchedProps where rank = 1)
-- MAGIC union all
-- MAGIC select 'Matched on address - on lot number' as query, (select count(*) from vmatchedProps where rank = 2)
-- MAGIC union all
-- MAGIC select 'Matched on address - on lot number equals house number' as query, (select count(*) from vmatchedProps where rank = 3)
-- MAGIC union all
-- MAGIC select 'Matched on address - on house number (same side, 5 either side)' as query, (select count(*) from vmatchedProps where rank = 4)
-- MAGIC union all
-- MAGIC select 'Matched on address - on street (same side)' as query, (select count(*) from vmatchedProps where rank = 5)
-- MAGIC union all
-- MAGIC select 'Matched on address - on street' as query, (select count(*) from vmatchedProps where rank = 6)
-- MAGIC union all
-- MAGIC select '    Of which - no house number or lot number' as query, count(*)
-- MAGIC from   vmatchedprops
-- MAGIC where  rank = 6
-- MAGIC and    lotNumber is null
-- MAGIC and    can_h1 = 0
-- MAGIC union all
-- MAGIC select '    Of which - lot number' as query, count(*)
-- MAGIC from   vmatchedprops
-- MAGIC where  rank = 6
-- MAGIC and    lotNumber is not null

-- COMMAND ----------

create or replace table curated.ACCESSCancelledActiveProps as
with t1 as (
            select *
            from   vIntermediateResults
            where  activeProperty is not null
            union all
            select cancelled, active
            from   vMatchedProps)
select *
from   t1
union all
select propertyNumber, null
from   vAllCancelledProps
where  propertyNumber not in (select propertyNumber from t1)


-- COMMAND ----------

select count(*)
from   vEndResult a, (select distinct propertyNumber from cleansed.access_z309_tpropmeter) pm
where  activeProperty is null
and    a.propertyNumber = pm.propertyNumber

-- COMMAND ----------

with t1 as (--count exceptions that do have consumption
select streetName, streetType, suburb, postcode, pa.* 
from   vEndResult e,
       cleansed.access_z309_tpropertyaddress pa,
       cleansed.access_z309_tstreetguide sg,
       (select distinct propertyNumber from cleansed.access_z309_tpropmeter) pm
where  pa.propertyNumber = e.propertyNumber
and    e.activeProperty is null
and    pa.streetguideCode = sg.streetguideCode
and    pa.houseNumber1 > 0
and    pm.propertyNumber = pa.propertyNumber)
select count(distinct streetName, streetType, suburb) 
from t1

-- COMMAND ----------



-- COMMAND ----------

with t1 as (--verify all streets not found do indeed only have cancelled properties on them
select streetName, streetType, suburb, postcode, pa.* 
from   vEndResult e,
       cleansed.access_z309_tpropertyaddress pa,
       cleansed.access_z309_tstreetguide sg,
       (select distinct propertyNumber from cleansed.access_z309_tpropmeter) pm
where  pa.propertyNumber = e.propertyNumber
and    e.activeProperty is null
and    pa.streetguideCode = sg.streetguideCode
and    pa.houseNumber1 > 0
and    pm.propertyNumber = pa.propertyNumber),
t2 as (
select distinct streetName, streetType, suburb, postcode
from t1)
select distinct t2.*
from   cleansed.access_z309_tproperty pr,
       cleansed.access_z309_tpropertyaddress pa,
       cleansed.access_z309_tstreetguide sg,
       t2
where  pa.streetguideCode = sg.streetguideCode      
and    pr.propertyTypeCode != '998'
and    t2.suburb = sg.suburb
and    t2.streetName = sg.streetName
and    t2.postcode = sg.postcode
and    pr.propertyNumber = pa.propertyNumber
-- and    pa.propertyNumber = 3119092
-- and    ltrim(lotNumber) = '2'

-- COMMAND ----------

select *
from   cleansed.access_z309_tstreetguide
where  streetName = 'MILTON'
and    postcode = '2770'

-- COMMAND ----------

select pr.propertyNumber, pr.propertyTypeCode, pa.*
from   cleansed.access_z309_tproperty pr, cleansed.access_z309_tpropertyaddress pa
where  streetguidecode = '036063'
and pa.propertyNumber = pr.propertyNumber
and pa.propertyNumber not in (select propertyNumber from vEndResult where activeProperty is not null)

-- COMMAND ----------

select *
from vendresult
where propertyNumber = 4293282

-- COMMAND ----------

select pr.propertyNumber, pr.propertyTypeCode, pa.*
from   cleansed.access_z309_tproperty pr, cleansed.access_z309_tpropertyaddress pa
where  streetguidecode = '001712'
and pa.propertyNumber = pr.propertyNumber


-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select *
-- MAGIC from   vmatchedProps
-- MAGIC where  active is not null  
-- MAGIC and lotNumber is not null

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC describe  cleansed.access_z309_tpropertyaddress
-- MAGIC -- where  propertyNumber = 3100864

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select 'Cancelled' as query, count(*)
-- MAGIC from   vACCESSCancelledAddr
-- MAGIC union all
-- MAGIC select 'Matched' as query, count(*)
-- MAGIC from   vmatchedProps
-- MAGIC where  active is not null

-- COMMAND ----------


