# Databricks notebook source
# MAGIC %run ../../Common/common-transform

# COMMAND ----------

TARGET_TABLE=_.Destination

# COMMAND ----------

# DBTITLE 1,Create table upfront as the main query needs it
spark.sql(f"""
	CREATE TABLE IF NOT EXISTS {TARGET_TABLE}
	(
		monthlyLeakageAggregateSK STRING NOT NULL,     
        SWCAggregated STRING,
        deliverySystem STRING,
        supplyZone STRING,
        pressureZone STRING,
        monthNumber INTEGER NOT NULL,
        yearNumber INTEGER NOT NULL,
		calculationDate DATE NOT NULL,
        networkTypeCode STRING,
  		leakageMLQuantity DECIMAL(18,10),  
	    demandMLQuantity DECIMAL(18,10),  
		supplyApportionedConsumptionMLQuantity DECIMAL(18,10), 
  		stoppedMeterConsumptionMLQuantity DECIMAL(18,10), 
  		unmeteredConstructionConsumptionMLQuantity DECIMAL(18,10),
  		unmeteredConnectedConsumptionMLQuantity DECIMAL(18,10),
    	aggregatedConsumptionMLQuantity DECIMAL(18,10),
		_BusinessKey		 STRING NOT NULL,
		_DLCuratedZoneTimeStamp TIMESTAMP NOT NULL,
		_recordEnd			 TIMESTAMP NOT NULL,
		_recordCurrent		 INTEGER NOT NULL,
		_recordDeleted		 INTEGER NOT NULL,
  		_recordStart		 TIMESTAMP NOT NULL
	)
 	{f"USING DELTA LOCATION '{_.DataLakePath}'" if len(TARGET_TABLE.split('.'))<=2 else ''}
"""
)

# COMMAND ----------

def CreateView(view: str, sql_: str):
    if len(view.split('.')) == 3:
        layer,schema_name,object_name = view.split('.')
        table_namespace = get_table_name(layer,schema_name,object_name)
        catalog_name = table_namespace.split('.')[0]
        spark.sql(f"USE CATALOG {catalog_name}")
    
        if spark.sql(f"SHOW VIEWS FROM {schema_name} LIKE '{object_name}'").count() == 1:
            sqlLines = f"ALTER VIEW {table_namespace} AS"
        else:
            sqlLines = f"CREATE VIEW {table_namespace} AS"       

        sqlLines += sql_
        print(sqlLines)
        spark.sql(sqlLines)
    else:
        raise Exception(f'View {view} needs to be in 3 dot notation.')   

# COMMAND ----------

view = f"curated.water_balance.monthlyLeakageAggregate"
sql_ = f"""
  SELECT *
  FROM {TARGET_TABLE}
  qualify Row_Number () over ( partition BY SWCAggregated,deliverySystem,supplyZone,pressureZone,monthNumber,yearNumber ORDER BY calculationDate desc ) = 1
"""

CreateView(view, sql_)

# COMMAND ----------

spark.sql(f"""
        DELETE FROM {TARGET_TABLE}
        WHERE calculationDate = current_date(); 
        """).display()

# COMMAND ----------

# DBTITLE 1,Create unmetered_connected temp view
spark.sql(f"""
create or replace temp view unmetered_connected as (
  with unmeteredbase as (
    select
      'Sydney Water' as SWCAggregated,
      case
        when netowrk.deliverySystem in ('DEL_CASCADE', 'DEL_ORCHARD_HILLS') then 'DEL_CASCADE + DEL_ORCHARD_HILLS'
        when netowrk.deliverySystem in ('DEL_POTTS_HILL', 'DEL_PROSPECT_EAST', null) then 'DEL_POTTS_HILL + DEL_PROSPECT_EAST'
        else netowrk.deliverySystem
      end deliverySystem,
      case
        when deliverySystem in (
          'DEL_CASCADE',
          'DEL_ORCHARD_HILLS',
          'DEL_POTTS_HILL',
          'DEL_PROSPECT_EAST'
        ) then 'Delivery System Combined'
        else 'Delivery System'
      end networkTypeCode,
      netowrk.supplyZone,
      netowrk.pressureArea,
      agg.consumptionQuantity / 1000 as consumptionQuantity,
      agg.unmeteredConnectedFlag,
      agg.unmeteredConstructionFlag,
      month(agg.reportDate) as monthNumber,
      year(agg.reportDate) as yearNumber,
      agg.propertyCount
    from
      {get_env()}curated.fact.consumptionaggregate agg
      join {get_env()}curated.dim.waternetwork netowrk
    where
      reportDate = (
        select
          max(reportDate)
        from
          {get_env()}curated.fact.consumptionaggregate
      )
      and agg.waterNetworkSK = netowrk.waterNetworkSK
      and agg.unmeteredConnectedFlag = 'Y'
  ),
  unallocated as (
    select
      network.supplyZone,
      network.pressureArea,
      unkn.consumptionQuantity / 1000 as consumptionQuantity,
      month(unkn.reportDate) as monthNumber,
      year(unkn.reportDate) as yearNumber,
      unkn.propertyCount
    from
      {get_env()}curated.fact.consumptionaggregate unkn
      join {get_env()}curated.dim.waternetwork network
    where
      reportDate = (
        select
          max(reportDate)
        from
          {get_env()}curated.fact.consumptionaggregate
      )
      and unkn.waterNetworkSK = network.waterNetworkSK
      and unkn.unmeteredConnectedFlag = 'Y'
      and deliverysystem = 'Unknown'
  )
  select
    SWCAggregated,
    deliverySystem,
    supplyZone,
    pressureArea,
    sum(consumptionQuantity) as consumptionQuantity,
    monthNumber,
    yearNumber,
    'Pressure Area' networkTypeCode,
    sum(propertyCount) as propertyCount
  from
    unmeteredbase
  where
    deliverysystem <> 'Unknown'
  group by
    all
  union
  select
    SWCAggregated,
    deliverySystem,
    supplyZone,
    NULL as pressureArea,
    sum(consumptionQuantity) as consumptionQuantity,
    monthNumber,
    yearNumber,
    'Supply Zone' networkTypeCode,
    sum(propertyCount) as propertyCount
  from
    unmeteredbase
  where
    deliverysystem <> 'Unknown'
  group by
    all
  union
    (
      with deliverysystemallocated as (
        select
          SWCAggregated,
          deliverySystem,
          NULL as supplyZone,
          NULL as pressureArea,
          sum(consumptionQuantity) as consumptionQuantity,
          monthNumber,
          yearNumber,
          networkTypeCode,
          sum(propertyCount) as propertyCount
        from
          unmeteredbase
        group by
          all
      )
      select
        x.SWCAggregated,
        x.deliverySystem,
        x.supplyZone,
        x.pressureArea,case
          when x.deliverySystem like '%POTTS_HILL%'
          and x.networktypecode = 'Delivery System Combined' then (x.consumptionQuantity + y.consumptionQuantity)
          else x.consumptionQuantity
        end as consumptionQuantity,
        x.monthNumber,
        x.yearNumber,
        x.networkTypeCode,
        case 
          when x.deliverySystem like '%POTTS_HILL%'
          and x.networktypecode = 'Delivery System Combined' then (x.propertyCount + y.propertyCount)
          else x.propertyCount
        end as propertyCount
      from
        deliverysystemallocated x
        left outer join unallocated y on x.monthNumber = y.monthNumber
        and x.yearNumber = y.yearNumber
      where
        x.deliverysystem <> 'Unknown'
    )
  union
  select
    SWCAggregated,
    NULL deliverySystem,
    NULL as supplyZone,
    NULL as pressureArea,
    sum(consumptionQuantity) as consumptionQuantity,
    monthNumber,
    yearNumber,
    'SWC' as networkTypeCode,
    sum(propertyCount) as propertyCount
  from
    unmeteredbase
  group by
    all
)
""")

# COMMAND ----------

# DBTITLE 1,Create unmetered_construction temp view
spark.sql(f"""
create or replace temp view unmetered_construction as (
  with unmeteredbase as (
    select
      'Sydney Water' as SWCAggregated,
      case
        when netowrk.deliverySystem in ('DEL_CASCADE', 'DEL_ORCHARD_HILLS') then 'DEL_CASCADE + DEL_ORCHARD_HILLS'
        when netowrk.deliverySystem in ('DEL_POTTS_HILL', 'DEL_PROSPECT_EAST') then 'DEL_POTTS_HILL + DEL_PROSPECT_EAST'
        else netowrk.deliverySystem
      end deliverySystem,
      case
        when deliverySystem in (
          'DEL_CASCADE',
          'DEL_ORCHARD_HILLS',
          'DEL_POTTS_HILL',
          'DEL_PROSPECT_EAST'
        ) then 'Delivery System Combined'
        else 'Delivery System'
      end networkTypeCode,
      netowrk.supplyZone,
      netowrk.pressureArea,
      agg.consumptionQuantity / 1000 as consumptionQuantity,
      agg.unmeteredConnectedFlag,
      agg.unmeteredConstructionFlag,
      month(agg.reportDate) as monthNumber,
      year(agg.reportDate) as yearNumber,
      agg.propertyCount
    from
      {get_env()}curated.fact.consumptionaggregate agg
      join {get_env()}curated.dim.waternetwork netowrk
    where
      reportDate = (
        select
          max(reportDate)
        from
          {get_env()}curated.fact.consumptionaggregate
      )
      and agg.waterNetworkSK = netowrk.waterNetworkSK
      and agg.unmeteredConstructionFlag = 'Y'
  ),
  unallocated as (
    select
      network.supplyZone,
      network.pressureArea,
      unkn.consumptionQuantity / 1000 as consumptionQuantity,
      month(unkn.reportDate) as monthNumber,
      year(unkn.reportDate) as yearNumber,
      unkn.propertyCount
    from
      {get_env()}curated.fact.consumptionaggregate unkn
      join {get_env()}curated.dim.waternetwork network
    where
      reportDate = (
        select
          max(reportDate)
        from
          {get_env()}curated.fact.consumptionaggregate
      )
      and unkn.waterNetworkSK = network.waterNetworkSK
      and unkn.unmeteredConstructionFlag = 'Y'
      and deliverysystem = 'Unknown'
  )
  select
    SWCAggregated,
    deliverySystem,
    supplyZone,
    pressureArea,
    sum(consumptionQuantity) as consumptionQuantity,
    monthNumber,
    yearNumber,
    'Pressure Area' networkTypeCode,
    sum(propertyCount) as propertyCount
  from
    unmeteredbase
  where
    deliverysystem <> 'Unknown'
  group by
    all
  union
  select
    SWCAggregated,
    deliverySystem,
    supplyZone,
    NULL as pressureArea,
    sum(consumptionQuantity) as consumptionQuantity,
    monthNumber,
    yearNumber,
    'Supply Zone' networkTypeCode,
    sum(propertyCount) as propertyCount
  from
    unmeteredbase
  where
    deliverysystem <> 'Unknown'
  group by
    all
  union
    (
      with deliverysystemallocated as (
        select
          SWCAggregated,
          deliverySystem,
          NULL as supplyZone,
          NULL as pressureArea,
          sum(consumptionQuantity) as consumptionQuantity,
          monthNumber,
          yearNumber,
          networkTypeCode,
          sum(propertyCount) as propertyCount
        from
          unmeteredbase
        group by
          all
      )
      select
        x.SWCAggregated,
        x.deliverySystem,
        x.supplyZone,
        x.pressureArea,
        case
          when x.deliverySystem like '%POTTS_HILL%'
          and x.networktypecode = 'Delivery System Combined' then (x.consumptionQuantity + y.consumptionQuantity)
          else x.consumptionQuantity
        end as consumptionQuantity,
        x.monthNumber,
        x.yearNumber,
        x.networkTypeCode,
        case 
          when x.deliverySystem like '%POTTS_HILL%'
          and x.networktypecode = 'Delivery System Combined' then (x.propertyCount + y.propertyCount)
          else x.propertyCount
        end as propertyCount
      from
        deliverysystemallocated x
        left outer join unallocated y on x.monthNumber = y.monthNumber
        and x.yearNumber = y.yearNumber
      where
        x.deliverysystem <> 'Unknown'
    )
  union
  select
    SWCAggregated,
    NULL deliverySystem,
    NULL as supplyZone,
    NULL as pressureArea,
    sum(consumptionQuantity) as consumptionQuantity,
    monthNumber,
    yearNumber,
    'SWC' as networkTypeCode,
    sum(propertyCount) as propertyCount
  from
    unmeteredbase
  group by
    all
)
        """)

# COMMAND ----------

# DBTITLE 1,Create demand_aggregated temp view
spark.sql(f"""
          create or replace temp view demand_aggregated as (
            with aggregatedbase as (
  select
    'Sydney Water' as SWCAggregated,
    deliverySystem,
    supplyZone,
    pressureArea,
    sum(metricValueNumber) as consumptionQuantity,
    monthNumber,
    yearNumber,
    networkTypeCode
  from
    {get_env()}curated.fact.demandaggregatedcomponents
  where
    calculationDate = (
      select
        max(calculationDate)
      from
        {get_env()}curated.fact.demandaggregatedcomponents
    )
  group by
    all
),
unallocated as (
  select
    'Sydney Water' as SWCAggregated,
    deliverySystem,
    sum(metricValueNumber) as consumptionQuantity,
    monthNumber,
    yearNumber
  from
    {get_env()}curated.fact.demandaggregatedcomponents
  where
    calculationDate = (
      select
        max(calculationDate)
      from
        {get_env()}curated.fact.demandaggregatedcomponents
    )
    and deliverySystem = 'Unknown'
    and networkTypeCode in ('Delivery System', 'Delivery System Combined')
  group by
    all
)
select
  SWCAggregated,
  deliverySystem,
  supplyZone,
  pressureArea,
  consumptionQuantity,
  monthNumber,
  yearNumber,
  networkTypeCode
from
  aggregatedbase
where
  networkTypeCode in ('Pressure Area', 'Supply Zone')
  and deliverySystem <> 'Unknown'
union
  (
    select
      x.SWCAggregated,
      x.deliverySystem,
      x.supplyZone,
      x.pressureArea,case
        when x.deliverySystem like '%POTTS_HILL%'
        and x.networktypecode = 'Delivery System Combined' then (
          x.consumptionQuantity + coalesce(y.consumptionQuantity, 0)
        )
        else x.consumptionQuantity
      end as consumptionQuantity,
      x.monthNumber,
      x.yearNumber,
      x.networkTypeCode
    from
      aggregatedbase x
      left outer join unallocated y on x.monthNumber = y.monthNumber
      and x.yearNumber = y.yearNumber
    where
      x.networkTypeCode in ('Delivery System', 'Delivery System Combined')
      and x.deliverySystem <> 'Unknown'
  )
union
select
  SWCAggregated,
  NULL as deliverySystem,
  NULL as supplyZone,
  NULL as pressureArea,
  sum(consumptionQuantity) as consumptionQuantity,
  monthNumber,
  yearNumber,
  'SWC' as networkTypeCode
from
  aggregatedbase
where
  networkTypeCode in ('Delivery System', 'Delivery System Combined')
group by
  all
          )
          """)

# COMMAND ----------

df = spark.sql(f"""
WITH REQUIRED_DATES as
(
  SELECT max(minDate) minDate 
  FROM
  (
    select
      trunc(if((select count(*) from {get_env()}curated.fact.monthlyLeakageAggregate) = 0
          ,min(reportDate),nvl2(min(reportDate),greatest(min(reportDate),dateadd(MONTH, -24, current_date())::DATE),null)),'mm') minDate
    from {get_env()}curated.water_balance.factwaternetworkdemand
    union all
    select
      trunc(if((select count(*) from {get_env()}curated.fact.monthlyLeakageAggregate) = 0
          ,minConsumptionDate,nvl2(minConsumptionDate,greatest(minConsumptionDate,dateadd(MONTH, -24, current_date())::DATE),null)),'mm') minDate
    from (
      select min(make_date(consumptionYear, consumptionMonth,1)) minConsumptionDate
      from {get_env()}curated.water_balance.monthlySupplyApportionedAggregate
    )
  )
),
DEMAND AS
( 
  SELECT
        year(reportDate) yearNumber
        ,month(reportDate) monthNumber  
        ,'Sydney Water' SWCAggregated
        ,deliverySystem
        ,supplyZone
        ,pressureArea pressureZone
        ,networkTypeCode
        ,coalesce(sum(demandQuantity),0) demandMLQuantity
  FROM {get_env()}curated.water_balance.factwaternetworkdemand
  WHERE reportDate >= (select minDate from required_dates)
  AND (networkTypeCode IN ('Supply Zone', 'Pressure Area')
    OR (networkTypeCode IN ('Delivery System', 'Delivery System Combined')
        AND deliverySystem NOT IN ('DEL_CASCADE','DEL_ORCHARD_HILLS')))
  GROUP BY ALL 
  UNION ALL    
  SELECT
        year(reportDate) yearNumber
        ,month(reportDate) monthNumber  
        ,'Sydney Water' SWCAggregated
        ,null deliverySystem
        ,null supplyZone
        ,null pressureZone
        ,'SWC' networkTypeCode
        ,sum(demandQuantity) demandMLQuantity
  FROM {get_env()}curated.water_balance.swcdemand
  WHERE reportDate >= (select minDate from required_dates)
  GROUP BY ALL   
),
supply_apportioned_consumption as 
(
  SELECT
        consumptionYear yearNumber
        ,consumptionMonth monthNumber  
        ,'Sydney Water' SWCAggregated
        ,case 
            when deliverySystem in ('DEL_CASCADE','DEL_ORCHARD_HILLS') 
              then 'DEL_CASCADE + DEL_ORCHARD_HILLS'
            when deliverySystem in ('DEL_POTTS_HILL','DEL_PROSPECT_EAST','Unknown') 
              then 'DEL_POTTS_HILL + DEL_PROSPECT_EAST'
            else              
              deliverySystem
         end deliverySystem  
        ,supplyZone
        ,pressureArea pressureZone
        ,case 
            when deliverySystem in ('DEL_CASCADE','DEL_ORCHARD_HILLS',
                                    'DEL_POTTS_HILL','DEL_PROSPECT_EAST','Unknown')
                and networkTypeCode = 'Delivery System'                                   
              then 'Delivery System Combined'
            else              
              networkTypeCode
         end networkTypeCode          
        ,coalesce(sum(totalMLQuantity),0) supplyApportionedConsumptionMLQuantity  
,coalesce(sum(totalPropertyCount), 0) totalPropertyCount  
  FROM {get_env()}curated.water_balance.monthlySupplyApportionedAggregate
  WHERE make_date(consumptionYear, consumptionMonth,1) >= (select minDate from required_dates)
  GROUP BY ALL
  UNION ALL
  SELECT
        consumptionYear yearNumber
        ,consumptionMonth monthNumber  
        ,'Sydney Water' SWCAggregated
        ,null deliverySystem  
        ,null supplyZone
        ,null pressureZone
        ,'SWC' networkTypeCode          
        ,coalesce(sum(totalMLQuantity),0) supplyApportionedConsumptionMLQuantity  
,coalesce(sum(totalPropertyCount), 0) totalPropertyCount  
  FROM {get_env()}curated.water_balance.monthlySupplyApportionedAggregate  
  WHERE make_date(consumptionYear, consumptionMonth,1) >= (select minDate from required_dates)
  AND networkTypeCode = 'Delivery System'
  GROUP BY ALL
),
unmeteredconnected as (
    select * from unmetered_connected
),
unmeteredconstruction as (
    select * from unmetered_construction
),
stoppedmeterconsumption as (
    with stoppedmeterbase as (select 'Sydney Water' as SWCAggregated,
        case 
            when netowrk.deliverySystem in ('DEL_CASCADE','DEL_ORCHARD_HILLS') 
              then 'DEL_CASCADE + DEL_ORCHARD_HILLS'
            when netowrk.deliverySystem in ('DEL_POTTS_HILL','DEL_PROSPECT_EAST') 
              then 'DEL_POTTS_HILL + DEL_PROSPECT_EAST'
            else              
              netowrk.deliverySystem
         end deliverySystem, 
       case 
            when deliverySystem in ('DEL_CASCADE','DEL_ORCHARD_HILLS',
                                    'DEL_POTTS_HILL','DEL_PROSPECT_EAST')
            then 'Delivery System Combined'
            else 'Delivery System' 
         end networkTypeCode, netowrk.supplyZone, netowrk.pressureArea, sum(agg.consumptionQuantity)/1000 as consumptionQuantity, month(agg.consumptionDate) as monthNumber, year(agg.consumptionDate) as yearNumber
         ,agg.propertyCount
        from {get_env()}curated.fact.stoppedmeteraggregate agg join {get_env()}curated.dim.waternetwork netowrk
        where calculationDate = (select max(calculationDate) from {get_env()}curated.fact.stoppedmeteraggregate) and agg.waterNetworkSK = netowrk.waterNetworkSK
        group by all
        ),
      unallocated as (
        select network.deliverysystem,network.supplyZone, network.pressureArea, sum(unkn.consumptionQuantity)/1000 as consumptionQuantity, month(unkn.consumptionDate) as monthNumber, year(unkn.consumptionDate) as yearNumber
        ,unkn.propertyCount
        from {get_env()}curated.fact.stoppedmeteraggregate unkn join {get_env()}curated.dim.waternetwork network
        where unkn.calculationDate = (select max(calculationDate) from {get_env()}curated.fact.stoppedmeteraggregate) and unkn.waterNetworkSK = network.waterNetworkSK and deliverysystem = 'Unknown' group by all
        ) 
        select SWCAggregated,deliverySystem,supplyZone,pressureArea,sum(consumptionQuantity) as consumptionQuantity,monthNumber,yearNumber,'Pressure Area' networkTypeCode
        ,sum(propertyCount) as propertyCount 
        from stoppedmeterbase where deliverysystem <> 'Unknown' group by all
        union
        select SWCAggregated,deliverySystem,supplyZone,NULL as pressureArea,sum(consumptionQuantity) as consumptionQuantity,monthNumber,yearNumber,'Supply Zone' networkTypeCode
        ,sum(propertyCount) as propertyCount
         from stoppedmeterbase where deliverysystem <> 'Unknown' group by all
        union
        (
          with deliverysystemallocated as (select SWCAggregated,deliverySystem,NULL as supplyZone,NULL as pressureArea,sum(consumptionQuantity) as consumptionQuantity,monthNumber,
            yearNumber,networkTypeCode
            ,sum(propertyCount) as propertyCount
            from stoppedmeterbase group by all) 
            select x.SWCAggregated,x.deliverySystem,x.supplyZone,x.pressureArea,case when x.deliverySystem like '%POTTS_HILL%' and x.networktypecode = 'Delivery System Combined' then ( x.consumptionQuantity+ y.consumptionQuantity) else x.consumptionQuantity end as consumptionQuantity,x.monthNumber,x.yearNumber,x.networkTypeCode
            ,x.propertyCount
            from deliverysystemallocated x left outer join unallocated y on x.monthNumber = y.monthNumber and x.yearNumber = y.yearNumber where x.deliverysystem <> 'Unknown' 
        )
        union
        select SWCAggregated,NULL deliverySystem,NULL as supplyZone,NULL as pressureArea,sum(consumptionQuantity) as consumptionQuantity,monthNumber,yearNumber,'SWC' as networkTypeCode
        ,sum(propertyCount) as propertyCount 
        from stoppedmeterbase group by all
        order by networkTypeCode
),
demandaggregated as (
        select * from demand_aggregated
)
SELECT d.SWCAggregated SWCAggregated
      ,nvl(d.deliverySystem,'') deliverySystem
      ,nvl(d.supplyZone,'') supplyZone
      ,nvl(d.pressureZone,'') pressureZone
      ,d.monthNumber monthNumber
      ,d.yearNumber yearNumber
      ,current_date() calculationDate
      ,d.networkTypeCode networkTypeCode
      ,(d.demandMLQuantity - (sac.supplyApportionedConsumptionMLQuantity + coalesce(connected.consumptionQuantity,0) + coalesce(construction.consumptionQuantity,0) + coalesce(stopped.consumptionQuantity,0) + coalesce(aggregated.consumptionQuantity,0)))::DECIMAL(18,10) leakageMLQuantity
      ,d.demandMLQuantity::DECIMAL(18,10) demandMLQuantity
      ,sac.supplyApportionedConsumptionMLQuantity::DECIMAL(18,10) supplyApportionedConsumptionMLQuantity
      ,sac.totalPropertyCount supplyApportionedPropertyCount
      ,coalesce(connected.consumptionQuantity,0)::DECIMAL(18,10) unmeteredConnectedConsumptionMLQuantity
      ,coalesce(connected.propertyCount, 0) unmeteredConnectedPropertyCount
      ,coalesce(construction.consumptionQuantity,0)::DECIMAL(18,10) unmeteredConstructionConsumptionMLQuantity
      ,coalesce(construction.propertyCount, 0) unmeteredConstructionPropertyCount
      ,coalesce(stopped.consumptionQuantity,0)::DECIMAL(18,10) stoppedMeterConsumptionMLQuantity
      ,coalesce(stopped.propertyCount, 0) stoppedMeterPropertyCount
      ,coalesce(aggregated.consumptionQuantity,0)::DECIMAL(18,10) aggregatedConsumptionMLQuantity
FROM demand d
JOIN supply_apportioned_consumption sac
  ON  ( d.networkTypeCode = sac.networkTypeCode
    and d.yearNumber = sac.yearNumber
    and d.monthNumber = sac.monthNumber
    and (d.SWCAggregated = sac.SWCAggregated and d.networkTypeCode = 'SWC'
      or (d.deliverySystem = sac.deliverySystem and d.networkTypeCode in ('Delivery System','Delivery System Combined'))
      or (d.supplyZone = sac.supplyZone and d.networkTypeCode = 'Supply Zone')
      or (d.pressureZone = sac.pressureZone and d.networkTypeCode = 'Pressure Area'))
  ) 
LEFT OUTER JOIN unmeteredconnected connected
   ON  ( d.networkTypeCode = connected.networkTypeCode
    -- and d.yearNumber = connected.yearNumber
    -- and d.monthNumber = connected.monthNumber
    and (d.SWCAggregated = connected.SWCAggregated and d.networkTypeCode = 'SWC'
      or (d.deliverySystem = connected.deliverySystem and d.networkTypeCode in ('Delivery System','Delivery System Combined'))
      or (d.supplyZone = connected.supplyZone and d.networkTypeCode = 'Supply Zone')
      or (d.pressureZone = connected.pressureArea and d.networkTypeCode = 'Pressure Area'))
  )
LEFT OUTER JOIN unmeteredconstruction construction
   ON  ( d.networkTypeCode = construction.networkTypeCode
    -- and d.yearNumber = construction.yearNumber
    -- and d.monthNumber = construction.monthNumber
    and (d.SWCAggregated = construction.SWCAggregated and d.networkTypeCode = 'SWC'
      or (d.deliverySystem = construction.deliverySystem and d.networkTypeCode in ('Delivery System','Delivery System Combined'))
      or (d.supplyZone = construction.supplyZone and d.networkTypeCode = 'Supply Zone')
      or (d.pressureZone = construction.pressureArea and d.networkTypeCode = 'Pressure Area'))
  )  
LEFT OUTER JOIN stoppedmeterconsumption stopped
   ON  ( d.networkTypeCode = stopped.networkTypeCode
    and d.yearNumber = stopped.yearNumber
    and d.monthNumber = stopped.monthNumber
    and (d.SWCAggregated = stopped.SWCAggregated and d.networkTypeCode = 'SWC'
      or (d.deliverySystem = stopped.deliverySystem and d.networkTypeCode in ('Delivery System','Delivery System Combined'))
      or (d.supplyZone = stopped.supplyZone and d.networkTypeCode = 'Supply Zone')
      or (d.pressureZone = stopped.pressureArea and d.networkTypeCode = 'Pressure Area'))
  )  
LEFT OUTER JOIN demandaggregated aggregated
   ON  ( d.networkTypeCode = aggregated.networkTypeCode
    and d.yearNumber = aggregated.yearNumber
    and d.monthNumber = aggregated.monthNumber
    and (d.SWCAggregated = aggregated.SWCAggregated and d.networkTypeCode = 'SWC'
      or (d.deliverySystem = aggregated.deliverySystem and d.networkTypeCode in ('Delivery System','Delivery System Combined'))
      or (d.supplyZone = aggregated.supplyZone and d.networkTypeCode = 'Supply Zone')
      or (d.pressureZone = aggregated.pressureArea and d.networkTypeCode = 'Pressure Area'))
  )    
"""
)

# COMMAND ----------

def Transform():
    # ------------- TABLES ----------------- #
    global df
    # ------------- JOINS ------------------ #
        
    # ------------- TRANSFORMS ------------- #
    _.Transforms = [
        f"SWCAggregated||'|'||deliverySystem||'|'||supplyZone||'|'||pressureZone||'|'||monthNumber||'|'||yearNumber||'|'||calculationDate {BK}"
        ,"SWCAggregated"
        ,"deliverySystem"
        ,"supplyZone"
        ,"pressureZone"
        ,"monthNumber"
        ,"yearNumber"
        ,"calculationDate"
        ,"networkTypeCode"        
        ,"leakageMLQuantity"        
        ,"demandMLQuantity"     
        ,"supplyApportionedConsumptionMLQuantity"
        ,"unmeteredConnectedConsumptionMLQuantity"
        ,"unmeteredConstructionConsumptionMLQuantity"
        ,"stoppedMeterConsumptionMLQuantity"
        ,"aggregatedConsumptionMLQuantity"
    ]
    df = df.selectExpr(
        _.Transforms
    )
    # ------------- CLAUSES ---------------- #

    # ------------- SAVE ------------------- #
    # display(df)
    # CleanSelf()
    Save(df, append=True)
    #DisplaySelf()
Transform()
