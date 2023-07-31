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

spark.sql(f"""
        DELETE FROM {TARGET_TABLE}
        WHERE calculationDate = current_date(); 
        """).display()

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
            when deliverySystem in ('DEL_POTTS_HILL','DEL_PROSPECT_EAST') 
              then 'DEL_POTTS_HILL + DEL_PROSPECT_EAST'
            else              
              deliverySystem
         end deliverySystem  
        ,supplyZone
        ,pressureArea pressureZone
        ,case 
            when deliverySystem in ('DEL_CASCADE','DEL_ORCHARD_HILLS',
                                    'DEL_POTTS_HILL','DEL_PROSPECT_EAST')
                and networkTypeCode = 'Delivery System'                                   
              then 'Delivery System Combined'
            else              
              networkTypeCode
         end networkTypeCode          
        ,coalesce(sum(totalMLQuantity),0) supplyApportionedConsumptionMLQuantity  
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
  FROM {get_env()}curated.water_balance.monthlySupplyApportionedAggregate  
  WHERE make_date(consumptionYear, consumptionMonth,1) >= (select minDate from required_dates)
  AND networkTypeCode = 'Delivery System'
  GROUP BY ALL
)
SELECT d.SWCAggregated SWCAggregated
      ,nvl(d.deliverySystem,'') deliverySystem
      ,nvl(d.supplyZone,'') supplyZone
      ,nvl(d.pressureZone,'') pressureZone
      ,d.monthNumber monthNumber
      ,d.yearNumber yearNumber
      ,current_date() calculationDate
      ,d.networkTypeCode networkTypeCode
      ,(d.demandMLQuantity - sac.supplyApportionedConsumptionMLQuantity)::DECIMAL(18,10) leakageMLQuantity
      ,d.demandMLQuantity::DECIMAL(18,10) demandMLQuantity
      ,sac.supplyApportionedConsumptionMLQuantity::DECIMAL(18,10) supplyApportionedConsumptionMLQuantity
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
