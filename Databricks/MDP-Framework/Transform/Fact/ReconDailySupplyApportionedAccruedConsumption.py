# Databricks notebook source
# MAGIC %run ../../Common/common-transform

# COMMAND ----------

_.Destination=_.Destination.replace('fact','water_balance')

# COMMAND ----------

TARGET_TABLE=_.Destination

# COMMAND ----------

# DBTITLE 1,Create table upfront as the main query needs it
spark.sql(f"""
	CREATE TABLE IF NOT EXISTS {TARGET_TABLE}
	(
		reconDailySupplyApportionedAccruedConsumptionSK STRING NOT NULL,     
		calculationDate DATE NOT NULL,
		ord DECIMAL(3,1),
  		description STRING,
  		accruedConsumptionMLQuantity DECIMAL(33,15),
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

simulationPeriodId = spark.sql(f"""
    SELECT distinct simulationPeriodId 
    FROM {get_env()}curated.fact.dailySupplyApportionedAccruedConsumption    
    qualify dense_rank() over (order by to_date(replace(simulationPeriodId, 'F_'),'MMM_yy') desc) = 1
""").collect()[0][0]
print(simulationPeriodId)

# COMMAND ----------

df = spark.sql(f"""
WITH REQUIRED_DATES as
(
	SELECT  minDate
         ,maxDate
	FROM
	(
    select 
      min(reportDate) minDate
      ,max(reportDate) maxDate
    from {get_env()}curated.fact.viewinterimwaternetworkdemand
  )
  WHERE minDate <= maxDate
)
,VPK AS
(
  SELECT *
        ,case when 
            vpk.recycledWaterNetworkDeliverySystem in (
              select deliverySystem from {get_env()}curated.water_balance.ApportionedRecycledWaterNetworkLookup
            ) 
            or vpk.recycledWaterNetworkDistributionSystem in (
              select distributionSystem from {get_env()}curated.water_balance.ApportionedRecycledWaterNetworkLookup
            )
            or vpk.recycledWaterNetworkSupplyZone in (
              select supplyZone from {get_env()}curated.water_balance.ApportionedRecycledWaterNetworkLookup
            )
            then 'Y'
          else 'N' end inRecycledWaterNetwork  
  FROM {get_env()}curated.water_balance.propertykey vpk  
)
,acc AS
(
  SELECT *
        ,billingQuantityEnergy/1000 accruedConsumptionMLQuantity
        ,min(dateFromWhichTimeSliceIsValid)
          over (partition by billingDocumentNumber,belzartName,installation) minDateFromWhichTimeSliceIsValid
        ,max(dateAtWhichATimeSliceExpires)
          over (partition by billingDocumentNumber,belzartName,installation) maxDateAtWhichATimeSliceExpires  
  FROM
  (
    SELECT *
    FROM {get_env()}cleansed.isu.0uc_sales_simu_01
    WHERE simulationPeriodId = '{simulationPeriodId}'
    qualify dense_rank() over (order by to_date(replace(simulationPeriodId, 'F_'),'MMM_yy') desc) = 1
  )
  qualify minDateFromWhichTimeSliceIsValid >= (select minDate from required_dates)
      and maxDateAtWhichATimeSliceExpires <= (select maxDate from required_dates)
)
,ITEMBASE3 AS (
  SELECT *
        ,if(vpk.waterNetworkDeliverySystem <> 'Unknown' and vpk.waterNetworkDeliverySystem is not null
          , 'Known', 'Unknown') deliverySystem
        ,vpk.inRecycledWaterNetwork
        ,if(vpk.inRecycledWaterNetwork='Y' and belzartName = 'Recycled water','Y',
          if(vpk.waterNetworkDeliverySystem <> 'Unknown' and vpk.waterNetworkDeliverySystem is not null
          ,'N','NA')) potableSubstitutionFlag
  FROM acc s
  LEFT JOIN {get_env()}curated.water_consumption.viewcontractaccount vca
  ON (vca.contractAccountNumber = S.contractAccountNumber
    AND vca._effectiveFrom <= S.maxDateAtWhichATimeSliceExpires
    AND vca._effectiveTo >= S.maxDateAtWhichATimeSliceExpires)  
  LEFT JOIN vpk
  ON (vpk.propertyNumber = vca.businessPartnerGroupNumber
    AND vpk._effectiveFrom <= S.maxDateAtWhichATimeSliceExpires
    AND vpk._effectiveTo >= S.maxDateAtWhichATimeSliceExpires)
  WHERE belzartName IN ('Water','Recycled water')  
)
,ITEM3 AS (
  SELECT 7 ord, '__minus Recycled water that are not potable' description
        ,sum(accruedConsumptionMLQuantity) accruedConsumptionMLQuantity     
  FROM itembase3
  WHERE belzartName = 'Recycled water' AND potableSubstitutionFlag = 'N'    
)
,ITEM4 AS (
  SELECT 4 ord, '__minus Unknown Delivery System' description
        ,sum(accruedConsumptionMLQuantity) accruedConsumptionMLQuantity     
  FROM itembase3
  WHERE 1=1--(belzartName = 'Water' OR (inRecycledWaterNetwork='Y' and belzartName = 'Recycled water'))  
  AND deliverySystem = 'Unknown'
)
,ITEM5 AS (
  SELECT 5 ord, '__minus accrued items with billing period that cannot be fully covered by the available water demand' description
        ,coalesce(sum(accruedConsumptionMLQuantity),0) accruedConsumptionMLQuantity
  FROM itembase3
  WHERE 1=1 --(belzartName = 'Water' OR potableSubstitutionFlag in ('Y','NA'))
  AND not(minDateFromWhichTimeSliceIsValid >= (select minDate from required_dates)
      and maxDateAtWhichATimeSliceExpires <= (select maxDate from required_dates))
)
,ITEM6 AS (
  SELECT 6 ord, '__minus accrued items that have 0 water supply for the meter period' description
        ,coalesce(sum(accruedConsumptionMLQuantity),0) accruedConsumptionMLQuantity
  FROM itembase3
  -- WHERE (belzartName = 'Water' OR (inRecycledWaterNetwork='Y' and belzartName = 'Recycled water'))  
  -- AND deliverySystem = 'Known'
  WHERE 1=1 --(belzartName = 'Water' OR potableSubstitutionFlag in ('Y','NA'))
  AND minDateFromWhichTimeSliceIsValid >= (select minDate from required_dates)
  AND maxDateAtWhichATimeSliceExpires <= (select maxDate from required_dates)
  AND (billingDocumentNumber,propertyNumber
      ,minDateFromWhichTimeSliceIsValid,maxDateAtWhichATimeSliceExpires
      ,if(inRecycledWaterNetwork='Y' and belzartName = 'Recycled water','Y','N')) 
       in (
        select billingDocumentNumber, propertyNumber
              ,accruedPeriodStartDate,accruedPeriodEndDate
              ,potableSubstitutionFlag
        from {get_env()}curated.fact.dailySupplyApportionedAccruedConsumption  
        where supplyApportionedKLQuantity is null
        and simulationPeriodId = '{simulationPeriodId}'
      )      
)
,SET_ AS (
  SELECT 1 ord, {f"'Accrued items with simulationPeriodId of {simulationPeriodId}'"} description
        ,sum(accruedConsumptionMLQuantity) accruedConsumptionMLQuantity
  FROM acc
  UNION ALL
  SELECT 2 ord, '__minus items that are not Water and Recycled water' description
        ,sum(accruedConsumptionMLQuantity) accruedConsumptionMLQuantity
  FROM acc
  WHERE belzartName not IN ('Water','Recycled water') OR belzartName is null
  UNION ALL
  SELECT *
  FROM item3
  -- UNION ALL
  -- SELECT *
  -- FROM item4
  UNION ALL
  SELECT *
  FROM item5  
  UNION ALL
  SELECT *
  FROM item6  
)
SELECT current_date() calculationDate
      ,ord::DECIMAL(3,1) ord
      ,description
      ,accruedConsumptionMLQuantity::DECIMAL(33,15) accruedConsumptionMLQuantity
FROM
(
    SELECT *
    FROM set_
    UNION ALL
    SELECT 8 ord, 'Final accrued items to be apportioned' description
        ,sum(if(ord=1,accruedConsumptionMLQuantity,0)) 
            - sum(if(ord between 2 and 7,accruedConsumptionMLQuantity,0))      
    FROM set_
    UNION ALL
    SELECT 9 ord, 'Actual accrued items that are apportioned' description
        ,sum(supplyApportionedKLQuantity) /1000 accruedConsumptionMLQuantity
    FROM {get_env()}curated.fact.dailySupplyApportionedAccruedConsumption  
    WHERE simulationPeriodId = '{simulationPeriodId}'
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
        f"ord||'|'||calculationDate {BK}"
        ,"*"
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
