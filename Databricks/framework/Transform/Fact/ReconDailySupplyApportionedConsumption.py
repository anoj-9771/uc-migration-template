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
		reconDailySupplyApportionedConsumptionSK STRING NOT NULL,     
		calculationDate DATE NOT NULL,
		ord DECIMAL(3,1),
  		description STRING,
  		billedConsumptionMLQuantity DECIMAL(33,15),
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

#Get 1st of the month timestamp for time travel
tables=['cleansed.isu.erch','cleansed.isu.dberchz1','cleansed.isu.dberchz2']
histories=None
for table in tables:
    history=spark.sql(f'describe history {get_env()}{table}').where("timestamp::DATE = trunc(current_date(),'mm')").select('timestamp')
    histories=histories.union(history) if histories else history
firstOfTheMonthTS=histories.selectExpr('max(timestamp)::STRING').collect()[0][0]    
print(firstOfTheMonthTS)

# COMMAND ----------

df = spark.sql(f"""
WITH REQUIRED_DATES as
(
	SELECT  minDate
         ,minDateAvailableData
         ,maxDate
	FROM
	(
    select 
      if((select count(*) from {get_env()}curated.fact.dailySupplyApportionedConsumption) = 0
          ,min(reportDate),nvl2(min(reportDate),greatest(min(reportDate),dateadd(MONTH, -24, current_date())::DATE),null)) minDate      
      ,min(reportDate) minDateAvailableData
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
,BILL AS (
  SELECT
        b.billingDocumentNumber
        ,db1.billingDocumentLineItemId
        ,db1.lineItemTypeCode
        ,b.contractId
        ,b.businessPartnerGroupNumber
        ,b.startBillingPeriod
        ,b.endBillingPeriod
        ,db1.validFromDate meterActiveStartDate
        ,db1.validToDate meterActiveEndDate
        ,min(db1.validFromDate)
          over (partition by b.billingDocumentNumber,b.businessPartnerGroupNumber,db2.equipmentNumber,db1.lineItemTypeCode,db2.logicalDeviceNumber,db2.logicalRegisterNumber,db2.registerNumber::INTEGER) minMeterActiveStartDate        
        -- get max meterActiveEndDate for overlapping meter period          
        ,max(db1.validToDate)
          over (partition by b.billingDocumentNumber,b.businessPartnerGroupNumber,db2.equipmentNumber,db1.lineItemTypeCode,db2.logicalDeviceNumber,db2.logicalRegisterNumber,db2.registerNumber::INTEGER) maxMeterActiveEndDate          
        ,db1.billingQuantityPlaceBeforeDecimalPoint / 1000 meteredWaterMLConsumption
        ,db2.equipmentNumber
        ,b.contractAccountNumber
        ,b.documentNotReleasedFlag isOutsortedFlag
        ,db2.logicalDeviceNumber
        ,db2.logicalRegisterNumber
        ,db2.registerNumber::INTEGER
        ,b.divisionCode
        ,b.division  
        ,b.billingSimulationIndicator
        ,db1.billingLineItemBudgetBillingFlag
        ,db2.suppressedMeterReadingDocumentID
        ,b.reversalDate
  FROM {get_env()}cleansed.isu.erch timestamp as of '{firstOfTheMonthTS}' b
  JOIN {get_env()}cleansed.isu.dberchz1 timestamp as of '{firstOfTheMonthTS}' db1
    ON b.billingDocumentNumber = db1.billingDocumentNumber
  JOIN {get_env()}cleansed.isu.dberchz2 timestamp as of '{firstOfTheMonthTS}' db2
    ON (db2.billingDocumentNumber = db1.billingDocumentNumber 
      AND db2.billingDocumentLineItemId = db1.billingDocumentLineItemId)  
  AND db1.validFromDate >= (select minDateAvailableData from required_dates)
  AND db1.validToDate >= (select minDate from required_dates)
  AND db1.validToDate <= (select maxDate from required_dates)  
)
,ITEMBASE6 AS (
  SELECT *
  FROM bill
  WHERE trim(billingSimulationIndicator) = ''
  AND billingLineItemBudgetBillingFlag = 'N'
  AND lineItemTypeCode IN ('ZDQUAN', 'ZRQUAN')
  AND ( reversalDate IS NULL OR reversalDate = '1900-01-01' OR reversalDate = '9999-12-31')
  AND trim(suppressedMeterReadingDocumentID) <> ''
)
,ITEMBASE8 AS (
  SELECT *
        ,if(vpk.waterNetworkDeliverySystem <> 'Unknown' and vpk.waterNetworkDeliverySystem is not null
         , 'Known', 'Unknown') deliverySystem
        ,vpk.inRecycledWaterNetwork
        ,if(vpk.inRecycledWaterNetwork='Y' and bc.lineItemTypeCode = 'ZRQUAN','Y',
          if(vpk.waterNetworkDeliverySystem <> 'Unknown' and vpk.waterNetworkDeliverySystem is not null
          ,'N','NA')) potableSubstitutionFlag
  FROM itembase6 bc
  LEFT JOIN vpk
  ON (vpk.propertyNumber = bc.businessPartnerGroupNumber
    AND vpk._effectiveFrom <= bc.maxMeterActiveEndDate
    AND vpk._effectiveTo >= bc.maxMeterActiveEndDate)  
)
,ITEM8 AS (
  SELECT 8 ord, '__minus ZRQUAN that are not potable' description
        ,sum(meteredWaterMLConsumption) billedConsumptionMLQuantity
  FROM itembase8
  WHERE lineItemTypeCode = 'ZRQUAN' AND potableSubstitutionFlag = 'N'  
)
,ITEM9 AS (
  SELECT 9 ord, '__minus Unknown Delivery System' description
        ,sum(meteredWaterMLConsumption) billedConsumptionMLQuantity        
  FROM itembase8
  WHERE 1=1--(lineItemTypeCode = 'ZDQUAN' OR (inRecycledWaterNetwork='Y' and lineItemTypeCode = 'ZRQUAN'))  
  AND deliverySystem = 'Unknown'
)
,ITEM10 AS (
  SELECT 10 ord, '__minus Bills that have 0 water supply for the meter period' description
        ,sum(meteredWaterMLConsumption) billedConsumptionMLQuantity        
  FROM itembase8
  -- WHERE (lineItemTypeCode = 'ZDQUAN' OR (inRecycledWaterNetwork='Y' and lineItemTypeCode = 'ZRQUAN'))  
  WHERE (lineItemTypeCode = 'ZDQUAN' OR potableSubstitutionFlag in ('Y','NA'))
  -- AND deliverySystem = 'Known'
  AND (billingDocumentNumber,businessPartnerGroupNumber,equipmentNumber
      ,logicalDeviceNumber,logicalRegisterNumber,registerNumber
      ,minMeterActiveStartDate,maxMeterActiveEndDate
      ,if(inRecycledWaterNetwork='Y' and lineItemTypeCode = 'ZRQUAN','Y',
          if(waterNetworkDeliverySystem <> 'Unknown' and waterNetworkDeliverySystem is not null
          ,'N','NA')))
       in (
        select billingDocumentNumber, propertyNumber, deviceNumber
              ,logicalDeviceNumber,logicalRegisterNumber,registerNumber
              ,meterActiveStartDate,meterActiveEndDate
              ,potableSubstitutionFlag
        from {get_env()}curated.fact.dailySupplyApportionedConsumption  
        where supplyApportionedKLQuantity is null
      )      
)
,UC1_fact AS (
  select 5.2 ord, {f"'{get_env()}curated.fact.billedwaterconsumption for the same period as item 1'"} description
        ,sum(meteredwaterconsumption)/1000 billedConsumptionMLQuantity
  from
    {get_env()}curated.fact.billedwaterconsumption
  WHERE meterActiveStartDate >= (select minDateAvailableData from required_dates)
    AND meterActiveEndDate >= (select minDate from required_dates)
    AND meterActiveEndDate <= (select maxDate from required_dates)
)
,SET1 AS (
  SELECT 1 ord, 'SAP Billing items with Meter Start Date >= '
                || (select minDateAvailableData from required_dates)
                ||' and Meter End Date between '
                || (select minDate from required_dates)
                ||' and '
                || (select maxDate from required_dates) description
        ,sum(meteredWaterMLConsumption) billedConsumptionMLQuantity
  FROM bill
  UNION ALL
  SELECT 2 ord, '__minus Simulation' description
        ,sum(meteredWaterMLConsumption) billedConsumptionMLQuantity        
  FROM bill
  WHERE not(trim(billingSimulationIndicator) = '') OR billingSimulationIndicator is null
  UNION ALL
  SELECT 3 ord, '__minus Line items that are not ZDQUAN and ZRQUAN' description
        ,sum(meteredWaterMLConsumption) billedConsumptionMLQuantity
  FROM bill
  WHERE trim(billingSimulationIndicator) = ''   
  AND not(lineItemTypeCode IN ('ZDQUAN', 'ZRQUAN')) OR lineItemTypeCode is null  
  UNION ALL
  SELECT 4 ord, '__minus Suppressed Meter Reading' description
        ,sum(meteredWaterMLConsumption) billedConsumptionMLQuantity
  FROM bill
  WHERE trim(billingSimulationIndicator) = ''   
  AND lineItemTypeCode IN ('ZDQUAN', 'ZRQUAN')
  AND not(trim(suppressedMeterReadingDocumentID) <> '') OR suppressedMeterReadingDocumentID is null    
)
,SET_ AS (
  SELECT *
  FROM set1 
  UNION ALL
  SELECT 5.1 ord, 'Sub-total' description
        ,sum(if(ord=1,billedConsumptionMLQuantity,0)) 
          - sum(if(ord between 2 and 4,billedConsumptionMLQuantity,0))      
  FROM set1  
  UNION ALL
  SELECT *
  FROM UC1_fact  
  UNION ALL
  SELECT 6 ord, '__minus Budget Billing' description
        ,sum(meteredWaterMLConsumption) billedConsumptionMLQuantity        
  FROM bill
  WHERE trim(billingSimulationIndicator) = ''   
  AND lineItemTypeCode IN ('ZDQUAN', 'ZRQUAN')
  AND trim(suppressedMeterReadingDocumentID) <> ''
  AND not(billingLineItemBudgetBillingFlag = 'N') OR billingLineItemBudgetBillingFlag is null
  UNION ALL
  SELECT 7 ord, '__minus Reversals' description
        ,sum(meteredWaterMLConsumption) billedConsumptionMLQuantity
  FROM bill
  WHERE trim(billingSimulationIndicator) = ''   
  AND billingLineItemBudgetBillingFlag = 'N'
  AND lineItemTypeCode IN ('ZDQUAN', 'ZRQUAN')
  AND trim(suppressedMeterReadingDocumentID) <> ''
  AND not( reversalDate IS NULL OR reversalDate = '1900-01-01' OR reversalDate = '9999-12-31')
  UNION ALL
  SELECT *
  FROM item8  
--   UNION ALL
--   SELECT *
--   FROM item9 
  UNION ALL
  SELECT *
  FROM item10
)
SELECT current_date() calculationDate
      ,ord::DECIMAL(3,1) ord
      ,description
      ,billedConsumptionMLQuantity::DECIMAL(33,15) billedConsumptionMLQuantity
FROM
(
  SELECT *
  FROM set_
  UNION ALL
  SELECT 11 ord, 'Final Billing items to be apportioned' description
        ,sum(if(ord=1,billedConsumptionMLQuantity,0)) 
          - sum(if(ord between 2 and 10 and round(ord) <> 5,billedConsumptionMLQuantity,0))      
  FROM set_
  UNION ALL
  SELECT 12 ord, 'Actual Billing items that are apportioned' description
          ,sum(supplyApportionedKLQuantity) /1000 billedConsumptionMLQuantity
  FROM
  (        
    SELECT *      
    FROM {get_env()}curated.fact.dailySupplyApportionedConsumption  
    WHERE meterActiveEndDate >= (select minDate from required_dates)
    -- qualify Row_Number () over ( partition BY consumptionDate,billingDocumentNumber,propertySK,deviceSK,potableSubstitutionFlag,logicalDeviceNumber,logicalRegisterNumber,registerNumber ORDER BY calculationDate desc ) = 1
  )
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
