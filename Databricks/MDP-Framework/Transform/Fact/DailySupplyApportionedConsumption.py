# Databricks notebook source
# MAGIC %run ../../Common/common-transform

# COMMAND ----------

TARGET_TABLE=_.Destination

# COMMAND ----------

# DBTITLE 1,Create table upfront as the main query needs it
spark.sql(f"""
	CREATE TABLE IF NOT EXISTS {TARGET_TABLE}
	(
		dailySupplyApportionedConsumptionSK STRING NOT NULL,     
		consumptionDate DATE NOT NULL,
		calculationDate DATE NOT NULL,
		propertySK STRING NOT NULL,
  		deviceSK STRING NOT NULL,
		waterNetworkSK STRING,
		propertyTypeHistorySK STRING,
		contractAccountSK STRING,
		contractSK STRING,
  		meterConsumptionBillingDocumentSK STRING,
    	locationSK STRING,
		installationSK STRING,
  		businessPartnerGroupSK STRING,
		propertyNumber STRING,
		LGA STRING,
		contractID STRING NOT NULL,  
		contractAccountNumber STRING,
		billingDocumentNumber STRING NOT NULL,
		deviceNumber STRING NOT NULL,    
		logicalDeviceNumber STRING,
		logicalRegisterNumber STRING,
  		registerNumber INTEGER,
		divisionCode STRING,
		division STRING,
		locationId STRING,
		installationNumber STRING,
		businessPartnerGroupNumber STRING,
  		supplyDeliverySystem STRING NOT NULL,
		isOutsortedFlag STRING,
		supplyErrorFlag STRING,
		supplyManualUsedFlag STRING,
		SWCUnbilledMeteredFlag STRING,
		potableSubstitutionFlag STRING,    
		supplyApportionedKLQuantity DECIMAL(18,10),  
		meteredConsumptionKLQuantity DECIMAL(18,10),  
		billingPeriodStartDate DATE,
		billingPeriodEndDate DATE,
		meterActiveStartDate DATE,
		meterActiveEndDate DATE,
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
CREATE OR REPLACE TEMPORARY VIEW REQUIRED_DATES as
(
	SELECT  minDate
         ,minDateAvailableData
         ,maxDate
	FROM
	(
    select 
      if((select count(*) from {TARGET_TABLE}) = 0
          ,min(reportDate),nvl2(min(reportDate),greatest(min(reportDate),dateadd(MONTH, -24, current_date())::DATE),null)) minDate      
      ,min(reportDate) minDateAvailableData
      ,max(reportDate) maxDate
    from {get_env()}curated.fact.viewinterimwaternetworkdemand
  )
  WHERE minDate <= maxDate
)
"""
)

# COMMAND ----------

minDate = spark.sql("select minDate::STRING from required_dates").collect()[0][0]
if minDate:
    spark.sql(f"""
            DELETE FROM {TARGET_TABLE}
            WHERE meterActiveEndDate >= '{minDate}'; 
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
WITH 
WATER_DEMAND AS
(
  SELECT * except(splitDeliverySystem)
        ,regexp_replace(splitDeliverySystem,'[\\\s"]','') splitDeliverySystem
  FROM
  (
    SELECT explode(split(deliverySystem, '[+]')) splitDeliverySystem
          , *
    FROM {get_env()}curated.fact.viewinterimwaternetworkdemand
    WHERE deliverySystem NOT IN ('DEL_CASCADE','DEL_ORCHARD_HILLS')
    AND networkTypeCode IN ('Delivery System', 'Delivery System Combined')
  )
),
BILLED_CONSUMPTION AS
(
  SELECT b.billingDocumentNumber
        ,db1.billingDocumentLineItemId
        ,db1.lineItemTypeCode
        ,b.contractId
        ,b.businessPartnerGroupNumber
        ,b.startBillingPeriod
        ,b.endBillingPeriod
        ,db1.validFromDate meterActiveStartDate
        ,db1.validToDate meterActiveEndDate
        ,max(db1.validToDate)
          over (partition by b.billingDocumentNumber,b.businessPartnerGroupNumber,db2.equipmentNumber,db1.lineItemTypeCode,db2.logicalDeviceNumber,db2.logicalRegisterNumber,db2.registerNumber::INTEGER) maxMeterActiveEndDate          
        ,db1.billingQuantityPlaceBeforeDecimalPoint meteredWaterConsumption
        ,db2.equipmentNumber
        ,b.contractAccountNumber
        ,b.documentNotReleasedFlag isOutsortedFlag
        ,db2.logicalDeviceNumber
        ,db2.logicalRegisterNumber
        ,db2.registerNumber::INTEGER
        ,b.divisionCode
        ,b.division
  FROM {get_env()}cleansed.isu.erch timestamp as of '{firstOfTheMonthTS}' b
  JOIN {get_env()}cleansed.isu.dberchz1 timestamp as of '{firstOfTheMonthTS}' db1
    ON b.billingDocumentNumber = db1.billingDocumentNumber
  JOIN {get_env()}cleansed.isu.dberchz2 timestamp as of '{firstOfTheMonthTS}' db2
    ON (db2.billingDocumentNumber = db1.billingDocumentNumber 
      AND db2.billingDocumentLineItemId = db1.billingDocumentLineItemId)       
  WHERE trim(b.billingSimulationIndicator) = ''
  AND db1.lineItemTypeCode IN ('ZDQUAN', 'ZRQUAN')
  AND trim(db2.suppressedMeterReadingDocumentID) <> ''
  AND db1.billingLineItemBudgetBillingFlag = 'N'
  AND ( b.reversalDate IS NULL OR b.reversalDate = '1900-01-01' OR b.reversalDate = '9999-12-31')
  -- AND b.startBillingPeriod >= (select minDateAvailableData from required_dates)
  -- AND b.endBillingPeriod >= (select minDate from required_dates)
  -- AND b.endBillingPeriod <= (select maxDate from required_dates)
  AND db1.validFromDate >= (select minDateAvailableData from required_dates)
  AND db1.validToDate >= (select minDate from required_dates)
  AND db1.validToDate <= (select maxDate from required_dates)  
),
VPK AS
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
        ,if(vpk.inferiorPropertyTypeCode='250','Y','N') SWCUnbilledMeteredFlag            
  FROM {get_env()}curated.water_balance.propertykey vpk  
),
BC_WITH_EXTRA_ATTR AS
(
  SELECT  
        bc.billingDocumentNumber
        ,bc.contractId
        ,bc.businessPartnerGroupNumber
        ,vpk.propertyNumber
        ,vpk.LGA
        ,bc.startBillingPeriod
        ,bc.endBillingPeriod            
        ,bc.contractAccountNumber      
        ,device.deviceSK
        ,device.deviceNumber
        ,vpk.propertySK
        ,vpk.propertyTypeHistorySK
        ,vpk.drinkingWaterNetworkSK 
        ,vpk.waterNetworkDeliverySystem
        ,vpk.waterNetworkDistributionSystem
        ,vpk.waterNetworkSupplyZone
        ,vpk.waterNetworkPressureArea
        ,vpk.recycledWaterNetworkDeliverySystem
        ,vpk.inferiorPropertyTypeCode
        ,bc.lineItemTypeCode
        ,case 
            when vpk.inRecycledWaterNetwork='Y' and bc.lineItemTypeCode = 'ZRQUAN' then 'Y'
            when vpk.waterNetworkDeliverySystem = 'Unknown' then 'NA'
            else 'N'
         end potableSubstitutionFlag
        -- ,if(vpk.waterNetworkDeliverySystem = 'Unknown',vpk.inRecycledWaterNetwork='Y' and bc.lineItemTypeCode = 'ZRQUAN','Y','N') potableSubstitutionFlag
        ,vpk.SWCUnbilledMeteredFlag
        ,dca.contractAccountSK
        ,dc.contractSK
        ,bc.isOutsortedFlag
        ,bc.logicalDeviceNumber
        ,bc.logicalRegisterNumber
        ,bc.registerNumber
        ,bc.divisionCode
        ,bc.division
        ,dl.locationId
        ,dl.locationSK
        ,dbpg.businessPartnerGroupSK
        ,dmcbd.meterConsumptionBillingDocumentSK
        ,di.installationSK
        ,di.installationNumber
        ,min(meterActiveStartDate) meterActiveStartDate
        ,max(meterActiveEndDate) meterActiveEndDate
        ,sum(meteredWaterConsumption) meteredWaterConsumption
  FROM billed_consumption bc
  JOIN {get_env()}curated.dim.device device
  ON (device.deviceNumber = bc.equipmentNumber
    AND device._RecordStart <= bc.maxMeterActiveEndDate
    AND device._RecordEnd >= bc.maxMeterActiveEndDate)
  JOIN vpk
  ON (vpk.propertyNumber = bc.businessPartnerGroupNumber
    AND vpk._effectiveFrom <= bc.maxMeterActiveEndDate
    AND vpk._effectiveTo >= bc.maxMeterActiveEndDate)
  LEFT JOIN {get_env()}curated.dim.contractaccount dca
  ON (dca.contractAccountNumber = bc.contractAccountNumber
    AND dca._RecordStart <= bc.maxMeterActiveEndDate
    AND dca._RecordEnd >= bc.maxMeterActiveEndDate)
  LEFT JOIN {get_env()}curated.dim.contract dc
  ON (dc.contractID = bc.contractId
    AND dc._RecordStart <= bc.maxMeterActiveEndDate
    AND dc._RecordEnd >= bc.maxMeterActiveEndDate)
  LEFT JOIN {get_env()}curated.dim.location dl
  ON (dl.locationId = bc.businessPartnerGroupNumber
    AND dl._RecordStart <= bc.maxMeterActiveEndDate
    AND dl._RecordEnd >= bc.maxMeterActiveEndDate)
  LEFT JOIN {get_env()}curated.dim.businessPartnerGroup dbpg
  ON (dbpg.businessPartnerGroupNumber = bc.businessPartnerGroupNumber
    AND dbpg._RecordStart <= bc.maxMeterActiveEndDate
    AND dbpg._RecordEnd >= bc.maxMeterActiveEndDate)
  LEFT JOIN {get_env()}curated.dim.meterConsumptionBillingDocument dmcbd
  ON (dmcbd.billingDocumentNumber = bc.billingDocumentNumber
    AND dmcbd._RecordStart <= bc.maxMeterActiveEndDate
    AND dmcbd._RecordEnd >= bc.maxMeterActiveEndDate)            
  LEFT JOIN {get_env()}curated.dim.installation di
  ON (di.installationNumber = dc.installationNumber
    AND di._RecordStart <= bc.maxMeterActiveEndDate
    AND di._RecordEnd >= bc.maxMeterActiveEndDate)    
  GROUP BY ALL  
),
APPORTIONED_BILLED_CONSUMPTION AS
(
  SELECT /*+ RANGE_JOIN(bc, 95) */ --95 is the usual period
         bc.*
        ,d.reportDate consumptionDate
        ,d.manualDemandFlag supplyManualUsedFlag
        ,try_divide(d.demandQuantity
            ,sum(d.demandQuantity) 
            over (partition by bc.billingDocumentNumber,bc.propertySK,bc.deviceSK,potableSubstitutionFlag,bc.logicalDeviceNumber,bc.logicalRegisterNumber,bc.registerNumber))
          * bc.meteredWaterConsumption supplyApportionedKLQuantity
        ,if(bc.waterNetworkDeliverySystem = 'Unknown','DEL_POTTS_HILL + DEL_PROSPECT_EAST',d.deliverySystem) supplyDeliverySystem
        ,nvl2(falseReadings,'Y','N') supplyErrorFlag
  FROM bc_with_extra_attr bc
  JOIN water_demand d
    ON ((d.splitDeliverySystem = bc.waterNetworkDeliverySystem
        or bc.waterNetworkDeliverySystem = 'Unknown'
           and splitDeliverySystem in ('DEL_POTTS_HILL') --DEL_POTTS_HILL + DEL_PROSPECT_EAST
        )
        AND d.reportDate between bc.meterActiveStartDate and bc.meterActiveEndDate)    
  WHERE lineItemTypeCode = 'ZDQUAN' OR potableSubstitutionFlag in ('Y','NA')
),
NEW_RECORDS AS
(
  SELECT consumptionDate
        ,propertySK
        ,deviceSK
        ,drinkingWaterNetworkSK waterNetworkSK
        ,propertyTypeHistorySK
        ,contractAccountSK
        ,contractSK
        ,meterConsumptionBillingDocumentSK
        ,locationSK
        ,installationSK
        ,businessPartnerGroupSK
        ,propertyNumber
        ,LGA
        ,contractID 
        ,contractAccountNumber
        ,billingDocumentNumber
        ,deviceNumber   
        ,logicalDeviceNumber
        ,logicalRegisterNumber
        ,registerNumber
        ,divisionCode
        ,division
        ,locationId
        ,installationNumber
        ,businessPartnerGroupNumber
        ,supplyDeliverySystem
        ,isOutsortedFlag
        ,supplyErrorFlag
        ,supplyManualUsedFlag
        ,SWCUnbilledMeteredFlag
        ,potableSubstitutionFlag   
        ,supplyApportionedKLQuantity  
        ,meteredWaterConsumption meteredConsumptionKLQuantity          
        ,startBillingPeriod billingPeriodStartDate
		    ,endBillingPeriod billingPeriodEndDate
        ,meterActiveStartDate
        ,meterActiveEndDate
  FROM apportioned_billed_consumption
  -- MINUS
  -- SELECT * except(calculationDate)
  -- FROM {get_env()}curated.fact.dailySupplyApportionedConsumption
  -- WHERE consumptionDate >= (select min(consumptionDate) from apportioned_billed_consumption)
  -- qualify Row_Number () over ( partition BY consumptionDate,billingDocumentNumber,propertySK,deviceSK,potableSubstitutionFlag,logicalDeviceNumber,logicalRegisterNumber,registerNumber ORDER BY calculationDate desc ) = 1
)
SELECT consumptionDate
      ,current_date() calculationDate
      ,propertySK
      ,deviceSK
      ,waterNetworkSK
      ,propertyTypeHistorySK
      ,contractAccountSK
      ,contractSK
      ,meterConsumptionBillingDocumentSK
      ,locationSK
      ,installationSK
      ,businessPartnerGroupSK
      ,propertyNumber::STRING
      ,LGA
      ,contractID
      ,contractAccountNumber
      ,billingDocumentNumber
      ,deviceNumber
      ,logicalDeviceNumber
      ,logicalRegisterNumber
      ,registerNumber
      ,divisionCode
      ,division
      ,locationId
      ,installationNumber
      ,businessPartnerGroupNumber
      ,supplyDeliverySystem
      ,isOutsortedFlag
      ,supplyErrorFlag
      ,supplyManualUsedFlag
      ,SWCUnbilledMeteredFlag
      ,potableSubstitutionFlag   
      ,supplyApportionedKLQuantity::DECIMAL(18,10) supplyApportionedKLQuantity
      ,meteredConsumptionKLQuantity::DECIMAL(18,10) meteredConsumptionKLQuantity
      ,billingPeriodStartDate
      ,billingPeriodEndDate
      ,meterActiveStartDate
      ,meterActiveEndDate
FROM new_records
"""
)

# COMMAND ----------

def Transform():
    # ------------- TABLES ----------------- #
    global df
    # ------------- JOINS ------------------ #
        
    # ------------- TRANSFORMS ------------- #
    _.Transforms = [
        f"billingDocumentNumber||'|'||propertySK||'|'||deviceSK||'|'||potableSubstitutionFlag||'|'||logicalDeviceNumber||'|'||logicalRegisterNumber||'|'||registerNumber||'|'||consumptionDate {BK}"
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
