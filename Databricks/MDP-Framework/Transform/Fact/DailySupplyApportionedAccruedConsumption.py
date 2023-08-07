# Databricks notebook source
# MAGIC %run ../../Common/common-transform

# COMMAND ----------

TARGET_TABLE=_.Destination

# COMMAND ----------

# DBTITLE 1,Create table upfront as the main query needs it
spark.sql(f"""
	CREATE TABLE IF NOT EXISTS {TARGET_TABLE}
	(
		dailySupplyApportionedAccruedConsumptionSK STRING NOT NULL,     
		consumptionDate DATE NOT NULL,
		calculationDate DATE NOT NULL,
		propertySK STRING NOT NULL,
		waterNetworkSK STRING,
		propertyTypeHistorySK STRING,
		contractAccountSK STRING,
		contractSK STRING,
    	locationSK STRING,
		installationSK STRING,
  		businessPartnerGroupSK STRING,
    	simulationPeriodId STRING,
		propertyNumber STRING,
  		LGA STRING,
		contractID STRING NOT NULL,  
		contractAccountNumber STRING,
		billingDocumentNumber STRING NOT NULL,
		divisionCode STRING,
		division STRING,
		locationId STRING,
		installationNumber STRING,  
		businessPartnerGroupNumber STRING,
  		supplyDeliverySystem STRING NOT NULL,
		supplyErrorFlag STRING,
		supplyManualUsedFlag STRING,
		SWCUnbilledMeteredFlag STRING,
		potableSubstitutionFlag STRING,    
		supplyApportionedKLQuantity DECIMAL(18,10),  
		accruedConsumptionKLQuantity DECIMAL(18,10),  
		accruedPeriodStartDate DATE,
		accruedPeriodEndDate DATE,
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

simulationPeriodId = spark.sql(f"""
    SELECT distinct simulationPeriodId 
    FROM {get_env()}cleansed.isu.0uc_sales_simu_01
    WHERE belzartName IN ('Water', 'Recycled water')
    qualify dense_rank() over (order by to_date(replace(simulationPeriodId, 'F_'),'MMM_yy') desc) = 1
""").collect()[0][0]
print(simulationPeriodId)
if simulationPeriodId:
    spark.sql(f"""
            DELETE FROM {TARGET_TABLE}
            WHERE simulationPeriodId = '{simulationPeriodId}'; 
            """).display()

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
),
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
ACCRUED_CONSUMPTION AS
(
  SELECT *
        ,min(dateFromWhichTimeSliceIsValid)
          over (partition by billingDocumentNumber,belzartName,installation) minDateFromWhichTimeSliceIsValid
        ,max(dateAtWhichATimeSliceExpires)
          over (partition by billingDocumentNumber,belzartName,installation) maxDateAtWhichATimeSliceExpires  
  FROM
  (
    SELECT *
    FROM {get_env()}cleansed.isu.0uc_sales_simu_01
    WHERE belzartName IN ('Water', 'Recycled water')
    qualify dense_rank() over (order by to_date(replace(simulationPeriodId, 'F_'),'MMM_yy') desc) = 1
  )
  qualify minDateFromWhichTimeSliceIsValid >= (select minDate from required_dates)
      and maxDateAtWhichATimeSliceExpires <= (select maxDate from required_dates)
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
AC_WITH_EXTRA_ATTR AS
(
  SELECT S.billingDocumentNumber
        -- ,S.billingLineItemForBillingDocuments
        -- ,S.lineItemType
        ,dc.contractID
        ,VCA.businessPartnerGroupNumber    
        ,vpk.propertyNumber
        ,vpk.LGA
        ,S.contractAccountNumber
        ,vpk.propertySK
        ,vpk.propertyTypeHistorySK
        ,vpk.drinkingWaterNetworkSK  
        ,vpk.waterNetworkDeliverySystem 
        ,case 
            when vpk.inRecycledWaterNetwork='Y' and S.belzartName='Recycled water' then 'Y'
            when vpk.waterNetworkDeliverySystem = 'Unknown' then 'NA'
            else 'N'
         end potableSubstitutionFlag        
        ,vpk.SWCUnbilledMeteredFlag                      
        ,S.simulationPeriodId
        ,S.belzartName
        ,S.installation
        ,dc.contractsk
        ,dca.contractAccountSK
        ,dl.locationId
        ,dl.locationSK
        ,dbpg.businessPartnerGroupSK
        ,di.installationSK
        ,di.installationNumber  
        ,ddt.divisionCode
        ,ddt.division
        -- ,S.dateFromWhichTimeSliceIsValid
        -- ,S.dateAtWhichATimeSliceExpires
        -- ,S.billingQuantityEnergy accruedConsumptionQuantity        
        ,min(S.dateFromWhichTimeSliceIsValid) dateFromWhichTimeSliceIsValid
        ,max(S.dateAtWhichATimeSliceExpires) dateAtWhichATimeSliceExpires
        ,sum(S.billingQuantityEnergy) accruedConsumptionKLQuantity
  FROM ACCRUED_CONSUMPTION S
  LEFT JOIN {get_env()}curated.water_consumption.viewcontractaccount vca
  ON (vca.contractAccountNumber = S.contractAccountNumber
    AND vca._effectiveFrom <= S.maxDateAtWhichATimeSliceExpires
    AND vca._effectiveTo >= S.maxDateAtWhichATimeSliceExpires)  
  LEFT JOIN {get_env()}curated.water_consumption.viewcontractaccount vca_latest
  ON (vca_latest.contractAccountNumber = S.contractAccountNumber
    AND vca_latest._effectiveTo = '9999-12-31 23:59:59.000')            
  JOIN vpk
  ON (vpk.propertyNumber = nvl(vca.businessPartnerGroupNumber, vca_latest.businessPartnerGroupNumber)
    AND vpk._effectiveFrom <= S.maxDateAtWhichATimeSliceExpires
    AND vpk._effectiveTo >= S.maxDateAtWhichATimeSliceExpires)        
  LEFT JOIN {get_env()}cleansed.isu.0division_text ddt
  ON (ddt.divisionCode = S.division)
  LEFT JOIN {get_env()}curated.dim.contractaccount dca
  ON (dca.contractAccountNumber = S.contractAccountNumber
    AND dca._RecordStart <= S.maxDateAtWhichATimeSliceExpires
    AND dca._RecordEnd >= S.maxDateAtWhichATimeSliceExpires)
  LEFT JOIN {get_env()}curated.dim.contract dc
  ON (dc.contractID = S.contract
    AND dc._RecordStart <= S.maxDateAtWhichATimeSliceExpires
    AND dc._RecordEnd >= S.maxDateAtWhichATimeSliceExpires)
  LEFT JOIN {get_env()}curated.dim.location dl
  ON (dl.locationId = vpk.propertyNumber
    AND dl._RecordStart <= S.maxDateAtWhichATimeSliceExpires
    AND dl._RecordEnd >= S.maxDateAtWhichATimeSliceExpires)
  LEFT JOIN {get_env()}curated.dim.businessPartnerGroup dbpg
  ON (dbpg.businessPartnerGroupNumber = vpk.propertyNumber
    AND dbpg._RecordStart <= S.maxDateAtWhichATimeSliceExpires
    AND dbpg._RecordEnd >= S.maxDateAtWhichATimeSliceExpires)
  LEFT JOIN {get_env()}curated.dim.installation di
  ON (di.installationNumber = S.installation
    AND di._RecordStart <= S.maxDateAtWhichATimeSliceExpires
    AND di._RecordEnd >= S.maxDateAtWhichATimeSliceExpires)
  GROUP BY ALL                   
),
APPORTIONED_ACCRUED_CONSUMPTION AS
(
  SELECT ac.*
        ,d.reportDate consumptionDate
        ,d.manualDemandFlag supplyManualUsedFlag
        ,try_divide(d.demandQuantity
           ,sum(d.demandQuantity) over (partition by ac.billingDocumentNumber,ac.propertySK,potableSubstitutionFlag,dateFromWhichTimeSliceIsValid,dateAtWhichATimeSliceExpires))
          * ac.accruedConsumptionKLQuantity supplyApportionedKLQuantity
        -- ,d.deliverySystem supplyDeliverySystem
        ,if(ac.waterNetworkDeliverySystem = 'Unknown','DEL_POTTS_HILL + DEL_PROSPECT_EAST',d.deliverySystem) supplyDeliverySystem
        ,nvl2(falseReadings,'Y','N') supplyErrorFlag
  FROM ac_with_extra_attr ac
  JOIN water_demand d
    ON ((d.splitDeliverySystem = ac.waterNetworkDeliverySystem
        or ac.waterNetworkDeliverySystem = 'Unknown'
           and splitDeliverySystem in ('DEL_POTTS_HILL') --DEL_POTTS_HILL + DEL_PROSPECT_EAST    
        )
        AND d.reportDate between ac.dateFromWhichTimeSliceIsValid and ac.dateAtWhichATimeSliceExpires)
  WHERE belzartName = 'Water' OR potableSubstitutionFlag in ('Y','NA')
),
NEW_RECORDS AS
(
  SELECT consumptionDate
        ,propertySK        
        ,drinkingWaterNetworkSK waterNetworkSK
        ,propertyTypeHistorySK
        ,contractAccountSK
        ,contractSK
        ,locationSK
        ,installationSK
        ,businessPartnerGroupSK
        ,simulationPeriodId
        ,propertyNumber
        ,LGA
        ,contractID 
        ,contractAccountNumber
        ,billingDocumentNumber
        ,divisionCode
        ,division
        ,locationId
        ,installationNumber
        ,businessPartnerGroupNumber
        ,supplyDeliverySystem
        ,supplyErrorFlag
        ,supplyManualUsedFlag
        ,SWCUnbilledMeteredFlag
        ,potableSubstitutionFlag   
        ,supplyApportionedKLQuantity  
        ,accruedConsumptionKLQuantity          
        ,dateFromWhichTimeSliceIsValid accruedPeriodStartDate
		,dateAtWhichATimeSliceExpires accruedPeriodEndDate
  FROM apportioned_accrued_consumption
  -- MINUS
  -- SELECT * except(calculationDate)
  -- FROM {get_env()}curated.fact.dailySupplyApportionedAccruedConsumption
  -- WHERE consumptionDate >= (select min(consumptionDate) from apportioned_accrued_consumption)
  -- qualify Row_Number () over ( partition BY consumptionDate,billingDocumentNumber,propertySK,potableSubstitutionFlag,accruedPeriodStartDate,accruedPeriodEndDate ORDER BY calculationDate desc ) = 1
)
SELECT consumptionDate
      ,current_date() calculationDate
      ,propertySK        
      ,waterNetworkSK
      ,propertyTypeHistorySK
      ,contractAccountSK
      ,contractSK
      ,locationSK
      ,installationSK
      ,businessPartnerGroupSK
      ,simulationPeriodId
      ,propertyNumber::STRING
      ,LGA
      ,contractID 
      ,contractAccountNumber
      ,billingDocumentNumber
      ,divisionCode
      ,division
      ,locationId
      ,installationNumber
      ,businessPartnerGroupNumber
      ,supplyDeliverySystem
      ,supplyErrorFlag
      ,supplyManualUsedFlag
      ,SWCUnbilledMeteredFlag
      ,potableSubstitutionFlag   
      ,supplyApportionedKLQuantity::DECIMAL(18,10) supplyApportionedKLQuantity
      ,accruedConsumptionKLQuantity::DECIMAL(18,10) accruedConsumptionKLQuantity
      ,accruedPeriodStartDate
      ,accruedPeriodEndDate
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
        f"billingDocumentNumber||'|'||propertySK||'|'||potableSubstitutionFlag||'|'||accruedPeriodStartDate||'|'||accruedPeriodEndDate||'|'||consumptionDate||'|'||calculationDate {BK}"
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
