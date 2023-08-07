# Databricks notebook source
# MAGIC %run ../../Common/common-transform

# COMMAND ----------

TARGET_TABLE=_.Destination

# COMMAND ----------

# DBTITLE 1,Create table upfront as the main query needs it
spark.sql(f"""
	CREATE TABLE IF NOT EXISTS {TARGET_TABLE}
	(
		monthlySupplyApportionedAggregateSK STRING NOT NULL,     
		consumptionDate DATE NOT NULL,
		calculationDate DATE NOT NULL,
		waterNetworkSK STRING,
  		LGA STRING,
  		totalKLQuantity DECIMAL(18,10),  
		billedKLQuantity DECIMAL(18,10),  
		accruedKLQuantity DECIMAL(18,10), 
  		SWCUnbilledMeteredKLQuantity DECIMAL(18,10), 
    	potableSubstitutionKLQuantity DECIMAL(18,10), 
		totalPropertyCount INTEGER,
		billedPropertyCount INTEGER,
		accruedPropertyCount INTEGER,
		SWCUnbilledMeteredPropertyCount INTEGER,
		potableSubstitutionPropertyCount INTEGER,
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

view = f"curated.water_balance.monthlySupplyApportionedAggregate"
sql_ = f"""
WITH latest_monthly_consumption as 
(
  SELECT *
  FROM {TARGET_TABLE}
  WHERE qualify Row_Number () over ( partition BY consumptionDate,waterNetworkSK,LGA ORDER BY calculationDate desc ) = 1 
),
monthly_zone_consumption as
(
  SELECT year(m.consumptionDate) consumptionYear
        ,month(m.consumptionDate) consumptionMonth
        ,m.calculationDate  
        ,dwn.deliverySystem
        ,dwn.supplyZone
        ,dwn.pressureArea
        ,case grouping_id(
                          year(m.consumptionDate)
                          ,month(m.consumptionDate)
                          ,m.calculationDate
                          ,dwn.deliverySystem 
                          ,dwn.supplyZone
                          ,dwn.pressureArea          
                          )
            when 3 then 'Delivery System'
            when 1 then 'Supply Zone'
            else 'Pressure Area'
         end networkTypeCode
        ,sum(m.totalKLQuantity)/1000 totalMLQuantity
        ,sum(m.billedKLQuantity)/1000 billedMLQuantity
        ,sum(m.accruedKLQuantity)/1000 accruedMLQuantity
        ,sum(m.SWCUnbilledMeteredKLQuantity)/1000 SWCUnbilledMeteredMLQuantity
        ,sum(m.potableSubstitutionKLQuantity)/1000 potableSubstitutionMLQuantity
        ,sum(m.totalPropertyCount) totalPropertyCount
        ,sum(m.billedPropertyCount) billedPropertyCount
        ,sum(m.accruedPropertyCount) accruedPropertyCount
        ,sum(m.SWCUnbilledMeteredPropertyCount) SWCUnbilledMeteredPropertyCount
        ,sum(m.potableSubstitutionPropertyCount) potableSubstitutionPropertyCount
  FROM latest_monthly_consumption m
  JOIN {get_env()}curated.dim.waternetwork dwn
  ON (m.waterNetworkSK = dwn.waterNetworkSK)  
  GROUP BY year(m.consumptionDate)
          ,month(m.consumptionDate)
          ,m.calculationDate
          ,dwn.deliverySystem 
          ,ROLLUP(dwn.supplyZone
                 ,dwn.pressureArea)
)
select *
from monthly_zone_consumption
"""

CreateView(view, sql_)

# COMMAND ----------

spark.sql(f"""
        DELETE FROM {TARGET_TABLE}
        WHERE calculationDate = current_date(); 
        """).display()

# COMMAND ----------

df = spark.sql(f"""
WITH REQUIRED_DATES as
(
	SELECT min(minDate) minDate 
  FROM
  (
    select
      trunc(if((select count(*) from {get_env()}curated.fact.monthlySupplyApportionedAggregate) = 0
          ,min(consumptionDate),nvl2(min(consumptionDate),greatest(min(consumptionDate),dateadd(MONTH, -24, current_date())::DATE),null)),'mm') minDate
    from {get_env()}curated.fact.dailySupplyApportionedConsumption
    union all
    select
      trunc(if((select count(*) from {get_env()}curated.fact.monthlySupplyApportionedAggregate) = 0
          ,min(consumptionDate),nvl2(min(consumptionDate),greatest(min(consumptionDate),dateadd(MONTH, -24, current_date())::DATE),null)),'mm') minDate
    from (
        select *          
        from {get_env()}curated.fact.dailySupplyApportionedAccruedConsumption
        qualify dense_rank() over (order by to_date(replace(simulationPeriodId, 'F_'),'MMM_yy') desc) = 1
    )      
  )
),
daily_apportioned_billed_consumption AS
(
  SELECT consumptionDate
        ,waterNetworkSK
        ,installationNumber
        ,propertyNumber
        ,LGA
        ,potableSubstitutionFlag
        ,SWCUnbilledMeteredFlag
        ,sum(supplyApportionedKLQuantity) supplyApportionedKLQuantity
  FROM
  (        
    SELECT *
    FROM {get_env()}curated.fact.dailySupplyApportionedConsumption
    WHERE consumptionDate >= (select minDate from required_dates)   
    -- qualify Row_Number () over ( partition BY consumptionDate,billingDocumentNumber,propertySK,deviceSK,potableSubstitutionFlag,logicalDeviceNumber,logicalRegisterNumber,registerNumber ORDER BY calculationDate desc ) = 1 
  )
  GROUP BY consumptionDate
          ,waterNetworkSK
          ,installationNumber
          ,propertyNumber
          ,LGA
          ,potableSubstitutionFlag
          ,SWCUnbilledMeteredFlag
),
daily_apportioned_accrued_consumption AS
(
  SELECT consumptionDate
        ,waterNetworkSK
        ,installationNumber
        ,propertyNumber
        ,LGA
        ,potableSubstitutionFlag
        ,SWCUnbilledMeteredFlag
        ,sum(supplyApportionedKLQuantity) supplyApportionedKLQuantity
  FROM
  (        
    SELECT *
    FROM {get_env()}curated.fact.dailySupplyApportionedAccruedConsumption
    WHERE consumptionDate >= (select minDate from required_dates)    
    qualify dense_rank() over (order by to_date(replace(simulationPeriodId, 'F_'),'MMM_yy') desc) = 1
    -- qualify Row_Number () over ( partition BY consumptionDate,billingDocumentNumber,propertySK,potableSubstitutionFlag,accruedPeriodStartDate,accruedPeriodEndDate ORDER BY calculationDate desc ) = 1 
  )
  GROUP BY consumptionDate
          ,waterNetworkSK
          ,installationNumber
          ,propertyNumber
          ,LGA
          ,potableSubstitutionFlag
          ,SWCUnbilledMeteredFlag
),
daily_apportioned_consumption as 
(
  SELECT coalesce(b.consumptionDate, a.consumptionDate) consumptionDate
        ,coalesce(b.waterNetworkSK, a.waterNetworkSK) waterNetworkSK
        ,coalesce(b.installationNumber, a.installationNumber) installationNumber
        ,coalesce(b.potableSubstitutionFlag, a.potableSubstitutionFlag) potableSubstitutionFlag
        ,b.propertyNumber billedPropertyNumber
        ,nvl2(b.consumptionDate,null,a.propertyNumber) accruedPropertyNumber
        ,coalesce(b.LGA, a.LGA) LGA
        ,b.SWCUnbilledMeteredFlag billedSWCUnbilledMeteredFlag
        ,a.SWCUnbilledMeteredFlag accruedSWCUnbilledMeteredFlag
        ,coalesce(b.supplyApportionedKLQuantity,0) billedSupplyApportionedKLQuantity
        ,nvl2(b.consumptionDate,0,coalesce(a.supplyApportionedKLQuantity,0)) accruedSupplyApportionedKLQuantity
  FROM daily_apportioned_billed_consumption b
  FULL JOIN daily_apportioned_accrued_consumption a
  ON (a.consumptionDate = b.consumptionDate
    AND a.waterNetworkSK = b.waterNetworkSK
    AND a.installationNumber = b.installationNumber
    AND a.potableSubstitutionFlag = b.potableSubstitutionFlag)  
),
monthly_apportioned_consumption as 
(
  SELECT trunc(consumptionDate,'mm') consumptionDate
        ,waterNetworkSK        
        ,LGA
        ,sum(billedSupplyApportionedKLQuantity) + sum(accruedSupplyApportionedKLQuantity) totalKLQuantity        
        ,sum(billedSupplyApportionedKLQuantity) billedKLQuantity
        ,sum(accruedSupplyApportionedKLQuantity) accruedKLQuantity
        ,sum(billedSupplyApportionedKLQuantity + accruedSupplyApportionedKLQuantity) 
            filter(where billedSWCUnbilledMeteredFlag='Y' or accruedSWCUnbilledMeteredFlag ='Y') SWCUnbilledMeteredKLQuantity
        ,sum(billedSupplyApportionedKLQuantity + accruedSupplyApportionedKLQuantity) 
            filter(where potableSubstitutionFlag='Y') potableSubstitutionKLQuantity
        ,count(distinct coalesce(billedPropertyNumber,accruedPropertyNumber)) totalPropertyCount
        ,count(distinct billedPropertyNumber) billedPropertyCount
        ,count(distinct accruedPropertyNumber) accruedPropertyCount
        ,count(distinct coalesce(billedPropertyNumber,accruedPropertyNumber)) 
            filter(where billedSWCUnbilledMeteredFlag='Y' or accruedSWCUnbilledMeteredFlag ='Y') SWCUnbilledMeteredPropertyCount 
        ,count(distinct coalesce(billedPropertyNumber,accruedPropertyNumber)) 
            filter(where potableSubstitutionFlag='Y') potableSubstitutionPropertyCount        
  FROM daily_apportioned_consumption
  GROUP BY trunc(consumptionDate,'mm')
          ,waterNetworkSK
          ,LGA
),
NEW_RECORDS AS
(
  SELECT *
  FROM monthly_apportioned_consumption
  -- MINUS
  -- SELECT * except(calculationDate)
  -- FROM {get_env()}curated.fact.monthlySupplyApportionedAggregate
  -- WHERE consumptionDate >= (select min(consumptionDate) from monthly_apportioned_consumption)
  -- qualify Row_Number () over ( partition BY consumptionDate,waterNetworkSK,LGA ORDER BY calculationDate desc ) = 1
)
SELECT consumptionDate
      ,current_date() calculationDate
      ,waterNetworkSK     
      ,LGA   
      ,totalKLQuantity::DECIMAL(18,10) totalKLQuantity
      ,billedKLQuantity::DECIMAL(18,10) billedKLQuantity
      ,accruedKLQuantity::DECIMAL(18,10) accruedKLQuantity
      ,SWCUnbilledMeteredKLQuantity::DECIMAL(18,10) SWCUnbilledMeteredKLQuantity
      ,potableSubstitutionKLQuantity::DECIMAL(18,10) potableSubstitutionKLQuantity
      ,totalPropertyCount::INTEGER totalPropertyCount
      ,billedPropertyCount::INTEGER billedPropertyCount
      ,accruedPropertyCount::INTEGER accruedPropertyCount
      ,SWCUnbilledMeteredPropertyCount::INTEGER SWCUnbilledMeteredPropertyCount
      ,potableSubstitutionPropertyCount::INTEGER potableSubstitutionPropertyCount
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
        f"waterNetworkSK||'|'||LGA||'|'||consumptionDate||'|'||calculationDate {BK}"
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
