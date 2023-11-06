# Databricks notebook source
# MAGIC %run ../../Common/common-transform

# COMMAND ----------

TARGET_TABLE=_.Destination

# COMMAND ----------

# DBTITLE 1,Create table upfront as the main query needs it
spark.sql(f"""
	CREATE TABLE IF NOT EXISTS {TARGET_TABLE}
	(
		monthlySupplyApportionedConsumptionSK STRING NOT NULL,     
		consumptionDate DATE NOT NULL,
		consumptionYear INTEGER,
  		consumptionMonth INTEGER,
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
		SWCUnbilledMeteredFlag STRING,
		potableSubstitutionFlag STRING,    
		billingPeriodStartDate DATE,
		billingPeriodEndDate DATE,
		firstDayOfMeterActiveMonth DATE,
		lastDayOfMeterActiveMonth DATE,  
		meterActiveMonthStartDate DATE,
		meterActiveMonthEndDate DATE,    
		meterActiveStartDate DATE,
		meterActiveEndDate DATE,
		totalMeteredWaterConsumptionKLQuantity DECIMAL(18,10),      
		monthlySupplyApportionedKLQuantity DECIMAL(18,10), 
  		totalMeterActiveDays INTEGER,
    	totalMeterActiveDaysPerMonth INTEGER,
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
	SELECT minDate 
  FROM
  (
    select
      if((select count(*) from {TARGET_TABLE}) = 0
          ,min(meterActiveEndDate),nvl2(min(meterActiveEndDate),greatest(min(meterActiveEndDate),dateadd(MONTH, -24, current_date())::DATE),null)) minDate
    from {get_env()}curated.fact.dailySupplyApportionedConsumption
  )
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

df = spark.sql(f"""
WITH monthly_apportioned_billed_consumption AS
(
  SELECT trunc(consumptionDate,'mm') consumptionDate
        ,year(consumptionDate)::INTEGER consumptionYear
        ,month(consumptionDate)::INTEGER consumptionMonth
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
        ,SWCUnbilledMeteredFlag
        ,potableSubstitutionFlag   
        ,billingPeriodStartDate
        ,billingPeriodEndDate
        ,d.monthStartDate firstDayOfMeterActiveMonth
        ,d.monthEndDate lastDayOfMeterActiveMonth        
        ,if(meterActiveStartDate between d.monthStartDate and d.monthEndDate
            ,meterActiveStartDate,d.monthStartDate) meterActiveMonthStartDate
        ,if(meterActiveEndDate between d.monthStartDate and d.monthEndDate
            ,meterActiveEndDate,d.monthEndDate) meterActiveMonthEndDate                    
        ,meterActiveStartDate
        ,meterActiveEndDate
        ,meteredConsumptionKLQuantity::DECIMAL(18,10) totalMeteredWaterConsumptionKLQuantity        
        ,sum(supplyApportionedKLQuantity)::DECIMAL(18,10) monthlySupplyApportionedKLQuantity
  FROM
  (        
    SELECT *
    FROM {get_env()}curated.fact.dailySupplyApportionedConsumption
    WHERE meterActiveEndDate >= (select minDate from required_dates)
    qualify Row_Number () over ( partition BY consumptionDate,billingDocumentNumber,propertySK,deviceSK,potableSubstitutionFlag,logicalDeviceNumber,logicalRegisterNumber,registerNumber ORDER BY calculationDate desc ) = 1 
  ) dsac
  JOIN {get_env()}curated.dim.date d
  ON (dsac.consumptionDate = d.calendarDate)   
  GROUP BY ALL
)
SELECT *
      ,sum(datediff(meterActiveMonthEndDate,meterActiveMonthStartDate) + 1) 
        over (partition by billingDocumentNumber,propertySK,deviceSK,potableSubstitutionFlag,logicalDeviceNumber,logicalRegisterNumber,registerNumber)::INTEGER totalMeterActiveDays
      ,(datediff(meterActiveMonthEndDate,meterActiveMonthStartDate) + 1)::INTEGER totalMeterActiveDaysPerMonth
FROM monthly_apportioned_billed_consumption
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
