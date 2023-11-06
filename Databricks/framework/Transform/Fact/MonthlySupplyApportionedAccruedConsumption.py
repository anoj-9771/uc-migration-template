# Databricks notebook source
# MAGIC %run ../../Common/common-transform

# COMMAND ----------

TARGET_TABLE=_.Destination

# COMMAND ----------

# DBTITLE 1,Create table upfront as the main query needs it
spark.sql(f"""
	CREATE TABLE IF NOT EXISTS {TARGET_TABLE}
	(
		monthlySupplyApportionedAccruedConsumptionSK STRING NOT NULL,     
		consumptionDate DATE NOT NULL,
		consumptionYear INTEGER,
  		consumptionMonth INTEGER,
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
		SWCUnbilledMeteredFlag STRING,
		potableSubstitutionFlag STRING,  
		firstDayOfAccruedActiveMonth DATE,
		lastDayOfAccruedActiveMonth DATE,  
		accruedActiveMonthStartDate DATE,
		accruedActiveMonthEndDate DATE, 
		accruedPeriodStartDate DATE,
		accruedPeriodEndDate DATE,     
		totalAccruedWaterConsumptionKLQuantity DECIMAL(18,10),      
		monthlySupplyApportionedKLQuantity DECIMAL(18,10),
  		totalAccruedActiveDays INTEGER,
    	totalAccruedActiveDaysPerMonth INTEGER,
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

# spark.sql(f"""
# CREATE OR REPLACE TEMPORARY VIEW REQUIRED_DATES as
# (
# 	SELECT minDate 
#   FROM
#   (
#     select
#       if((select count(*) from {TARGET_TABLE}) = 0
#           ,min(accruedPeriodEndDate),nvl2(min(accruedPeriodEndDate),greatest(min(accruedPeriodEndDate),dateadd(MONTH, -24, current_date())::DATE),null)) minDate
#     from {get_env()}curated.fact.dailySupplyApportionedAccruedConsumption
#   )
# )
# """
# )

# COMMAND ----------

# minDate = spark.sql("select minDate::STRING from required_dates").collect()[0][0]
# if minDate:
#     spark.sql(f"""
#             DELETE FROM {TARGET_TABLE}
#             WHERE accruedPeriodEndDate >= '{minDate}'; 
#             """).display()

# COMMAND ----------

simulationPeriodId = spark.sql(f"""
    SELECT distinct simulationPeriodId 
    FROM {get_env()}curated.fact.dailySupplyApportionedAccruedConsumption    
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
WITH monthly_apportioned_accrued_consumption AS
(
  SELECT trunc(consumptionDate,'mm') consumptionDate
        ,year(consumptionDate)::INTEGER consumptionYear
        ,month(consumptionDate)::INTEGER consumptionMonth
        ,propertySK
        ,waterNetworkSK
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
        ,SWCUnbilledMeteredFlag
        ,potableSubstitutionFlag
        ,d.monthStartDate firstDayOfMeterActiveMonth
        ,d.monthEndDate lastDayOfMeterActiveMonth        
        ,if(accruedPeriodStartDate between d.monthStartDate and d.monthEndDate
            ,accruedPeriodStartDate,d.monthStartDate) accruedActiveMonthStartDate
        ,if(accruedPeriodEndDate between d.monthStartDate and d.monthEndDate
            ,accruedPeriodEndDate,d.monthEndDate) accruedActiveMonthEndDate  
        ,accruedPeriodStartDate            
        ,accruedPeriodEndDate
        ,accruedConsumptionKLQuantity::DECIMAL(18,10) totalAccruedWaterKLConsumption        
        ,sum(supplyApportionedKLQuantity)::DECIMAL(18,10) monthlySupplyApportionedKLQuantity
  FROM
  (        
    SELECT *
    FROM {get_env()}curated.fact.dailySupplyApportionedAccruedConsumption
    -- WHERE accruedPeriodEndDate >= (select minDate from required_dates)
    qualify dense_rank() over (order by to_date(replace(simulationPeriodId, 'F_'),'MMM_yy') desc) = 1
    -- qualify Row_Number () over ( partition BY consumptionDate,billingDocumentNumber,propertySK,potableSubstitutionFlag,accruedPeriodStartDate,accruedPeriodEndDate ORDER BY calculationDate desc ) = 1 
  ) dsac
  JOIN {get_env()}curated.dim.date d
  ON (dsac.consumptionDate = d.calendarDate)   
  GROUP BY ALL
)
SELECT *
      ,sum(datediff(accruedActiveMonthEndDate,accruedActiveMonthStartDate) + 1) 
        over (partition by billingDocumentNumber,propertySK,potableSubstitutionFlag,accruedPeriodStartDate,accruedPeriodEndDate)::INTEGER totalAccruedActiveDays
      ,(datediff(accruedActiveMonthEndDate,accruedActiveMonthStartDate) + 1)::INTEGER totalAccruedActiveDaysPerMonth
FROM monthly_apportioned_accrued_consumption
"""
)

# COMMAND ----------

def Transform():
    # ------------- TABLES ----------------- #
    global df
    # ------------- JOINS ------------------ #
        
    # ------------- TRANSFORMS ------------- #
    _.Transforms = [
        f"billingDocumentNumber||'|'||propertySK||'|'||potableSubstitutionFlag||'|'||accruedPeriodStartDate||'|'||accruedPeriodEndDate||'|'||consumptionDate {BK}"
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
