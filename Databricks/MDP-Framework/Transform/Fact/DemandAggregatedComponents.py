# Databricks notebook source
# MAGIC %run ../../Common/common-transform 

# COMMAND ----------

# MAGIC %run ../../Common/common-helpers 

# COMMAND ----------

# MAGIC %run ../Views/viewAggregatedComponentsConfiguration_UC2

# COMMAND ----------

DEFAULT_TARGET = 'curated'

# COMMAND ----------

df = spark.sql(f"""
with date_df as (
  select distinct monthstartdate,year(monthstartdate) as yearnumber,month(monthstartdate) as monthnumber, date_format(monthStartDate,'MMM') as monthname, trunc(current_date(),'Month') as calculationDate
  from {get_env()}curated.dim.date where monthStartDate between add_months(current_date(),-25) and add_months(current_date(),-1)
  order by yearnumber desc, monthnumber desc
),
tfsu_df as (
  select property.propertyNumber,property.superiorPropertyTypeCode,property.superiorPropertyType,property.waterNetworkDeliverySystem deliverySystem,property.waterNetworkDistributionSystem distributionSystem,property.waterNetworkSupplyZone supplyZone,property.waterNetworkPressureArea pressureArea,property.propertySK,property.propertyTypeHistorySK,property.drinkingWaterNetworkSK,installation.installationNumber,installation.divisionCode,installation.division,device.deviceSize,device.functionClassCode,device.deviceID
  from {get_env()}curated.water_balance.propertyKey property 
  inner join {get_env()}curated.water_consumption.viewinstallation installation on property.propertyNumber = installation.propertyNumber and property.superiorPropertyTypeCode not in (902)
  inner join {get_env()}curated.water_consumption.viewdevice device on installation.installationNumber = device.installationNumber
  where property.currentFlag = 'Y' and installation.divisionCode = 10 and installation.currentFlag = 'Y' and device.functionClassCode in ('1000') and device.currentFlag = 'Y' and device.deviceSize >= 40
),
waternetwork_df as (
  select deliverySystem,distributionSystem,supplyZone,pressureArea,waterNetworkSK,_RecordCurrent from {get_env()}curated.dim.waternetwork
),
sharepointConfig_df as (
  with base as (
    select config.zoneName, config.zonetypename, config.metricTypeName, coalesce(config.metricValueNumber,0) as  metricValueNumber, config.yearnumber, config.monthName,
      decode(config.monthName,'Jan',1,'Feb',2,'Mar',3,'Apr',4,'May',5,'Jun',6,'Jul',7,'Aug',8,'Sep',9,'Oct',10,'Nov',11,'Dec',12 ) as monthnumber, d.calculationDate
      from {get_env()}curated.water_balance.AggregatedComponentsConfiguration config inner join date_df d on config.yearnumber = d.yearnumber and d.monthnumber = decode(config.monthName,'Jan',1,'Feb',2,'Mar',3,'Apr',4,'May',5,'Jun',6,'Jul',7,'Aug',8,'Sep',9,'Oct',10,'Nov',11,'Dec',12 )
  ) select distinct TFSUduration.zoneName,TFSUduration.zonetypename,
    case  when TFSUduration.metrictypename like 'TFSUMonthlyTestingOfFireSprinklerSystemsTest%' then 'monthlyTestingFireSprinklerSystemsFactor' 
          when TFSUduration.metrictypename like 'TFSUAnnualTestingOfSprinklerSystemResidentialAndCommercialFireServicesTest%' then 'annualTestingSprinklerSystemsResidentialAndCommercialFactor'
          when TFSUduration.metrictypename like 'TFSUAnnualTestingOfSprinklerSystemLargeIndustrialServicesTest%' then 'annualTestingSprinklerSystemsLargeIndustrialFactor'
          when TFSUduration.metrictypename like 'TFSUMonthlyTestingOfHydrantSystemsTest%' then 'monthlyTestingHydrantSystemsFactor'
          when TFSUduration.metrictypename like 'TFSUAnnualTestingOfHydrantSystemsResidentialAndCommercialFireServicesTest%' then 'annualTestingHydrantSystemsResidentialAndCommercialFactor'
          when TFSUduration.metrictypename like 'TFSUAnnualTestingOfHydrantSystemsLargeIndustrialServicesTest%' then 'annualTestingHydrantSystemsLargeIndustrialFactor'
          else TFSUduration.metrictypename end metrictypename,TFSUduration.metricValueNumber*TFSUrate.metricValueNumber as metricValueNumber,
          TFSUduration.yearnumber,TFSUduration.monthname,TFSUduration.monthnumber,TFSUduration.calculationDate,'Y' TFSU
    from base TFSUduration, base TFSUrate where TFSUduration.metricTypeName like '%TFSU%' and TFSUduration.metricTypeName like '%Duration%' and TFSUrate.metricTypeName like '%TFSU%' and TFSUrate.metricTypeName like '%Rate%' and replace(TFSUduration.metricTypeName,'Duration','') = replace(TFSUrate.metricTypeName,'FlowRate','') and TFSUduration.yearnumber = TFSUrate.yearnumber and TFSUduration.monthname = TFSUrate.monthname
    union
    select base.*,'N' from base where metricTypeName not like '%TFSU%'
),
waternetworkdemand_df as (
  select deliverySystem,distributionSystem,supplyZone,pressureArea,networkTypeCode,sum(demandQuantity) as demandQuantity, year(reportDate) as yearnumber, month(reportdate) as monthnumber
  from {get_env()}curated.water_balance.factwaternetworkdemand inner join date_df d on d.yearnumber = year(reportDate) and d.monthnumber = month(reportdate) 
  where deliverySystem NOT IN ('DEL_PROSPECT_EAST','DEL_POTTS_HILL','DEL_CASCADE','DEL_ORCHARD_HILLS','DESALINATION PLANT') 
  and networkTypeCode in ('Pressure Area','Supply Zone','Delivery System','Delivery System Combined')
  group by deliverySystem,distributionSystem,supplyZone,pressureArea,networkTypeCode,year(reportDate),month(reportdate)
), 
monthlySupplyApportioned_df as (
  Select waterNetworkSK as supply_waterNetworkSK,LGA,totalKLQuantity as supply_totalKLQuantity,year(consumptionDate) as yearnumber,month(consumptionDate) as monthnumber 
  from {get_env()}curated.fact.monthlysupplyapportionedaggregate sup inner join date_df d on d.yearnumber = year(consumptionDate) and d.monthnumber = month(consumptionDate) 
  where sup.calculationDate = (select max(calculationDate) from {get_env()}curated.fact.monthlysupplyapportionedaggregate)
),
monthlyTestingFireSprinklerSystems as (
  with SprinklerSystemsbase as (
    select deliverySystem,distributionSystem,supplyZone,pressureArea,count (Distinct deviceID) as deviceCount
    from tfsu_df where superiorPropertyType IN ('COMMERCIAL','COMMUNITY SERVS','INDUSTRIAL','MASTER STRATA','OCCUPIED LAND','UTILITIES') 
    group by deliverySystem,distributionSystem,supplyZone,pressureArea
  ) select deliverySystem,distributionSystem,supplyZone,pressureArea,'monthlyTestingFireSprinklerSystems' as metricTypeName,(deviceCount*metricvaluenumber)/1000000 as metricvaluenumber from SprinklerSystemsbase join sharepointConfig_df where TFSU = 'Y' and metrictypename = 'monthlyTestingFireSprinklerSystemsFactor'
),
monthlyTestingHydrantSystems as (
  with HydrantSystemsbase as (
    select deliverySystem,distributionSystem,supplyZone,pressureArea,count (Distinct deviceID) as deviceCount
    from tfsu_df where superiorPropertyType IN ('COMMERCIAL','COMMUNITY SERVS','MASTER STRATA','OCCUPIED LAND','UTILITIES') 
    group by deliverySystem,distributionSystem,supplyZone,pressureArea
  ) select deliverySystem,distributionSystem,supplyZone,pressureArea,'monthlyTestingHydrantSystems' as metricTypeName,(deviceCount*metricvaluenumber)/1000000 as metricvaluenumber from HydrantSystemsbase join sharepointConfig_df where TFSU = 'Y' and metrictypename = 'monthlyTestingHydrantSystemsFactor'
),
annualTestingSprinklerSystems as (
  with annualTestingResidential as (
    select deliverySystem,distributionSystem,supplyZone,pressureArea,count (Distinct deviceID) as deviceCount
    from tfsu_df where superiorPropertyType IN ('COMMERCIAL','COMMUNITY SERVS','MASTER STRATA','OCCUPIED LAND','UTILITIES') 
    group by deliverySystem,distributionSystem,supplyZone,pressureArea
  ),
  annualTestingIndustrial as (
    select deliverySystem,distributionSystem,supplyZone,pressureArea,count (Distinct deviceID) as deviceCount
    from tfsu_df where superiorPropertyType IN ('INDUSTRIAL') 
    group by deliverySystem,distributionSystem,supplyZone,pressureArea
  ),
  annualTestingConsolidated as ( 
  select deliverySystem,distributionSystem,supplyZone,pressureArea,(deviceCount*metricvaluenumber)/(12*1000000) as metricvaluenumber from annualTestingResidential join sharepointConfig_df where TFSU = 'Y' and metrictypename = 'annualTestingSprinklerSystemsResidentialAndCommercialFactor'
  union
  select deliverySystem,distributionSystem,supplyZone,pressureArea,(deviceCount*metricvaluenumber)/(12*1000000) as metricvaluenumber from annualTestingIndustrial join sharepointConfig_df where TFSU = 'Y' and metrictypename = 'annualTestingSprinklerSystemsLargeIndustrialFactor'
  ) 
  select deliverySystem,distributionSystem,supplyZone,pressureArea,'annualTestingSprinklerSystems' as metrictypename,sum(metricvaluenumber) as metricvaluenumber from annualTestingConsolidated cons group by all
),
annualTestingHydrantSystems as (
  with annualTestingHydrantResidential as (
    select deliverySystem,distributionSystem,supplyZone,pressureArea,count (Distinct deviceID) as deviceCount
    from tfsu_df where superiorPropertyType IN ('COMMERCIAL','COMMUNITY SERVS','MASTER STRATA','OCCUPIED LAND','UTILITIES') 
    group by deliverySystem,distributionSystem,supplyZone,pressureArea
  ),
  annualTestingHydrantIndustrial as (
    select deliverySystem,distributionSystem,supplyZone,pressureArea,count (Distinct deviceID) as deviceCount
    from tfsu_df where superiorPropertyType IN ('INDUSTRIAL') 
    group by deliverySystem,distributionSystem,supplyZone,pressureArea
  ),
  annualTestingHydrantConsolidated as ( 
  select deliverySystem,distributionSystem,supplyZone,pressureArea,(deviceCount*metricvaluenumber)/(12*1000000) as metricvaluenumber from annualTestingHydrantResidential join sharepointConfig_df where TFSU = 'Y' and metrictypename = 'annualTestingHydrantSystemsResidentialAndCommercialFactor'
  union
  select deliverySystem,distributionSystem,supplyZone,pressureArea,(deviceCount*metricvaluenumber)/(12*1000000) as metricvaluenumber from annualTestingHydrantIndustrial join sharepointConfig_df where TFSU = 'Y' and metrictypename = 'annualTestingHydrantSystemsLargeIndustrialFactor'
  ) 
  select deliverySystem,distributionSystem,supplyZone,pressureArea,'annualTestingHydrantSystems' as metrictypename,sum(metricvaluenumber) as metricvaluenumber from annualTestingHydrantConsolidated group by all
),
testingofFireServicesUse as (
  with tfsbase as (
    select deliverySystem,distributionSystem,supplyZone,pressureArea,'Pressure Area' networkTypeCode,metricTypeName,metricValueNumber from monthlyTestingFireSprinklerSystems
    union
    select deliverySystem,distributionSystem,supplyZone,pressureArea,'Pressure Area' networkTypeCode,metricTypeName,metricValueNumber from monthlyTestingHydrantSystems
    union
    select deliverySystem,distributionSystem,supplyZone,pressureArea,'Pressure Area' networkTypeCode,metricTypeName,metricValueNumber from annualTestingSprinklerSystems
    union
    select deliverySystem,distributionSystem,supplyZone,pressureArea,'Pressure Area' networkTypeCode,metricTypeName,metricValueNumber from annualTestingHydrantSystems
  ),
  tfspressurearea as (
    select tfsbase.*, d.yearnumber, d.monthnumber, d.monthname, d.calculationDate from tfsbase inner join date_df d
  ) select * from tfspressurearea where pressureArea <> 'Unknown'
    union
    select deliverySystem,distributionSystem,supplyZone,NULL pressureArea,'Supply Zone' networkTypeCode,metricTypeName,sum(metricValueNumber) as metricValueNumber,yearnumber,monthnumber,monthname,calculationDate from tfspressurearea where supplyZone <> 'Unknown' group by all
    union
    (with deliverybase as 
        (select deliverySystem,NULL distributionSystem,NULL supplyZone,NULL pressureArea,'Delivery System' networkTypeCode,metricTypeName,sum(metricValueNumber) as metricValueNumber,
        yearnumber,monthnumber,monthname,calculationDate from tfspressurearea group by all)
        select case when networkTypeCode = 'Delivery System' and deliverySystem in ('DEL_PROSPECT_EAST','DEL_POTTS_HILL','Unknown') then 'DEL_POTTS_HILL + DEL_PROSPECT_EAST' 
                    when networkTypeCode = 'Delivery System' and deliverySystem in ('DEL_CASCADE','DEL_ORCHARD_HILLS') then 'DEL_CASCADE + DEL_ORCHARD_HILLS'
               else deliverySystem end deliverySystem,distributionSystem,supplyZone,pressureArea,
               case when networkTypeCode = 'Delivery System' and deliverySystem in ('DEL_PROSPECT_EAST','DEL_POTTS_HILL','Unknown') then 'Delivery System Combined' 
                    when networkTypeCode = 'Delivery System' and deliverySystem in ('DEL_CASCADE','DEL_ORCHARD_HILLS') then 'Delivery System Combined'
               else networkTypeCode end networkTypeCode,metricTypeName,sum(metricValueNumber) as metricValueNumber,yearNumber,monthNumber,monthName,calculationDate  
        from  deliverybase group by all
    ) 
),
unauthorisedUse as ( 
  select deliverySystem,distributionSystem,supplyZone,pressureArea,networkTypeCode,'unauthorisedUse' as metricTypeName,demandQuantity*config.metricValueNumber as metricValueNumber,config.yearnumber,config.monthnumber,config.monthname,config.calculationDate from waternetworkdemand_df demand join sharepointConfig_df config where config.metrictypename = 'UnauthorisedUse' and config.yearnumber = demand.yearnumber and config.monthnumber = demand.monthnumber
),
meterUnderRegistration as (
  with meterbase as (
    select deliverySystem,distributionSystem,supplyZone,pressureArea,'Pressure Area' networkTypeCode,'meterUnderRegistration' as metricTypeName,(sum(supply_totalKLQuantity)/1000) as totalMLQuantity,supply.yearnumber,supply.monthnumber from monthlySupplyApportioned_df supply 
    inner join waternetwork_df network on supply.supply_waterNetworkSK = network.waterNetworkSK 
    group by all
  ),
  meterUnderRegistrationPressureArea as (
    select deliverySystem,distributionSystem,supplyZone,pressureArea,networkTypeCode,meterbase.metricTypeName,totalMLQuantity*config.metricValueNumber as metricValueNumber,config.yearnumber,config.monthnumber,config.monthname,config.calculationDate from meterbase 
    inner join sharepointConfig_df config where config.metrictypename = 'MeterUnderRegistration' and config.yearnumber = meterbase.yearnumber and config.monthnumber = meterbase.monthnumber
  ) select * from meterUnderRegistrationPressureArea where pressureArea <> 'Unknown'
    union
    select deliverySystem,distributionSystem,supplyZone,NULL pressureArea,'Supply Zone' networkTypeCode,metricTypeName,sum(metricValueNumber) as metricValueNumber,yearnumber,monthnumber,monthname,calculationDate from meterUnderRegistrationPressureArea where supplyZone <> 'Unknown' group by all
    union
    (with deliverybase as 
        (select deliverySystem,NULL distributionSystem,NULL supplyZone,NULL pressureArea,'Delivery System' networkTypeCode,metricTypeName,sum(metricValueNumber) as metricValueNumber,
        yearnumber,monthnumber,monthname,calculationDate from meterUnderRegistrationPressureArea group by all)
        select case when networkTypeCode = 'Delivery System' and deliverySystem in ('DEL_PROSPECT_EAST','DEL_POTTS_HILL','Unknown') then 'DEL_POTTS_HILL + DEL_PROSPECT_EAST' 
                    when networkTypeCode = 'Delivery System' and deliverySystem in ('DEL_CASCADE','DEL_ORCHARD_HILLS') then 'DEL_CASCADE + DEL_ORCHARD_HILLS'
               else deliverySystem end deliverySystem,distributionSystem,supplyZone,pressureArea,
               case when networkTypeCode = 'Delivery System' and deliverySystem in ('DEL_PROSPECT_EAST','DEL_POTTS_HILL','Unknown') then 'Delivery System Combined' 
                    when networkTypeCode = 'Delivery System' and deliverySystem in ('DEL_CASCADE','DEL_ORCHARD_HILLS') then 'Delivery System Combined'
               else networkTypeCode end networkTypeCode,metricTypeName,sum(metricValueNumber) as metricValueNumber,yearNumber,monthNumber,monthName,calculationDate  
        from  deliverybase group by all
    )
),
unmeteredSTPs as (
  with unmeteredSTPsPressureArea as (
    select deliverySystem,distributionSystem,supplyZone,pressureArea,'Pressure Area' networkTypeCode,'unmeteredSTPs' as metricTypeName,sum(config.metricValueNumber) as metricValueNumber,config.yearnumber,config.monthnumber,config.monthname,config.calculationDate from waternetwork_df network inner join sharepointConfig_df config on config.zoneName = network.pressureArea and config.metricValueNumber is not null and config.metricTypeName = 'UnmeteredSTPs' and config.zoneTypeName = 'PressureZone' and network._RecordCurrent = 1 group by all
  ) select * from unmeteredSTPsPressureArea  where pressureArea <> 'Unknown'
    union
    select deliverySystem,distributionSystem,supplyZone,NULL pressureArea,'Supply Zone' networkTypeCode,metricTypeName,sum(metricValueNumber) as metricValueNumber,yearnumber,monthnumber,monthname,calculationDate from unmeteredSTPsPressureArea where supplyZone <> 'Unknown' group by all
    union
    (with deliverybase as 
        (select deliverySystem,NULL distributionSystem,NULL supplyZone,NULL pressureArea,'Delivery System' networkTypeCode,metricTypeName,sum(metricValueNumber) as metricValueNumber,
        yearnumber,monthnumber,monthname,calculationDate from unmeteredSTPsPressureArea group by all)
        select case when networkTypeCode = 'Delivery System' and deliverySystem in ('DEL_PROSPECT_EAST','DEL_POTTS_HILL','Unknown') then 'DEL_POTTS_HILL + DEL_PROSPECT_EAST' 
                    when networkTypeCode = 'Delivery System' and deliverySystem in ('DEL_CASCADE','DEL_ORCHARD_HILLS') then 'DEL_CASCADE + DEL_ORCHARD_HILLS'
               else deliverySystem end deliverySystem,distributionSystem,supplyZone,pressureArea,
               case when networkTypeCode = 'Delivery System' and deliverySystem in ('DEL_PROSPECT_EAST','DEL_POTTS_HILL','Unknown') then 'Delivery System Combined' 
                    when networkTypeCode = 'Delivery System' and deliverySystem in ('DEL_CASCADE','DEL_ORCHARD_HILLS') then 'Delivery System Combined'
               else networkTypeCode end networkTypeCode,metricTypeName,sum(metricValueNumber) as metricValueNumber,yearNumber,monthNumber,monthName,calculationDate  
        from  deliverybase group by all
    )
),
unmeteredNonSTPs as ( 
  with totalSystemInputCurrMonth as (
    select sum(demandQuantity) as demandQuantity,yearnumber,monthnumber from waternetworkdemand_df where networkTypeCode like 'Delivery System%' group by yearnumber,monthnumber
  )
  select deliverySystem,distributionSystem,supplyZone,pressureArea,networkTypeCode,'unmeteredNonSTPs' as metricTypeName,(demand.demandQuantity*config.metricValueNumber)/monthlyTotal.demandQuantity as metricValueNumber,config.yearnumber,config.monthnumber,config.monthname,config.calculationDate from waternetworkdemand_df demand 
  inner join sharepointConfig_df config on config.metrictypename = 'SWUnmeteredNonSTPS' and config.yearnumber = demand.yearnumber and config.monthnumber = demand.monthnumber
  inner join totalSystemInputCurrMonth monthlyTotal on monthlyTotal.yearnumber = demand.yearnumber and monthlyTotal.monthnumber = demand.monthnumber
),
fireRescueNSWUse as ( 
  with totalSystemInputCurrMonth as (
    select sum(demandQuantity) as demandQuantity,yearnumber,monthnumber from waternetworkdemand_df where networkTypeCode like 'Delivery System%' group by yearnumber,monthnumber
  )
  select deliverySystem,distributionSystem,supplyZone,pressureArea,networkTypeCode,'fireRescueNSWUse' as metricTypeName,(demand.demandQuantity*config.metricValueNumber)/monthlyTotal.demandQuantity as metricValueNumber,config.yearnumber,config.monthnumber,config.monthname,config.calculationDate from waternetworkdemand_df demand 
  inner join sharepointConfig_df config on config.metrictypename = 'FireRescueNSWUse' and config.yearnumber = demand.yearnumber and config.monthnumber = demand.monthnumber
  inner join totalSystemInputCurrMonth monthlyTotal on monthlyTotal.yearnumber = demand.yearnumber and monthlyTotal.monthnumber = demand.monthnumber
),
sydneyWaterOperationalUse as (
  with swopsbase as (
    select lower(regexp_replace(regexp_replace(demand.deliverySystem,'DEL_',""),'[+ _]',"")) deliverySystemFormatted, demand.* from waternetworkdemand_df demand 
  ),
  deliverySystemInputCurrMonth as (
    select deliverySystem,sum(demandQuantity) as demandQuantity,lower(regexp_replace(regexp_replace(deliverySystem,'DEL_',""),'[+ _]',"")) deliverySystemFormatted,yearnumber,monthnumber from waternetworkdemand_df where networkTypeCode like 'Delivery System%' group by deliverySystem,lower(regexp_replace(regexp_replace(deliverySystem,'DEL_',""),'[+ _]',"")),yearnumber,monthnumber
  ) 
  select base.deliverySystem,distributionSystem,supplyZone,pressureArea,networkTypeCode,'sydneyWaterOperationalUse' as metricTypeName,(base.demandQuantity*config.metricValueNumber)/systeminput.demandQuantity as metricValueNumber,config.yearnumber,config.monthnumber,config.monthname,config.calculationDate from swopsbase base 
  inner join deliverySystemInputCurrMonth systeminput on base.deliverySystemFormatted = systeminput.deliverySystemFormatted and base.yearnumber = systeminput.yearnumber and base.monthnumber = systeminput.monthnumber
  inner join sharepointConfig_df config on config.metrictypename = 'SWOperationalUse' and config.yearnumber = base.yearnumber and config.monthnumber = base.monthnumber and config.metricValueNumber is not null and config.zoneTypeName = 'DeliverySystem' and base.deliverySystemFormatted = lower(config.zoneName)
),
ruralFireServicesUse as (
  with rfsbase as (
    select yearnumber, monthnumber, supply_waternetworksk, sum(supply_totalKLQuantity)/1000 as totalMLQuantity from monthlySupplyApportioned_df where LGA in ('Blacktown','Blue Mountains','Camden','Campbelltown','Fairfield','Hawkesbury','Hornsby','Liverpool','Penrith','Shellharbour','The Hills Shire','Wingecarribee','Wollondilly','Wollongong') group by yearnumber, monthnumber, supply_waternetworksk
  ),
  combinedLGA as (
    select yearnumber, monthnumber, sum(supply_totalKLQuantity)/1000 as LGAtotalMLQuantity from monthlySupplyApportioned_df where LGA in ('Blacktown','Blue Mountains','Camden','Campbelltown','Fairfield','Hawkesbury','Hornsby','Liverpool','Penrith','Shellharbour','The Hills Shire','Wingecarribee','Wollondilly','Wollongong') group by yearnumber, monthnumber
  ),
  rfsbasepressurearea as (
    select deliverySystem,distributionSystem,supplyZone,pressureArea,'Pressure Area' networkTypeCode,'ruralFireServicesUse' as metricTypeName,(rfsbase.totalMLQuantity*config.metricValueNumber)/combinedLGA.LGAtotalMLQuantity as metricValueNumber,config.yearnumber,config.monthnumber,config.monthname,config.calculationDate from rfsbase 
    inner join waternetwork_df network on rfsbase.supply_waternetworksk = network.waternetworksk
    inner join combinedLGA on combinedLGA.yearnumber = rfsbase.yearnumber and combinedLGA.monthnumber = rfsbase.monthnumber
    inner join sharepointConfig_df config on config.metrictypename = 'RFSUse' and config.yearnumber = rfsbase.yearnumber and config.monthnumber = rfsbase.monthnumber
  )
    select * from rfsbasepressurearea  where pressureArea <> 'Unknown'
    union
    select deliverySystem,distributionSystem,supplyZone,NULL pressureArea,'Supply Zone' networkTypeCode,metricTypeName,sum(metricValueNumber) as metricValueNumber,yearnumber,monthnumber,monthname,calculationDate from rfsbasepressurearea where supplyZone <> 'Unknown' group by all
    union
    (with deliverybase as 
        (select deliverySystem,NULL distributionSystem,NULL supplyZone,NULL pressureArea,'Delivery System' networkTypeCode,metricTypeName,sum(metricValueNumber) as metricValueNumber,
        yearnumber,monthnumber,monthname,calculationDate from rfsbasepressurearea group by all)
        select case when networkTypeCode = 'Delivery System' and deliverySystem in ('DEL_PROSPECT_EAST','DEL_POTTS_HILL','Unknown') then 'DEL_POTTS_HILL + DEL_PROSPECT_EAST' 
                    when networkTypeCode = 'Delivery System' and deliverySystem in ('DEL_CASCADE','DEL_ORCHARD_HILLS') then 'DEL_CASCADE + DEL_ORCHARD_HILLS'
               else deliverySystem end deliverySystem,distributionSystem,supplyZone,pressureArea,
               case when networkTypeCode = 'Delivery System' and deliverySystem in ('DEL_PROSPECT_EAST','DEL_POTTS_HILL','Unknown') then 'Delivery System Combined' 
                    when networkTypeCode = 'Delivery System' and deliverySystem in ('DEL_CASCADE','DEL_ORCHARD_HILLS') then 'Delivery System Combined'
               else networkTypeCode end networkTypeCode,metricTypeName,sum(metricValueNumber) as metricValueNumber,yearNumber,monthNumber,monthName,calculationDate  
        from  deliverybase group by all
    )  
)
select * from testingofFireServicesUse
union
select * from unauthorisedUse
union
select * from meterUnderRegistration
union
select * from unmeteredSTPs
union
select * from unmeteredNonSTPs
union
select * from fireRescueNSWUse
union
select * from sydneyWaterOperationalUse
union
select * from ruralFireServicesUse
"""
)

# COMMAND ----------

def Transform():
    global df

    # ------------- TABLES ----------------- # 

    # ------------- JOINS ------------------ #

    # ------------- TRANSFORMS ------------- #
    _.Transforms = [
        f"metricTypeName||'|'||coalesce(deliverySystem,'')||'|'||coalesce(distributionSystem,'')||'|'||coalesce(supplyZone,'')||'|'||coalesce(pressureArea,'')||'|'||yearNumber||'|'||monthName||'|'||calculationDate {BK}"
        ,"deliverySystem deliverySystem"        
        ,"distributionSystem distributionSystem"
        ,"supplyZone supplyZone"        
        ,"pressureArea pressureArea"
        ,"networkTypeCode networkTypeCode"
        ,"cast(yearNumber as int) yearNumber"
        ,"cast (monthNumber as int) monthNumber"
        ,"monthName monthName"
        ,"calculationDate calculationDate"
        ,"metricTypeName metricTypeName"
        ,"metricValueNumber metricValueNumber"
    ]
    df = df.selectExpr(
        _.Transforms
    )

    # ------------- SAVE ------------------- #
    # display(df)
    # CleanSelf()
    Save(df)
    #DisplaySelf()
Transform()
