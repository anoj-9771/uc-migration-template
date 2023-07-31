# Databricks notebook source
# MAGIC %run ../../Common/common-transform 

# COMMAND ----------

# MAGIC %run ../../Common/common-helpers 

# COMMAND ----------

DEFAULT_TARGET = 'curated'

# COMMAND ----------

def CalculateDemandAggregated(property_df,installation_df,device_df,sharepointConfig_df,waternetworkdemand_df,waternetwork_df,monthlySupplyApportioned_df,yearnumber,monthName,monthnumber):
    
    tfsumetrics_df = sharepointConfig_df.filter("metricTypeName like '%TFSU%'")

    # ------------- JOINS ------------------ #
    tfsu_df = property_df.join(installation_df, (property_df.propertyNumber==installation_df.install_propertyNumber) & ~col("superiorPropertyTypeCode").isin('902'),"inner") \
        .join(device_df, (installation_df.installationNumber == device_df.device_installationNumber),"inner")

    # --- Multiplication Factors ---#
    monthlyTestingFireSprinklerSystemsFactor = tfsumetrics_df.filter("metricTypeName='TFSUMonthlyTestingOfFireSprinklerSystemsTestDuration'").select("metricValueNumber").first()['metricValueNumber'] * tfsumetrics_df.filter("metricTypeName='TFSUMonthlyTestingOfFireSprinklerSystemsTestFlowRate'").select("metricValueNumber").first()['metricValueNumber']

    annualTestingSprinklerSystemsResidentialAndCommercialFactor = tfsumetrics_df.filter("metricTypeName='TFSUAnnualTestingOfSprinklerSystemResidentialAndCommercialFireServicesTestDuration'").select("metricValueNumber").first()['metricValueNumber'] * tfsumetrics_df.filter("metricTypeName='TFSUAnnualTestingOfSprinklerSystemResidentialAndCommercialFireServicesTestFlowRate'").select("metricValueNumber").first()['metricValueNumber']

    annualTestingSprinklerSystemsLargeIndustrialFactor = tfsumetrics_df.filter("metricTypeName='TFSUAnnualTestingOfSprinklerSystemLargeIndustrialServicesTestDuration'").select("metricValueNumber").first()['metricValueNumber'] * tfsumetrics_df.filter("metricTypeName='TFSUAnnualTestingOfSprinklerSystemLargeIndustrialServicesTestFlowRate'").select("metricValueNumber").first()['metricValueNumber']

    monthlyTestingHydrantSystemsFactor = tfsumetrics_df.filter("metricTypeName='TFSUMonthlyTestingOfHydrantSystemsTestDuration'").select("metricValueNumber").first()['metricValueNumber'] * tfsumetrics_df.filter("metricTypeName='TFSUMonthlyTestingOfHydrantSystemsTestFlowRate'").select("metricValueNumber").first()['metricValueNumber']

    annualTestingHydrantSystemsResidentialAndCommercialFactor = tfsumetrics_df.filter("metricTypeName='TFSUAnnualTestingOfHydrantSystemsResidentialAndCommercialFireServicesTestDuration'").select("metricValueNumber").first()['metricValueNumber'] * tfsumetrics_df.filter("metricTypeName='TFSUAnnualTestingOfHydrantSystemsResidentialAndCommercialFireServicesTestFlowRate'").select("metricValueNumber").first()['metricValueNumber']

    annualTestingHydrantSystemsLargeIndustrialFactor = tfsumetrics_df.filter("metricTypeName='TFSUAnnualTestingOfHydrantSystemsLargeIndustrialServicesTestDuration'").select("metricValueNumber").first()['metricValueNumber'] * tfsumetrics_df.filter("metricTypeName='TFSUAnnualTestingOfHydrantSystemsLargeIndustrialServicesTestFlowRate'").select("metricValueNumber").first()['metricValueNumber']

    unauthorisedUseFactor = sharepointConfig_df.filter("metricTypeName='UnauthorisedUse'").select("metricValueNumber").first()['metricValueNumber']
    meterUnderRegistrationFactor = sharepointConfig_df.filter("metricTypeName='MeterUnderRegistration'").select("metricValueNumber").first()['metricValueNumber']

    unmeteredNonSTPsFactor = sharepointConfig_df.filter("metricTypeName = 'SWUnmeteredNonSTPS'").select("metricValueNumber").first()['metricValueNumber']
    totalSystemInputCurrMonth = waternetworkdemand_df.filter("networkTypeCode like 'Delivery System%'").agg(sum('demandQuantity').alias('demandQuantity')).select("demandQuantity").first()['demandQuantity']

    fireRescueNSWUseFactor = sharepointConfig_df.filter("metricTypeName = 'FireRescueNSWUse'").select("metricValueNumber").first()['metricValueNumber']    

    deliverySystemInputCurrMonth = waternetworkdemand_df.filter("networkTypeCode like 'Delivery System%'").groupBy("deliverySystem").agg(sum('demandQuantity').alias('demandQuantity')) \
        .withColumn("deliverySystemFormatted",lower(regexp_replace(regexp_replace(waternetworkdemand_df.deliverySystem,'DEL_',""),'[+ _]',""))) \
        .select(col("deliverySystem").alias("delivery_deliverySystem"),"deliverySystemFormatted",col("demandQuantity").alias("delivery_demandQuantity"))

    monthlySupplyLGA_df = monthlySupplyApportioned_df.filter("LGA in ('Blacktown','Blue Mountains','Camden','Campbelltown','Fairfield','Hawkesbury','Hornsby','Liverpool','Penrith','Shellharbour','The Hills Shire','Wingecarribee','Wollondilly','Wollongong')")    
    monthlySupplyLGA = monthlySupplyLGA_df.agg((sum('supply_totalKLQuantity')/1000).alias('supply_totalMLQuantity')).first()['supply_totalMLQuantity']
    ruralFireServicesUseFactor = sharepointConfig_df.filter("metricTypeName = 'RFSUse'").select("metricValueNumber").first()['metricValueNumber']

    # --- TFSU Metrics Calculation ---#
    #monthlyTestingFireSprinklerSystems
    monthlyTestingFireSprinklerSystems = tfsu_df.where(col('superiorPropertyType').isin('COMMERCIAL','COMMUNITY SERVS','INDUSTRIAL','MASTER STRATA','OCCUPIED LAND','UTILITIES')) \
        .groupBy("waterNetworkDeliverySystem","waterNetworkDistributionSystem","waterNetworkSupplyZone","waterNetworkPressureArea") \
        .agg(countDistinct('deviceID').alias("deviceCount")) \
        .withColumn("metricTypeName", lit('monthlyTestingFireSprinklerSystems')) 
    monthlyTestingFireSprinklerSystems = monthlyTestingFireSprinklerSystems.withColumn("metricValueNumber",(monthlyTestingFireSprinklerSystems.deviceCount * monthlyTestingFireSprinklerSystemsFactor)/1000000) \
        .select("waterNetworkDeliverySystem","waterNetworkDistributionSystem","waterNetworkSupplyZone","waterNetworkPressureArea","metricTypeName","metricValueNumber")  

    #annualTestingSprinklerSystems
    annualTestingSprinklerSystems = tfsu_df.where(col('superiorPropertyType').isin('COMMERCIAL','COMMUNITY SERVS','MASTER STRATA','OCCUPIED LAND','UTILITIES')) \
        .groupBy("waterNetworkDeliverySystem","waterNetworkDistributionSystem","waterNetworkSupplyZone","waterNetworkPressureArea").agg(countDistinct('deviceID').alias("deviceCount")) 
    annualTestingSprinklerSystems = annualTestingSprinklerSystems.withColumn("metricValueNumber", (annualTestingSprinklerSystems.deviceCount * annualTestingSprinklerSystemsResidentialAndCommercialFactor)/(12*1000000))  
    annualTestingSprinklerSystemsLargeIndustrial = tfsu_df.where(col('superiorPropertyType').isin('INDUSTRIAL')).groupBy("waterNetworkDeliverySystem","waterNetworkDistributionSystem","waterNetworkSupplyZone","waterNetworkPressureArea").agg(countDistinct('deviceID').alias("deviceCount")) 
    annualTestingSprinklerSystemsLargeIndustrial = annualTestingSprinklerSystemsLargeIndustrial.withColumn("metricValueNumber", (annualTestingSprinklerSystemsLargeIndustrial.deviceCount * annualTestingSprinklerSystemsLargeIndustrialFactor)/(12*1000000))  
    annualTestingSprinklerSystems = annualTestingSprinklerSystems.unionAll(annualTestingSprinklerSystemsLargeIndustrial).groupBy("waterNetworkDeliverySystem","waterNetworkDistributionSystem","waterNetworkSupplyZone","waterNetworkPressureArea").agg(sum('metricValueNumber').alias("metricValueNumber")) \
        .withColumn("metricTypeName", lit('annualTestingSprinklerSystems')) \
        .select("waterNetworkDeliverySystem","waterNetworkDistributionSystem","waterNetworkSupplyZone","waterNetworkPressureArea","metricTypeName","metricValueNumber")

    #monthlyTestingHydrantSystems
    monthlyTestingHydrantSystems = tfsu_df.where(col('superiorPropertyType').isin('COMMERCIAL','COMMUNITY SERVS','MASTER STRATA','OCCUPIED LAND','UTILITIES')) \
        .groupBy("waterNetworkDeliverySystem","waterNetworkDistributionSystem","waterNetworkSupplyZone","waterNetworkPressureArea") \
        .agg(countDistinct('deviceID').alias("deviceCount")) \
        .withColumn("metricTypeName", lit('monthlyTestingHydrantSystems')) 
    monthlyTestingHydrantSystems = monthlyTestingHydrantSystems.withColumn("metricValueNumber",(monthlyTestingHydrantSystems.deviceCount * monthlyTestingHydrantSystemsFactor)/1000000) \
        .select("waterNetworkDeliverySystem","waterNetworkDistributionSystem","waterNetworkSupplyZone","waterNetworkPressureArea","metricTypeName","metricValueNumber")   

    
    #annualTestingHydrantSystems
    annualTestingHydrantSystems = tfsu_df.where(col('superiorPropertyType').isin('COMMERCIAL','COMMUNITY SERVS','MASTER STRATA','OCCUPIED LAND','UTILITIES')) \
        .groupBy("waterNetworkDeliverySystem","waterNetworkDistributionSystem","waterNetworkSupplyZone","waterNetworkPressureArea").agg(countDistinct('deviceID').alias("deviceCount")) 
    annualTestingHydrantSystems = annualTestingHydrantSystems.withColumn("metricValueNumber", (annualTestingHydrantSystems.deviceCount * annualTestingHydrantSystemsResidentialAndCommercialFactor)/(12*1000000))  
    annualTestingHydrantSystemsLargeIndustrial = tfsu_df.where(col('superiorPropertyType').isin('INDUSTRIAL')).groupBy("waterNetworkDeliverySystem","waterNetworkDistributionSystem","waterNetworkSupplyZone","waterNetworkPressureArea").agg(countDistinct('deviceID').alias("deviceCount")) 
    annualTestingHydrantSystemsLargeIndustrial = annualTestingHydrantSystemsLargeIndustrial.withColumn("metricValueNumber", (annualTestingHydrantSystemsLargeIndustrial.deviceCount * annualTestingHydrantSystemsLargeIndustrialFactor)/(12*1000000))  
    annualTestingHydrantSystems = annualTestingHydrantSystems.unionAll(annualTestingHydrantSystemsLargeIndustrial).groupBy("waterNetworkDeliverySystem","waterNetworkDistributionSystem","waterNetworkSupplyZone","waterNetworkPressureArea").agg(sum('metricValueNumber').alias("metricValueNumber")) \
        .withColumn("metricTypeName", lit('annualTestingHydrantSystems')) \
        .select("waterNetworkDeliverySystem","waterNetworkDistributionSystem","waterNetworkSupplyZone","waterNetworkPressureArea","metricTypeName","metricValueNumber")

    #TestingofFireServicesUse
    testingofFireServicesUse = monthlyTestingFireSprinklerSystems \
        .unionByName(annualTestingSprinklerSystems,allowMissingColumns=True) \
        .unionByName(monthlyTestingHydrantSystems,allowMissingColumns=True) \
        .unionByName(annualTestingHydrantSystems,allowMissingColumns=True)
    testingofFireServicesUse = testingofFireServicesUse.withColumn("reporting_deliverySystem", when(testingofFireServicesUse.waterNetworkDeliverySystem == "DEL_PROSPECT_EAST","DEL_POTTS_HILL + DEL_PROSPECT_EAST").when(testingofFireServicesUse.waterNetworkDeliverySystem == "DEL_POTTS_HILL","DEL_POTTS_HILL + DEL_PROSPECT_EAST").when(testingofFireServicesUse.waterNetworkDeliverySystem == "DEL_CASCADE","DEL_CASCADE + DEL_ORCHARD_HILLS").when(testingofFireServicesUse.waterNetworkDeliverySystem == "DEL_ORCHARD_HILLS","DEL_CASCADE + DEL_ORCHARD_HILLS").otherwise(testingofFireServicesUse.waterNetworkDeliverySystem)) \
        .withColumn("networkTypeCode",lit('Pressure Area')) \
        .select(col("reporting_deliverySystem").alias("waterNetworkDeliverySystem"),"waterNetworkDistributionSystem","waterNetworkSupplyZone","waterNetworkPressureArea","networkTypeCode","metricTypeName","metricValueNumber")

    testingofFireServicesUseSupplyZone = testingofFireServicesUse.groupBy("waterNetworkDeliverySystem","waterNetworkDistributionSystem","waterNetworkSupplyZone","metricTypeName").agg(sum('metricValueNumber').alias('metricValueNumber')) \
        .withColumn("networkTypeCode",lit('Supply Zone')) 

    testingofFireServicesUseDeliverySystem = testingofFireServicesUse.groupBy("waterNetworkDeliverySystem","metricTypeName").agg(sum('metricValueNumber').alias('metricValueNumber')) \
        .withColumn("networkTypeCode",lit('Delivery System'))     
    
    #UnauthorisedUse
    unauthorisedUse = waternetworkdemand_df.withColumn("metricValueNumber",(waternetworkdemand_df.demandQuantity*unauthorisedUseFactor)) \
        .withColumn("metricTypeName", lit('unauthorisedUse')) \
        .select(col("deliverySystem").alias("waterNetworkDeliverySystem"),col("distributionSystem").alias("waterNetworkDistributionSystem"),col("supplyZone").alias("waterNetworkSupplyZone"),col("pressureArea").alias("waterNetworkPressureArea"),"networkTypeCode","metricTypeName","metricValueNumber")

    # #meterUnderRegistration
    # meterUnderRegistration = waternetworkdemand_df.withColumn("metricValueNumber",(waternetworkdemand_df.demandQuantity*meterUnderRegistrationFactor)) \
    #     .withColumn("metricTypeName", lit('meterUnderRegistration')) \
    #     .select(col("deliverySystem").alias("waterNetworkDeliverySystem"),col("distributionSystem").alias("waterNetworkDistributionSystem"),col("supplyZone").alias("waterNetworkSupplyZone"),col("pressureArea").alias("waterNetworkPressureArea"),"networkTypeCode","metricTypeName","metricValueNumber")

    #meterUnderRegistration
    meterUnderRegistration = monthlySupplyApportioned_df.join(waternetwork_df,(monthlySupplyApportioned_df.supply_waterNetworkSK == waternetwork_df.waterNetworkSK),"inner") \
        .groupBy("waternetwork_deliverySystem","waternetwork_distributionSystem","waternetwork_supplyZone","waternetwork_pressureArea") \
        .agg((sum('supply_totalKLQuantity')/1000).alias("supplyQuantity"))
    meterUnderRegistration = meterUnderRegistration.withColumn("metricTypeName", lit('meterUnderRegistration')) \
        .withColumn("networktypecode", lit('Pressure Area')) \
        .withColumn("metricValueNumber",(meterUnderRegistration.supplyQuantity*meterUnderRegistrationFactor)) \
        .select(col("waternetwork_deliverySystem").alias("waterNetworkDeliverySystem"),col("waternetwork_distributionSystem").alias("waterNetworkDistributionSystem"),col("waternetwork_supplyZone").alias("waterNetworkSupplyZone"),col("waternetwork_pressureArea").alias("waterNetworkPressureArea"),"networkTypeCode","metricTypeName","metricValueNumber")

    meterUnderRegistrationSupplyZone = meterUnderRegistration.groupBy("waterNetworkDeliverySystem","waterNetworkDistributionSystem","waterNetworkSupplyZone","metricTypeName").agg(sum('metricValueNumber').alias('metricValueNumber')) \
        .withColumn("networkTypeCode",lit('Supply Zone')) 

    meterUnderRegistrationDeliverySystem = meterUnderRegistration.groupBy("waterNetworkDeliverySystem","metricTypeName").agg(sum('metricValueNumber').alias('metricValueNumber')) \
        .withColumn("networkTypeCode",lit('Delivery System'))         


    #UnmeteredSTPs    
    unmeteredSTPs = sharepointConfig_df.where(col("metricValueNumber").isNotNull() & (col("metricTypeName") == 'UnmeteredSTPs') & (col("zoneTypeName") == 'PressureZone')) \
        .withColumn("metricTypeName",lit('unmeteredSTPs')) \
        .withColumn("metricValueNumber",lit(sharepointConfig_df.metricValueNumber)) \
        .withColumn("networkTypeCode",lit('Pressure Area')) 
    unmeteredSTPs = unmeteredSTPs.join(waternetwork_df,(unmeteredSTPs.zoneName == waternetwork_df.waternetwork_pressureArea),"inner") 

    unmeteredSTPsSupplyZone = unmeteredSTPs.groupBy("waternetwork_supplyZone").agg(sum('metricValueNumber').alias('metricValueNumber')) \
        .withColumn("metricTypeName",lit('unmeteredSTPs')) \
        .withColumn("networkTypeCode",lit('Supply Zone')) 

    # unmeteredSTPsDistributionSystem = unmeteredSTPs.groupBy("waternetwork_distributionSystem").agg(sum('metricValueNumber').alias('metricValueNumber')) \
    #     .withColumn("metricTypeName",lit('unmeteredSTPs')) \
    #     .withColumn("networkTypeCode",lit('Distribution System')) 

    unmeteredSTPsDeliverySystem = unmeteredSTPs.groupBy("waternetwork_deliverySystem").agg(sum('metricValueNumber').alias('metricValueNumber')) \
        .withColumn("metricTypeName",lit('unmeteredSTPs')) \
        .withColumn("networkTypeCode",lit('Delivery System'))         

    unmeteredSTPs = unmeteredSTPs.unionByName(unmeteredSTPsSupplyZone,allowMissingColumns=True) \
        .unionByName(unmeteredSTPsDeliverySystem,allowMissingColumns=True)
    unmeteredSTPs = unmeteredSTPs.withColumn("reporting_deliverySystem", when(unmeteredSTPs.waternetwork_deliverySystem == "DEL_PROSPECT_EAST","DEL_POTTS_HILL + DEL_PROSPECT_EAST").when(unmeteredSTPs.waternetwork_deliverySystem == "DEL_POTTS_HILL","DEL_POTTS_HILL + DEL_PROSPECT_EAST").when(unmeteredSTPs.waternetwork_deliverySystem == "DEL_CASCADE","DEL_CASCADE + DEL_ORCHARD_HILLS").when(unmeteredSTPs.waternetwork_deliverySystem == "DEL_ORCHARD_HILLS","DEL_CASCADE + DEL_ORCHARD_HILLS").otherwise(unmeteredSTPs.waternetwork_deliverySystem)) \
        .select(col("reporting_deliverySystem").alias("waterNetworkDeliverySystem"),col("waternetwork_distributionSystem").alias("waterNetworkDistributionSystem"),col("waternetwork_supplyZone").alias("waterNetworkSupplyZone"),coalesce(col("zoneName"),col('waternetwork_pressureArea')).alias("waterNetworkPressureArea"),"networkTypeCode","metricTypeName","metricValueNumber").dropDuplicates()    
    
    #UnmeteredNonSTPs
    unmeteredNonSTPs = waternetworkdemand_df.withColumn("metricValueNumber",(waternetworkdemand_df.demandQuantity*unmeteredNonSTPsFactor)/totalSystemInputCurrMonth) \
        .withColumn("metricTypeName", lit('unmeteredNonSTPs')) \
        .select(col("deliverySystem").alias("waterNetworkDeliverySystem"),col("distributionSystem").alias("waterNetworkDistributionSystem"),col("supplyZone").alias("waterNetworkSupplyZone"),col("pressureArea").alias("waterNetworkPressureArea"),"networkTypeCode","metricTypeName","metricValueNumber")

    #FireRescueNSWUse 
    fireRescueNSWUse = waternetworkdemand_df.withColumn("metricValueNumber",(waternetworkdemand_df.demandQuantity*fireRescueNSWUseFactor)/totalSystemInputCurrMonth) \
        .withColumn("metricTypeName", lit('fireRescueNSWUse')) \
        .select(col("deliverySystem").alias("waterNetworkDeliverySystem"),col("distributionSystem").alias("waterNetworkDistributionSystem"),col("supplyZone").alias("waterNetworkSupplyZone"),col("pressureArea").alias("waterNetworkPressureArea"),"networkTypeCode","metricTypeName","metricValueNumber")        
    
    # #SydneyWaterOperationalUse
    sydneyWaterOperationalUse = waternetworkdemand_df.withColumn("pressure_deliverySystemFormatted",lower(regexp_replace(regexp_replace(waternetworkdemand_df.deliverySystem,'DEL_',""),'[+ _]',"")))
    sydneyWaterOperationalUse = sydneyWaterOperationalUse.join(deliverySystemInputCurrMonth, (sydneyWaterOperationalUse.pressure_deliverySystemFormatted==deliverySystemInputCurrMonth.deliverySystemFormatted),"inner") \
        .join(sharepointConfig_df, (col("metricValueNumber").isNotNull() & (col("metricTypeName") == 'SWOperationalUse') & (col("zoneTypeName") == 'DeliverySystem') & (sydneyWaterOperationalUse.pressure_deliverySystemFormatted == lower(sharepointConfig_df.zoneName))),"inner") \
        .withColumn("metricValueNumber",(sydneyWaterOperationalUse.demandQuantity*sharepointConfig_df.metricValueNumber)/(deliverySystemInputCurrMonth.delivery_demandQuantity)) \
        .withColumn("metricTypeName", lit('sydneyWaterOperationalUse')) \
        .select(col("deliverySystem").alias("waterNetworkDeliverySystem"),col("distributionSystem").alias("waterNetworkDistributionSystem"),col("supplyZone").alias("waterNetworkSupplyZone"),col("pressureArea").alias("waterNetworkPressureArea"),"networkTypeCode","metricTypeName","metricValueNumber")

    #ruralFireServicesUse
    ruralFireServicesUse = monthlySupplyApportioned_df.join(waternetwork_df,(monthlySupplyApportioned_df.supply_waterNetworkSK == waternetwork_df.waterNetworkSK),"inner") \
        .groupBy("waternetwork_deliverySystem","waternetwork_distributionSystem","waternetwork_supplyZone","waternetwork_pressureArea") \
        .agg((sum('supply_totalKLQuantity')/1000).alias("supplyQuantity"))
    ruralFireServicesUse = ruralFireServicesUse.withColumn("metricTypeName", lit('ruralFireServicesUse')) \
        .withColumn("networktypecode", lit('Pressure Area')) \
        .withColumn("metricValueNumber",(ruralFireServicesUse.supplyQuantity*ruralFireServicesUseFactor)/(monthlySupplyLGA)) \
        .select(col("waternetwork_deliverySystem").alias("waterNetworkDeliverySystem"),col("waternetwork_distributionSystem").alias("waterNetworkDistributionSystem"),col("waternetwork_supplyZone").alias("waterNetworkSupplyZone"),col("waternetwork_pressureArea").alias("waterNetworkPressureArea"),"networkTypeCode","metricTypeName","metricValueNumber")

    ruralFireServicesUseSupplyZone = ruralFireServicesUse.groupBy("waterNetworkDeliverySystem","waterNetworkDistributionSystem","waterNetworkSupplyZone","metricTypeName").agg(sum('metricValueNumber').alias('metricValueNumber')) \
        .withColumn("networkTypeCode",lit('Supply Zone')) 

    ruralFireServicesUseDeliverySystem = ruralFireServicesUse.groupBy("waterNetworkDeliverySystem").agg(sum('metricValueNumber').alias('metricValueNumber')) \
        .withColumn("networkTypeCode",lit('Delivery System'))   


    demandaggregatedf = testingofFireServicesUse \
        .unionByName(testingofFireServicesUseSupplyZone,allowMissingColumns=True) \
        .unionByName(testingofFireServicesUseDeliverySystem,allowMissingColumns=True) \
        .unionByName(unauthorisedUse,allowMissingColumns=True) \
        .unionByName(meterUnderRegistration,allowMissingColumns=True) \
        .unionByName(meterUnderRegistrationSupplyZone,allowMissingColumns=True) \
        .unionByName(meterUnderRegistrationDeliverySystem,allowMissingColumns=True) \
        .unionByName(unmeteredSTPs,allowMissingColumns=True) \
        .unionByName(unmeteredNonSTPs,allowMissingColumns=True) \
        .unionByName(fireRescueNSWUse,allowMissingColumns=True) \
        .unionByName(sydneyWaterOperationalUse,allowMissingColumns=True) \
        .unionByName(ruralFireServicesUse,allowMissingColumns=True) \
        .unionByName(ruralFireServicesUseSupplyZone,allowMissingColumns=True) \
        .unionByName(ruralFireServicesUseDeliverySystem,allowMissingColumns=True) \
        .withColumn("yearNumber",lit(yearnumber)) \
        .withColumn("monthName",lit(monthName)) \
        .withColumn("monthNumber",lit(monthnumber)) \
        .withColumn("calculationDate",trunc(current_date(),'month'))   

    return demandaggregatedf

# COMMAND ----------

def Transform():
    global df
    df = None
    # ------------- TABLES ----------------- #
    
    property_df = GetTable(f"{get_table_namespace(f'{DEFAULT_TARGET}', 'viewpropertyKey')}") \
    .select("propertyNumber","superiorPropertyTypeCode","superiorPropertyType","waterNetworkDeliverySystem","waterNetworkDistributionSystem","waterNetworkSupplyZone","waterNetworkPressureArea","propertySK","propertyTypeHistorySK","drinkingWaterNetworkSK").filter("currentFlag = 'Y'")

    installation_df = GetTable(f"{get_env()}curated.water_consumption.viewinstallation") \
    .select(col("propertyNumber").alias("install_propertyNumber"),"installationNumber","divisionCode","division").filter("divisionCode = 10").filter("currentFlag = 'Y'")

    device_df = GetTable(f"{get_env()}curated.water_consumption.viewdevice") \
    .select(col("installationNumber").alias("device_installationNumber"),"deviceSize","functionClassCode","deviceID").filter("functionClassCode in ('1000')").filter("currentFlag = 'Y'").filter("deviceSize >= 40")

    waternetwork_df = GetTable(f"{get_env()}curated.dim.waternetwork").filter("ispotablewaternetwork = 'Y'").filter("`_RecordCurrent` = 1") \
        .select(col("deliverySystem").alias("waternetwork_deliverySystem"),col("distributionSystem").alias("waternetwork_distributionSystem"),col("supplyZone").alias("waternetwork_supplyZone"),col("pressureArea").alias("waternetwork_pressureArea"),"waterNetworkSK")
    
    date_df = spark.sql(f"""
                        select distinct monthstartdate,year(monthstartdate) as yearnumber,month(monthstartdate) as monthnumber, date_format(monthStartDate,'MMM') as monthname 
                        from {get_table_namespace(f'{DEFAULT_TARGET}', 'dimdate')} where monthStartDate between add_months(current_date(),-25) and add_months(current_date(),-1)
                        order by yearnumber desc, monthnumber desc
                        """)

    # ------------- JOINS ------------------ #
    aggregatedf = None
    # targetTableFqn = f"{_.Destination}"
    # if (TableExists(targetTableFqn)):
    #     truncateTable = spark.sql(f"truncate table {targetTableFqn}")

    for i in date_df.collect():
        sharepointConfig_df = spark.sql(f"""
                                select config.zoneName, config.zonetypename, config.metricTypeName, coalesce(config.metricValueNumber,0) as  metricValueNumber
                                from {get_env()}curated.water_balance.AggregatedComponentsConfiguration config where config.yearnumber = {i.yearnumber} and config.monthName = '{i.monthname}'""")
        
        waternetworkdemand_df = GetTable(f"{get_env()}curated.water_balance.factwaternetworkdemand").filter(f"year(reportDate) = {i.yearnumber}").filter(f"month(reportdate) = {i.monthnumber}") \
            .filter("deliverySystem NOT IN ('DEL_PROSPECT_EAST','DEL_POTTS_HILL','DEL_CASCADE','DEL_ORCHARD_HILLS','DESALINATION PLANT')") \
            .filter("networkTypeCode in ('Pressure Area','Supply Zone','Delivery System','Delivery System Combined')") \
            .groupBy("deliverySystem","distributionSystem","supplyZone","pressureArea","networkTypeCode").agg(sum('demandQuantity').alias("demandQuantity"))
               
        monthlySupplyApportioned_df = spark.sql(f"""
                                                Select waterNetworkSK as supply_waterNetworkSK,LGA,totalKLQuantity as supply_totalKLQuantity from {get_env()}curated.fact.monthlysupplyapportionedaggregate
                                                where year(consumptionDate) = {i.yearnumber} and month(consumptionDate) = {i.monthnumber} 
                                                and calculationDate = (select max(calculationDate) from {get_env()}curated.fact.monthlysupplyapportionedaggregate)
                                                """)

        aggregatedf = CalculateDemandAggregated(property_df,installation_df,device_df,sharepointConfig_df,waternetworkdemand_df,waternetwork_df,monthlySupplyApportioned_df,i.yearnumber,i.monthname,i.monthnumber)

        print(f"Calculation: {i.yearnumber}-{i.monthname}")

        # ------------- TRANSFORMS ------------- #
        _.Transforms = [
            f"metricTypeName||'|'||coalesce(waterNetworkDeliverySystem,'')||'|'||coalesce(waterNetworkDistributionSystem,'')||'|'||coalesce(waterNetworkSupplyZone,'')||'|'||coalesce(waterNetworkPressureArea,'')||'|'||yearNumber||'|'||monthName||'|'||calculationDate {BK}"
            ,"waterNetworkDeliverySystem deliverySystem"        
            ,"waterNetworkDistributionSystem distributionSystem"
            ,"waterNetworkSupplyZone supplyZone"        
            ,"waterNetworkPressureArea pressureArea"
            ,"networkTypeCode networkTypeCode"
            ,"yearNumber yearNumber"
            ,"monthNumber monthNumber"
            ,"monthName monthName"
            ,"calculationDate calculationDate"
            ,"metricTypeName metricTypeName"
            ,"metricValueNumber metricValueNumber"
        ]
        df = aggregatedf.selectExpr(
            _.Transforms
        )

        # ------------- SAVE ------------------- #
        SaveAndContinue(df, append=True)
pass
Transform()

# COMMAND ----------

# CleanSelf()
