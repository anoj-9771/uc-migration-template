# Databricks notebook source
# MAGIC %run ../../Common/common-transform 

# COMMAND ----------

# MAGIC %run ../../Common/common-helpers 

# COMMAND ----------

DEFAULT_TARGET = 'curated'

# COMMAND ----------

def CalculateDemandAggregated(property_df,installation_df,device_df,sharepointConfig_df,waternetworkdemand_df,waternetwork_df,yearnumber,monthName):
    
    tfsumetrics_df = sharepointConfig_df.filter("metricTypeName like '%TFSU%'")

    # ------------- JOINS ------------------ #
    tfsu_df = property_df.join(installation_df, (property_df.propertyNumber==installation_df.install_propertyNumber) & ~col("superiorPropertyTypeCode").isin('902'),"inner") \
        .join(device_df, (installation_df.installationNumber == device_df.device_installationNumber),"inner").cache()

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
    totalSystemInputCurrMonth = waternetworkdemand_df.filter("networkTypeCode = 'Delivery System'").agg(sum('demandQuantity').alias('demandQuantity')).select("demandQuantity").first()['demandQuantity']

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

    
    # annualTestingHydrantSystems
    annualTestingHydrantSystems = tfsu_df.where(col('superiorPropertyType').isin('COMMERCIAL','COMMUNITY SERVS','MASTER STRATA','OCCUPIED LAND','UTILITIES')) \
        .groupBy("waterNetworkDeliverySystem","waterNetworkDistributionSystem","waterNetworkSupplyZone","waterNetworkPressureArea").agg(countDistinct('deviceID').alias("deviceCount")) 
    annualTestingHydrantSystems = annualTestingHydrantSystems.withColumn("metricValueNumber", (annualTestingHydrantSystems.deviceCount * annualTestingHydrantSystemsResidentialAndCommercialFactor)/(12*1000000))  
    annualTestingHydrantSystemsLargeIndustrial = tfsu_df.where(col('superiorPropertyType').isin('INDUSTRIAL')).groupBy("waterNetworkDeliverySystem","waterNetworkDistributionSystem","waterNetworkSupplyZone","waterNetworkPressureArea").agg(countDistinct('deviceID').alias("deviceCount")) 
    annualTestingHydrantSystemsLargeIndustrial = annualTestingHydrantSystemsLargeIndustrial.withColumn("metricValueNumber", (annualTestingHydrantSystemsLargeIndustrial.deviceCount * annualTestingHydrantSystemsLargeIndustrialFactor)/(12*1000000))  
    annualTestingHydrantSystems = annualTestingHydrantSystems.unionAll(annualTestingHydrantSystemsLargeIndustrial).groupBy("waterNetworkDeliverySystem","waterNetworkDistributionSystem","waterNetworkSupplyZone","waterNetworkPressureArea").agg(sum('metricValueNumber').alias("metricValueNumber")) \
        .withColumn("metricTypeName", lit('annualTestingHydrantSystems')) \
        .select("waterNetworkDeliverySystem","waterNetworkDistributionSystem","waterNetworkSupplyZone","waterNetworkPressureArea","metricTypeName","metricValueNumber")

    #UnauthorisedUse
    unauthorisedUse = waternetworkdemand_df.withColumn("metricValueNumber",(waternetworkdemand_df.demandQuantity*unauthorisedUseFactor)/100) \
        .withColumn("metricTypeName", lit('unauthorisedUse')) \
        .select(col("deliverySystem").alias("waterNetworkDeliverySystem"),col("distributionSystem").alias("waterNetworkDistributionSystem"),col("supplyZone").alias("waterNetworkSupplyZone"),col("pressureArea").alias("waterNetworkPressureArea"),"networkTypeCode","metricTypeName","metricValueNumber")

    #meterUnderRegistration
    meterUnderRegistration = waternetworkdemand_df.withColumn("metricValueNumber",(waternetworkdemand_df.demandQuantity*meterUnderRegistrationFactor)/100) \
        .withColumn("metricTypeName", lit('meterUnderRegistration')) \
        .select(col("deliverySystem").alias("waterNetworkDeliverySystem"),col("distributionSystem").alias("waterNetworkDistributionSystem"),col("supplyZone").alias("waterNetworkSupplyZone"),col("pressureArea").alias("waterNetworkPressureArea"),"networkTypeCode","metricTypeName","metricValueNumber")

    #UnmeteredSTPs    
    unmeteredSTPs = sharepointConfig_df.where(col("metricValueNumber").isNotNull() & (col("metricTypeName") == 'UnmeteredSTPs') & (col("zoneTypeName") == 'PressureZone')) \
        .withColumn("metricTypeName",lit('unmeteredSTPs')) \
        .withColumn("metricValueNumber",lit(sharepointConfig_df.metricValueNumber)) \
        .withColumn("networkTypeCode",lit('Pressure Area')) 
    unmeteredSTPs = unmeteredSTPs.join(waternetwork_df,(unmeteredSTPs.zoneName == waternetwork_df.waternetwork_pressureArea),"inner") 

    unmeteredSTPsSupplyZone = unmeteredSTPs.groupBy("waternetwork_supplyZone").agg(sum('metricValueNumber').alias('metricValueNumber')) \
        .withColumn("metricTypeName",lit('unmeteredSTPs')) \
        .withColumn("networkTypeCode",lit('Supply Zone')) 

    unmeteredSTPsDistributionSystem = unmeteredSTPs.groupBy("waternetwork_distributionSystem").agg(sum('metricValueNumber').alias('metricValueNumber')) \
        .withColumn("metricTypeName",lit('unmeteredSTPs')) \
        .withColumn("networkTypeCode",lit('Distribution System')) 

    unmeteredSTPsDeliverySystem = unmeteredSTPs.groupBy("waternetwork_deliverySystem").agg(sum('metricValueNumber').alias('metricValueNumber')) \
        .withColumn("metricTypeName",lit('unmeteredSTPs')) \
        .withColumn("networkTypeCode",lit('Delivery System'))         

    unmeteredSTPs = unmeteredSTPs.unionByName(unmeteredSTPsSupplyZone,allowMissingColumns=True) \
        .unionByName(unmeteredSTPsDistributionSystem,allowMissingColumns=True) \
        .unionByName(unmeteredSTPsDeliverySystem,allowMissingColumns=True) \
        .select(col("waternetwork_deliverySystem").alias("waterNetworkDeliverySystem"),col("waternetwork_distributionSystem").alias("waterNetworkDistributionSystem"),col("waternetwork_supplyZone").alias("waterNetworkSupplyZone"),coalesce(col("zoneName"),col('waternetwork_pressureArea')).alias("waterNetworkPressureArea"),"networkTypeCode","metricTypeName","metricValueNumber").dropDuplicates()    
    
    #UnmeteredNonSTPs
    unmeteredNonSTPs = waternetworkdemand_df.withColumn("metricValueNumber",(waternetworkdemand_df.demandQuantity*unmeteredNonSTPsFactor)/totalSystemInputCurrMonth) \
        .withColumn("metricTypeName", lit('unmeteredNonSTPs')) \
        .select(col("deliverySystem").alias("waterNetworkDeliverySystem"),col("distributionSystem").alias("waterNetworkDistributionSystem"),col("supplyZone").alias("waterNetworkSupplyZone"),col("pressureArea").alias("waterNetworkPressureArea"),"networkTypeCode","metricTypeName","metricValueNumber")
    
    demandaggregatedf = monthlyTestingFireSprinklerSystems \
        .unionByName(annualTestingSprinklerSystems,allowMissingColumns=True) \
        .unionByName(monthlyTestingHydrantSystems,allowMissingColumns=True) \
        .unionByName(annualTestingHydrantSystems,allowMissingColumns=True) \
        .unionByName(unauthorisedUse,allowMissingColumns=True) \
        .unionByName(meterUnderRegistration,allowMissingColumns=True) \
        .unionByName(unmeteredSTPs,allowMissingColumns=True) \
        .unionByName(unmeteredNonSTPs,allowMissingColumns=True) \
        .withColumn("yearNumber",lit(yearnumber)) \
        .withColumn("monthName",lit(monthName))

    return demandaggregatedf

# COMMAND ----------

def Transform():
    global df
    df = None
    # ------------- TABLES ----------------- #
    
    property_df = GetTable(f"{get_table_namespace(f'{DEFAULT_TARGET}', 'viewpropertyKey')}") \
    .select("propertyNumber","superiorPropertyTypeCode","superiorPropertyType","waterNetworkDeliverySystem","waterNetworkDistributionSystem","waterNetworkSupplyZone","waterNetworkPressureArea","propertySK","propertyTypeHistorySK","drinkingWaterNetworkSK").filter("currentFlag = 'Y'").cache()

    installation_df = GetTable(f"{get_env()}curated.water_consumption.viewinstallation") \
    .select(col("propertyNumber").alias("install_propertyNumber"),"installationNumber","divisionCode","division").filter("divisionCode = 10").filter("currentFlag = 'Y'").cache()

    device_df = GetTable(f"{get_env()}curated.water_consumption.viewdevice") \
    .select(col("installationNumber").alias("device_installationNumber"),"deviceSize","functionClassCode","deviceID").filter("functionClassCode in ('1000')").filter("currentFlag = 'Y'").filter("deviceSize >= 40").cache()

    waternetwork_df = GetTable(f"{get_env()}curated.dim.waternetwork").filter("ispotablewaternetwork = 'Y'").filter("`_RecordCurrent` = 1") \
        .select(col("deliverySystem").alias("waternetwork_deliverySystem"),col("distributionSystem").alias("waternetwork_distributionSystem"),col("supplyZone").alias("waternetwork_supplyZone"),col("pressureArea").alias("waternetwork_pressureArea")).cache()
    
    date_df = spark.sql(f"""
                        select distinct monthstartdate,year(monthstartdate) as yearnumber,month(monthstartdate) as monthnumber, date_format(monthStartDate,'MMM') as monthname 
                        from {get_table_namespace(f'{DEFAULT_TARGET}', 'dimdate')} where monthStartDate between add_months(current_date(),-24) and current_date() 
                        order by yearnumber, monthnumber
                        """)

    # ------------- JOINS ------------------ #
    aggregatedf = None
    for i in date_df.collect():
        
        sharepointConfig_df = spark.sql(f"""
                                select config.zoneName, config.zonetypename, config.metricTypeName, coalesce(config.metricValueNumber,0) as  metricValueNumber
                                from {get_env()}curated.water_balance.AggregatedComponentsConfiguration config where config.yearnumber = {i.yearnumber} and config.monthName = '{i.monthname}'""").cache()
        
        waternetworkdemand_df = GetTable(f"{get_env()}curated.water_balance.factwaternetworkdemand").filter(f"year(reportDate) = {i.yearnumber}").filter(f"month(reportdate) = {i.monthnumber}") \
            .groupBy("deliverySystem","distributionSystem","supplyZone","pressureArea","networkTypeCode").agg(sum('demandQuantity').alias("demandQuantity")).cache()
        
        aggregatedf = CalculateDemandAggregated(property_df,installation_df,device_df,sharepointConfig_df,waternetworkdemand_df,waternetwork_df,i.yearnumber,i.monthname)

        if df is None:
            df = aggregatedf
        else:
            df = df.unionByName(aggregatedf)    


    # ------------- TRANSFORMS ------------- #
    _.Transforms = [
        f"metricTypeName||'|'||waterNetworkDeliverySystem||'|'||waterNetworkDistributionSystem||'|'||waterNetworkSupplyZone||'|'||waterNetworkPressureArea||'|'||yearNumber||'|'||monthName {BK}"
        ,"waterNetworkDeliverySystem deliverySystem"        
        ,"waterNetworkDistributionSystem distributionSystem"
        ,"waterNetworkSupplyZone supplyZone"        
        ,"waterNetworkPressureArea pressureArea"
        ,"networkTypeCode networkTypeCode"
        ,"yearNumber yearNumber"
        ,"monthName monthName"
        ,"metricTypeName metricTypeName"
        ,"metricValueNumber metricValueNumber"
    ]
    df = df.selectExpr(
        _.Transforms
    )
    # ------------- CLAUSES ---------------- #

    # ------------- SAVE ------------------- #
    # display(df)
    CleanSelf()
    Save(df)
    #DisplaySelf()
pass
Transform()
