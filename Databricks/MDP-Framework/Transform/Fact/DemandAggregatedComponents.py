# Databricks notebook source
# MAGIC %run ../../Common/common-transform 

# COMMAND ----------

# MAGIC %run ../../Common/common-helpers 

# COMMAND ----------

DEFAULT_TARGET = 'curated'

# COMMAND ----------

def Transform():
    global df
    # ------------- TABLES ----------------- #
    
    property_df = GetTable(f"{get_table_namespace(f'{DEFAULT_TARGET}', 'viewpropertyKey')}") \
    .select("propertyNumber","superiorPropertyTypeCode","superiorPropertyType","waterNetworkDeliverySystem","waterNetworkDistributionSystem","waterNetworkSupplyZone","waterNetworkPressureArea","propertySK","propertyTypeHistorySK","drinkingWaterNetworkSK").filter("currentFlag = 'Y'")

    installation_df = GetTable(f"{get_env()}curated.water_consumption.viewinstallation") \
    .select(col("propertyNumber").alias("install_propertyNumber"),"installationNumber","divisionCode","division").filter("divisionCode = 10").filter("currentFlag = 'Y'")

    device_df = GetTable(f"{get_env()}curated.water_consumption.viewdevice") \
    .select(col("installationNumber").alias("device_installationNumber"),"deviceSize","functionClassCode","deviceID").filter("functionClassCode in ('1000')").filter("currentFlag = 'Y'").filter("deviceSize >= 40")

    sharepointConfig_df = spark.sql(f"""
                                with Base as (select year(current_date()) as currentYear, date_format(current_date(),'MMM') as currentMonth)
                                select config.metricTypeName, config.metricValueNumber 
                                from {get_env()}curated.water_balance.AggregatedComponentsConfiguration config inner join Base on config.yearnumber = currentYear and config.monthName = currentMonth
                                """).cache()
    
    tfsumetrics_df = sharepointConfig_df.filter("metricTypeName like '%TFSU%'")
    
    date_df = GetTable(f"{get_table_namespace(f'{DEFAULT_TARGET}', 'dimdate')}") \
    .select("calendarDate","financialYear","quarterOfFinancialYear","monthOfFinancialYear")

    waternetworkdemand_df = GetTable(f"{get_env()}curated.water_balance.factwaternetworkdemand").filter("year(reportDate) = year(current_date())").filter("month(reportdate) = month(current_date())") \
        .groupBy("deliverySystem","distributionSystem","supplyZone","pressureArea","networkTypeCode").agg(sum('demandQuantity').alias("demandQuantity"))

    # ------------- JOINS ------------------ #

    tfsu_df = property_df.join(installation_df, (property_df.propertyNumber==installation_df.install_propertyNumber) & ~col("superiorPropertyTypeCode").isin('902'),"inner") \
        .join(device_df, (installation_df.installationNumber == device_df.device_installationNumber),"inner").cache()

    # --- Multiplication Factors ---#
    monthlyTestingFireSprinklerSystemsFactor = tfsumetrics_df.filter("metricTypeName='TFSUMonthlyTestingOfFireSprinklerSystemsTestDuration'").select("metricValueNumber").first()['metricValueNumber'] * tfsumetrics_df.filter("metricTypeName='TFSUMonthlyTestingOfFireSprinklerSystemsTestFlowRate'").select("metricValueNumber").first()['metricValueNumber']

    annualTestingSprinklerSystemsResidentialAndCommercialFactor = tfsumetrics_df.filter("metricTypeName='TFSUAnnualTestingOfSprinklerSystemResidentialAndCommercialFireServicesTestDuration'").select("metricValueNumber").first()['metricValueNumber'] * tfsumetrics_df.filter("metricTypeName='TFSUAnnualTestingOfSprinklerSystemResidentialAndCommercialFireServicesTestFlowRate'").select("metricValueNumber").first()['metricValueNumber']

    annualTestingSprinklerSystemsLargeIndustrialFactor = tfsumetrics_df.filter("metricTypeName='TFSUAnnualTestingOfSprinklerSystemLargeIndustrialServicesTestDuration'").select("metricValueNumber").first()['metricValueNumber'] * tfsumetrics_df.filter("metricTypeName='TFSUAnnualTestingOfSprinklerSystemLargeIndustrialServicesTestFlowRate'").select("metricValueNumber").first()['metricValueNumber']

    monthlyTestingHydrantSystemsFactor = tfsumetrics_df.filter("metricTypeName='TFSUMonthlyTestingOfHydrantSystemsTestDuration'").select("metricValueNumber").first()['metricValueNumber'] * tfsumetrics_df.filter("metricTypeName='TFSUMonthlyTestingOfHydrantSystemsTestFlowRate'").select("metricValueNumber").first()['metricValueNumber']

    annualTestingHydrantSystemsResidentialAndCommercialFactor = tfsumetrics_df.filter("metricTypeName='TFSUAnnualTestingOfSprinklerSystemResidentialAndCommercialFireServicesTestDuration'").select("metricValueNumber").first()['metricValueNumber'] * tfsumetrics_df.filter("metricTypeName='TFSUAnnualTestingOfSprinklerSystemResidentialAndCommercialFireServicesTestFlowRate'").select("metricValueNumber").first()['metricValueNumber']

    annualTestingHydrantSystemsLargeIndustrialFactor = tfsumetrics_df.filter("metricTypeName='TFSUAnnualTestingOfHydrantSystemsLargeIndustrialServicesTestDuration'").select("metricValueNumber").first()['metricValueNumber'] * tfsumetrics_df.filter("metricTypeName='TFSUAnnualTestingOfHydrantSystemsLargeIndustrialServicesTestFlowRate'").select("metricValueNumber").first()['metricValueNumber']

    unauthorisedUseFactor = sharepointConfig_df.filter("metricTypeName='UnauthorisedUse'").select("metricValueNumber").first()['metricValueNumber']
    meterUnderRegistrationFactor = sharepointConfig_df.filter("metricTypeName='MeterUnderRegistration'").select("metricValueNumber").first()['metricValueNumber']

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


    ##FOR UAT - START
    NoFireServicesSWCpotableWaterSupplySystem  = tfsu_df.where(col('superiorPropertyType').isin('COMMERCIAL','COMMUNITY SERVS','INDUSTRIAL','MASTER STRATA','OCCUPIED LAND','UTILITIES')) \
        .groupBy("superiorPropertyType","waterNetworkDeliverySystem","waterNetworkDistributionSystem","waterNetworkSupplyZone","waterNetworkPressureArea") \
        .agg(countDistinct('deviceID').alias("deviceCount")) \
        .withColumn("metricTypeName", lit('NoFireServicesSWCpotableWaterSupplySystem'))    
    NoLargeIndustrialServices = tfsu_df.where(col('superiorPropertyType').isin('INDUSTRIAL')) \
        .groupBy("superiorPropertyType","waterNetworkDeliverySystem","waterNetworkDistributionSystem","waterNetworkSupplyZone","waterNetworkPressureArea") \
        .agg(countDistinct('deviceID').alias("deviceCount")) \
        .withColumn("metricTypeName", lit('NoLargeIndustrialServices')) 
    NoResidentialCommercialFireServices  = tfsu_df.where(col('superiorPropertyType').isin('COMMERCIAL','COMMUNITY SERVS','MASTER STRATA','OCCUPIED LAND','UTILITIES')) \
        .groupBy("superiorPropertyType","waterNetworkDeliverySystem","waterNetworkDistributionSystem","waterNetworkSupplyZone","waterNetworkPressureArea") \
        .agg(countDistinct('deviceID').alias("deviceCount")) \
        .withColumn("metricTypeName", lit('NoResidentialCommercialFireServices')) 

    fireServicesPropertyGroupAggregatedf = NoFireServicesSWCpotableWaterSupplySystem.union(NoLargeIndustrialServices).union(NoResidentialCommercialFireServices)
    fireServicesPropertyGroupAggregatedf.createOrReplaceTempView("fireServicesPropertyGroupAggregateView")
    spark.sql(f"""Create or replace table {get_env()}curated.water_balance.fireServicesPropertyGroupAggregate as select * from fireServicesPropertyGroupAggregateView""")       
    ##FOR UAT - END 

    #UnauthorisedUse
    unauthorisedUse = waternetworkdemand_df.withColumn("metricValueNumber",(waternetworkdemand_df.demandQuantity*unauthorisedUseFactor)/100) \
        .withColumn("metricTypeName", lit('unauthorisedUse')) \
        .select(col("deliverySystem").alias("waterNetworkDeliverySystem"),col("distributionSystem").alias("waterNetworkDistributionSystem"),col("supplyZone").alias("waterNetworkSupplyZone"),col("pressureArea").alias("waterNetworkPressureArea"),"networkTypeCode","metricTypeName","metricValueNumber")

    #meterUnderRegistration
    meterUnderRegistration = waternetworkdemand_df.withColumn("metricValueNumber",(waternetworkdemand_df.demandQuantity*meterUnderRegistrationFactor)/100) \
        .withColumn("metricTypeName", lit('meterUnderRegistration')) \
        .select(col("deliverySystem").alias("waterNetworkDeliverySystem"),col("distributionSystem").alias("waterNetworkDistributionSystem"),col("supplyZone").alias("waterNetworkSupplyZone"),col("pressureArea").alias("waterNetworkPressureArea"),"networkTypeCode","metricTypeName","metricValueNumber")
    
    df = monthlyTestingFireSprinklerSystems \
        .unionByName(annualTestingSprinklerSystems,allowMissingColumns=True) \
        .unionByName(monthlyTestingHydrantSystems,allowMissingColumns=True) \
        .unionByName(annualTestingHydrantSystems,allowMissingColumns=True) \
        .unionByName(unauthorisedUse,allowMissingColumns=True) \
        .unionByName(meterUnderRegistration,allowMissingColumns=True) \
        .withColumn("yearNumber",expr("year(current_date())")) \
        .withColumn("monthName",expr("date_format(current_date(),'MMM')"))

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
    # CleanSelf()
    Save(df)
    #DisplaySelf()
pass
Transform()
