# Databricks notebook source
# MAGIC %run ../../Common/common-transform

# COMMAND ----------

DEFAULT_TARGET = 'curated_v3'

# COMMAND ----------

def Transform():
    global df
    # ------------- TABLES ----------------- #

    property_df = GetTable(f"{DEFAULT_TARGET}.dimproperty") \
    .select("propertySK","propertyNumber","parentArchitecturalObjectNumber","waterNetworkSK_drinkingWater","buildingFeeDate").filter("_RecordDeleted = 0").filter("_RecordEnd = '9999-12-31 23:59:59.000'")

    water_network_df = GetTable(f"{DEFAULT_TARGET}.dimWaterNetwork") \
    .select("waterNetworkSK","deliverySystem","distributionSystem","supplyZone","pressureArea").filter("_RecordDeleted = 0").filter("isPotableWaterNetwork = 'Y'").filter("_RecordEnd = '9999-12-31 23:59:59.000'")

    property_history_df = GetTable(f"{DEFAULT_TARGET}.dimpropertytypehistory") \
    .select("propertyTypeHistorySK","sourceSystemCode",col("propertyNumber").alias("prophist_propertyNumber"),"superiorPropertyTypeCode","superiorPropertyType","inferiorPropertyTypeCode","inferiorPropertyType").filter("_RecordDeleted = 0").filter("_RecordEnd = '9999-12-31 23:59:59.000'")
        
    property_service_df = GetTable(f"{DEFAULT_TARGET}.dimpropertyservice") \
    .select(col("propertyNumber").alias("service_propertyNumber"),"fixtureAndFittingCharacteristicCode","fixtureAndFittingCharacteristic").filter("_RecordDeleted = 0").filter("_RecordEnd = '9999-12-31 23:59:59.000'")

    joint_service_parent_df = GetTable(f"{DEFAULT_TARGET}.dimPropertyRelation") \
    .select(col("property1Number").alias("service_propertyNumber"),col("relationshipTypeCode1").alias("relationshipTypeCode"),col("relationshipType1").alias("relationshipType")).filter("relationshipTypeCode1 in ('J', 'L')").filter("_RecordDeleted = 0").filter("_RecordEnd = '9999-12-31 23:59:59.000'")

    joint_service_child_df = GetTable(f"{DEFAULT_TARGET}.dimPropertyRelation") \
    .select("property2Number","relationshipTypeCode2","relationshipType2").filter("relationshipTypeCode2 in ('F', 'M')").filter("_RecordDeleted = 0").filter("_RecordEnd = '9999-12-31 23:59:59.000'")
  
    water_installation_df = GetTable(f"{DEFAULT_TARGET}.diminstallation") \
    .select(col("propertyNumber").alias("install_propertyNumber"),"installationNumber","divisionCode","division",).filter("divisionCode = 10").filter("_RecordDeleted = 0").filter("_RecordEnd = '9999-12-31 23:59:59.000'")

    water_installation_history_df = GetTable(f"{DEFAULT_TARGET}.diminstallationhistory") \
    .select(col("installationNumber").alias("history_installationNumber"),"portionNumber","portionText").filter("_RecordDeleted = 0").filter("_RecordEnd = '9999-12-31 23:59:59.000'")

    rate_type_df = GetTable(f"{DEFAULT_TARGET}.dimInstallationFacts") \
    .select(col("installationNumber").alias("rate_installationNumber"),"rateTypeCode","rateType","operandCode","validToDate").filter("operandCode == 'WRT-WSSC'").filter("_RecordEnd = '9999-12-31 23:59:59.000'")

    date_df = GetTable(f"{DEFAULT_TARGET}.dimdate") \
    .select("calendarDate","yearEndDate","yearStartDate","monthEndDate","monthStartDate").filter("calendarDate = current_date()")    

    billingdocument_df = GetTable(f"{DEFAULT_TARGET}.dimmeterconsumptionbillingdocument") \
    .select("meterConsumptionBillingDocumentSK","isOutsortedFlag").filter("_RecordDeleted = 0")

    factbilled_df = GetTable(f"{DEFAULT_TARGET}.factbilledwaterconsumption") \
    .select(col("meterConsumptionBillingDocumentSK").alias("fact_meterConsumptionBillingDocumentSK"),"propertyNumber")


    # ------------- JOINS ------------------ #
    joint_service_df = joint_service_parent_df.union(joint_service_child_df).distinct()

    ##Unmetered Connected Properties
    water_installation_df = rate_type_df.join(water_installation_df, (water_installation_df.installationNumber == rate_type_df.rate_installationNumber), "inner") \
        .drop("rate_installationNumber","operandCode","validToDate") \
        .join(water_installation_history_df, (water_installation_df.installationNumber == water_installation_history_df.history_installationNumber), "inner")

    unmeteredConnecteddf = property_df.join(water_network_df, (property_df.waterNetworkSK_drinkingWater == water_network_df.waterNetworkSK), 'inner') \
        .join(property_history_df , (property_df.propertyNumber == property_history_df.prophist_propertyNumber), 'inner') \
        .join(property_service_df,(property_df.propertyNumber == property_service_df.service_propertyNumber) & (property_service_df.fixtureAndFittingCharacteristicCode == 'Z101'),"inner") \
        .drop("service_propertyNumber") \
        .join(joint_service_df, (property_df.propertyNumber == joint_service_df.service_propertyNumber),"left") \
        .join(water_installation_df, (property_df.propertyNumber == water_installation_df.install_propertyNumber),"left")
       
    df_parent = unmeteredConnecteddf.select(col("propertyNumber").alias("parent_propertyNumber"),col("inferiorPropertyTypeCode").alias("parent_inferiorPropertyTypeCode"), \
        col("inferiorPropertyType").alias("parent_inferiorPropertyType"),col("portionNumber").alias("parent_portionNumber"), \
        col("portionText").alias("parent_portionText"),col("rateTypeCode").alias("parent_rateTypeCode"),col("rateType").alias("parent_rateType"), \
        col("relationshipTypeCode").alias("parent_relationshipTypeCode"),col("relationshipType").alias("parent_relationshipType"))

    unmeteredConnecteddf = unmeteredConnecteddf.join(df_parent, (unmeteredConnecteddf.parentArchitecturalObjectNumber == df_parent.parent_propertyNumber), "left")
    unmeteredConnecteddf = unmeteredConnecteddf.where(((col("portionNumber") == 'UNM_01') & (col("relationshipTypeCode").isNull()) & (col("parent_relationshipTypeCode").isNull()) & \
        ~col("inferiorPropertyTypeCode").isin('073', '225', '998', '245', '084', '212', '223', '249')) & ((col("parent_portionNumber") == 'UNM_01') | (col("parent_portionNumber").isNull()))) \
        .withColumn("reportDate",current_timestamp()) \
        .join(date_df).withColumn("unmeteredConnectedFlag",lit('Y')).withColumn("unmeteredConstructionFlag",lit('')) \
        .withColumn("consumptionKLMonth",((180 / (datediff("yearEndDate", "yearStartDate") + 1)) * (datediff("monthEndDate", "monthStartDate") + 1)))

    unmeteredConnecteddf = unmeteredConnecteddf.select("propertyNumber","sourceSystemCode","propertySK","propertyTypeHistorySK","waterNetworkSK","reportDate","consumptionKLMonth","unmeteredConnectedFlag","unmeteredConstructionFlag")
    
    ##Unmetered Construction Properties
    billingdocument_df = billingdocument_df.join(factbilled_df, (factbilled_df.fact_meterConsumptionBillingDocumentSK == billingdocument_df.meterConsumptionBillingDocumentSK) & (billingdocument_df.isOutsortedFlag == 'N'), 'inner').select(col("propertyNumber").alias("billingdocument_PropertyNum"))

    unmeteredConstructiondf = property_df.join(water_network_df, (property_df.waterNetworkSK_drinkingWater == water_network_df.waterNetworkSK), 'inner') \
        .join(property_history_df , (property_df.propertyNumber == property_history_df.prophist_propertyNumber), 'inner') \
        .join(property_service_df,(property_df.propertyNumber == property_service_df.service_propertyNumber) & (property_service_df.fixtureAndFittingCharacteristicCode == 'Z177'),"left") \
        .drop("service_propertyNumber") \
        .join(billingdocument_df, (property_df.propertyNumber == billingdocument_df.billingdocument_PropertyNum),"left_anti" )

    
    unmeteredConstructiondf = unmeteredConstructiondf.where((col("inferiorPropertyTypeCode").isin('089','214','215','237', '236')) & col("buildingFeeDate").isNotNull()) \
        .withColumn("reportDate",current_timestamp())  \
        .join(date_df).withColumn("unmeteredConnectedFlag",lit('N')).withColumn("unmeteredConstructionFlag",lit('Y')) \
        .withColumn("consumptionKLMonth",lit(62.5 / 12))

    unmeteredConstructiondf = unmeteredConstructiondf.select("propertyNumber","sourceSystemCode","propertySK","propertyTypeHistorySK","waterNetworkSK","reportDate","consumptionKLMonth","unmeteredConnectedFlag","unmeteredConstructionFlag")    


    df = unmeteredConnecteddf.unionByName(unmeteredConstructiondf, allowMissingColumns=True)

    # ------------- TRANSFORMS ------------- #
    _.Transforms = [
        f"propertyNumber {BK}"
        ,"sourceSystemCode sourceSystemCode"
        ,"propertyNumber propertyNumber"
        ,"propertySK propertySK"
        ,"propertyTypeHistorySK propertyTypeHistorySK"
        ,"waterNetworkSK waterNetworkSK"        
        ,"reportDate reportDate"
        ,"unmeteredConnectedFlag unmeteredConnectedFlag"     
        ,"unmeteredConstructionFlag unmeteredConstructionFlag"
        ,"consumptionKLMonth consumptionKLMonth"   
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
