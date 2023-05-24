# Databricks notebook source
# MAGIC %run ../../Common/common-transform

# COMMAND ----------

DEFAULT_TARGET = 'curated_v3'

# COMMAND ----------

def Transform():
    global df
    # ------------- TABLES ----------------- #
    
    property_df = GetTable(f"{DEFAULT_TARGET}.viewpropertyKey") \
    .select("propertyNumber","superiorPropertyTypeCode","superiorPropertyType","inferiorPropertyTypeCode","inferiorPropertyType","architecturalObjectTypeCode","architecturalObjectType","parentArchitecturalObjectNumber","parentArchitecturalObjectTypeCode","parentArchitecturalObjectType","waterNetworkDeliverySystem","waterNetworkDistributionSystem","waterNetworkSupplyZone","waterNetworkPressureArea","propertySK","propertyTypeHistorySK","drinkingWaterNetworkSK","buildingFeeDate").filter("currentFlag = 'Y'")
        
    property_service_df = GetTable(f"{DEFAULT_TARGET}.viewpropertyservice") \
    .select(col("propertyNumber").alias("service_propertyNumber"),"fixtureAndFittingCharacteristicCode","fixtureAndFittingCharacteristic","currentFlag","currentRecordFlag","validToDate").filter("currentFlag = 'Y'")

    joint_service_parent_df = GetTable(f"{DEFAULT_TARGET}.viewPropertyRelation") \
    .select(col("property1Number").alias("service_propertyNumber"),col("relationshipTypeCode1").alias("relationshipTypeCode"),col("relationshipType1").alias("relationshipType")).filter("relationshipTypeCode1 in ('J', 'L')").filter("currentFlag = 'Y'")

    joint_service_child_df = GetTable(f"{DEFAULT_TARGET}.viewPropertyRelation") \
    .select("property2Number","relationshipTypeCode2","relationshipType2").filter("relationshipTypeCode2 in ('F', 'M')").filter("currentFlag = 'Y'")
  
    water_installation_df = GetTable(f"{DEFAULT_TARGET}.viewinstallation") \
    .select(col("propertyNumber").alias("install_propertyNumber"),"installationNumber","divisionCode","division","industrySystem","portionNumber","portionText").filter("divisionCode = 10").filter("currentFlag = 'Y'")

    rate_type_df = GetTable(f"{DEFAULT_TARGET}.dimInstallationFacts") \
    .select(col("installationNumber").alias("rate_installationNumber"),"rateTypeCode","rateType","operandCode","validToDate").filter("operandCode == 'WRT-WSSC'").filter("validToDate == '9999-12-31'")

    date_df = GetTable(f"{DEFAULT_TARGET}.dimdate") \
    .select("calendarDate","yearEndDate","yearStartDate","monthEndDate","monthStartDate").filter("calendarDate = current_date()") 

    billingdocument_df = GetTable(f"{DEFAULT_TARGET}.dimmeterconsumptionbillingdocument") \
    .select("meterConsumptionBillingDocumentSK","isOutsortedFlag").filter("_RecordDeleted = 0")

    factbilled_df = GetTable(f"{DEFAULT_TARGET}.factbilledwaterconsumption") \
    .select(col("meterConsumptionBillingDocumentSK").alias("fact_meterConsumptionBillingDocumentSK"),"propertyNumber")

    # ------------- JOINS ------------------ #
    joint_service_df = joint_service_parent_df.union(joint_service_child_df).distinct()

    water_installation_df = rate_type_df.join(water_installation_df, (water_installation_df.installationNumber == rate_type_df.rate_installationNumber), "inner") \
        .drop("rate_installationNumber","operandCode","validToDate")

    unmeteredConnecteddf = property_df.join(property_service_df,(property_df.propertyNumber == property_service_df.service_propertyNumber) & (property_service_df.fixtureAndFittingCharacteristicCode == 'Z101') & \
        (property_service_df.currentRecordFlag == 'Y') & (property_service_df.validToDate == '9999-12-31'),"inner") \
        .drop("service_propertyNumber") \
        .join(joint_service_df, (property_df.propertyNumber == joint_service_df.service_propertyNumber),"left") \
        .join(water_installation_df, (property_df.propertyNumber == water_installation_df.install_propertyNumber),"left")
       
    df_parent = unmeteredConnecteddf.select(col("propertyNumber").alias("parent_propertyNumber"),col("inferiorPropertyTypeCode").alias("parent_inferiorPropertyTypeCode"), \
        col("inferiorPropertyType").alias("parent_inferiorPropertyType"),col("industrySystem").alias("parent_industrySystem"),col("portionNumber").alias("parent_portionNumber"), \
        col("portionText").alias("parent_portionText"),col("rateTypeCode").alias("parent_rateTypeCode"),col("rateType").alias("parent_rateType"), \
        col("relationshipTypeCode").alias("parent_relationshipTypeCode"),col("relationshipType").alias("parent_relationshipType"))

    unmeteredConnecteddf = unmeteredConnecteddf.join(df_parent, (unmeteredConnecteddf.parentArchitecturalObjectNumber == df_parent.parent_propertyNumber), "left")
    unmeteredConnecteddf = unmeteredConnecteddf.where(((col("portionNumber") == 'UNM_01') & (col("relationshipTypeCode").isNull()) & (col("parent_relationshipTypeCode").isNull()) & \
        ~col("inferiorPropertyTypeCode").isin('073', '225', '998', '245', '084', '212', '223', '249')) & ((col("parent_portionNumber") == 'UNM_01') | (col("parent_portionNumber").isNull()))) \
        .withColumn("reportDate",current_timestamp())

    unmeteredConnecteddf = unmeteredConnecteddf.join(date_df).withColumn("unmeteredConnectedFlag",lit('Y')).withColumn("unmeteredConstructionFlag",lit('N')) \
        .withColumn("consumptionKLMonth",((180 / (datediff("yearEndDate", "yearStartDate") + 1)) * (datediff("monthEndDate", "monthStartDate") + 1)))

    unmeteredConnecteddf = unmeteredConnecteddf.select("propertyNumber","reportDate","propertySK","propertyTypeHistorySK","superiorPropertyTypeCode","superiorPropertyType","inferiorPropertyTypeCode","inferiorPropertyType","drinkingWaterNetworkSK","waterNetworkDeliverySystem","waterNetworkDistributionSystem","waterNetworkSupplyZone","waterNetworkPressureArea","consumptionKLMonth","unmeteredConnectedFlag","unmeteredConstructionFlag")     

    ##Unmetered Construction Properties
    billingdocument_df = billingdocument_df.join(factbilled_df, (factbilled_df.fact_meterConsumptionBillingDocumentSK == billingdocument_df.meterConsumptionBillingDocumentSK) & (billingdocument_df.isOutsortedFlag == 'N'), 'inner').select(col("propertyNumber").alias("billingdocument_PropertyNum"))

    unmeteredConstructiondf = property_df.where(col("inferiorPropertyTypeCode").isin('236') & (col("buildingFeeDate").isNotNull())) \
        .union(property_df.join(property_service_df,(property_df.propertyNumber == property_service_df.service_propertyNumber) & (property_service_df.fixtureAndFittingCharacteristicCode == 'Z177') & \
        (property_service_df.currentRecordFlag == 'Y') & (property_service_df.validToDate == '9999-12-31'),"inner").where(col("inferiorPropertyTypeCode").isin('089','214','215','237', '236') & (col("buildingFeeDate").isNotNull())) \
        .drop("service_propertyNumber","fixtureAndFittingCharacteristicCode","fixtureAndFittingCharacteristic","currentFlag","currentRecordFlag","validToDate")) \
        .withColumn("reportDate",current_timestamp())

    unmeteredConstructiondf = unmeteredConstructiondf.join(billingdocument_df, (unmeteredConstructiondf.propertyNumber == billingdocument_df.billingdocument_PropertyNum),"left_anti" ) \
        .join(date_df).withColumn("unmeteredConnectedFlag",lit('N')).withColumn("unmeteredConstructionFlag",lit('Y')) \
        .withColumn("consumptionKLMonth",lit(62.5 / 12)) 

    unmeteredConstructiondf = unmeteredConstructiondf.select("propertyNumber","reportDate","propertySK","propertyTypeHistorySK","superiorPropertyTypeCode","superiorPropertyType","inferiorPropertyTypeCode","inferiorPropertyType","drinkingWaterNetworkSK","waterNetworkDeliverySystem","waterNetworkDistributionSystem","waterNetworkSupplyZone","waterNetworkPressureArea","consumptionKLMonth","unmeteredConnectedFlag","unmeteredConstructionFlag")    

    df = unmeteredConnecteddf.unionByName(unmeteredConstructiondf, allowMissingColumns=True)

    # ------------- TRANSFORMS ------------- #
    _.Transforms = [
        f"propertyNumber||'|'||reportDate {BK}"
        ,"propertySK propertySK"
        ,"propertyTypeHistorySK propertyTypeHistorySK"
        ,"drinkingWaterNetworkSK waterNetworkSK"
        ,"reportDate reportDate"        
        ,"propertyNumber propertyNumber"
        ,"unmeteredConnectedFlag unmeteredConnectedFlag"     
        ,"unmeteredConstructionFlag unmeteredConstructionFlag"
        ,"consumptionKLMonth consumptionQuantity"
        
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
