# Databricks notebook source
# MAGIC %run ../../Common/common-transform 

# COMMAND ----------

# MAGIC %run ../../Common/common-helpers 

# COMMAND ----------

DEFAULT_TARGET = 'curated'

# COMMAND ----------

spark.sql(f"""
        DELETE FROM {TARGET_TABLE}
        WHERE calculationDate = current_date(); 
        """).display()

# COMMAND ----------

def Transform():
    global df
    # ------------- TABLES ----------------- #
    
    property_df = GetTable(f"{get_table_namespace(f'{DEFAULT_TARGET}', 'viewpropertyKey')}") \
    .select(col("propertyNumber").alias("property_propertyNumber"),"superiorPropertyTypeCode","superiorPropertyType","inferiorPropertyTypeCode","inferiorPropertyType","architecturalObjectTypeCode","architecturalObjectType","parentArchitecturalObjectNumber","parentArchitecturalObjectTypeCode","parentArchitecturalObjectType","waterNetworkDeliverySystem","waterNetworkDistributionSystem","waterNetworkSupplyZone","waterNetworkPressureArea","propertySK","propertyTypeHistorySK","drinkingWaterNetworkSK","buildingFeeDate").filter("currentFlag = 'Y'")
    
    date_df = GetTable(f"{get_table_namespace(f'{DEFAULT_TARGET}', 'dimdate')}") \
    .select("calendarDate","financialYear","quarterOfFinancialYear","monthOfFinancialYear")

    factbilled_df = GetTable(f"{get_env()}curated.fact.billedwaterconsumption") \
    .select("propertyNumber","businessPartnerGroupNumber","deviceNumber","logicalRegisterNumber","billingDocumentNumber","meteredWaterConsumption","meterConsumptionBillingLineItemSK","meterConsumptionBillingDocumentSK","isReversedFlag")

    billinglineitem_df = GetTable(f"{get_env()}curated.dim.meterconsumptionbillinglineitem") \
    .select("statisticalAnalysisRateType","statisticalAnalysisRateTypeCode","billingClassCode","billingClass","validFromDate","validToDate",col("meterConsumptionBillingLineItemSK").alias("line_meterConsumptionBillingLineItemSK"),"meterReadingReasonCode","meterReadingTypeCode","lineItemTypeCode").filter("_RecordDeleted = 0")

    billingdocument_df = GetTable(f"{get_env()}curated.dim.meterconsumptionbillingdocument") \
    .select("meterReadingUnit",col("meterConsumptionBillingDocumentSK").alias("doc_meterConsumptionBillingDocumentSK"),"lastChangedDate").filter("_RecordDeleted = 0")

    devicehistory_df = GetTable(f"{get_env()}curated.dim.devicehistory") \
    .select(col("deviceNumber").alias("history_deviceNumber"),"activityReasonCode","activityReason",col("deviceRemovalDate").alias("history_deviceRemovalDate")) \
    .filter("_RecordDeleted = 0")

    rate_df = GetTable(f"{get_env()}cleansed.isu.zdmt_rate_type") \
    .select("functionClassText","deviceSize","ratetypecode",col("billingClassCode").alias("rate_billingClassCode"),"consumptionPerYear").filter("billingClassCode in ('BUS','RES')").filter("sopaFlag is not null") \
    .withColumn("consumptionPerDay",col("consumptionPerYear")/365).dropDuplicates()

    monthlyapportioned_df = GetTable(f"{get_env()}curated.fact.monthlyapportionedconsumption") \
    .select("firstDayOfMeterActiveMonth","lastDayOfMeterActiveMonth","totalMeterActiveDaysPerMonth",col("billingDocumentNumber").alias("apportioned_billingDocumentNumber"),col("deviceNumber").alias("apportioned_deviceNumber"),"meterActiveMonthStartDate","meterActiveMonthEndDate","billingPeriodStartDate","billingPeriodEndDate")

    # ------------- JOINS ------------------ #

    df = factbilled_df.join(billinglineitem_df, (factbilled_df.meterConsumptionBillingLineItemSK == billinglineitem_df.line_meterConsumptionBillingLineItemSK) & \
        (billinglineitem_df.meterReadingReasonCode == 22) & (billinglineitem_df.lineItemTypeCode == 'ZDQUAN') & (factbilled_df.meteredWaterConsumption == 0) & (factbilled_df.isReversedFlag == 'N'),"inner") \
        .withColumn("unmeteredDays", datediff(billinglineitem_df.validToDate,billinglineitem_df.validFromDate)+1) \
        .withColumn("deviceRemovalDate", billinglineitem_df.validToDate+1) \
        .join(billingdocument_df,(factbilled_df.meterConsumptionBillingDocumentSK == billingdocument_df.doc_meterConsumptionBillingDocumentSK) & (billingdocument_df.meterReadingUnit != 'STANPIPE') & \
        (col("lastChangedDate").isNotNull()), "inner") 

    df = df.join(devicehistory_df, (df.deviceNumber==devicehistory_df.history_deviceNumber) & (col("history_deviceRemovalDate").isNotNull()) & col("activityReasonCode").isin('02','07','08','13') & \
        (df.deviceRemovalDate ==devicehistory_df.history_deviceRemovalDate),"inner") \
        .join(monthlyapportioned_df, (df.billingDocumentNumber == monthlyapportioned_df.apportioned_billingDocumentNumber) & \
        (df.deviceNumber == monthlyapportioned_df.apportioned_deviceNumber),"inner") \
        .join(property_df, (df.propertyNumber == property_df.property_propertyNumber), "inner")
    
    df = df.join(date_df,(df.firstDayOfMeterActiveMonth==date_df.calendarDate),"inner") \
        .join(rate_df,(df.statisticalAnalysisRateTypeCode==rate_df.ratetypecode) & (df.billingClassCode==rate_df.rate_billingClassCode),"inner") \
        .withColumn("consumptionQuantity",df.totalMeterActiveDaysPerMonth * rate_df.consumptionPerDay) \
        .withColumn("calculationDate",trunc(current_date(),'month'))

    df = df.select("propertyNumber","propertySK","deviceNumber","billingDocumentNumber","drinkingWaterNetworkSK","calculationDate","billingPeriodStartDate","billingPeriodEndDate","firstDayOfMeterActiveMonth","lastDayOfMeterActiveMonth","meterActiveMonthStartDate","meterActiveMonthEndDate","totalMeterActiveDaysPerMonth","consumptionQuantity","meterConsumptionBillingDocumentSK").dropDuplicates()

    # ------------- TRANSFORMS ------------- #
    _.Transforms = [
        f"propertyNumber||'|'||firstDayOfMeterActiveMonth||'|'||calculationDate||'|'||deviceNumber||'|'||meterActiveMonthStartDate {BK}"
        ,"propertySK propertySK"
        ,"propertyNumber propertyNumber"
        ,"deviceNumber deviceNumber"        
        ,"drinkingWaterNetworkSK waterNetworkSK"
        ,"calculationDate calculationDate"        
        ,"firstDayOfMeterActiveMonth consumptionDate"
        ,"billingPeriodStartDate billingPeriodStartDate"
        ,"billingPeriodEndDate billingPeriodEndDate"
        ,"firstDayOfMeterActiveMonth firstDayOfMeterActiveMonth"
        ,"lastDayOfMeterActiveMonth lastDayOfMeterActiveMonth"
        ,"meterActiveMonthStartDate meterActiveMonthStartDate"
        ,"meterActiveMonthEndDate meterActiveMonthEndDate"
        ,"totalMeterActiveDaysPerMonth totalMeterActiveDaysPerMonth"
        ,"consumptionQuantity consumptionQuantity"
        ,"billingDocumentNumber billingDocumentNumber"
        ,"meterConsumptionBillingDocumentSK meterConsumptionBillingDocumentSK"
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

# COMMAND ----------

# runflag = spark.sql(f"""
#           select * from (select first_value(calendarDate) over (order by calendarDate asc) as businessDay from {get_env()}curated.dim.date where month(calendarDate) = month(current_date()) and year(calendarDate) = year(current_date()) and isWeekDayFlag = 'Y' and coalesce(isHolidayFlag,'N') = 'N' limit 1) where businessDay = current_date()""")
 
# if runflag.count() > 0:
#     print("Running on first business day of the month")
#     Transform()
# else:
#     # print("Skipping - Runs only on first business day of the month")  
#     dbutils.notebook.exit('{"Counts": {"SpotCount": 0}}')
