# Databricks notebook source
# MAGIC %run ../../Common/common-transform

# COMMAND ----------

TARGET = DEFAULT_TARGET

# COMMAND ----------

# CleanSelf()

# COMMAND ----------

def Transform():
    global df
    # ------------- TABLES ----------------- #
    business_date = "changedDate"
    target_date = "assetMeterChangeTimestamp"
    df = get_recent_cleansed_records(f"{SOURCE}","maximo","assetMeter",business_date,target_date) \
    .withColumn("sourceValidToTimestamp",lit(expr(f"CAST('{DEFAULT_END_DATE}' AS TIMESTAMP)"))) \
    .withColumn("sourceRecordCurrent",expr("CAST(1 AS INT)"))
    df = load_sourceValidFromTimeStamp(df,business_date)

    asset_df = GetTable(f"{TARGET}.dimAsset").select("assetSK","assetNumber")
    meter_df = GetTable(get_table_name(f"{SOURCE}","maximo","meter")).select("meter",col("description").alias("assetMeterDescription"))
    measure_unit_df = GetTable(get_table_name(f"{SOURCE}","maximo","measureUnit")).select("unitOfMeasure",col("description").alias("unitOfMeasureDescription"))
   
    # ------------- JOINS ------------------ #
    
    df = df.join(meter_df,"meter","left") \
    .join(asset_df,df.asset == asset_df.assetNumber,"inner") \
    .join(measure_unit_df,"unitOfMeasure","left")

    df = df \
    .withColumn("sourceBusinessKey",concat_ws('|',df.assetSK, df.meter)) 

    # ------------- TRANSFORMS ------------- #
    _.Transforms = [
        f"sourceBusinessKey {BK}"
        ,"assetSK assetFK"
        ,"meter assetMeterName"
        ,"assetMeterDescription assetMeterDescription"
        ,"active assetMeterActiveIndicator"
        ,"assetMeterId assetMeterIdentifier"
        ,"changedBy assetMeterChangedByUserName"
        ,"changedDate assetMeterChangeTimestamp"
        ,"lastReading assetMeterLastReadingQuantity"
        ,"lastReadingDate assetMeterLastReadingTimestamp"
        ,"lastReadingInspector assetMeterLastReadingInspectorCode"
        ,"unitOfMeasure assetMeterUnitOfMeasureName"
        ,"unitOfMeasureDescription assetMeterUnitOfMeasureDescription"
        ,"sourceValidFromTimestamp"
        ,"sourceValidToTimestamp"
        ,"sourceRecordCurrent"
        ,"sourceBusinessKey"
      
        
    ]
    df = df.selectExpr(
        _.Transforms
    )
    # ------------- CLAUSES ---------------- #

    # ------------- SAVE ------------------- #

    # Updating Business SCD columns for existing records
    try:
        # Select all the records from the existing curated table matching the new records to update the business SCD columns - sourceValidToTimestamp,sourceRecordCurrent.
        existing_data = spark.sql(f"""select * from {DEFAULT_TARGET}.{TableName}""") 
        matched_df = existing_data.join(df.select("assetFK","assetMeterName",col("sourceValidFromTimestamp").alias("new_changed_date")),["assetFK","assetMeterName"],"inner")\
        .filter("_recordCurrent == 1").filter("sourceRecordCurrent == 1")

        matched_df =matched_df.withColumn("sourceValidToTimestamp",expr("new_changed_date - INTERVAL 1 SECOND")) \
        .withColumn("sourceRecordCurrent",expr("CAST(0 AS INT)"))

        df = df.unionByName(matched_df.selectExpr(df.columns))
    except Exception as exp:
        print(exp)
        
#     display(df)

    Save(df)
    #DisplaySelf()
pass
Transform()

# COMMAND ----------

# MAGIC %sql
# MAGIC select assetMeterSK, count(1) from curated.dimassetmeter group by assetMeterSK having count(1) >1

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace view curated_v3.dimassetmeter AS (select * from curated.dimassetmeter)

# COMMAND ----------


