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
    df = get_recent_records(f"{SOURCE}","maximo_assetMeter",business_date,target_date) \
    .withColumn("rank",rank().over(Window.partitionBy("asset","meter").orderBy(col("rowStamp").desc()))).filter("rank == 1")

    asset_df = GetTable(f"{get_table_namespace(f'{TARGET}', 'dimAsset')}")\
        .filter("_recordCurrent == 1").filter("_recordDeleted == 0")\
        .select("assetNumber")
        
    meter_df = GetTable(get_table_name(f"{SOURCE}","maximo","meter"))\
        .filter("_RecordDeleted == 0")\
        .withColumn("rank",rank().over(Window.partitionBy("meter").orderBy(col("rowStamp").desc()))).filter("rank == 1")\
        .select("meter",col("description").alias("assetMeterDescription"))
    
    measure_unit_df = GetTable(get_table_name(f"{SOURCE}","maximo","measureUnit"))\
        .filter("_RecordDeleted == 0")\
        .withColumn("rank",rank().over(Window.partitionBy("unitOfMeasure").orderBy(col("rowStamp").desc()))).filter("rank == 1")\
        .select("unitOfMeasure",col("description").alias("unitOfMeasureDescription"))
   
    # ------------- JOINS ------------------ #
    
    df = df.join(meter_df,"meter","left") \
    .join(asset_df,df.asset == asset_df.assetNumber,"inner") \
    .join(measure_unit_df,"unitOfMeasure","left")

    df = df \
    .withColumn("sourceBusinessKey",concat_ws('|',df.assetNumber, df.meter))\
    .withColumn("sourceValidToTimestamp",lit(expr(f"CAST('{DEFAULT_END_DATE}' AS TIMESTAMP)"))) \
    .withColumn("sourceRecordCurrent",expr("CAST(1 AS INT)"))
    df = load_sourceValidFromTimeStamp(df,business_date)

    # ------------- TRANSFORMS ------------- #
    _.Transforms = [
        f"sourceBusinessKey {BK}"
        ,"assetNumber"
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
    ).drop_duplicates()
    # ------------- CLAUSES ---------------- #

    # ------------- SAVE ------------------- #

  
#     display(df)

    Save(df)
    #DisplaySelf()
pass
Transform()
