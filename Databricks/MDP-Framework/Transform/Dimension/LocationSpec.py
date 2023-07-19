# Databricks notebook source
# MAGIC %run ../../Common/common-transform 

# COMMAND ----------

TARGET= DEFAULT_TARGET

# COMMAND ----------

# CleanSelf()

# COMMAND ----------

def Transform():
    global df
    # ------------- TABLES ----------------- #
    business_date = 'changedDate'
    target_date = "locationSpecChangedTimestamp"
    windowSpec = Window.partitionBy("location","attribute")
    df = get_recent_records(f"{SOURCE}","maximo_locationSpec",business_date,target_date)\
        .withColumn("rank",rank().over(windowSpec.orderBy(col("rowStamp").desc()))).filter("rank == 1").drop("rank")
    
    asset_location_df = GetTable(f"{get_table_namespace(f'{TARGET}', 'dimAssetLocation')}")\
        .filter("_recordCurrent == 1").filter("_recordDeleted == 0")\
        .select("assetLocationSK","assetLocationName","sourceValidFromTimestamp","sourceValidToTimestamp")
    
    class_structure_df = GetTable(get_table_name(f"{SOURCE}","maximo","classStructure"))\
        .filter("_RecordDeleted == 0")\
        .withColumn("rank",rank().over(Window.partitionBy("classStructure").orderBy(col("rowStamp").desc()))).filter("rank == 1")\
        .select("classStructure",'classification',col('description').alias('classPath'))
    
    classification_df = GetTable(get_table_name(f"{SOURCE}","maximo","classification"))\
        .filter("_RecordDeleted == 0")\
        .withColumn("rank",rank().over(Window.partitionBy("classification").orderBy(col("rowStamp").desc()))).filter("rank == 1")\
        .select('classification',col('description').alias('classificationDescription'))
    
    measure_unit_df = GetTable(get_table_name(f"{SOURCE}","maximo","measureUnit"))\
        .filter("_RecordDeleted == 0")\
        .withColumn("rank",rank().over(Window.partitionBy("unitOfMeasure").orderBy(col("rowStamp").desc()))).filter("rank == 1")\
        .select("unitOfMeasure",col("description").alias("unitOfMeasureDescription"))
   
    # ------------- JOINS ------------------ #
    
    df = df.join(asset_location_df,(df.location == asset_location_df.assetLocationName) & (df.changedDate.between (asset_location_df.sourceValidFromTimestamp,asset_location_df.sourceValidToTimestamp)),"left").drop("sourceValidFromTimestamp","sourceValidToTimestamp") \
    .join(class_structure_df,"classStructure","left") \
    .join(classification_df,"classification","left") \
    .join(measure_unit_df,"unitOfMeasure","left")

    df = df \
    .withColumn("sourceBusinessKey",concat_ws('|',df.attribute,df.location)) \
    .withColumn("sourceValidToTimestamp",lit(expr(f"CAST('{DEFAULT_END_DATE}' AS TIMESTAMP)"))) \
    .withColumn("sourceRecordCurrent",expr("CAST(1 AS INT)"))
    df = load_sourceValidFromTimeStamp(df,business_date)


    # ------------- TRANSFORMS ------------- #
    _.Transforms = [
        f"sourceBusinessKey {BK}"
        ,"assetLocationSK assetLocationFK"
        ,"attribute locationSpecAttributeIdentifier"
        ,"location locationSpecName"
        ,"locationSpecSection locationSpecAttributeSectionName"
        ,"alphanumericValue locationSpecText"
        ,"numericValue locationSpecNumericValue"
        ,"unitOfMeasure locationSpecUnitOfMeasureName"
        ,"unitOfMeasureDescription locationSpecAttributeUnitOfMeasureDescription"
        ,"classification locationSpecNodeClassificationName"
        ,"classPath locationSpecNodeClassificationPath"
        ,"classificationDescription locationSpecNodeClassificationDescription"
        ,"changedBy locationSpecChangedByUserName"
        ,"changedDate locationSpecChangedTimestamp"
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
