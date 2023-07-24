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
    target_date ="assetSpecChangedTimestamp"
    df = get_recent_records(f"{SOURCE}","maximo_assetSpec",business_date,target_date)\
        .withColumn("rank",rank().over(Window.partitionBy("asset","attribute","linearSpecificationId","assetspecSection").orderBy(col("rowStamp").desc()))).filter("rank == 1")\

    asset_df = GetTable(f"{get_table_namespace(f'{TARGET}', 'dimAsset')}")\
        .filter("_recordCurrent == 1").filter("_recordDeleted == 0")\
        .select("assetNumber")

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
    
    df = df.join(asset_df,df.asset == asset_df.assetNumber,"inner") \
    .join(class_structure_df,"classStructure","left") \
    .join(classification_df,"classification","left") \
    .join(measure_unit_df,"unitOfMeasure","left")

    df =df.withColumn("sourceBusinessKey",concat_ws('|', df.assetNumber, df.attribute, df.linearSpecificationId, df.assetspecSection
)) \
    .withColumn("sourceValidToTimestamp",lit(expr(f"CAST('{DEFAULT_END_DATE}' AS TIMESTAMP)"))) \
    .withColumn("sourceRecordCurrent",expr("CAST(1 AS INT)"))
    df = load_sourceValidFromTimeStamp(df,business_date)
    # ------------- TRANSFORMS ------------- #
    _.Transforms = [
        f"sourceBusinessKey {BK}"
        ,"assetNumber"
        ,"attribute assetSpecAttributeIdentifier"
        ,"linearSpecificationId assetSpecLinearSpecificationId"
        ,"assetspecSection assetSpecSectionCode"
        ,"assetSpecID assetSpecIdentifier"
        ,"alphanumericValue assetSpecText"
        ,"classStructure assetSpecClassStructureIdentifier"
        ,"classification assetSpecNodeClassificationName"
        ,"classPath assetSpecNodeClassificationPathName"
        ,"classificationDescription assetSpecNodeClassificationDescription"
        ,"unitOfMeasure assetSpecAttributeUnitOfMeasureName"
        ,"unitOfMeasureDescription assetSpecAttributeUnitOfMeasureDescription"
        ,"changedBy assetSpecChangedByUserName"
        ,"changedDate assetSpecChangedTimestamp"
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
