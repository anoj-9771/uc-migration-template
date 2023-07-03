# Databricks notebook source
# MAGIC %run ../../Common/common-transform 

# COMMAND ----------

# MAGIC %run ../../Common/common-helpers 

# COMMAND ----------



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
    df = get_recent_records(f"{SOURCE}","maximo_locationSpec",business_date,target_date)
    df = df \
    .withColumn("sourceBusinessKey",concat_ws('|',df.attribute,df.location)) \
    .withColumn("sourceValidToTimestamp",lit(expr(f"CAST('{DEFAULT_END_DATE}' AS TIMESTAMP)"))) \
    .withColumn("sourceRecordCurrent",expr("CAST(1 AS INT)"))
    df = load_sourceValidFromTimeStamp(df,business_date)

    asset_location_df = GetTable(f"{get_table_namespace(f'{TARGET}', 'dimAssetLocation')}").select("assetLocationSK","assetLocationName")
    class_structure_df = GetTable(get_table_name(f"{SOURCE}","maximo","classStructure")).select("classStructure",'classification',col('description').alias('classPath'))
    classification_df = GetTable(get_table_name(f"{SOURCE}","maximo","classification")).select('classification',col('description').alias('classificationDescription'))
    measure_unit_df = GetTable(get_table_name(f"{SOURCE}","maximo","measureUnit")).select("unitOfMeasure",col("description").alias("unitOfMeasureDescription"))
   
    # ------------- JOINS ------------------ #
    
    df = df.join(asset_location_df,df.location == asset_location_df.assetLocationName,"left") \
    .join(class_structure_df,"classStructure","left") \
    .join(classification_df,"classification","left") \
    .join(measure_unit_df,"unitOfMeasure","left")

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
    )
    # ------------- CLAUSES ---------------- #

    # ------------- SAVE ------------------- #
    # Updating Business SCD columns for existing records
    try:
        # Select all the records from the existing curated table matching the new records to update the business SCD columns - sourceValidToTimestamp,sourceRecordCurrent.
        existing_data = spark.sql(f"""select * from {get_table_namespace(f'{DEFAULT_TARGET}', f'{TableName}')}""") 
        matched_df = existing_data.join(df.select("locationSpecAttributeIdentifier","locationSpecName",col("sourceValidFromTimestamp").alias("new_changed_date")),["locationSpecAttributeIdentifier","locationSpecName"],"inner")\
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
# MAGIC select count(1), locationSpecSK from {get_table_namespace('curated', 'dimlocationSpec')} group by locationSpecSK having count(1) > 1

# COMMAND ----------


