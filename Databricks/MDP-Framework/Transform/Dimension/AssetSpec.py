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
    target_date ="assetSpecChangedTimestamp"
    df = get_recent_cleansed_records(f"{SOURCE}","maximo","assetSpec",business_date,target_date)
    dimAsset_df = GetTable(f"{get_table_namespace(f'{TARGET}', 'dimAsset')}").select("assetSK","assetNumber")
    class_structure_df = GetTable(get_table_name(f"{SOURCE}","maximo","classStructure")).select("classStructure",'classification',col('description').alias('classPath'))
    classification_df = GetTable(get_table_name(f"{SOURCE}","maximo","classification")).select('classification',col('description').alias('classificationDescription'))
    measure_unit_df = GetTable(get_table_name(f"{SOURCE}","maximo","measureUnit")).select("unitOfMeasure",col("description").alias("unitOfMeasureDescription"))
   
    # ------------- JOINS ------------------ #
    
    df = df.join(dimAsset_df,df.asset == dimAsset_df.assetNumber,"inner") \
    .join(class_structure_df,"classStructure","left") \
    .join(classification_df,"classification","left") \
    .join(measure_unit_df,"unitOfMeasure","left")

    df =df.withColumn("sourceBusinessKey",concat_ws('|', df.assetSK, df.attribute, df.linearSpecificationId, df.assetspecSection
)) \
    .withColumn("sourceValidToTimestamp",lit(expr(f"CAST('{DEFAULT_END_DATE}' AS TIMESTAMP)"))) \
    .withColumn("sourceRecordCurrent",expr("CAST(1 AS INT)"))
    df = load_sourceValidFromTimeStamp(df,business_date)
    # ------------- TRANSFORMS ------------- #
    _.Transforms = [
        f"sourceBusinessKey {BK}"
        ,"assetSK assetFK"
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
    )
    # ------------- CLAUSES ---------------- #

    # ------------- SAVE ------------------- #
    try:
        # Select all the records from the existing curated table matching the new records to update the business SCD columns - sourceValidToTimestamp,sourceRecordCurrent.
        existing_data = spark.sql(f"""select * from {get_table_namespace(f'{DEFAULT_TARGET}', f'{TableName}')}""") 
        matched_df = existing_data.join(df.select("assetSK", "assetSpecAttributeIdentifier", "assetSpecLinearSpecificationId", "assetSpecSectionCode",col("sourceValidFromTimestamp").alias("new_changed_date")),["assetSK", "assetSpecAttributeIdentifier", "assetSpecLinearSpecificationId", "assetSpecSectionCode"],"inner")\
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
# MAGIC SELECT count(1), AssetSpecSk FROM {get_table_namespace('curated', 'dimassetspec')} GROUP BY assetSpecSK having count(1)> 1

# COMMAND ----------

# DBTITLE 1, 
# MAGIC %sql
# MAGIC create or replace view {get_table_namespace('curated', 'dimassetspec')} AS (select * from {get_table_namespace('curated', 'dimassetspec')})

# COMMAND ----------


