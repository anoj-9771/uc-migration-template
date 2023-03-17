# Databricks notebook source
# MAGIC %run ../../Common/common-transform

# COMMAND ----------

TARGET= DEFAULT_TARGET

# COMMAND ----------

def Transform():
    global df
    # ------------- TABLES ----------------- #
    df = GetTable(f"{SOURCE}.maximo_locationSpec")
    asset_location_df = GetTable(f"{TARGET}.dimAssetLocation").select("assetLocationSK","assetLocation")
    class_structure_df = GetTable(f"{SOURCE}.maximo_classStructure").select("classStructure",'classification',col('description').alias('classPath'))
    classification_df = GetTable(f"{SOURCE}.maximo_classification").select('classification',col('description').alias('classificationDescription'))
    measure_unit_df = GetTable(f"{SOURCE}.maximo_measureUnit").select("unitOfMeasure",col("description").alias("unitOfMeasureDescription"))
   
    # ------------- JOINS ------------------ #
    
    df = df.join(asset_location_df,df.location == asset_location_df.assetLocation,"left") \
    .join(class_structure_df,"classStructure","left") \
    .join(classification_df,"classification","left") \
    .join(measure_unit_df,"unitOfMeasure","left")

    # ------------- TRANSFORMS ------------- #
    _.Transforms = [
        f"assetLocationSK||'|'||attribute {BK}"
        ,"assetLocationSK assetLocationFK"
        ,"attribute attribute"
        ,"section section"
        ,"alphanumericValue alphaNumericValue"
        ,"numericValue numericValue"
        ,"unitOfMeasure unitOfMeasure"
        ,"unitOfMeasureDescription unitOfMeasureDescription"
        ,"classification classification"
        ,"classPath classPath"
        ,"classificationDescription classificationDescription"
        ,"changedBy changedBy"
        ,"changedDate changedDate"
    ]
    df = df.selectExpr(
        _.Transforms
    )
    # ------------- CLAUSES ---------------- #

    # ------------- SAVE ------------------- #
#     display(df)
    # CleanSelf()
    Save(df)
    #DisplaySelf()
pass
Transform()

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(1), locationSpecSK from curated_v2.dimlocationSpec group by locationSpecSK having count(1) > 1

# COMMAND ----------


