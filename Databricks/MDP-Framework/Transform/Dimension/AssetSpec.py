# Databricks notebook source
# MAGIC %run ../../Common/common-transform

# COMMAND ----------

TARGET= DEFAULT_TARGET

# COMMAND ----------

def Transform():
    global df
    # ------------- TABLES ----------------- #
    df = GetTable(f"{SOURCE}.maximo_assetSpec")
    dimAsset_df = GetTable(f"{TARGET}.dimAsset").select("assetSK","assetNumber")
    class_structure_df = GetTable(f"{SOURCE}.maximo_classStructure").select("classStructure",'classification',F.col('description').alias('classPath'))
    classification_df = GetTable(f"{SOURCE}.maximo_classification").select('classification',F.col('description').alias('classificationDescription'))
    measure_unit_df = GetTable(f"{SOURCE}.maximo_measureUnit").select("unitOfMeasure",F.col("description").alias("unitOfMeasureDescription"))
   
    # ------------- JOINS ------------------ #
    
    df = df.join(dimAsset_df,df.asset == dimAsset_df.assetNumber,"left") \
    .join(class_structure_df,"classStructure","left") \
    .join(classification_df,"classification","left") \
    .join(measure_unit_df,"unitOfMeasure","left")

    # ------------- TRANSFORMS ------------- #
    _.Transforms = [
        f"assetSK||'|'||attribute {BK}"
        ,"assetSK assetFK"
        ,"attribute attribute"
        ,"assetSpecID assetSpecID"
        ,"section section"
        ,"alphanumericValue alphaNumericValue"
        ,"classStructure classStructure"
        ,"classification classification"
        ,"classPath classPath"
        ,"classificationDescription classificationDescription"
        ,"unitOfMeasure unitOfMeasure"
        ,"unitOfMeasureDescription unitOfMeasureDescription"
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


