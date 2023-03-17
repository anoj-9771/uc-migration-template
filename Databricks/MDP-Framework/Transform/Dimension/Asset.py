# Databricks notebook source
# MAGIC %run ../../Common/common-transform

# COMMAND ----------

TARGET = DEFAULT_TARGET

# COMMAND ----------

def Transform():
    global df
    # ------------- TABLES ----------------- #
    df = GetTable(f"{SOURCE}.maximo_asset")
    asset_location_df = GetTable(f"{TARGET}.dimAssetLocation").select(col("assetLocation").alias("location"),"assetLocationSK","locationType")
    class_structure_df = GetTable(f"{SOURCE}.maximo_classStructure").select("classStructure","classification",col("description").alias("classificationPath"))
    classification_df = GetTable(f"{SOURCE}.maximo_classification").select("classification",col("description").alias("classificationDescription"))
    
    

    # ------------- JOINS ------------------ #
    
    df = df.join(asset_location_df,"location","left") \
    .join(class_structure_df,"classStructure","left") \
    .join(classification_df,"classification","left") \
    .withColumn("linearFlag",when(df.masteredInGis == 'Y','Y').when(asset_location_df.locationType == "SYSAREA",'Y').otherwise('N'))

    # ------------- TRANSFORMS ------------- #
    _.Transforms = [
        f"asset {BK}"
        ,"assetLocationSK assetLocationFK"
        ,"asset assetNumber"
        ,"description assetDescription"
        ,"status assetStatus"
        ,"masteredInGis masteredInGIS"
        ,"cmelCode cmelCode"
        ,"installationDate installationDate"
        ,"mainAsset mainAsset"
        ,"maintenanceStrategy maintenanceStrategy"
        ,"serviceContract serviceContract"
        ,"serviceType serviceType"
        ,"classification classification"
        ,"classificationPath classificationPath"
        ,"linearFlag linearFlag"
      
        
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
