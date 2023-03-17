# Databricks notebook source
# MAGIC %run ../../Common/common-transform

# COMMAND ----------

TARGET = DEFAULT_TARGET

# COMMAND ----------

def Transform():
    global df
    # ------------- TABLES ----------------- #
    df = GetTable(f"{SOURCE}.maximo_assetMeter")
    asset_df = GetTable(f"{TARGET}.dimAsset").select("assetSK","assetNumber")
    meter_df = GetTable(f"{SOURCE}.maximo_meter").select("meter",F.col("description").alias("assetMeterDescription"))
    measure_unit_df = GetTable(f"{SOURCE}.maximo_measureUnit").select("unitOfMeasure",F.col("description").alias("unitOfMeasureDescription"))
   
    # ------------- JOINS ------------------ #
    
    df = df.join(meter_df,"meter","left") \
    .join(asset_df,df.asset == asset_df.assetNumber,"left") \
    .join(measure_unit_df,"unitOfMeasure","left")

    # ------------- TRANSFORMS ------------- #
    _.Transforms = [
        f"meter {BK}"
        ,"assetSK assetFK"
        ,"meter assetMeter"
        ,"assetMeterDescription assetMeterDescription"
        ,"active isActive"
        ,"assetMeterId assetMeterId"
        ,"changedBy changedBy"
        ,"changedDate changedDate"
        ,"lastReading lastReading"
        ,"lastReadingDate lastReadingDate"
        ,"lastReadingInspector lastReadingInspector"
        ,"unitOfMeasure unitOfMeasure"
        ,"unitOfMeasureDescription unitOfMeasureDescription"
      
        
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


