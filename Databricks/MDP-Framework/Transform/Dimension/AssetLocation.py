# Databricks notebook source
# MAGIC %run ../../Common/common-transform

# COMMAND ----------

def Transform():
    global df
    # ------------- TABLES ----------------- #
    df = GetTable(f"{SOURCE}.maximo_locations").alias('LO')
    locoper_df = GetTable(f"{SOURCE}.maximo_locoper").drop('site') \
    .withColumn("assetLocationShortDescription", expr("LEFT(facility,2)")) \
    .alias('OP')
    location_df = GetTable(f"{SOURCE}.maximo_locations").select(col("location").alias("facility"),col("Description").alias("assetLocationDescription")).alias('FAC')
    swchierarchy_df = GetTable(f"{SOURCE}.maximo_swchierarchy").select("code",col("description").alias("operationalAreaDescription")).alias('HI')
   
    # ------------- JOINS ------------------ #
    
    df = df.join(locoper_df, "location","left") \
    .join(location_df, "facility","left") \
    .join(swchierarchy_df, expr("OP.operationalArea = HI.code"))

    # ------------- TRANSFORMS ------------- #
    _.Transforms = [
        f"location {BK}"
        ,"location assetLocation"
        ,"site site"
        ,"description description"
        ,"facility facility"
        ,"assetLocationShortDescription assetLocationShortDescription"
        ,"assetLocationDescription assetLocationDescription"
        ,"classStructure classStructure"
        ,"type locationType"
        ,"criticality locationCriticality"
        ,"product product"
        ,"status status"
        ,"statusDate statusDate"
        ,"masteredInGis masteredInGIS"
        ,"level locationLevel"
        ,"maintenanceStrategy maintenanceStrategy"
        ,"allowableDowntimeHrs allowableDownTimeHours"
        ,"operationalArea operationalArea"
        ,"operationalAreaDescription operationalAreaDescription"
        ,"operationalGroup operationalGroup"
        
       
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
