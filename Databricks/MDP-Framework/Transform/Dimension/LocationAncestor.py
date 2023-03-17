# Databricks notebook source
# MAGIC %run ../../Common/common-transform

# COMMAND ----------

TARGET = DEFAULT_TARGET

# COMMAND ----------

def Transform():
    global df
    # ------------- TABLES ----------------- #
    df = GetTable(f"{SOURCE}.maximo_locations")
    assetLocation_df = GetTable(f"{TARGET}.dimAssetLocation").filter("_recordCurrent == 1").select("assetLocationSK",col("assetLocation").alias("location"))
    locoper_df = GetTable(f"{SOURCE}.maximo_locoper")
    ancLocoper_df = GetTable(f"{SOURCE}.maximo_locoper").select(col("location").alias("ancestorLocation"),col("level").alias("ancestorLevel"))
    lochierarchy_df = GetTable(f"{SOURCE}.maximo_lochierarchy")
    locancestor_df = GetTable(f"{SOURCE}.maximo_locancestor")
    locations_df = GetTable(f"{SOURCE}.maximo_locations").select(col("location").alias("ancestorLocation"),col("description").alias("ancestorDescription"))
    
    # ------------- JOINS ------------------ #
    
    df = df.join(assetLocation_df,"location","left") \
    .join(locoper_df,"location","left") \
    .join(lochierarchy_df,"location","left") \
    .join(locancestor_df,"location","left") \
    .join(locations_df,locations_df.ancestorLocation == locancestor_df.searchLocationHierarchy,"left") \
    .join(ancLocoper_df,"ancestorLocation","left") \
    .withColumn("parent", when(lochierarchy_df.parent == locations_df.ancestorLocation, 'Yes').otherwise('No'))

    # ------------- TRANSFORMS ------------- #
    _.Transforms = [
        f"location||'|'||ancestorLocation {BK}"
        ,"assetLocationSK assetLocationFK"
        ,"location location"
        ,"level level"
        ,"children hasChildren"
        ,"description locationDescription"
        ,"ancestorLocation ancestorLocation"
        ,"ancestorDescription ancestorDescription"
        ,"ancestorLevel ancestorLevel"
        ,"parent parent"
       
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


