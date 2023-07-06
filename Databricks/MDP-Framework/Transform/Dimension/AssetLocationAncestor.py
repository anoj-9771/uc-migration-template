# Databricks notebook source
# MAGIC %run ../../Common/common-transform 

# COMMAND ----------

# MAGIC %run ../../Common/common-helpers 

# COMMAND ----------



# COMMAND ----------

TARGET = DEFAULT_TARGET

# COMMAND ----------

# CleanSelf()

# COMMAND ----------

def Transform():
    global df
    # ------------- TABLES ----------------- #
    business_date = "changedDate"
    target_date = "assetLocationAncestorChangedTimestamp"
    locationanc_windowSpec  = Window.partitionBy("location","changedDate_date_part")
    df = get_recent_records(f"{SOURCE}","maximo_locations", business_date, target_date) \
    .withColumn("changedDate_date_part",to_date(col("changedDate"))) \
    .withColumn("sourceValidToTimestamp",lit(expr(f"CAST('{DEFAULT_END_DATE}' AS TIMESTAMP)"))) \
    .withColumn("sourceRecordCurrent",expr("CAST(1 AS INT)")).withColumn("rank",rank().over(locationanc_windowSpec.orderBy(col(business_date).desc()))).filter("rank == 1").drop("rank")
    df = load_sourceValidFromTimeStamp(df,business_date)

    assetLocation_df = GetTable(f"{get_table_namespace(f'{TARGET}', 'dimAssetLocation')}").filter("_recordCurrent == 1").select("assetLocationSK",col("assetLocationName").alias("location"))
    locoper_df = GetTable(get_table_name(f"{SOURCE}","maximo","locoper")).filter("_RecordDeleted == 0")
    ancLocoper_df = GetTable(get_table_name(f"{SOURCE}","maximo","locoper")).filter("_RecordDeleted == 0").select(col("location").alias("ancestorLocation"),col("locoperLevel").alias("ancestorLevel"))
    lochierarchy_df = GetTable(get_table_name(f"{SOURCE}","maximo","lochierarchy")).filter("_RecordDeleted == 0").select(col("location").alias("lochierarchy_location"),"parent","children","lochierarchySystem")
    locancestor_df = GetTable(get_table_name(f"{SOURCE}","maximo","locancestor")).filter("_RecordDeleted == 0").select(col("location").alias("locancestor_location"),"searchLocationHierarchy","locancestorSystem")
    locations_df = GetTable(get_table_name(f"{SOURCE}","maximo","locations")).withColumn("rank",rank().over(Window.partitionBy("location").orderBy(col(business_date).desc()))).filter("rank == 1").drop("rank").filter("_RecordDeleted == 0").select(col("location").alias("ancestorLocation"),col("description").alias("ancestorDescription"))
    
    # ------------- JOINS ------------------ #
    
    df = df.join(assetLocation_df,"location","left") \
    .join(locoper_df,"location","left") \
    .join(lochierarchy_df,df.location==lochierarchy_df.lochierarchy_location,"left") \
    .join(locancestor_df,(df.location == locancestor_df.locancestor_location) & (coalesce(lochierarchy_df.lochierarchySystem, locancestor_df.locancestorSystem) == locancestor_df.locancestorSystem),"left") \
    .join(locations_df,locations_df.ancestorLocation == locancestor_df.searchLocationHierarchy,"left") \
    .join(ancLocoper_df,"ancestorLocation","left") \
    .withColumn("parent", when(lochierarchy_df.parent == locations_df.ancestorLocation, 'Yes').otherwise('No')) 

    df=df.withColumn("locationAncestorSystem",df.locancestorSystem)
    df = df.withColumn("etl_key",concat_ws('|',df.locationAncestorSystem,df.location,df.ancestorLocation, df.changedDate_date_part)) \
    .withColumn("sourceBusinessKey",concat_ws('|',df.locationAncestorSystem,df.location,df.ancestorLocation)) 

    # ------------- TRANSFORMS ------------- #
    _.Transforms = [
        f"etl_key {BK}"
        ,"assetLocationSK assetLocationFK"
        ,"locationAncestorSystem assetLocationAncestorHierarchySystemName"
        ,"location assetLocationIdentifier"
        ,"locoperLevel assetLocationOperationalLevelIdentifer"
        ,"children assetLocationHierarchyChildIndicator"
        ,"description assetLocationHierarchyDescription"
        ,"ancestorLocation assetLocationAncestorName"
        ,"ancestorDescription assetLocationAncestorDescription"
        ,"ancestorLevel assetLocationAncestorLevelCode"
        ,"parent assetParentLocationName"
        ,"changedDate assetLocationAncestorChangedTimestamp"
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
# Updating Business SCD columns for existing records
    try:
        # Select all the records from the existing curated table matching the new records to update the business SCD columns - sourceValidToTimestamp,sourceRecordCurrent.
        existing_data = spark.sql(f"""select * from {get_table_namespace(f'{DEFAULT_TARGET}', f'{TableName}')}""") 
        matched_df = existing_data.join(df.select("assetLocationAncestorHierarchySystemName","assetLocationIdentifier","assetLocationAncestorName",col("sourceValidFromTimestamp").alias("new_changed_date")),["assetLocationAncestorHierarchySystemName","assetLocationIdentifier","assetLocationAncestorName"],"inner")\
        .filter("_recordCurrent == 1").filter("sourceRecordCurrent == 1")

        matched_df =matched_df.withColumn("sourceValidToTimestamp",expr("new_changed_date - INTERVAL 1 SECOND")) \
        .withColumn("sourceRecordCurrent",expr("CAST(0 AS INT)"))

        # Existing location with missing rows also needs to be updated as "sourceRecordCurrent = 0 and sourceValidToTimestamp = ?"
        missing_df = existing_data.join(df.select("assetLocationIdentifier",col("changedDate").alias("new_changed_date")),"assetLocationIdentifier","inner")\
        .filter("_recordCurrent == 1").filter("sourceRecordCurrent == 1")

        missing_df =missing_df.withColumn("sourceValidToTimestamp",expr("NOW() - INTERVAL 1 SECOND")) \
        .withColumn("sourceRecordCurrent",expr("CAST(0 AS INT)"))

        df = df.unionByName(matched_df.selectExpr(df.columns)).unionByName(missing_df.selectExpr(df.columns))
    except Exception as exp:
        print(exp)

#     display(df)
    Save(df)
    #DisplaySelf()
pass
Transform()

# COMMAND ----------

# MAGIC %sql
# MAGIC select Count(1),assetLocationAncestorSK from {get_table_namespace('curated', 'dimAssetLocationAncestor')} GROUP BY assetLocationAncestorSK having count(1)>1

# COMMAND ----------


