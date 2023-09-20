# Databricks notebook source
# MAGIC %run ../../Common/common-transform 

# COMMAND ----------

#  CleanSelf()

# COMMAND ----------

def Transform():
    global df
    # ------------- TABLES ----------------- #
    business_date = "changedDate"
    target_date = "assetLocationChangedTimestamp"
    windowSpec  = Window.partitionBy("location")

    df = get_recent_records(f"{SOURCE}","maximo_locations",business_date,target_date)\
        .withColumn("rank",rank().over(windowSpec.orderBy(col("rowStamp").desc()))).filter("rank == 1")\
        .drop("rank").alias('LO')
    
    df = df \
        .withColumn("sourceBusinessKey",df.location) \
        .withColumn("sourceValidToTimestamp",lit(expr(f"CAST('{DEFAULT_END_DATE}' AS TIMESTAMP)"))) \
        .withColumn("sourceRecordCurrent",expr("CAST(1 AS INT)"))
    
    df = load_sourceValidFromTimeStamp(df,business_date)
     
    locoper_df = GetTable(get_table_name(f"{SOURCE}","maximo","locoper"))\
        .filter("_RecordDeleted == 0")\
        .withColumn("rank",rank().over(windowSpec.orderBy(col("rowStamp").desc()))).filter("rank == 1")\
        .drop("rank","site","rowStamp") \
        .withColumn("assetLocationFacilityShortCode", expr("LEFT(facility,2)")) \
        .alias('OP')
    
    location_df = GetTable(get_table_name(f"{SOURCE}","maximo","locations"))\
        .filter("_RecordDeleted == 0")\
        .withColumn("rank",rank().over(windowSpec.orderBy(col("rowStamp").desc()))).filter("rank == 1")\
        .select(col("location").alias("facility"),\
            col("Description").alias("facilityDescription"))\
        .alias('FAC')


    swchierarchy_df = GetTable(get_table_name(f"{SOURCE}","maximo","swchierarchy"))\
        .filter("_RecordDeleted == 0")\
        .withColumn("rank",rank().over(Window.partitionBy("code").orderBy(col("rowStamp").desc()))).filter("rank == 1")\
        .select("code",col("description").alias("operationalAreaDescription")).alias('HI')

    
   
    # ------------- JOINS ------------------ #
    
    df = df.join(locoper_df, "location","left") \
    .join(location_df, "facility","left") \
    .join(swchierarchy_df, expr("OP.operationalArea = HI.code"),"left")

    # ------------- TRANSFORMS ------------- #
    _.Transforms = [
        f"sourceBusinessKey {BK}"
        ,"location assetLocationName"
        ,"site assetLocationSiteIdentifier"
        ,"description assetLocationDescription"
        ,"assetLocationFacilityShortCode"
        ,"owner assetLocationOwnerName"
        ,"facility assetLocationFacilityCode"
        ,"facilityDescription assetLocationFacilityDescription"
        ,"classStructure assetLocationClassStructureIdentifier"
        ,"type assetLocationTypeCode"
        ,"criticality assetLocationCriticalPriorityCode"
        ,"product assetLocationProductTypeName"
        ,"status assetLocationStatusDescription"
        ,"statusDate assetLocationStatusTimestamp"
        ,"masteredInGis assetLocationMasteredInGISIndicator"
        ,"locoperLevel assetLocationPrimaryHierarchyLevelCode"
        ,"maintenanceStrategy assetMaintenanceStrategyCode"
        ,"allowableDowntimeHrs assetLocationMaxDowntimeAllowableHourQuantity"
        ,"operationalArea assetLocationOperationalAreaCode"
        ,"operationalAreaDescription assetLocationOperationalAreaDescription"
        ,"operationalGroup assetLocationOperationalGroupCode"
        ,"changedDate assetLocationChangedTimestamp"
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
