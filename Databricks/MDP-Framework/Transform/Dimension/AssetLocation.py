# Databricks notebook source
# MAGIC %run ../../Common/common-transform 

# COMMAND ----------

# MAGIC %run ../../Common/common-helpers 

# COMMAND ----------



# COMMAND ----------

#  CleanSelf()

# COMMAND ----------



# COMMAND ----------

def Transform():
    global df
    # ------------- TABLES ----------------- #
    business_date = "changedDate"
    target_date = "assetLocationChangedTimestamp"
    windowSpec  = Window.partitionBy("location")
    df = get_recent_records(f"{SOURCE}","maximo_locations",business_date,target_date).withColumn("rank",rank().over(windowSpec.orderBy(col(business_date).desc()))).filter("rank == 1").drop("rank").alias('LO')
    df = df \
    .withColumn("sourceBusinessKey",df.location) \
    .withColumn("sourceValidToTimestamp",lit(expr(f"CAST('{DEFAULT_END_DATE}' AS TIMESTAMP)"))) \
    .withColumn("sourceRecordCurrent",expr("CAST(1 AS INT)"))
    df = load_sourceValidFromTimeStamp(df,business_date)
     
    locoper_df = GetTable(get_table_name(f"{SOURCE}","maximo","locoper")).filter("_RecordDeleted == 0").drop('site') \
    .withColumn("assetLocationFacilityShortCode", expr("LEFT(facility,2)")) \
    .alias('OP')
    location_df = GetTable(get_table_name(f"{SOURCE}","maximo","locations")).filter("_RecordDeleted == 0").select(col("location").alias("facility"),col("Description").alias("facilityDescription"),business_date).withColumn("rank",rank().over(Window.partitionBy("facility").orderBy(col(business_date).desc()))).filter("rank == 1").drop("rank", business_date).alias('FAC')
    swchierarchy_df = GetTable(get_table_name(f"{SOURCE}","maximo","swchierarchy")).filter("_RecordDeleted == 0").select("code",col("description").alias("operationalAreaDescription")).alias('HI')

    
   
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
    # Updating Business SCD columns for existing records
    try:
        # Select all the records from the existing curated table matching the new records to update the business SCD columns - sourceValidToTimestamp,sourceRecordCurrent.
        existing_data = spark.sql(f"""select * from {get_table_namespace(f'{DEFAULT_TARGET}', f'{TableName}')}""") 
        matched_df = existing_data.join(df.select("assetLocationName",col("sourceValidFromTimestamp").alias("new_changed_date")),"assetLocationName","inner")\
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
# MAGIC select count(1),assetLocationSK from {get_table_namespace('curated', 'dimassetlocation')} GROUP BY assetLocationSK having count(1) >1

# COMMAND ----------


