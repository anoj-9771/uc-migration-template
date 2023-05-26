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
    df = get_recent_cleansed_records(f"{SOURCE}","maximo","locations",business_date,target_date).withColumn("rank",rank().over(windowSpec.orderBy(col(business_date).desc()))).filter("rank == 1").drop("rank").alias('LO')
    df = df \
    .withColumn("sourceBusinessKey",df.location) \
    .withColumn("sourceValidToTimestamp",lit(expr(f"CAST('{DEFAULT_END_DATE}' AS TIMESTAMP)"))) \
    .withColumn("sourceRecordCurrent",expr("CAST(1 AS INT)"))
    df = load_sourceValidFromTimeStamp(df,business_date)
     
    locoper_df = GetTable(get_table_name(f"{SOURCE}","maximo","locoper")).drop('site') \
    .withColumn("assetLocationFacilityShortCode", expr("LEFT(facility,2)")) \
    .alias('OP')
    location_df = GetTable(get_table_name(f"{SOURCE}","maximo","locations")).select(col("location").alias("facility"),col("Description").alias("facilityDescription")).alias('FAC')
    swchierarchy_df = GetTable(get_table_name(f"{SOURCE}","maximo","swchierarchy")).select("code",col("description").alias("operationalAreaDescription")).alias('HI')

    
   
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
        ,"level assetLocationPrimaryHierarchyLevelCode"
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
        existing_data = spark.sql(f"""select * from {DEFAULT_TARGET}.{TableName}""") 
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
# MAGIC select count(1),assetLocationSK from curated.dimassetlocation GROUP BY assetLocationSK having count(1) >1

# COMMAND ----------

# MAGIC %sql
# MAGIC drop view if exists curated_v3.dimassetlocation

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace view curated_v3.dimassetlocation As (select * from curated.dimassetlocation)

# COMMAND ----------


