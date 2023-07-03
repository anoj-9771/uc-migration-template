# Databricks notebook source
# MAGIC %run ../../Common/common-transform 

# COMMAND ----------

# MAGIC %run ../../Common/common-helpers 

# COMMAND ----------



# COMMAND ----------

#  CleanSelf()

# COMMAND ----------

def Transform():
    global df
    # ------------- TABLES ----------------- #
    business_date = "changedDate"
    target_date = "workOrderJobPlanChangedTimestamp"
    df = get_recent_records(f"{SOURCE}","maximo_jobplan",business_date,target_date)
    df = df \
    .withColumn("sourceBusinessKey",concat_ws('|', df.jobPlan, df.revision)) \
    .withColumn("sourceValidToTimestamp",lit(expr(f"CAST('{DEFAULT_END_DATE}' AS TIMESTAMP)"))) \
    .withColumn("sourceRecordCurrent",expr("CAST(1 AS INT)"))
    df = load_sourceValidFromTimeStamp(df,business_date)
    # ------------- JOINS ------------------ #
    
    

    # ------------- TRANSFORMS ------------- #
    _.Transforms = [
        f"sourceBusinessKey {BK}"
        ,"jobPlan workOrderJobPlanNumber"
        ,"revision workOrderJobPlanRevisionNumber"
        ,"site workOrderJobSiteIdentifier"
        ,"organization workOrderJobPlanOrganizationName"
        ,"masterJobPlan workOrderMasterJobPlanName"
        ,"jobPlanId workOrderJobPlanIdentifier"
        ,"classStructure workOrderJobPlanClassStructureId"
        ,"duration workOrderJobPlanDurationHoursQuantity"
        ,"description workOrderJobPlanTaskDescription"
        ,"woPriority workOrderJobPlanWorkOrderPriorityCode"
        ,"product workOrderJobPlanWaterProductIdentifier"
        ,"workType workOrderJobPlanWorkOrderWorkTypeCode"
        ,"taskCode workOrderJobPlanParentTaskCode" 
        ,"serviceType workOrderJobPlanServiceTypeIdentifier" 
        ,"status workOrderJobPlanStatusCode"
        ,"cloneType workOrderJobPlanCloneTypeIdentifier"
        ,"jobPlanLevel workOrderJobPlanSkillLevelNumber"
        ,"changedBy workOrderJobPlanChangedByUserName"
        ,"changedDate workOrderJobPlanChangedTimestamp"
        ,"sourceValidFromTimestamp"
        ,"sourceValidToTimestamp"
        ,"sourceRecordCurrent"
        ,"sourceBusinessKey"
    ]
    df = df.selectExpr(
        _.Transforms
    )
    # ------------- CLAUSES ---------------- #

    # ------------- SAVE ------------------- #
    # Updating Business SCD columns for existing records
    try:
        # Select all the records from the existing curated table matching the new records to update the business SCD columns - sourceValidToTimestamp,sourceRecordCurrent.
        existing_data = spark.sql(f"""select * from {get_table_namespace(f'{DEFAULT_TARGET}', f'{TableName}')}""") 
        matched_df = existing_data.join(df.select("workOrderJobPlanNumber","workOrderJobPlanRevisionNumber",col("sourceValidFromTimestamp").alias("new_change_date")),["workOrderJobPlanNumber","workOrderJobPlanRevisionNumber"],"inner")\
        .filter("_recordCurrent == 1").filter("sourceRecordCurrent == 1")

        matched_df =matched_df.withColumn("sourceValidToTimestamp",expr("new_change_date - INTERVAL 1 SECOND")) \
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
# MAGIC select count(1),workOrderJobPlanSK from {get_table_namespace('curated', 'dimWorkOrderJobPlan')} GROUP BY workOrderJobPlanSK having count(1)>1

# COMMAND ----------


