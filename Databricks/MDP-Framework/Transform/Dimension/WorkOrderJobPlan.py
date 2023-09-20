# Databricks notebook source
# MAGIC %run ../../Common/common-transform 

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

    #-----------ENSURE NO DUPLICATES---------#
    windowSpec  = Window.partitionBy("jobPlan","revision")
    df = df.withColumn("rank",rank().over(windowSpec.orderBy(col("rowStamp").desc()))).filter("rank == 1").drop("rank")

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
    
#     display(df)
    Save(df)
    #DisplaySelf()
pass
Transform()
