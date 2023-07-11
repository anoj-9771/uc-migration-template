# Databricks notebook source
# MAGIC %run ../../Common/common-transform 

# COMMAND ----------

  # CleanSelf()

# COMMAND ----------

TARGET = DEFAULT_TARGET

# COMMAND ----------

def Transform():
    global df
    # ------------- TABLES ----------------- #
    df = GetTable(f"{get_table_namespace(f'{SOURCE}', 'maximo_failurereport')}")\
        .filter("_RecordDeleted == 0")\
        .withColumn("rank",rank().over(Window.partitionBy("failureReportId","workOrder").orderBy(col("rowStamp").desc()))).filter("rank == 1").drop("rank")

    factworkorder_df = GetTable(f"{get_table_namespace(f'{TARGET}', 'factWorkOrder')}")\
        .filter("_recordCurrent == 1").filter("_recordDeleted == 0")\
        .select("workOrderCreationId","workOrderSK","assetFK","workOrderChangeTimestamp")\
        .withColumn("changedDate_date_part",to_date(col("workOrderChangeTimestamp")))
    
    failureCode_df = GetTable(get_table_name(f"{SOURCE}","maximo","failureCode"))\
        .filter("_RecordDeleted == 0")\
        .withColumn("rank",rank().over(Window.partitionBy("failureCode").orderBy(col("rowStamp").desc()))).filter("rank == 1")\
        .select("failureCode",col("description").alias("failureDescription"))
    
    # ------------- JOINS ------------------ #
    df = df.join(failureCode_df,"failureCode", "left") \
    .join(factworkorder_df,df.workOrder == factworkorder_df.workOrderCreationId,"inner") 
   
    # ------------- TRANSFORMS ------------- #
    _.Transforms = [
        f"failureReportId||'|'||changedDate_date_part {BK}"
        ,"failureReportId workOrderFailureReportId"
        ,"workOrderChangeTimestamp workOrderFailureReportChangeTimestamp"
        ,"assetFK"
        ,"workOrderSK workOrderFK"
        ,"ticket workOrderFailureReportTicketIdentifier"
        ,"ticketClass workOrderFailureReportTicketClass"
        ,"failureCode workOrderFailureReportCode"
        ,"failureDescription workOrderFailureReportDescription"
        ,"line workOrderFailureReportListId"
        ,"type workOrderFailureReportType"
        ,"rowStamp workOrderFailureReportRowIdentifer"
        ,"changedDate_date_part snapshotDate"
       
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

# COMMAND ----------


