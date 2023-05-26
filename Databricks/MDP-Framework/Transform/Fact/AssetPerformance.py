# Databricks notebook source
# MAGIC %run ../../Common/common-transform

# COMMAND ----------

TARGET = DEFAULT_TARGET

# COMMAND ----------

# CleanSelf()

# COMMAND ----------

def Transform():
    global df
    # ------------- TABLES ----------------- #
    w = Window().partitionBy("assetFK")
    df = spark.sql(f"select assetFK, snapshotDate as latest_snapshotDate,assetLocationFK,workOrderTrendDate from (select assetFK, snapshotDate,assetLocationFK,workOrderTrendDate, row_number() over(partition by assetFK order by snapshotDate desc) as rownumb from {TARGET}.factworkorder)dt where rownumb = 1")

    workOrder_df = GetTable(f"{DEFAULT_TARGET}.factworkorder").select("assetFK","snapshotDate","breakdownMaintenanceWorkOrderRepairHour","workOrderCreationId","WorkTypeCode","workOrderClassDescription","workOrderCompliantIndicator","breakdownMaintenanceWorkOrderTargetHour","workOrderStatusDescription","actualWorkOrderLaborCostAmount","actualWorkOrderMaterialCostAmount","actualWorkOrderServiceCostAmount","actualWorkOrderLaborCostFromActivityAmount","actualWorkOrderMaterialCostFromActivityAmount","actualWorkOrderServiceCostFromActivityAmount","workOrderChildIndicator","externalStatusCode") .withColumn("rank",rank().over(w.orderBy(col("snapshotDate").desc()))) \
    .filter("rank == 1").drop("rank") 
    workOrder_df = workOrder_df.withColumnRenamed("assetFK","wo_assetFK")
    assetLocation_df = GetTable(f"{TARGET}.dimAssetLocation").select("assetLocationSK","assetLocationStatusDescription")
    asset_df = GetTable(f"{TARGET}.dimAsset").select("assetSK","assetNetworkLengthPerKilometerValue")

    
    # ------------- JOINS ------------------ #
    
    df = df.join(workOrder_df,(workOrder_df.wo_assetFK == df.assetFK) & (workOrder_df.snapshotDate <= df.latest_snapshotDate), "left") \
    .join(assetLocation_df,df.assetLocationFK == assetLocation_df.assetLocationSK,"left") \
    .join(asset_df,df.assetFK == asset_df.assetSK,"left")   
    
    # ------------- TRANSFORMS ------------- #

    
    df = df.withColumn("breakdownMaintenanceWorkOrderTotalRepairHourQuantity",sum(expr("case when breakdownMaintenanceWorkOrderRepairHour is not null then breakdownMaintenanceWorkOrderRepairHour else 0 end")).over(w)) \
    .withColumn("breakdownMaintenanceTotalWorkOrderCount",sum(expr("case when WorkTypeCode = 'BM' and workOrderClassDescription = 'WORKORDER' then 1 else 0 end")).over(w)) \
    .withColumn("compliantBreakdownMaintenanceWorkOrderCount",sum(expr("case when WorkTypeCode = 'BM' and workOrderClassDescription = 'WORKORDER' and workOrderCompliantIndicator = 'YES' then 1 else 0 end")).over(w)) \
    .withColumn("breakdownMaintenanceWorkOrderTotalTargetHour",sum(expr("case when breakdownMaintenanceWorkOrderTargetHour is not null then breakdownMaintenanceWorkOrderTargetHour else 0 end")).over(w)) \
    .withColumn("preventiveMaintenanceWorkOrderTotalCount",sum(expr("case when WorkTypeCode = 'PM' and workOrderClassDescription = 'WORKORDER' and workOrderCompliantIndicator = 'YES' and workOrderStatusDescription in ('CLOSE','COMP','FINISHED') then 1 else 0 end")).over(w)) \
    .withColumn("breakdownMaintenanceTotalWorkOrderRaisedCount",sum(expr("case when WorkTypeCode = 'BM' and workOrderStatusDescription NOT IN ('CAN', 'CANDUP', 'DRAFT') then 1 else 0 end")).over(w)) \
    .withColumn("breakdownMaintenanceWorkOrderTotalCostAmount",sum(expr("case when WorkTypeCode = 'BM' and workOrderStatusDescription NOT IN ('CAN', 'CANDUP', 'DRAFT') and assetLocationStatusDescription = 'OPERATING' then coalesce(actualWorkOrderLaborCostAmount,0)+coalesce(actualWorkOrderMaterialCostAmount,0)+coalesce(actualWorkOrderServiceCostAmount,0)+coalesce(actualWorkOrderLaborCostFromActivityAmount,0)+coalesce(actualWorkOrderMaterialCostFromActivityAmount,0)+coalesce(actualWorkOrderServiceCostFromActivityAmount,0) else 0 end")).over(w)) \
    .withColumn("correctiveMaintenanceWorkOrderTotalCostAmount",sum(expr("case when WorkTypeCode = 'CM' and workOrderStatusDescription NOT IN ('CAN', 'CANDUP', 'DRAFT') and assetLocationStatusDescription = 'OPERATING' then coalesce(actualWorkOrderLaborCostAmount,0)+coalesce(actualWorkOrderMaterialCostAmount,0)+coalesce(actualWorkOrderServiceCostAmount,0)+coalesce(actualWorkOrderLaborCostFromActivityAmount,0)+coalesce(actualWorkOrderMaterialCostFromActivityAmount,0)+coalesce(actualWorkOrderServiceCostFromActivityAmount,0) else 0 end")).over(w)) \
    .withColumn("preventiveMaintenanceWorkOrderTotalCostAmount",sum(expr("case when WorkTypeCode = 'PM' and workOrderStatusDescription NOT IN ('CAN', 'CANDUP', 'DRAFT') and assetLocationStatusDescription = 'OPERATING' then coalesce(actualWorkOrderLaborCostAmount,0)+coalesce(actualWorkOrderMaterialCostAmount,0)+coalesce(actualWorkOrderServiceCostAmount,0)+coalesce(actualWorkOrderLaborCostFromActivityAmount,0)+coalesce(actualWorkOrderMaterialCostFromActivityAmount,0)+coalesce(actualWorkOrderServiceCostFromActivityAmount,0) else 0 end")).over(w)) \
    .withColumn("preventiveMaintenanceWorkOrderFinishedCount",sum(expr("case when WorkTypeCode = 'PM' and workOrderStatusDescription in ('CLOSE','COMP','FINISHED') and workOrderChildIndicator = 'NO' then 1 else 0 end")).over(w)) \
    .withColumn("correctiveMaintenanceWorkOrderFinishedCount",sum(expr("case when WorkTypeCode = 'CM' and workOrderStatusDescription in ('CLOSE','COMP','FINISHED') then 1 else 0 end")).over(w)) \
    .withColumn("breakdownMaintenanceWorkOrderFailedLengthValue",sum(expr("case when WorkTypeCode = 'BM' and externalStatusCode in ('CAN', 'CANDUP', 'DRAFT') then assetNetworkLengthPerKilometerValue else 0 end")).over(w))

    df = df.withColumn("etl_key",concat_ws('|',df.assetSK,df.assetLocationFK,df.workOrderTrendDate))

    _.Transforms = [
        f"etl_key {BK}"
        ,"assetSK assetFK"
        ,"assetLocationSK assetLocationFK"
        ,"workOrderTrendDate"
        ,"breakdownMaintenanceWorkOrderTotalRepairHourQuantity"
        ,"breakdownMaintenanceTotalWorkOrderCount"
        ,"compliantBreakdownMaintenanceWorkOrderCount"
        ,"breakdownMaintenanceWorkOrderTotalTargetHour"
        ,"preventiveMaintenanceWorkOrderTotalCount"
        ,"breakdownMaintenanceTotalWorkOrderRaisedCount"
        ,"breakdownMaintenanceWorkOrderTotalCostAmount"
        ,"correctiveMaintenanceWorkOrderTotalCostAmount"
        ,"preventiveMaintenanceWorkOrderTotalCostAmount"
        ,"preventiveMaintenanceWorkOrderFinishedCount"
        ,"correctiveMaintenanceWorkOrderFinishedCount"
        ,"breakdownMaintenanceWorkOrderFailedLengthValue"
        ,"workOrderTrendDate snapshotDate"
       
    ]
    df = df.selectExpr(
        _.Transforms
    ).drop_duplicates()
    # ------------- CLAUSES ---------------- #

    # ------------- SAVE ------------------- #
    # display(df)
    # print(df.count())
    Save(df)
    #DisplaySelf()
pass
Transform()

# COMMAND ----------

# MAGIC %sql
# MAGIC Select assetPerformanceSK, count(1) from curated_v3.factAssetPerformance group by assetPerformanceSK having count(1) > 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from curated_v3.factAssetPerformance where assetPerformanceSK is null

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from curated_v3.factworkorder where assetFK = '0f92d140871d227a3d90f8011009c7e1'

# COMMAND ----------

# MAGIC %sql
# MAGIC select location,assetLocationSK, wo.changeDate, loc.sourceValidFromDateTime, loc.sourceValidToDateTime from cleansed.maximo_workorder wo left join curated_v3.dimassetlocation loc on wo.location = loc.assetLocationName  where workOrder = '1535605' 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from curated_v3.dimAsset where assetSK = '0f92d140871d227a3d90f8011009c7e1'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from curated_v3.dimAssetLocation where assetLocationSK = '19bf63f14235eb43ddaf2e2bea639b19'

# COMMAND ----------


