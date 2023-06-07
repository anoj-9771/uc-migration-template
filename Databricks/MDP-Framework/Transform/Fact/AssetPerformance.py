# Databricks notebook source
# MAGIC %run ../../Common/common-transform

# COMMAND ----------

CleanSelf()

# COMMAND ----------

workorderTotalCount_df = spark.sql(f""" 
select 
wo.assetFK,
wo.assetLocationFK,
dd.calendardate as snapshotDate,
'workorderTotalCount' as assetPerformanceMeasureName,
COUNT (DISTINCT workOrderCreationId) as assetPerformanceMeasureValue
FROM {get_table_namespace('curated','viewfactworkorder')} wo

INNER JOIN {get_table_namespace("curated_v3","dimdate")} dd
on wo.workOrderFinishedDate = dd.calendardate

LEFT JOIN
(
select 
'PM' as workOrderWorkTypeCode,
'YES' as workOrderChildIndicator
) dt
on wo.workOrderWorkTypeCode = dt.workOrderWorkTypeCode
and wo.workOrderChildIndicator = dt.workOrderChildIndicator

where dt.workOrderWorkTypeCode is NULL
and wo.workOrderStatusDescription IN ('CLOSE','COMP','FINISHED')
and wo.workOrderClassDescription = 'WORKORDER'
group by
wo.assetFK,
wo.assetLocationFK,
dd.calendardate
""")

# COMMAND ----------

breakdownMaintenanceWorkOrderTotalRepairHourQuantity_df = spark.sql(f"""
select 
wo.assetFK,
wo.assetLocationFK,
dd.calendardate as snapshotDate,
'breakdownMaintenanceWorkOrderTotalRepairHourQuantity' as assetPerformanceMeasureName,
SUM(wo.breakdownMaintenanceWorkOrderRepairHour) as assetPerformanceMeasureValue
FROM {get_table_namespace('curated','viewfactworkorder')} wo
INNER JOIN {get_table_namespace("curated_v3","dimdate")} dd
on wo.workOrderFinishedDate = dd.calendardate

WHERE wo.workOrderClassDescription = 'WORKORDER'
and wo.workOrderWorkTypeCode = 'BM'

group by
wo.assetFK,
wo.assetLocationFK,
dd.calendardate
""")

# COMMAND ----------

breakdownMaintenanceTotalWorkOrderCount_df = spark.sql(f"""
select 
wo.assetFK,
wo.assetLocationFK,
dd.calendardate as snapshotDate,
'breakdownMaintenanceTotalWorkOrderCount' as assetPerformanceMeasureName,
count( DISTINCT wo.workOrderCreationId) as assetPerformanceMeasureValue
FROM {get_table_namespace('curated','viewfactworkorder')} wo
INNER JOIN {get_table_namespace("curated_v3","dimdate")} dd
on wo.workOrderFinishedDate = dd.calendardate

WHERE wo.workOrderClassDescription = 'WORKORDER'
and wo.workOrderWorkTypeCode = 'BM'

group by
wo.assetFK,
wo.assetLocationFK,
dd.calendardate
""")

# COMMAND ----------

compliantBreakdownMaintenanceWorkOrderCount_df = spark.sql(f"""
select 
wo.assetFK,
wo.assetLocationFK,
dd.calendardate as snapshotDate,
'compliantBreakdownMaintenanceWorkOrderCount' as assetPerformanceMeasureName,
count( DISTINCT wo.workOrderCreationId) as assetPerformanceMeasureValue
FROM {get_table_namespace('curated','viewfactworkorder')} wo
INNER JOIN {get_table_namespace("curated_v3","dimdate")} dd
on wo.workOrderFinishedDate = dd.calendardate

WHERE wo.workOrderClassDescription = 'WORKORDER'
and wo.workOrderWorkTypeCode = 'BM'
and wo.workOrderCompliantIndicator = 'YES'

group by
wo.assetFK,
wo.assetLocationFK,
dd.calendardate
""")

# COMMAND ----------

breakdownMaintenanceWorkOrderTotalTargetHour_df = spark.sql(f"""
select 
wo.assetFK,
wo.assetLocationFK,
dd.calendardate as snapshotDate,
'breakdownMaintenanceWorkOrderTotalTargetHour' as assetPerformanceMeasureName,
SUM(wo.breakdownMaintenanceWorkOrderTargetHour) as assetPerformanceMeasureValue
FROM {get_table_namespace('curated','viewfactworkorder')} wo
INNER JOIN {get_table_namespace("curated_v3","dimdate")} dd
on wo.workOrderFinishedDate = dd.calendardate

WHERE wo.workOrderClassDescription = 'WORKORDER'
and wo.workOrderWorkTypeCode = 'BM'

group by
wo.assetFK,
wo.assetLocationFK,
dd.calendardate""")

# COMMAND ----------

preventiveMaintenanceWorkOrderTotalCount_df = spark.sql(f"""select 
wo.assetFK,
wo.assetLocationFK,
dd.calendardate as snapshotDate,
'preventiveMaintenanceWorkOrderTotalCount' as assetPerformanceMeasureName,
count( DISTINCT wo.workOrderCreationId) as assetPerformanceMeasureValue
FROM {get_table_namespace('curated','viewfactworkorder')} wo
INNER JOIN {get_table_namespace("curated_v3","dimdate")} dd
on wo.workOrderFinishedDate = dd.calendardate

WHERE wo.workOrderClassDescription = 'WORKORDER'
and wo.workOrderWorkTypeCode = 'PM'
and wo.workOrderChildIndicator = 'NO'
and wo.workOrderStatusDescription in ('COMP','CLOSE','FINISHED')

group by
wo.assetFK,
wo.assetLocationFK,
dd.calendardate""")

# COMMAND ----------

breakdownMaintenanceTotalWorkOrderRaisedCount_df = spark.sql(f"""select 
wo.assetFK,
wo.assetLocationFK,
dd.calendardate as snapshotDate,
'breakdownMaintenanceTotalWorkOrderRaisedCount' as assetPerformanceMeasureName,
count( DISTINCT wo.workOrderCreationId) as assetPerformanceMeasureValue
FROM {get_table_namespace('curated','viewfactworkorder')} wo
INNER JOIN {get_table_namespace("curated_v3","dimdate")} dd
on wo.workOrderTrendDate = dd.calendardate

WHERE wo.workOrderWorkTypeCode = 'BM'
and wo.workOrderStatusDescription in ('CAN','CANDUP','DRAFT')

group by
wo.assetFK,
wo.assetLocationFK,
dd.calendardate""")

# COMMAND ----------

breakdownMaintenanceWorkOrderTotalCostAmount_df = spark.sql(f"""SELECT
wo.assetFK,
wo.assetLocationFK,
dd.calendardate as snapshotDate,
'breakdownMaintenanceWorkOrderTotalCostAmount' as assetPerformanceMeasureName,
SUM(nvl(wo.actualWorkOrderLaborCostAmount,0)+
nvl(wo.actualWorkOrderMaterialCostAmount,0)+
nvl(wo.actualWorkOrderServiceCostAmount,0)+
nvl(wo.actualWorkOrderLaborCostFromActivityAmount,0)+
nvl(wo.actualWorkOrderMaterialCostFromActivityAmount,0)+
nvl(wo.actualWorkOrderServiceCostFromActivityAmount,0)) as assetPerformanceMeasureValue 
FROM {get_table_namespace('curated','viewfactworkorder')} wo

INNER JOIN {get_table_namespace("curated_v3","dimdate")} dd
on wo.workOrderFinishedDate = dd.calendardate

INNER JOIN {get_table_namespace("curated","dimAssetLocation")} alo 
on wo.assetLocationFK = alo.assetLocationSK

WHERE alo.assetLocationStatusDescription = 'OPERATING' 
and wo.workOrderStatusDescription NOT in ('CAN', 'CANDUP', 'DRAFT')
and wo.workOrderWorkTypeCode = 'BM'

group by 
wo.assetFK,
wo.assetLocationFK,
dd.calendardate""")

# COMMAND ----------

correctiveMaintenanceWorkOrderTotalCostAmount_df = spark.sql(f"""SELECT
wo.assetFK,
wo.assetLocationFK,
dd.calendardate as snapshotDate,
'correctiveMaintenanceWorkOrderTotalCostAmount' as assetPerformanceMeasureName,
SUM(nvl(wo.actualWorkOrderLaborCostAmount,0)+
nvl(wo.actualWorkOrderMaterialCostAmount,0)+
nvl(wo.actualWorkOrderServiceCostAmount,0)+
nvl(wo.actualWorkOrderLaborCostFromActivityAmount,0)+
nvl(wo.actualWorkOrderMaterialCostFromActivityAmount,0)+
nvl(wo.actualWorkOrderServiceCostFromActivityAmount,0)) as assetPerformanceMeasureValue 
FROM {get_table_namespace('curated','viewfactworkorder')} wo

INNER JOIN {get_table_namespace("curated_v3","dimdate")} dd
on wo.workOrderFinishedDate = dd.calendardate

INNER JOIN {get_table_namespace("curated","dimAssetLocation")} alo 
on wo.assetLocationFK = alo.assetLocationSK

WHERE alo.assetLocationStatusDescription = 'OPERATING' 
and wo.workOrderStatusDescription NOT in ('CAN', 'CANDUP', 'DRAFT')
and wo.workOrderWorkTypeCode = 'CM'

group by 
wo.assetFK,
wo.assetLocationFK,
dd.calendardate""")

# COMMAND ----------

preventiveMaintenanceWorkOrderTotalCostAmount_df = spark.sql(f"""SELECT
wo.assetFK,
wo.assetLocationFK,
dd.calendardate as snapshotDate,
'preventiveMaintenanceWorkOrderTotalCostAmount' as assetPerformanceMeasureName,
SUM(nvl(wo.actualWorkOrderLaborCostAmount,0)+
nvl(wo.actualWorkOrderMaterialCostAmount,0)+
nvl(wo.actualWorkOrderServiceCostAmount,0)+
nvl(wo.actualWorkOrderLaborCostFromActivityAmount,0)+
nvl(wo.actualWorkOrderMaterialCostFromActivityAmount,0)+
nvl(wo.actualWorkOrderServiceCostFromActivityAmount,0)) as assetPerformanceMeasureValue
FROM {get_table_namespace('curated','viewfactworkorder')} wo

INNER JOIN {get_table_namespace("curated_v3","dimdate")} dd
on wo.workOrderFinishedDate = dd.calendardate

INNER JOIN {get_table_namespace("curated","dimAssetLocation")} alo 
on wo.assetLocationFK = alo.assetLocationSK

WHERE alo.assetLocationStatusDescription = 'OPERATING' 
and wo.workOrderStatusDescription NOT in ('CAN', 'CANDUP', 'DRAFT')
and wo.workOrderWorkTypeCode = 'PM'

group by 
wo.assetFK,
wo.assetLocationFK,
dd.calendardate""")






# COMMAND ----------

preventiveMaintenanceWorkOrderFinishedCount_df = spark.sql(f"""select 
wo.assetFK,
wo.assetLocationFK,
dd.calendardate as snapshotDate,
'preventiveMaintenanceWorkOrderFinishedCount' as assetPerformanceMeasureName,
count( DISTINCT wo.workOrderCreationId) as assetPerformanceMeasureValue
FROM {get_table_namespace('curated','viewfactworkorder')} wo
INNER JOIN {get_table_namespace("curated_v3","dimdate")} dd
on wo.workOrderFinishedDate = dd.calendardate

WHERE
wo.workOrderWorkTypeCode = 'PM' and
wo.workOrderChildIndicator = 'NO' and
wo.workOrderStatusDescription in ('CLOSE','COMP','FINISHED')

group by
wo.assetFK,
wo.assetLocationFK,
dd.calendardate""")

# COMMAND ----------

correctiveMaintenanceWorkOrderFinishedCount_df = spark.sql(f"""select 
wo.assetFK,
wo.assetLocationFK,
dd.calendardate as snapshotDate,
'correctiveMaintenanceWorkOrderFinishedCount' as assetPerformanceMeasureName,
count( DISTINCT wo.workOrderCreationId) as assetPerformanceMeasureValue
FROM {get_table_namespace('curated','viewfactworkorder')} wo
INNER JOIN {get_table_namespace("curated_v3","dimdate")} dd
on wo.workOrderFinishedDate = dd.calendardate

WHERE
wo.workOrderWorkTypeCode = 'CM' and
wo.workOrderStatusDescription in ('CLOSE','COMP','FINISHED')

group by
wo.assetFK,
wo.assetLocationFK,
dd.calendardate""")

# COMMAND ----------

breakdownMaintenanceWorkOrderFailedLengthValue_df = spark.sql(f"""select 
wo.assetFK,
wo.assetLocationFK,
dd.calendardate as snapshotDate,
'breakdownMaintenanceWorkOrderFailedLengthValue' as assetPerformanceMeasureName,
SUM(da.assetNetworkLengthPerKilometerValue) as assetPerformanceMeasureValue
FROM {get_table_namespace('curated','viewfactworkorder')} wo
INNER JOIN {get_table_namespace("curated_v3","dimdate")} dd
on wo.workOrderFinishedDate = dd.calendardate

INNER JOIN {get_table_namespace('curated','dimasset')} da
on wo.assetFK = da.assetSK
and da.sourceRecordCurrent = 1

WHERE wo.workOrderWorkTypeCode = 'BM' 
and wo.workOrderStatusDescription not in ('CAN', 'CANDUP', 'DRAFT')

group by 
wo.assetFK,
wo.assetLocationFK,
dd.calendardate""")

# COMMAND ----------

workorderBacklogCount_df = spark.sql(f"""select 
wo.assetFK,
wo.assetLocationFK,
dd.calendardate as snapshotDate,
'workorderBacklogCount'  as assetPerformanceMeasureName,
COUNT (DISTINCT workOrderCreationId) as assetPerformanceMeasureValue
FROM {get_table_namespace('curated','viewfactworkorder')} wo

INNER JOIN {get_table_namespace("curated_v3","dimdate")} dd
on wo.workOrderTolerancedDueDate = dd.calendardate

LEFT JOIN
(
select 
'PM' as workOrderWorkTypeCode,
'YES' as workOrderChildIndicator
) dt
on wo.workOrderWorkTypeCode = dt.workOrderWorkTypeCode
and wo.workOrderChildIndicator = dt.workOrderChildIndicator

where dt.workOrderWorkTypeCode is NULL
and wo.workOrderStatusDescription NOT IN ('CAN','CANDUP','DRAFT')
and (workOrderFinishedDate is NULL or workOrderFinishedDate>workOrderTolerancedDueDate)
group by 
wo.assetFK,
wo.assetLocationFK,
dd.calendardate
""")

# COMMAND ----------

from functools import reduce
from pyspark.sql import DataFrame

# COMMAND ----------

def Transform():
    global df
    # ------------- TABLES ----------------- #
    
    

    dfs = [workorderTotalCount_df, 
           breakdownMaintenanceWorkOrderTotalRepairHourQuantity_df, 
           breakdownMaintenanceTotalWorkOrderCount_df, 
           compliantBreakdownMaintenanceWorkOrderCount_df, 
           breakdownMaintenanceWorkOrderTotalTargetHour_df, 
           preventiveMaintenanceWorkOrderTotalCount_df, 
           breakdownMaintenanceTotalWorkOrderRaisedCount_df, 
           breakdownMaintenanceWorkOrderTotalCostAmount_df, 
           correctiveMaintenanceWorkOrderTotalCostAmount_df, 
           preventiveMaintenanceWorkOrderTotalCostAmount_df, 
           preventiveMaintenanceWorkOrderFinishedCount_df, 
           correctiveMaintenanceWorkOrderFinishedCount_df, 
           breakdownMaintenanceWorkOrderFailedLengthValue_df, 
           workorderBacklogCount_df]
    df = reduce(DataFrame.union, dfs)

    
    # ------------- JOINS ------------------ #
    
    
    # ------------- TRANSFORMS ------------- #

    df = df.na.drop(subset=["assetFK"]) \
        .withColumn("etl_key",concat_ws('|',df.assetFK,df.assetLocationFK,df.snapshotDate, df.assetPerformanceMeasureName))

    _.Transforms = [
        f"etl_key {BK}"
        ,"assetFK"
        ,"assetLocationFK"
        ,"snapshotDate"
        ,"assetPerformanceMeasureName"
        ,"assetPerformanceMeasureValue"
    ]
    df = df.selectExpr(
        _.Transforms
    ).drop_duplicates()
    # ------------- CLAUSES ---------------- #

    # ------------- SAVE ------------------- #
    # display(df)
    Save(df)
    #DisplaySelf()
pass
Transform()

# COMMAND ----------


