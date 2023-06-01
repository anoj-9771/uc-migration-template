# Databricks notebook source
# MAGIC %run ../../Common/common-helpers

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

dbutils.widgets.text("system_code","")

# COMMAND ----------

systemCode = dbutils.widgets.get("system_code")

# COMMAND ----------

df = (
    spark.table("controldb.dbo_extractLoadManifest")
    .where(f"systemCode = '{systemCode}'")
    .withColumn("WatermarkColumnMapped", expr("""
    case  
    when WatermarkColumn like '%M_DATE%' then 'sourceRecordModifiedDateTime'
    when WatermarkColumn like '%EFF_FROM_DT%' then 'effectiveFromDateTime'
    when WatermarkColumn like '%HT_CRT_DT%' then 'sourceRecordCreationDateTime'
    end
    """)
    )
)
dedupeList = ['point_cnfgn','point_limit','rtu','event','iicats_work_orders','tsv','qlty_config','wfp_daily_demand_archive','dly_prof_cnfgn','hierarchy_cnfgn','point_cnfgn','point_limit','scxuser','tsv_point_cnfgn','wkly_prof_cnfgn','scx_facility','site_hierarchy','scx_point']

sourceRecordSystemFilterList = ['dly_prof_cnfgn','hierarchy_cnfgn','scxuser','tsv_point_cnfgn','wkly_prof_cnfgn']

exportConfigFilterList = ['site_hierarchy','scx_facility','scx_point']

dedupQuery = 'dedupe = 1'
filterQuery = 'sourceRecordSystemId in(89,90)'

for i in df.collect():
    partitionKey = i.BusinessKeyColumn.replace(f",{i.WatermarkColumnMapped}","")
    # if in dedup list create a view containing dedupe logic
    # if also in filter list add sys filter
    whereClause = 'where sourceRecordSystemId in(89,90)' if sourceRecordSystemFilterList.count(i.SourceTableName) > 0 else ''
    if dedupeList.count(i.SourceTableName) > 0:        
        sql = (f"""
        create or replace view {get_table_namespace('cleansed', f'{i.DestinationSchema}_{i.DestinationTableName}_current)'} as
        with cteDedup as(
          select *, row_number() over (partition by {partitionKey} order by {i.WatermarkColumnMapped} desc) dedupe
          from {get_table_namespace('cleansed', f'{i.DestinationSchema}_{i.DestinationTableName}')}
          {whereClause}
        )
        select * except(dedupe)
        from cteDedup 
        where dedupe = 1
        """)
    # else create basic view with filter logic if applicable
    else:
        sql = (f"""
        create or replace view {get_table_namespace('cleansed', f'{i.DestinationSchema}_{i.DestinationTableName}_current')} as
        select *
        from {get_table_namespace('cleansed', f'{i.DestinationSchema}_{i.DestinationTableName}')} 
        {whereClause}
        """)
    print(sql)
    spark.sql(sql)
