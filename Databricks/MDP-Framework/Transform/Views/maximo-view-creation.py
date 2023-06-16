# Databricks notebook source
# DBTITLE 1,Input system code if run manually. sequence is cleansed views followed by curated views
# MAGIC %run ../../Common/common-helpers

# COMMAND ----------

from pyspark.sql.functions import *
dbutils.widgets.text("system_code","maximo")

# COMMAND ----------

systemCode = dbutils.widgets.get("system_code")

# COMMAND ----------

dedupeList = ('RELATEDRECORD', 'PM', 'PERSONGROUP', 'PERSONGROUPTEAM', 'WORKORDER', 'ASSET', 'LOCATIONS')

# COMMAND ----------

#Cleansed view SR
spark.sql(f"""
CREATE OR REPLACE VIEW {get_table_namespace('cleansed', 'maximo_viewmaximosr')} AS
SELECT
  *
FROM
  {get_table_namespace('cleansed', 'maximo_ticket')}
WHERE
  ticketClass IN (
    SELECT
      synonymdomainValue
    FROM
      {get_table_namespace('cleansed', 'maximo_synonymdomain')}
    WHERE
      synonymdomainDomain = 'TKCLASS'
      AND internalValue = 'SR'
  )
""")

# COMMAND ----------

#Cleansed view WOACTIVITY
spark.sql(f"""
CREATE OR REPLACE VIEW {get_table_namespace('cleansed', 'maximo_viewmaximowoactivity')} AS
SELECT
  *
FROM
  {get_table_namespace('cleansed', 'maximo_workorder')}
WHERE
  workorderClass IN (
    SELECT
      synonymdomainValue
    from
      {get_table_namespace('cleansed', 'maximo_synonymdomain')}
    WHERE
      synonymdomainDomain = 'WOCLASS'
      AND internalValue = 'ACTIVITY'
  )
""")

# COMMAND ----------

#Curated view WOACTIVITY
spark.sql(f"""
CREATE OR REPLACE VIEW {get_table_namespace('cleansed', 'maximo_viewmaximowoactivitycurrent')} as
        with cteDedup as(
          select *, row_number() over (partition by site,workOrder order by rowStamp desc) dedupe
          from {get_table_namespace('cleansed', 'maximo_viewmaximowoactivity')}
        )
        select * EXCEPT (dedupe)
        from cteDedup 
        where dedupe = 1
""")        

# COMMAND ----------

df = (
    spark.table("controldb.dbo_extractLoadManifest")
    .filter(f"systemCode like '%{systemCode}%'")
    .filter(f"SourceTableName in {dedupeList}")
    .withColumn("WatermarkColumnMapped", expr("""
    case  
    when WatermarkColumn like '%rowstamp%' then 'rowStamp'
    end
    """)
    )
)

display(df)

# COMMAND ----------

dedupQuery = 'dedupe = 1'
excludedColumns = 'dedupe'

for i in df.collect():
    partitionKey = i.BusinessKeyColumn.replace(f",{i.WatermarkColumnMapped}","").replace(f"{i.WatermarkColumnMapped},","")
    # if in dedup list create a view containing dedupe logic
    if dedupeList.count(i.SourceTableName) > 0:
        whereClause = 'where dedupe = 1'
        sql = (f"""
        create or replace view {get_table_namespace('cleansed', f'{i.DestinationSchema}_{i.DestinationTableName}current')} as
        with cteDedup as(
          select *, row_number() over (partition by {partitionKey} order by {i.WatermarkColumnMapped} desc) dedupe
          from {get_table_namespace('cleansed', f'{i.DestinationSchema}_{i.DestinationTableName}')}
        )
        select * EXCEPT ({excludedColumns})
        from cteDedup 
        {whereClause}
        """)
    print(sql)
    spark.sql(sql)
