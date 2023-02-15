# Databricks notebook source
# DBTITLE 1,Input system code if run manually. sequence is cleansed views followed by curated views
from pyspark.sql.functions import *
dbutils.widgets.text("system_code","")

# COMMAND ----------

systemCode = dbutils.widgets.get("system_code")

# COMMAND ----------

dedupeList = ('RELATEDRECORD', 'PM', 'PERSONGROUP', 'PERSONGROUPTEAM', 'WORKORDER')

# COMMAND ----------

#Cleansed view SR
spark.sql("""
CREATE OR REPLACE VIEW cleansed.vw_maximo_sr AS
SELECT
  *
FROM
  cleansed.maximo_ticket
WHERE
  class IN (
    SELECT
      value
    FROM
      cleansed.maximo_synonymdomain
    WHERE
      domain = 'TKCLASS'
      AND internalValue = 'SR'
  )
""")

# COMMAND ----------

#Cleansed view WOACTIVITY
spark.sql("""
CREATE OR REPLACE VIEW cleansed.vw_maximo_woactivity AS
SELECT
  *
FROM
  cleansed.maximo_workorder
WHERE
  class IN (
    SELECT
      value
    from
      cleansed.maximo_synonymdomain
    WHERE
      domain = 'WOCLASS'
      AND internalValue = 'ACTIVITY'
  )
""")

# COMMAND ----------

#Curated view WOACTIVITY
spark.sql("""
CREATE OR REPLACE VIEW curated.vw_maximo_WOACTIVITY as
        with cteDedup as(
          select *, row_number() over (partition by site,workOrder order by rowStamp desc) dedupe
          from cleansed.vw_maximo_WOACTIVITY
        )
        select * EXCEPT (dedupe)
        from cteDedup 
        where dedupe = 1
""")        

# COMMAND ----------

df = (
    spark.table("controldb.dbo_extractLoadManifest")
    .filter(f"systemCode = '{systemCode}'")
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

for i in df.rdd.collect():
    partitionKey = i.BusinessKeyColumn.replace(f",{i.WatermarkColumnMapped}","")
    # if in dedup list create a view containing dedupe logic
    if dedupeList.count(i.SourceTableName) > 0:
        whereClause = 'where dedupe = 1'
        sql = (f"""
        create or replace view curated.vw_{i.DestinationSchema}_{i.DestinationTableName} as
        with cteDedup as(
          select *, row_number() over (partition by {partitionKey} order by {i.WatermarkColumnMapped} desc) dedupe
          from cleansed.{i.DestinationSchema}_{i.DestinationTableName}
        )
        select * EXCEPT ({excludedColumns})
        from cteDedup 
        {whereClause}
        """)
    print(sql)
    spark.sql(sql)
