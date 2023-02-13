# Databricks notebook source
from pyspark.sql.functions import *
dbutils.widgets.text("system_code","")

# COMMAND ----------

systemCode = dbutils.widgets.get("system_code")

# COMMAND ----------

dedupeList = ('RELATEDRECORD', 'PM', 'PERSONGROUP', 'PERSONGROUPTEAM', 'WOACTIVITY', 'WORKORDER')

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

# COMMAND ----------

# SR View
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

#WOACTIVITY View
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
