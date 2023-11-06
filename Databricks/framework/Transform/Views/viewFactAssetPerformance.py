# Databricks notebook source
# MAGIC %run ../../Common/common-helpers

# COMMAND ----------

# MAGIC %run ../../Common/common-transform

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {get_table_namespace(f'{DEFAULT_TARGET}', 'viewFactAssetPerformance')} AS
(
   select * except(rownumb) from (
            select *, row_number() over(partition by assetFK order by snapshotDate desc) as rownumb from {get_table_namespace(f'{DEFAULT_TARGET}', 'factAssetPerformance')} where _recordCurrent=1
            )dt where rownumb = 1
)
""")

