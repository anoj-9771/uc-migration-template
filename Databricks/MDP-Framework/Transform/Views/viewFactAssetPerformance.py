# Databricks notebook source
# MAGIC %run ../../Common/common-transform

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {DEFAULT_TARGET}.viewFactAssetPerformance AS
(
   select * except(rownumb) from (
            select *, row_number() over(partition by assetFK order by snapshotDate desc) as rownumb from {DEFAULT_TARGET}.factAssetPerformance where _recordCurrent=1
            )dt where rownumb = 1
)
""")

