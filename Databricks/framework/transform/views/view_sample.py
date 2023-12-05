# Databricks notebook source
# MAGIC %run ../../Common/common-helpers

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {get_env()}curated.product_name.view1')}
AS
SELECT
col1
,col2
,col3
,col4
FROM
  {get_env()}curated.fact.fact_sample
WHERE col1 = 'filter_condition'
""")
