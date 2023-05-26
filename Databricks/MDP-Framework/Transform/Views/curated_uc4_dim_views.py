# Databricks notebook source
table_list = [
'dimAssetContract'
,'dimAssetLocationAncestor'
,'dimAssetLocation'
,'dimAssetMeter'
,'dimAssetSpec'
,'dimAsset'
,'dimLocationSpec'
,'dimWorkOrderJobPlan'
,'dimWorkOrderProblemType'
]

# COMMAND ----------

target_schema = "curated"

# COMMAND ----------

for table in table_list:
    try:
        spark.sql(f"""CREATE OR REPLACE VIEW {target_schema}.view{table} AS SELECT * FROM {target_schema}.{table} where _recordCurrent = 1""")
        print(f"*******VIEW Created {target_schema}.{table}")
    except Exception as e:
        print(f"*******VIEW Creation FAIL {target_schema}.{table} Error: {e}")
        pass
  

# COMMAND ----------


