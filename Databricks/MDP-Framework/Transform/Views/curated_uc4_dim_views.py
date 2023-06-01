# Databricks notebook source
# MAGIC %run ../../Common/common-helpers

# COMMAND ----------

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
        new_table_namespace = get_table_namespace(f'{target_schema}', f'{table}')
        spark.sql(f"""CREATE OR REPLACE VIEW {new_table_namespace} AS SELECT * FROM {new_table_namespace} where _recordCurrent = 1""")
        print(f"*******VIEW Created {new_table_namespace}")
    except Exception as e:
        print(f"*******VIEW Creation FAIL {new_table_namespace} Error: {e}")
        pass
  

# COMMAND ----------


