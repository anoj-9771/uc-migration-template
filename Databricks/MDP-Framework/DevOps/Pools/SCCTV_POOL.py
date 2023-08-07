# Databricks notebook source
# MAGIC %run ../../Common/common-workspace

# COMMAND ----------

template = {
    "instance_pool_name": "SCCTV_POOL",
    "min_idle_instances": 0,
    "max_capacity": 12,
    "node_type_id": "Standard_DS5_v2",
    "idle_instance_autotermination_minutes": 10,
    "enable_elastic_disk": True,
    "preloaded_spark_versions": [
        "10.4.x-cpu-ml-scala2.12"
    ],
    "azure_attributes": {
        "availability": "ON_DEMAND_AZURE"
    },
    "custom_tags": {
        "product" : "sewer cctv application",
    }
}
print(CreateOrEditPool(template))

# COMMAND ----------


