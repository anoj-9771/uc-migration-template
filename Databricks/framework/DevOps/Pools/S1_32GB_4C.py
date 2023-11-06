# Databricks notebook source
# MAGIC %run ../../Common/common-workspace

# COMMAND ----------

template = {
    'instance_pool_name': 'S1_32GB_4C',
    'min_idle_instances': 0,
    'node_type_id': 'Standard_E4ds_v4',
    'custom_tags': {
        'product': 'Framework ETL'
    },
    'idle_instance_autotermination_minutes': 10,
    'enable_elastic_disk': "True",
    'azure_attributes': {
        'availability': 'ON_DEMAND_AZURE'
    },
    'default_tags': {}
}
print(CreateOrEditPool(template))
