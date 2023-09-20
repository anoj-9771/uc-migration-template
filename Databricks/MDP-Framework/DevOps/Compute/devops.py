# Databricks notebook source
# MAGIC %run ../../Common/common-workspace

# COMMAND ----------

template = {
    "num_workers": 1,
    "cluster_name": "devops",
    "spark_version": "13.0.x-scala2.12",
    "spark_conf": {
        "spark.databricks.delta.preview.enabled": "true"
    },
    "azure_attributes": {
        "first_on_demand": 1,
        "availability": "ON_DEMAND_AZURE",
        "spot_bid_max_price": -1
    },
    "node_type_id": "Standard_F4",
    "driver_node_type_id": "Standard_F4",
    "ssh_public_keys": [],
    "custom_tags": {
        "product": "DevOps"
    },
    "spark_env_vars": {
        "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
    },
    "autotermination_minutes": 10,
    "init_scripts": [],
    "single_user_name": GetServicePrincipalId(),
    "enable_local_disk_encryption": False,
    "data_security_mode": "SINGLE_USER",
    "runtime_engine": "STANDARD"
}
libraries = [
]

print(CreateOrEditCluster(template, librariesList=libraries)) 
