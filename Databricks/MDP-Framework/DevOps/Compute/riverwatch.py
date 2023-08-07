# Databricks notebook source
# MAGIC %run ../../Common/common-workspace

# COMMAND ----------

clusterTemplate = {
    "num_workers": 2,
    "cluster_name": "riverwatch",
    "spark_version": "12.2.x-cpu-ml-scala2.12",
    "spark_conf": {
        "spark.databricks.delta.preview.enabled": "true"
    },
    "azure_attributes": {
        "first_on_demand": 1,
        "availability": "ON_DEMAND_AZURE",
        "spot_bid_max_price": -1
    },
    "node_type_id": "Standard_DS3_v2",
    "driver_node_type_id": "Standard_DS3_v2",
    "ssh_public_keys": [],
    "custom_tags": {
        "product": "Riverwatch"
    },
    "spark_env_vars": {
        "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
    },
    "autotermination_minutes": 30,
    "enable_elastic_disk": True,
    "init_scripts": [],
    "single_user_name": GetServicePrincipalId(),
    "enable_local_disk_encryption": False,
    "data_security_mode": "SINGLE_USER",
    "runtime_engine": "STANDARD"
}
libraries = [
        { "pypi": { "package": "mlflow==1.30.0" } }
        ,{ "pypi": { "package": "pybbn==3.2.1" } }
    ]

print(CreateOrEditCluster(clusterTemplate, librariesList=libraries))
