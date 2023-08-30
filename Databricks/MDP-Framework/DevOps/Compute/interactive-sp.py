# Databricks notebook source
# MAGIC %run ../../Common/common-workspace

# COMMAND ----------

template = {
    "autoscale": {
        "min_workers": 1,
        "max_workers": 4
    },
    "cluster_name": "interactive-sp",
    "spark_version": "13.3.x-scala2.12",
    "spark_conf": {
        "spark.sql.session.timeZone": "Australia/Sydney",
        "spark.databricks.libraries.enableMavenResolution": "false",
        "spark.databricks.delta.preview.enabled": "true"
    },
    "azure_attributes": {
        "first_on_demand": 1,
        "availability": "ON_DEMAND_AZURE",
        "spot_bid_max_price": -1
    },
    "node_type_id": "Standard_E4ds_v4",
    "driver_node_type_id": "Standard_E4ds_v4",
    "ssh_public_keys": [],
    "custom_tags": {
        "product": "EDP Framework 2.0",
        "PythonUDF.enabled": "true"
    },
    "spark_env_vars": {
        "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
    },
    "autotermination_minutes": 60,
    "enable_elastic_disk": "true",
    "init_scripts": [],
    "single_user_name": GetServicePrincipalId(),
    "enable_local_disk_encryption": False,
    "data_security_mode": "SINGLE_USER",
    "runtime_engine": "PHOTON"
}
libraries = [
    { "maven" : { "coordinates": "com.databricks:spark-xml_2.12:0.15.0" } }
    ,{ "maven" : { "coordinates": "com.microsoft.azure:azure-sqldb-spark:1.0.2" } }
]

print(CreateOrEditCluster(template, librariesList=libraries))

# COMMAND ----------


