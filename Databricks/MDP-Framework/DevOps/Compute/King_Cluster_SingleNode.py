# Databricks notebook source
# MAGIC %run ../../Common/common-workspace

# COMMAND ----------

clusterTemplate = {
    "num_workers": 0,
    "cluster_name": "King_Cluster_SingleNode",
    "spark_version": "12.2.x-scala2.12",
    "spark_conf": {
        "spark.master": "local[*, 4]",
        "spark.databricks.cluster.profile": "singleNode",
        "spark.sql.session.timeZone": "Australia/Sydney",
        "spark.databricks.delta.preview.enabled": "true"
    },
    "azure_attributes": {
        "first_on_demand": 1,
        "availability": "ON_DEMAND_AZURE",
        "spot_bid_max_price": -1
    },
    "node_type_id": "Standard_E32s_v3",
    "driver_node_type_id": "Standard_E32s_v3",
    "ssh_public_keys": [],
    "custom_tags": {
        "product": "Framework ETL 2.0",
        "ResourceClass": "SingleNode"
    },
    "spark_env_vars": {
        "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
    },
    "autotermination_minutes": 10,
    "enable_elastic_disk": "true",
    "init_scripts": [],
    "single_user_name": GetServicePrincipalId(),
    "enable_local_disk_encryption": "false",
    "data_security_mode": "SINGLE_USER",
    "runtime_engine": "STANDARD"
}
libraries = [
    { "maven" : { "coordinates": "com.microsoft.azure:azure-sqldb-spark:1.0.2" } }
]

print(CreateOrEditCluster(clusterTemplate, librariesList=libraries)) if CurrentNotebookName().lower() != spark.conf.get("spark.databricks.clusterUsageTags.clusterName") else print("Cant edit yourself!")
