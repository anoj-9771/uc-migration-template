# Databricks notebook source
# MAGIC %run ../../Common/common-workspace

# COMMAND ----------

clusterTemplate = {
    "num_workers": 6,
    "cluster_name": "sewer-cctv",
    "spark_version": "10.4.x-cpu-ml-scala2.12",
    "spark_conf": {
        "spark.databricks.delta.preview.enabled": "true"
    },
    "azure_attributes": {
        "first_on_demand": 1,
        "availability": "ON_DEMAND_AZURE",
        "spot_bid_max_price": -1
    },
    "node_type_id": "Standard_DS5_v2",
    "ssh_public_keys": [],
    "custom_tags": {
        "product": "sewer cctv application"
    },    
    "spark_env_vars": {},
    "autotermination_minutes": 30,
    "enable_elastic_disk": True,
    "init_scripts": [],
    "enable_local_disk_encryption": False,
    "runtime_engine": "STANDARD"
}
libraries = [
        { "maven" : { "coordinates": "com.microsoft.azure:synapseml_2.12:0.9.5" } }
        ,{ "whl": "dbfs:/mnt/blob-sewercctvmodel/MainModel-0.1.0-py3-none-any.whl" }
        ,{ "pypi": { "package": "mlflow==1.27.0" } }
        ,{ "pypi": { "package": "opencv-python==4.6.0.66" } }
    ]

print(CreateOrEditCluster(clusterTemplate, librariesList=libraries))
print(UpdateClusterPermissionByName(clusterTemplate["cluster_name"], [{ "group_name": "AI/ML-Team", "permission_level": "CAN_RESTART" }]))
