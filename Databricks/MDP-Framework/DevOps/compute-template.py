# Databricks notebook source
# MAGIC %run ../Common/common-workspace

# COMMAND ----------

libraryTemplate = {
    "libraries": [
        { "maven" : { "coordinates": "com.microsoft.azure:azure-sqldb-spark:1.0.2" } }
        ,{ "maven" : { "coordinates": "com.databricks:spark-xml_2.12:0.15.0" } }
        #,{ "maven" : { "coordinates": "com.crealytics:spark-excel_2.12:3.1.2_0.16.5-pre1" } }
        #,{ "jar" : "dbfs:/FileStore/jars/edda63ff_ead1_4e79_8aff_fc35161ab4eb-azure_cosmos_spark_3_1_2_12_4_8_0-53136.jar" }
    ]
}

# COMMAND ----------

clusterTemplate = {
    "autoscale": {
        "min_workers": 1,
        "max_workers": 1
    },
    "cluster_name": "interactive-sp",
    "spark_version": "12.2.x-scala2.12",
    "spark_conf": {
        "spark.databricks.libraries.enableMavenResolution": "false",
        "spark.sql.session.timeZone": "Australia/Sydney",
        "spark.databricks.delta.preview.enabled": "true"
    },
    "azure_attributes": {},
    "ssh_public_keys": [],
    "custom_tags": {
        "product" : "Data Engineering Team"
    },
    "spark_env_vars": {
    },
    "autotermination_minutes": 40,
    "init_scripts": [],
    "instance_pool_id": GetPoolIdByName("pool-small"),
    "single_user_name": GetServicePrincipalId(),
    "data_security_mode": "SINGLE_USER",
    "runtime_engine": "PHOTON"
}
#print(CreateOrEditCluster(clusterTemplate, librariesList=libraryTemplate))

# COMMAND ----------

clusterTemplate = {
    "autoscale": {
        "min_workers": 1,
        "max_workers": 1
    },
    "cluster_name": "interactive-acl",
    "spark_version": "12.2.x-scala2.12",
    "spark_conf": {
        "spark.sql.session.timeZone": "Australia/Sydney",
        "spark.databricks.delta.preview.enabled": "true"
    },
    "azure_attributes": {},
    "ssh_public_keys": [],
    "custom_tags": {
    },
    "spark_env_vars": {
    },
    "autotermination_minutes": 20,
    "init_scripts": [],
    "instance_pool_id": GetPoolIdByName("pool-small"),
    "driver_instance_pool_id": GetPoolIdByName("pool-small"),
    "runtime_engine": "PHOTON"
}

# COMMAND ----------

clusterTemplate = {
    "autoscale": {
        "min_workers": 1,
        "max_workers": 1
    },
    "cluster_name": "interactive-o3bj",
    "spark_version": "12.2.x-scala2.12",
    "spark_conf": {
    },
    "azure_attributes": {},
    "ssh_public_keys": [],
    "custom_tags": {},
    "spark_env_vars": {
    },
    "autotermination_minutes": 20,
    "init_scripts": [],
    "instance_pool_id": GetPoolIdByName("pool-small"),
    "single_user_name": "o3bj@sydneywater.com.au",
    "driver_instance_pool_id": GetPoolIdByName("pool-small"),
    "data_security_mode": "SINGLE_USER",
    "runtime_engine": "PHOTON"
}
#print(CreateOrEditCluster(clusterTemplate))

# COMMAND ----------

sqlWarehouseTemplate = {
    "name": "Data Analysts - SWC",
    "cluster_size" : "X-Small",
    "min_num_clusters": 1,
    "max_num_clusters": 2,
    "auto_stop_mins": 20,
    "tags": {
    },
    "spot_instance_policy":"COST_OPTIMIZED",
    "enable_photon": "true",
    "enable_serverless_compute": "false",
    "channel": {
        "name": "CHANNEL_NAME_CURRENT"
    }
}
#CreateSqlWarehouse(sqlWarehouseTemplate)

# COMMAND ----------

poolSmall = {
    "instance_pool_name": "pool-small",
    "node_type_id": "Standard_DS3_v2",
    "min_idle_instances": 0,
    "max_capacity": 10,
    "idle_instance_autotermination_minutes": 20,
    "azure_attributes": {
        "availability": "SPOT_AZURE",
        "spot_bid_max_price": -1.0
    }
}
poolMedium = {
    "instance_pool_name": "pool-medium",
    "node_type_id": "Standard_D4as_v5",
    #"node_type_id": "Standard_DS4_v2",
    "min_idle_instances": 0,
    "max_capacity": 4,
    "idle_instance_autotermination_minutes": 10,
    "preloaded_spark_versions": [
        "10.4.x-photon-scala2.12"
    ],
    "azure_attributes": {
        "availability": "SPOT_AZURE",
        "availability": "SPOT_WITH_FALLBACK_AZURE",
        #"availability": "ON_DEMAND_AZURE",
        "spot_bid_max_price": -1.0
    }
}
poolLarge = {
    "instance_pool_name": "pool-large",
    "node_type_id": "Standard_DS5_v2",
    "min_idle_instances": 0,
    "max_capacity": 2,
    "idle_instance_autotermination_minutes": 10,
    "azure_attributes": {
        "availability": "SPOT_AZURE",
        "spot_bid_max_price": -1.0
    }
}
