# Databricks notebook source
import requests
import json
from ast import literal_eval
from pyspark.sql.functions import *

# COMMAND ----------

INSTANCE_NAME = "https://australiaeast.azuredatabricks.net"
SECRET_SCOPE = "ADS"
DATABRICKS_PAT_SECRET_NAME = "databricks-token"

# COMMAND ----------

def CurrentNotebookPath():
    return "/".join(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().split("/")[:-1])

# COMMAND ----------

def GetAuthenticationHeader():
    pat = dbutils.secrets.get(scope = SECRET_SCOPE, key = DATABRICKS_PAT_SECRET_NAME)
    headers = {
        'Authorization': f'Bearer {pat}',
    }
    return headers

# COMMAND ----------

def JasonToDataFrame(jsonInput):
    jsonData = json.dumps(jsonInput)
    jsonDataList = []
    jsonDataList.append(jsonData)
    jsonRDD = sc.parallelize(jsonDataList)
    return spark.read.json(jsonRDD)

# COMMAND ----------

def ListClusters():
    headers = GetAuthenticationHeader()
    url = f'{INSTANCE_NAME}/api/2.0/clusters/list'
    response = requests.get(url, headers=headers)
    jsonResponse = response.json()
    return JasonToDataFrame(jsonResponse)

# COMMAND ----------

def ListWorkspaces(path="/"):
    headers = GetAuthenticationHeader()
    url = f'{INSTANCE_NAME}/api/2.0/workspace/list'
    data_path = '{{"path": "{0}"}}'.format(path)
    response = requests.get(url, headers=headers, data=data_path)
    jsonResponse = response.json()
    return JasonToDataFrame(jsonResponse)

# COMMAND ----------

def GetNotebookName(path):
    list = path.split("/")
    count=len(list)
    return list[count-1]

# COMMAND ----------

def GetNotebookName(path):
    list = path.split("/")
    count=len(list)
    return list[count-1]

# COMMAND ----------

sqlWarehouseTemplate = {
  "name": "Data Analysts - SWC",
  "cluster_size" : "X-Small",
  "min_num_clusters": 1,
  "max_num_clusters": 2,
  "auto_stop_mins": 30,
  "tags": {
  },
  "spot_instance_policy":"COST_OPTIMIZED",
  "enable_photon": "true",
  "enable_serverless_compute": "false",
  "channel": {
    "name": "CHANNEL_NAME_CURRENT"
  }
}

# COMMAND ----------

def CreateSqlWarehouse():
    headers = GetAuthenticationHeader()
    url = f'{INSTANCE_NAME}/api/2.0/sql/warehouses'
    response = requests.post(url, json=sqlWarehouseTemplate, headers=headers)
    jsonResponse = response.json()
    print(jsonResponse)

# COMMAND ----------

def EditSqlWarehouse(id):
    headers = GetAuthenticationHeader()
    url = f'{INSTANCE_NAME}/api/2.0/sql/warehouses/{id}/edit'
    response = requests.post(url, json=sqlWarehouseTemplate, headers=headers)
    jsonResponse = response.json()
    print(jsonResponse)

# COMMAND ----------

clusterTemplate = {
    "cluster_name": "interactive",
    "autoscale": {
        "min_workers": 2,
        "max_workers": 8
    },
    "spark_version": "10.4.x-scala2.12",
    "spark_conf": {
        "spark.sql.session.timeZone": "Australia/Sydney",
        "spark.databricks.delta.preview.enabled": "true"
    },
    "azure_attributes": {
        "first_on_demand": 1,
        "availability": "SPOT_WITH_FALLBACK_AZURE",
        "spot_bid_max_price": -1
    },
    "node_type_id": "Standard_DS3_v2",
    "driver_node_type_id": "Standard_DS3_v2",
    "custom_tags": {},
    "autotermination_minutes": 60,
    "enable_elastic_disk": "true",
    "runtime_engine": "PHOTON"
}

# COMMAND ----------

def CreateCluster():
    headers = GetAuthenticationHeader()
    url = f'{INSTANCE_NAME}/api/2.0/clusters/create'
    response = requests.post(url, json=clusterTemplate, headers=headers)
    jsonResponse = response.json()
    print(jsonResponse)

# COMMAND ----------

def EditCluster(id):
    headers = GetAuthenticationHeader()
    url = f'{INSTANCE_NAME}/api/2.0/clusters/edit'
    clusterTemplate["cluster_id"] = id
    response = requests.post(url, json=clusterTemplate, headers=headers)
    jsonResponse = response.json()
    print(jsonResponse)

# COMMAND ----------

instancePoolTemplate = {
    "instance_pool_name": "my-pool-2",
    "node_type_id": "Standard_D4_v2",
    "node_type_id": "Standard_E4ds_v4",
    "min_idle_instances": 0,
    "max_capacity": 10,
    "idle_instance_autotermination_minutes": 30,
    "azure_attributes": {
        "availability": "SPOT_AZURE",
        "spot_bid_max_price": -1.0
    }
}

# COMMAND ----------

def CreatePool():
    headers = GetAuthenticationHeader()
    url = f'{INSTANCE_NAME}/api/2.0/instance-pools/create'
    response = requests.post(url, json=instancePoolTemplate, headers=headers)
    jsonResponse = response.json()
    print(jsonResponse)
#CreatePool()

# COMMAND ----------

def EditPool(id):
    headers = GetAuthenticationHeader()
    url = f'{INSTANCE_NAME}/api/2.0/instance-pools/edit'
    instancePoolTemplate["instance_pool_id"] = id
    response = requests.post(url, json=instancePoolTemplate, headers=headers)
    jsonResponse = response.json()
    print(jsonResponse)
#EditPool("1021-053147-plugs1-pool-j93qmhkl")

# COMMAND ----------

def GetPool(id):
    headers = GetAuthenticationHeader()
    url = f'{INSTANCE_NAME}/api/2.0/instance-pools/get'
    instancePoolTemplate["instance_pool_id"] = id
    response = requests.get(url, headers=headers, data=instancePoolTemplate)
    jsonResponse = response.json()
    return jsonResponse
#print(GetPool("1021-053147-plugs1-pool-j93qmhkl"))

# COMMAND ----------

def ListPools():
    headers = GetAuthenticationHeader()
    url = f'{INSTANCE_NAME}/api/2.0/instance-pools/list'
    response = requests.get(url, headers=headers)
    jsonResponse = response.json()
    return jsonResponse
#print(ListPools())

# COMMAND ----------


