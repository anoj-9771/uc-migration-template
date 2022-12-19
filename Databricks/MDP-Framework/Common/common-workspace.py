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

def JsonToDataFrame(jsonInput):
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
    return JsonToDataFrame(jsonResponse)

# COMMAND ----------

def ListWorkspaces(path="/"):
    headers = GetAuthenticationHeader()
    url = f'{INSTANCE_NAME}/api/2.0/workspace/list'
    data_path = '{{"path": "{0}"}}'.format(path)
    response = requests.get(url, headers=headers, data=data_path)
    jsonResponse = response.json()
    return JsonToDataFrame(jsonResponse)

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

# COMMAND ----------

def PinCluster(id):
    headers = GetAuthenticationHeader()
    url = f'{INSTANCE_NAME}/api/2.0/clusters/pin'
    response = requests.post(url, json={ "cluster_id": id }, headers=headers)
    jsonResponse = response.json()
    print(jsonResponse)

# COMMAND ----------

def CreatePool(template):
    headers = GetAuthenticationHeader()
    url = f'{INSTANCE_NAME}/api/2.0/instance-pools/create'
    response = requests.post(url, json=template, headers=headers)
    jsonResponse = response.json()
    print(jsonResponse)
#CreatePool(poolMedium)

# COMMAND ----------

def EditPool(id):
    headers = GetAuthenticationHeader()
    url = f'{INSTANCE_NAME}/api/2.0/instance-pools/edit'
    instancePoolTemplate["instance_pool_id"] = id
    response = requests.post(url, json=instancePoolTemplate, headers=headers)
    jsonResponse = response.json()
    print(jsonResponse)

# COMMAND ----------

def GetPool(id):
    headers = GetAuthenticationHeader()
    url = f'{INSTANCE_NAME}/api/2.0/instance-pools/get'
    instancePoolTemplate["instance_pool_id"] = id
    response = requests.get(url, headers=headers, data=instancePoolTemplate)
    jsonResponse = response.json()
    return jsonResponse

# COMMAND ----------

def ListPools():
    headers = GetAuthenticationHeader()
    url = f'{INSTANCE_NAME}/api/2.0/instance-pools/list'
    response = requests.get(url, headers=headers)
    jsonResponse = response.json()
    return jsonResponse

# COMMAND ----------

def GetPoolIdByName(name):
    v = [a for a in ListPools()['instance_pools'] if a["instance_pool_name"]==name]
    return "" if len(v) == 0 else v[0]["instance_pool_id"] 

# COMMAND ----------

def CreateCluster(cluster, pin=True):
    headers = GetAuthenticationHeader()
    url = f'{INSTANCE_NAME}/api/2.0/clusters/create'
    response = requests.post(url, json=cluster, headers=headers)
    jsonResponse = response.json()
    clusterId = jsonResponse["cluster_id"]
    if pin:
        PinCluster(clusterId)
    InstallLibraries(clusterId)
    print(jsonResponse)

# COMMAND ----------

clusterTemplate = {
    "autoscale": {
        "min_workers": 1,
        "max_workers": 6
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
    "custom_tags": {},
    "cluster_name": "interactive",
    "runtime_engine": "PHOTON",
    "autotermination_minutes": 30
}

# COMMAND ----------

libraryTemplate = {
  "libraries": [
    {
      "maven": {
        "coordinates": "com.microsoft.azure:azure-sqldb-spark:1.0.2"
      }
    },
    {
      "maven": {
        "coordinates": "com.databricks:spark-xml_2.12:0.15.0"
      }
    },
    {
      "maven": {
        "coordinates": "com.crealytics:spark-excel_2.12:3.1.2_0.16.5-pre1"
      }
    },
    {
      "jar": "dbfs:/FileStore/jars/edda63ff_ead1_4e79_8aff_fc35161ab4eb-azure_cosmos_spark_3_1_2_12_4_8_0-53136.jar"
    }
  ]
}

# COMMAND ----------

def EditCluster(id):
    headers = GetAuthenticationHeader()
    url = f'{INSTANCE_NAME}/api/2.0/clusters/edit'
    clusterTemplate["cluster_id"] = id
    response = requests.post(url, json=clusterTemplate, headers=headers)
    jsonResponse = response.json()
    print(jsonResponse)

# COMMAND ----------

def InstallLibraries(clusterId):
    headers = GetAuthenticationHeader()
    url = f'{INSTANCE_NAME}/api/2.0/libraries/install'
    libraryTemplate["cluster_id"] = clusterId
    response = requests.post(url, json=libraryTemplate, headers=headers)
    jsonResponse = response.json()
    print(jsonResponse)

# COMMAND ----------

def ListClusters():
    headers = GetAuthenticationHeader()
    url = f'{INSTANCE_NAME}/api/2.0/clusters/list'
    response = requests.get(url, headers=headers)
    jsonResponse = response.json()
    return jsonResponse

# COMMAND ----------

def GetClusterIdByName(name):
    v = [a for a in ListClusters()['clusters'] if a["cluster_name"]==name]
    return "" if len(v) == 0 else v[0]["cluster_id"]

# COMMAND ----------

def CreateClusterForPool(clusterName, poolName):
    cluster = clusterTemplate
    cluster["cluster_name"] = clusterName
    cluster["instance_pool_id"] = GetPoolIdByName(poolName)
    CreateCluster(cluster)
#CreateClusterForPool("interactive", "pool-small")

# COMMAND ----------

def GetGroupByName(name):
    headers = GetAuthenticationHeader()
    url = f'{INSTANCE_NAME}/api/2.0/preview/scim/v2/Groups?filter=displayName+eq+{name}'
    response = requests.get(url, headers=headers)
    jsonResponse = response.json()
    return jsonResponse

# COMMAND ----------

def CreateUser(user, groupId=None):
    newUser = {
        "schemas": [ "urn:ietf:params:scim:schemas:core:2.0:User" ],
        "userName": f"{user}",
        "groups": [
            {
            "value":f"{groupId}"
            }
        ]
    }
    headers = GetAuthenticationHeader()
    url = f'{INSTANCE_NAME}/api/2.0/preview/scim/v2/Users'
    response = requests.post(url, json=newUser, headers=headers)
    jsonResponse = response.json()
    return jsonResponse

# COMMAND ----------

def CreateUser(user, groupId=None):
    newUser = {
        "schemas": [ "urn:ietf:params:scim:schemas:core:2.0:User" ],
        "userName": f"{user}",
        "groups": [
            {
            "value":f"{groupId}"
            }
        ]
    }
    headers = GetAuthenticationHeader()
    url = f'{INSTANCE_NAME}/api/2.0/preview/scim/v2/Users'
    response = requests.post(url, json=newUser, headers=headers)
    jsonResponse = response.json()
    return jsonResponse

# COMMAND ----------

def GetUserByName(name):
    headers = GetAuthenticationHeader()
    url = f'{INSTANCE_NAME}/api/2.0/preview/scim/v2/Users?filter=userName+eq+{name}'
    response = requests.get(url, headers=headers)
    jsonResponse = response.json()
    return jsonResponse

# COMMAND ----------

def DeleteUser(id):
    headers = GetAuthenticationHeader()
    url = f'{INSTANCE_NAME}/api/2.0/preview/scim/v2/Users/{id}'
    response = requests.delete(url, headers=headers)

# COMMAND ----------

def CreateUsers(list, groupId=None):
    for u in list:
        if spark.sql(f"SHOW USERS LIKE '{u}*'").count() > 0:
            continue
        j = CreateUser(u, groupId)

# COMMAND ----------

def DeleteUsers(list):
    for u in list:
        if spark.sql(f"SHOW USERS LIKE '{u}*'").count() == 0:
            continue
        j = GetUserByName(u)
        id = j["Resources"][0]["id"]
        #print(id)
        DeleteUser(id)

# COMMAND ----------

def UpdateGroup(groupId, addGroupId):
    json = {
      "schemas": [ "urn:ietf:params:scim:api:messages:2.0:PatchOp" ],
      "Operations": [
        {
          "op":"add",
          "value": {
            "members": [
              {
                "value":f"{addGroupId}"
              }
            ]
          }
        }
      ]
    }
    headers = GetAuthenticationHeader()
    url = f'{INSTANCE_NAME}/api/2.0/preview/scim/v2/Groups/{groupId}'
    response = requests.patch(url, json=json, headers=headers)
    jsonResponse = response.json()
    return jsonResponse

# COMMAND ----------

def AssignGroup(groupName, targetGroupName):
    groupId = GetGroupByName(targetGroupName)["Resources"][0]["id"]
    addGroupId = GetGroupByName(groupName)["Resources"][0]["id"]
    return UpdateGroup(groupId, addGroupId)
