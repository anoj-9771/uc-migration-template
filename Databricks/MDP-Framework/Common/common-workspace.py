# Databricks notebook source
import requests, json
from ast import literal_eval
from pyspark.sql.functions import *

# COMMAND ----------

INSTANCE_NAME = "https://australiaeast.azuredatabricks.net"
SECRET_SCOPE = "ADS" if not(any([i for i in json.loads(spark.conf.get("spark.databricks.clusterUsageTags.clusterAllTags")) if i["key"] == "Application" and "hackathon" in i["value"].lower()])) else "ADS-AKV"
DATABRICKS_PAT_SECRET_NAME = "databricks-token"

# COMMAND ----------

def CurrentNotebookPath():
    return "/".join(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().split("/")[:-1])

# COMMAND ----------

def GetServicePrincipalId():
    return dbutils.secrets.get(scope = SECRET_SCOPE, key = "daf-serviceprincipal-app-id")

# COMMAND ----------

def GetAuthenticationHeader(key=DATABRICKS_PAT_SECRET_NAME):
    pat = dbutils.secrets.get(scope = SECRET_SCOPE, key = key)
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
    return response.json()

# COMMAND ----------

def GetClusterByName(name):
    return [i for i in ListClusters()["clusters"] if i["cluster_name"] == name][0]

# COMMAND ----------

def ChangeClusterOwner(clusterId, newOwner):
    json={
        "cluster_id": f"{clusterId}", "owner_username": f"{newOwner}"
    }
    headers = GetAuthenticationHeader()
    url = f'{INSTANCE_NAME}/api/2.0/clusters/change-owner'
    response = requests.post(url, json=json, headers=headers)
    jsonResponse = response.json()
    return jsonResponse

# COMMAND ----------

def GetClusterByName(name):
    v = [a for a in ListClusters()['clusters'] if a["cluster_name"]==name]
    return "" if len(v) == 0 else v[0]

# COMMAND ----------

def ListWorkspaces(path="/"):
    headers = GetAuthenticationHeader()
    url = f'{INSTANCE_NAME}/api/2.0/workspace/list'
    data_path = '{{"path": "{0}"}}'.format(path)
    response = requests.get(url, headers=headers, data=data_path)
    return response.json()

# COMMAND ----------

def GetWorkspacePath(path):
    root = "/".join(path.split("/")[:-1])
    folder = "/".join(path.split("/")[-1:])
    for i in ListWorkspaces(root)["objects"]:
        if i["path"] == path:
            return i

# COMMAND ----------

def GetWorkspaceStatus(path):
    headers = GetAuthenticationHeader()
    url = f'{INSTANCE_NAME}/api/2.0/workspace/list'
    data_path = '{{"path": "{0}"}}'.format(path)
    response = requests.get(url, headers=headers, data=data_path)
    return response.json()

# COMMAND ----------

def GetWorkspacePathId(path):
    return GetWorkspaceStatus(path)["objects"][0]["object_id"]

# COMMAND ----------

def RecursiveListWorkspacePath(path, list = None):
    list = [] if list is None else list
    c = GetWorkspacePath(path)
    list.extend([{ "path" : c["path"] , "id" : c["object_id"] }])
    pathListing = ListWorkspaces(path)
    for p in (JsonToDataFrame(pathListing).selectExpr("explode(objects) o")
        .select("o.*", "o.path").collect()):
        #print(p.path)
        list.extend([{ "path" : p.path , "id" : p.object_id}])

        if (p.object_type == "DIRECTORY"):
            RecursiveListWorkspacePath(p.path, list)
    return list

# COMMAND ----------

def WorkspaceCreateDirectory(path):
    headers = GetAuthenticationHeader()
    url = f"{INSTANCE_NAME}/api/2.0/workspace/mkdirs"
    data = json.dumps( { "path": path } )
    response = requests.post(url, headers=headers, data=data)
    return response.json()

# COMMAND ----------

def WorkspaceImport(path, content, language="py", isBase64=False):
    import base64
    headers = GetAuthenticationHeader()
    url = f"{INSTANCE_NAME}/api/2.0/workspace/import"
    data = json.dumps( { 
        "path": path
        ,"content": content if isBase64 else base64.b64encode(bytes(content, "ascii")).decode("utf-8")
        ,"language": "PYTHON" if language.upper() == "PY" else language.upper()
        ,"overwrite": "true"
        ,"format": "SOURCE" })
    response = requests.post(url, headers=headers, data=data)
    return response.json()

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

def CreateSqlWarehouse(sqlWarehouseTemplate):
    headers = GetAuthenticationHeader()
    url = f'{INSTANCE_NAME}/api/2.0/sql/warehouses'
    response = requests.post(url, json=sqlWarehouseTemplate, headers=headers)
    return response.json()

# COMMAND ----------

def UpdateWarehousePermission(id, list):
    headers = GetAuthenticationHeader()
    url = f'{INSTANCE_NAME}/api/2.0/permissions/warehouses/{id}'
    jsonData = {
        "access_control_list": [
            *list
        ]
    }
    response = requests.patch(url, json=jsonData, headers=headers)
    return response.json()

# COMMAND ----------

def UpdateWarehousePermissionByName(warehouseName, permissionList):
    id = GetWarehouseByName(warehouseName)["id"]
    return UpdateWarehousePermission(id, permissionList)

# COMMAND ----------

def EditSqlWarehouse(id):
    headers = GetAuthenticationHeader()
    url = f'{INSTANCE_NAME}/api/2.0/sql/warehouses/{id}/edit'
    response = requests.post(url, json=sqlWarehouseTemplate, headers=headers)
    jsonResponse = response.json()
    print(jsonResponse)

# COMMAND ----------

def PinCluster(id):
    headers = GetAuthenticationHeader()
    url = f'{INSTANCE_NAME}/api/2.0/clusters/pin'
    response = requests.post(url, json={ "cluster_id": id }, headers=headers)
    return response.json()

# COMMAND ----------

def ListPools():
    headers = GetAuthenticationHeader()
    url = f'{INSTANCE_NAME}/api/2.0/instance-pools/list'
    response = requests.get(url, headers=headers)
    return response.json()

# COMMAND ----------

def GetPoolIdByName(name):
    v = [a for a in ListPools()['instance_pools'] if a["instance_pool_name"]==name]
    if len(v) == 0:
        raise Exception(f"{name} Not found!")
    return v[0]["instance_pool_id"] 

# COMMAND ----------

def GetPool(id):
    headers = GetAuthenticationHeader()
    url = f'{INSTANCE_NAME}/api/2.0/instance-pools/get'
    response = requests.get(url, json={"instance_pool_id" : id}, headers=headers)
    return response.json()

# COMMAND ----------

def CreatePool(template):
    headers = GetAuthenticationHeader()
    url = f'{INSTANCE_NAME}/api/2.0/instance-pools/create'
    response = requests.post(url, json=template, headers=headers)
    return response.json()

# COMMAND ----------

def EditPool(template):
    headers = GetAuthenticationHeader()
    url = f'{INSTANCE_NAME}/api/2.0/instance-pools/edit'
    response = requests.post(url, json=template, headers=headers)
    return response.json()

# COMMAND ----------

def TerminateCluster(clusterId):
    headers = GetAuthenticationHeader()
    url = f'{INSTANCE_NAME}/api/2.0/clusters/delete'
    response = requests.post(url, json = { "cluster_id" : clusterId }, headers=headers)
    return response.json()

# COMMAND ----------

def CreateCluster(cluster):
    headers = GetAuthenticationHeader()
    url = f'{INSTANCE_NAME}/api/2.0/clusters/create'
    response = requests.post(url, json=cluster, headers=headers)
    return response.json()

# COMMAND ----------

def EditCluster(clusterTemplate):
    cluster = [i for i in ListClusters()["clusters"] if i["cluster_name"] == clusterTemplate["cluster_name"] ][0]
    headers = GetAuthenticationHeader()
    url = f'{INSTANCE_NAME}/api/2.0/clusters/edit'
    clusterTemplate["cluster_id"] = cluster["cluster_id"]
    response = requests.post(url, json=clusterTemplate, headers=headers)
    return clusterTemplate

# COMMAND ----------

def CreateOrEditCluster(cluster, pin=True, createNew=False, librariesList=None):
    jsonResponse = EditCluster(cluster) if createNew or not(any([i for i in ListClusters()["clusters"] if i["cluster_name"] == clusterTemplate["cluster_name"]])) == False else CreateCluster(cluster)
    clusterId = jsonResponse["cluster_id"]
    PinCluster(clusterId) if pin else ()
    InstallLibraries(clusterId, librariesList) if librariesList is not None else ()
    TerminateCluster(clusterId)
    return jsonResponse

# COMMAND ----------

def GetClusterPermission(id):
    headers = GetAuthenticationHeader()
    url = f'{INSTANCE_NAME}/api/2.0/permissions/clusters/{id}'
    response = requests.get(url, headers=headers)
    return response.json()

# COMMAND ----------

def UpdateClusterPermission(id, list, overwrite=False):
    headers = GetAuthenticationHeader()
    url = f'{INSTANCE_NAME}/api/2.0/permissions/clusters/{id}'

    jsonData = {
        "access_control_list": [
            *list
        ]
    }
    response = (requests.patch(url, json=jsonData, headers=headers) if not(overwrite) else requests.put(url, json=jsonData, headers=headers))
    return response.json()

# COMMAND ----------

def UpdateClusterPermissionByName(clusterName, permissionList, overwrite=False):
    id = GetClusterByName(clusterName)["cluster_id"]
    return UpdateClusterPermission(id, permissionList)

# COMMAND ----------

def InstallLibraries(clusterId, libraryList):
    headers = GetAuthenticationHeader()
    url = f'{INSTANCE_NAME}/api/2.0/libraries/install'
    libraryTemplate["cluster_id"] = clusterId
    response = requests.post(url, json=libraryList, headers=headers)
    return response.json()

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

def GetUser(id):
    headers = GetAuthenticationHeader()
    url = f'{INSTANCE_NAME}/api/2.0/preview/scim/v2/Users/{id}'
    response = requests.get(url, headers=headers)
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

def CreateGroup(name):
    headers = GetAuthenticationHeader()
    url = f'{INSTANCE_NAME}/api/2.0/preview/scim/v2/Groups'

    jsonData = {
        "displayName": name
    }
    response = requests.post(url, json=jsonData, headers=headers)
    return response.json()

# COMMAND ----------

def DeleteGroup(groupId):
    headers = GetAuthenticationHeader()
    url = f'{INSTANCE_NAME}/api/2.0/preview/scim/v2/Groups/{groupId}'
    response = requests.delete(url, headers=headers)
    return response

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

# COMMAND ----------

def GetUsers():
    headers = GetAuthenticationHeader()
    url = f'{INSTANCE_NAME}/api/2.0/preview/scim/v2/Users'
    response = requests.get(url, headers=headers)
    jsonResponse = response.json()
    return jsonResponse

# COMMAND ----------

def AddGroupMembers(groupId, userIds):
    jsonOperation = {
      "schemas": [ "urn:ietf:params:scim:api:messages:2.0:PatchOp" ],
      "Operations": [
        {
          "op":"add",
          "value": {
            "members": [ { "value" : f"{i}" } for i in userIds] }
        }
      ]
    }
    
    headers = GetAuthenticationHeader()
    url = f'{INSTANCE_NAME}/api/2.0/preview/scim/v2/Groups/{groupId}'
    response = requests.patch(url, json=jsonOperation, headers=headers)
    jsonResponse = response.json()

# COMMAND ----------

def EditGroupMembersByName(groupName, users):
    userIds = [ i["id"] for i in GetUsers()["Resources"] if i["userName"] in [ f"{i}@sydneywater.com.au" for i in users] ]
    groupId = GetGroupByName(groupName)["Resources"][0]["id"]
    AddGroupMembers(groupId, userIds)

# COMMAND ----------

def ListUsersInGroup(name):
    df = JsonToDataFrame(GetGroupByName(name)).selectExpr("explode(Resources[0].members) col").select("col.*")
    display(df)

# COMMAND ----------

def AddUsersToGroup(groupName, users):
    groupId = GetGroupByName(groupName)["Resources"][0]["id"]
    CreateUsers(users)
    allUsers = GetUsers()
    userIds = [ i["id"] for i in allUsers["Resources"] if i["userName"] in users ]
    jsonOperation = {
      "schemas": [ "urn:ietf:params:scim:api:messages:2.0:PatchOp" ],
      "Operations": [
        {
          "op":"add",
          "value": {
            "members": [ { "value" : f"{i}" } for i in userIds] }
        }
      ]
    }
    
    headers = GetAuthenticationHeader()
    url = f'{INSTANCE_NAME}/api/2.0/preview/scim/v2/Groups/{groupId}'
    response = requests.patch(url, json=jsonOperation, headers=headers)
    jsonResponse = response.json()

# COMMAND ----------

def ListWarehouses():
    headers = GetAuthenticationHeader()
    url = f'{INSTANCE_NAME}/api/2.0/sql/warehouses'
    response = requests.get(url, headers=headers)
    jsonResponse = response.json()
    return jsonResponse

# COMMAND ----------

def GetWarehouseByName(name):
    return [i for i in ListWarehouses()["warehouses"] if i["name"] == name][0]

# COMMAND ----------

def StartWarehouse(id):
    headers = GetAuthenticationHeader()
    url = f'{INSTANCE_NAME}/api/2.0/sql/warehouses/{id}/start'
    response = requests.post(url, headers=headers)
    jsonResponse = response.json()
    return jsonResponse

# COMMAND ----------

def GetFirstWarehouse():
    return [c["id"] for c in ListWarehouses()["warehouses"]][0]

# COMMAND ----------

def StartFirstWarehouse():
    StartWarehouse(GetFirstWarehouse())

# COMMAND ----------

#NOTE: DEVELOPMENTAL USING databricks-sql-connector
def ExecuteSqlDWCommand(warehouseId, commands):
    from databricks import sql
    pat = dbutils.secrets.get(scope = SECRET_SCOPE, key = DATABRICKS_PAT_SECRET_NAME)
    connection = sql.connect(
        server_hostname = "australiaeast.azuredatabricks.net",
        http_path= f"/sql/1.0/warehouses/{warehouseId}",
        access_token= f'{pat}'
    )

    cursor = connection.cursor()

    for c in commands:
        cursor.execute(c)

    cursor.close()
    connection.close()

# COMMAND ----------

def CreateContext(clusterId):
    command = {
       "clusterId": f"{clusterId}",
       "language": "sql"
    }
    headers = GetAuthenticationHeader()
    url = f'{INSTANCE_NAME}/api/1.2/contexts/create'
    response = requests.post(url, json=command, headers=headers)
    jsonResponse = response.json()
    
    if jsonResponse.get("error") is not None:
        raise Exception(str(jsonResponse["error"])) 

    return jsonResponse

# COMMAND ----------

def GetCommandStatus(commandId, contextId, clusterId):
    command = {
       "clusterId": f"{clusterId}",
       "contextId": f"{contextId}",
       "commandId": f"{commandId}",
    }
    headers = GetAuthenticationHeader()
    url = f'{INSTANCE_NAME}/api/1.2/commands/status'
    response = requests.get(url, params=command, headers=headers)
    jsonResponse = response.json()
    #print(command)
    return jsonResponse

# COMMAND ----------

def ExecuteCommand(sql, clusterId=None, contextId=None, waitOnCompletion=True):
    import time
    clusterId = clusterId if clusterId is not None else spark.conf.get("spark.databricks.clusterUsageTags.clusterId")
    contextId = contextId if contextId is not None else CreateContext(clusterId)["id"]
    
    command = {
       "clusterId": f"{clusterId}",
       "contextId": f"{contextId}",
       "language": "sql",
       "command": f"{sql}"
    }
    #print(command)
    headers = GetAuthenticationHeader()
    url = f'{INSTANCE_NAME}/api/1.2/commands/execute'
    response = requests.post(url, json=command, headers=headers)
    jsonResponse = response.json()
    
    commandId = jsonResponse["id"]
    
    if waitOnCompletion:
        while True:
            s = GetCommandStatus(commandId, contextId, clusterId) 
            print(s)
            #print("Running...")
            time.sleep(10)

            if s["status"] not in ("Running", "Queued"):
                print(f"Done! {s}")
                break
    
    #print(GetCommandStatus(commandId, contextId, clusterId))
    return jsonResponse

# COMMAND ----------

def GetFirstAclEnabledCluster():
    return [c for c in ListClusters()["clusters"] if c["cluster_source"] == "UI" and "spark.databricks.acl.dfAclsEnabled" in c["spark_conf"]][0]

# COMMAND ----------

def RevokePermissionsForGroup(groupName, database):
    df = spark.sql(f"SHOW TABLES FROM {database}")
    clusterId = GetFirstAclEnabledCluster()["cluster_id"]
    sql = "\n".join([f"REVOKE ALL PRIVILEGES ON TABLE {i.database}.{i.tableName} FROM `{groupName}`;" for i in df.collect()])
    ExecuteCommand(sql, clusterId)

# COMMAND ----------

def GetCluster(clusterId):
    headers = GetAuthenticationHeader()
    url = f'{INSTANCE_NAME}/api/2.0/clusters/get'
    response = requests.get(url, {"cluster_id": f"{clusterId}"}, headers=headers)
    jsonResponse = response.json()
    return jsonResponse

# COMMAND ----------

def StartCluster(clusterId, waitOnCompletion=True):
    import time
    command = {
       "cluster_id": f"{clusterId}"
    }
    headers = GetAuthenticationHeader()
    url = f'{INSTANCE_NAME}/api/2.0/clusters/start'
    response = requests.post(url, json=command, headers=headers)
    jsonResponse = response.json()

    if waitOnCompletion:
        while True:
            s = GetCluster(clusterId) 

            if s["state"] in ("RUNNING"):
                name = s["cluster_name"]
                print(f"{name} Started!")
                return s
                break
            if s["state"] in ("TERMINATED", "TERMINATING"):
                print(f"TERMINATED/TERMINATING, retry.")
                break
            print(s["state"])
            time.sleep(20)
    #return jsonResponse

# COMMAND ----------

def StartFirstAclEnabledCluster():
    return StartCluster(GetFirstAclEnabledCluster()["cluster_id"])

# COMMAND ----------

def ListGroups():
    headers = GetAuthenticationHeader()
    url = f'{INSTANCE_NAME}/api/2.0/preview/scim/v2/Groups'
    response = requests.get(url, headers=headers)
    jsonResponse = response.json()
    return jsonResponse

# COMMAND ----------

def ListCatalogs():
    headers = GetAuthenticationHeader()
    url = f'{INSTANCE_NAME}/api/2.1/unity-catalog/catalogs'
    response = requests.get(url, headers=headers)
    return response.json()

# COMMAND ----------

def ListExternalLocations():
    headers = GetAuthenticationHeader()
    url = f'{INSTANCE_NAME}/api/2.1/unity-catalog/external-locations'
    response = requests.get(url, headers=headers)
    return response.json()

# COMMAND ----------

def UpdatePermission(securable_type, full_name, grants):
    headers = GetAuthenticationHeader()
    url = f'{INSTANCE_NAME}/api/2.1/unity-catalog/permissions/{securable_type}/{full_name}'
    permissions = { "changes": [ *grants ] }
    response = requests.patch(url, json=permissions, headers=headers)
    return response.json()

# COMMAND ----------

def GetObjectPermissions(type, id):
    headers = GetAuthenticationHeader()
    url = f'{INSTANCE_NAME}/api/2.0/permissions/{type}/{id}'
    jsonData = {
    }
    response = requests.get(url, json=jsonData, headers=headers)
    return response.json()

# COMMAND ----------

def PutObjectPermissions(type, id, aclList):
    headers = GetAuthenticationHeader()
    url = f'{INSTANCE_NAME}/api/2.0/permissions/{type}/{id}'
    jsonData = { "access_control_list": [ *aclList] }
    response = requests.put(url, json=jsonData, headers=headers)
    return response.json()

# COMMAND ----------

def ListExternalLocations():
    headers = GetAuthenticationHeader()
    url = f"{INSTANCE_NAME}/api/2.1/unity-catalog/external-locations"
    response = requests.get(url, headers=headers)
    return response.json()

# COMMAND ----------

def CreateExternalLocation(name, abfssPath, credential):
    headers = GetAuthenticationHeader()
    url = f"{INSTANCE_NAME}/api/2.1/unity-catalog/external-locations"
    data = {
        "name" : name,
        "skip_validation" : True,
        "url" : abfssPath,
        "read_only" : False,
        "credential_name" : credential
    }
    response = requests.post(url, json=data, headers=headers)
    return response.json()

# COMMAND ----------

def ListCredentials():
    headers = GetAuthenticationHeader()
    url = f"{INSTANCE_NAME}/api/2.1/unity-catalog/storage-credentials"
    response = requests.get(url, headers=headers)
    return response.json()

# COMMAND ----------

def ListSchemas(name):
    headers = GetAuthenticationHeader()
    url = f"{INSTANCE_NAME}/api/2.1/unity-catalog/schemas?catalog_name={name}"
    response = requests.get(url, headers=headers)
    return response.json()

# COMMAND ----------

def CreateSchema(name, catalogName, storageRoot):
    headers = GetAuthenticationHeader()
    url = f"{INSTANCE_NAME}/api/2.1/unity-catalog/schemas"
    data = {
        "name": name,
        "catalog_name": catalogName,
        "storage_root": storageRoot,
        "properties": {
        }
    }
    response = requests.post(url, json=data, headers=headers)
    return response.json()

# COMMAND ----------

def TransferObjectOwner(objectType, objectId):
    headers = GetAuthenticationHeader()
    url = f'{INSTANCE_NAME}/api/2.0/preview/sql/permissions/{objectType}/{objectId}/transfer'
    jsonData = { "access_control_list": [ *list ] }
    response = (requests.patch(url, json=jsonData, headers=headers) if not(overwrite) else requests.put(url, json=jsonData, headers=headers))
    return response.json()

# COMMAND ----------

def CreateJob(template):
    headers = GetAuthenticationHeader()
    url = f'{INSTANCE_NAME}/api/2.1/jobs/create'
    response = requests.post(url, json=template, headers=headers)
    return response.json()
