# Databricks notebook source
# MAGIC %run ../Common/common-workspace

# COMMAND ----------

# MAGIC %run ../Common/common-account

# COMMAND ----------

clusterTemplate = {
    "autoscale": {
        "min_workers": 1,
        "max_workers": 1
    },
    "spark_version": "12.2.x-scala2.12",
    "spark_conf": {
        "spark.sql.session.timeZone": "Australia/Sydney",
        "spark.databricks.delta.preview.enabled": "true"
    },
    "azure_attributes": {},
    "ssh_public_keys": [],
    "custom_tags": {
        "product" : "Datalab"
        ,"datalab" : ""
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

# DBTITLE 1,Welcome to $datalab$
_defaultNotebookTemplate = """
print("$datalab$")
"""

# COMMAND ----------

dataLabRequest = {
    "name" : "datalab03"
    ,"use_compute": True
    ,"use_files": True
    ,"install_libraries": []
    ,"group_members": [
        "3bj"
    ]
}

# COMMAND ----------

def CreateDatalab(request):
    name = request.get("name")
    groupMembers = request.get("group_members")

    # VALIDATE
    if name is None or groupMembers is None:
        raise Exception("Bad parameters!")

    url = "/".join([i for i in ListExternalLocations()["external_locations"] if "datalab" in i["url"]][0]["url"].split("/")[0:3])
    datalabPath = f"{url}/{name}"
    groupName = f"datalab-{name}"
    credential = [i for i in ListCredentials()["storage_credentials"] if "-prod-01" in i["name"]][0]["name"]
    extLocationName_mgd = f"datalab_{name}_mgd"
    extLocationName_files = f"datalab_{name}_files"
    admin = "All-G3-Admins"
    useCompute = request.get("use_compute") or False
    useFiles = request.get("use_files") or False

    # ACCOUNT GROUP
    print(CreateAccountGroup(groupName))
    print(UpdateAccountGroupMembers(groupName, groupMembers))
    CreateOrUpdatePermissionAssignment(GetWorkspaceId(), [i for i in ListAccountGroups()["Resources"] if groupName in i["displayName"]][0]["id"])

    # EXTERNAL LOCATION
    print(CreateExternalLocation(extLocationName_mgd, f"{datalabPath}/tables", credential))
    spark.sql(f"ALTER EXTERNAL LOCATION `{extLocationName_mgd}` OWNER TO `{admin}`")
    print(UpdatePermission("EXTERNAL_LOCATION", f"{extLocationName_mgd}", [ {"principal" : admin, "add" : [ "ALL_PRIVILEGES" ]}]))

    # SCHEMA
    print(CreateSchema(name, "datalab", f"{datalabPath}/tables"))
    spark.sql(f"ALTER SCHEMA `datalab`.`{name}` OWNER TO `{admin}`")
    print(UpdatePermission("SCHEMA", f"datalab.{name}", [ { "principal" : groupName, "add" : [ "ALL_PRIVILEGES" ]} ]))

    # FILES
    if useFiles: 
        print(CreateExternalLocation(extLocationName_files, f"{datalabPath}/files", credential))
        spark.sql(f"ALTER EXTERNAL LOCATION `{extLocationName_files}` OWNER TO `{admin}`")
        print(UpdatePermission("EXTERNAL_LOCATION", f"{extLocationName_files}", [ {"principal" : admin, "add" : [ "ALL_PRIVILEGES" ]}]))
        print(UpdatePermission("EXTERNAL_LOCATION", f"{extLocationName_files}", [ {"principal" : groupName, "add" : [ "CREATE_EXTERNAL_TABLE", "READ_FILES", "WRITE_FILES" ]}]))
        dbutils.fs.mkdirs(f"{datalabPath}/files/folder1")

    # CLUSTER
    if useCompute:
        clusterTemplate["cluster_name"] = f"datalab-{name}"
        clusterTemplate["custom_tags"]["datalab"] = name
        print(CreateOrEditCluster(clusterTemplate))
        print(UpdateClusterPermissionByName(clusterTemplate["cluster_name"], [{ "group_name": groupName, "permission_level": "CAN_RESTART" }]))

    # WORKSPACE
    if useCompute or useFiles:
        WorkspaceCreateDirectory(f"/datalab/{name}")
        WorkspaceImport(f"/datalab/{name}/readme", _defaultNotebookTemplate.replace("$datalab$", name))
        print(PutObjectPermissions("directories", [i for i in GetWorkspaceStatus("/datalab")["objects"] if name in i["path"]][0]["object_id"], [ { "group_name": groupName, "permission_level": "CAN_MANAGE" } ]))

#CreateDatalab(dataLabRequest)
