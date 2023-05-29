# Databricks notebook source
# MAGIC %run ../Common/common-workspace

# COMMAND ----------

def GetPrefix(suffix="_"):
    try:
        prefix = dbutils.secrets.get("ADS" if not(any([i for i in json.loads(spark.conf.get("spark.databricks.clusterUsageTags.clusterAllTags")) if i["key"] == "Application" and "hackathon" in i["value"].lower()])) else "ADS-AKV", 'databricks-env')
        return suffix if terminator in prefix else prefix.replace("_", "-")
    except:
        return None

# COMMAND ----------

def ComputeGrants(groupName):
    groupName = GetPrefix("-")+groupName
    print(UpdateWarehousePermissionByName("Starter Warehouse", [{ "group_name": groupName, "permission_level": "CAN_USE" }]))
    print(UpdateClusterPermissionByName("interactive-3bj", [{ "group_name": groupName, "permission_level": "CAN_ATTACH_TO" }]))
#ComputeGrants("L1-Official")
#ComputeGrants("G1-Data-Developer")

# COMMAND ----------

def CatalogGrants(groupName):
    prefix = GetPrefix()
    groupName = GetPrefix("-")+groupName
    for i in ["cleansed", "curated"]:
        UpdatePermission("CATALOG", f"{prefix}{i}", [ {"principal" : f"{groupName}", "add" : [ "USE_CATALOG", "USE_SCHEMA", "SELECT" ]} ])
#CatalogGrants("G1-Data-Developer")

# COMMAND ----------

#print(GetObjectPermissions("directories", GetWorkspacePathId("/datalabs")))
#print(PutObjectPermissions("directories", GetWorkspacePathId("/datalabs"), [ { "group_name": "ppd-G1-Data-Developer", "permission_level": "CAN_READ" } ]))

# COMMAND ----------


