# Databricks notebook source
# MAGIC %run ../Common/common-workspace

# COMMAND ----------

# MAGIC %run ../Common/common-unity-catalog-helper

# COMMAND ----------

def ComputeGrants(groupName):
    groupName = GetPrefix("-")+groupName
    print(UpdateWarehousePermissionByName([c["name"] for c in ListWarehouses()["warehouses"]][0], [{ "group_name": groupName, "permission_level": "CAN_USE" }]))
    print(UpdateClusterPermissionByName("interactive-uc", [{ "group_name": groupName, "permission_level": "CAN_ATTACH_TO" }]))
#ComputeGrants("L1-Official")
#ComputeGrants("G1-Data-Developer")

# COMMAND ----------

def CatalogGrants(groupName):
    prefix = GetPrefix()
    groupName = GetPrefix("-")+groupName
    for i in ["cleansed", "curated"]:
        print(UpdatePermission("CATALOG", f"{prefix}{i}", [ {"principal" : f"{groupName}", "add" : [ "USE_CATALOG", "USE_SCHEMA", "SELECT" ]} ]))
#CatalogGrants("G1-Data-Developer")

# COMMAND ----------

#print(GetObjectPermissions("directories", GetWorkspacePathId("/datalabs")))
#print(PutObjectPermissions("directories", GetWorkspacePathId("/datalabs"), [ { "group_name": "ppd-G1-Data-Developer", "permission_level": "CAN_READ" } ]))

# COMMAND ----------


