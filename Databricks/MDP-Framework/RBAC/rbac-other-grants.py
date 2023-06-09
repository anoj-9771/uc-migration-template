# Databricks notebook source
# MAGIC %run ../Common/common-workspace

# COMMAND ----------

# MAGIC %run ../Common/common-unity-catalog-helper

# COMMAND ----------

def GetLegacyGroupName(groupName):
    env = GetEnvironmentTag().lower()
    sub = "" if "prod" in env else "non"
    return f"A-Azure-rg-swc{sub}prod01-{env}-daf-01-{groupName}"

# COMMAND ----------

def G3Admins(groupName):
    prefix = GetPrefix()
    groupName = GetPrefix("-")+groupName
    sql = []
    for i in ["raw", "cleansed", "curated", "rejected", "semantic", "stage"]:
        sql.append(f"ALTER CATALOG {prefix}{i} SET OWNER TO `{groupName}`;")
        print(UpdatePermission("CATALOG", f"{prefix}{i}", [ {"principal" : f"{groupName}", "add" : [ "ALL_PRIVILEGES" ]} ]))
    # DATALAB
    datalabSchema = GetPrefix("")
    datalabSchema = "swc" if datalabSchema == "" else datalabSchema
    sql.append(f"ALTER SCHEMA `datalab`.`{datalabSchema}` SET OWNER TO `{groupName}`;")
    #print(UpdatePermission("SCHEMA", f"datalab.{datalabSchema}", [ {"principal" : f"{groupName}", "add" : [ "ALL_PRIVILEGES" ]} ]))
    sql = "\n".join(sql)
    #print(sql)
    ExecuteCommand(sql)
G3Admins("G3-Admins")

# COMMAND ----------

def G1DataDeveloper(groupName):
    prefix = GetPrefix()
    groupName = GetPrefix("-")+groupName
    for i in ["cleansed", "curated"]:
        print(UpdatePermission("CATALOG", f"{prefix}{i}", [ {"principal" : f"{groupName}", "add" : [ "USE_CATALOG", "USE_SCHEMA", "SELECT" ]} ]))
    # DATALAB
    datalabSchema = GetPrefix("")
    datalabSchema = "swc" if datalabSchema == "" else datalabSchema
    print(UpdatePermission("SCHEMA", f"datalab.{datalabSchema}", [ {"principal" : f"{groupName}", "add" : [ "ALL_PRIVILEGES" ]} ]))
G1DataDeveloper("G1-Data-Developer")

# COMMAND ----------

def ComputeGrants(groupName):
    groupName =groupName
    print(UpdateWarehousePermissionByName([c["name"] for c in ListWarehouses()["warehouses"]][0], [{ "group_name": groupName, "permission_level": "CAN_USE" }]))
    print(UpdateClusterPermissionByName("interactive-uc", [{ "group_name": groupName, "permission_level": "CAN_RESTART" }]))
ComputeGrants(GetPrefix("-")+"L1-Official")
ComputeGrants(GetPrefix("-")+"G1-Data-Developer")

# COMMAND ----------

def DataLabGrants(groupName):
    prefix = GetPrefix("")
    print(UpdatePermission("CATALOG", f"datalab", [ {"principal" : f"{groupName}", "add" : [ "USE_CATALOG" ]} ]))
    print(UpdatePermission("SCHEMA", f"datalab.{prefix}", [ {"principal" : f"{groupName}", "add" : [ "ALL_PRIVILEGES" ]} ]))
#DataLabGrants(GetPrefix("-")+"L1-Official")
DataLabGrants(GetLegacyGroupName("DataAnalystAdvUsr"))

# COMMAND ----------

#print(GetObjectPermissions("directories", GetWorkspacePathId("/datalabs")))
#print(PutObjectPermissions("directories", GetWorkspacePathId("/datalabs"), [ { "group_name": "ppd-G1-Data-Developer", "permission_level": "CAN_READ" } ]))

# COMMAND ----------


