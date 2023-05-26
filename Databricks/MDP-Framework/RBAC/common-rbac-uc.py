# Databricks notebook source
# MAGIC %run ../Common/common-workspace

# COMMAND ----------

# MAGIC %run ../Common/common-spark

# COMMAND ----------

CUSTOM_GROUPS = []
RBAC_TABLE = "edp.f_rbac_commands"

# COMMAND ----------

def GetEnvironmentTag():
    j = json.loads(spark.conf.get("spark.databricks.clusterUsageTags.clusterAllTags"))
    return [x['value'] for x in j if x['key'] == 'Environment'][0]

# COMMAND ----------

def PopuateGroups():
    global CUSTOM_GROUPS
    CUSTOM_GROUPS = []
    env = GetEnvironmentTag().lower()

    for i in ["", f"-{env}"]:
        try:
            r = dbutils.notebook.run(f"../RBAC/CustomGroups/rbac-groups{i}", 0, {})
            j = json.loads(r.replace("\'", "\""))
            CUSTOM_GROUPS.extend(j["CUSTOM_GROUPS"])
        except Exception as e:
            print(e)
#PopuateGroups()

# COMMAND ----------

CUSTOM_GROUPS = [
    {
        "Name" : "L1-Official"
        ,"Level" : 1
        ,"OtherCommands" : [
        ]
        ,"TableFilter" : [
            "cleansed.bom_*"
            ,"cleansed.beachwatch_*"
        ]
        ,"Users" : []
        ,"AADGroups" : [ "cleansed-official" ]
    },
    {
        "Name" : "L2-Sensitive"
        ,"Level" : 2
        ,"ParentGroup" : "L1-Official"
        ,"TableFilter" : [
            "cleansed.iicats_event"
        ]
        ,"Users" : []
        ,"AADGroups" : [ "cleansed-sensitive" ]
    },
    {
        "Name" : "L3-Sensitive-Other"
        ,"Level" : 3
        ,"ParentGroup" : "L2-Sensitive"
        ,"TableFilter" : [
            "cleansed.maximo_servrectrans"
        ]
        ,"Users" : []
        ,"AADGroups" : [ "sensitive-others" ]
    }
]

# COMMAND ----------

CUSTOM_GROUPS = [
    {
        "Name" : "L1-Official"
        ,"Level" : 1
        ,"OtherCommands" : [
        ]
        ,"TableFilter" : [
            "cleansed.bom_*"
        ]
        ,"Users" : []
        ,"AADGroups" : [ "cleansed-official" ]
    },
    {
        "Name" : "L2-Sensitive"
        ,"Level" : 2
        ,"ParentGroup" : "L1-Official"
        ,"TableFilter" : [
            "cleansed.bom_fortdenision_tide"
        ]
        ,"Users" : []
        ,"AADGroups" : [ "cleansed-sensitive" ]
    },
    {
        "Name" : "L3-Sensitive-Other"
        ,"Level" : 3
        ,"ParentGroup" : "L2-Sensitive"
        ,"TableFilter" : [
            "cleansed.bom_weatherforecast"
        ]
        ,"Users" : []
        ,"AADGroups" : [ "sensitive-others" ]
    }
]

# COMMAND ----------

def GetCatalogPrefix():
    try:
        return dbutils.secrets.get("ADS" if not(any([i for i in json.loads(spark.conf.get("spark.databricks.clusterUsageTags.clusterAllTags")) if i["key"] == "Application" and "hackathon" in i["value"].lower()])) else "ADS-AKV", 'databricks-env')
    except:
        return None

# COMMAND ----------

def UpdatePermission(securable_type, full_name, grants):
    headers = GetAuthenticationHeader()
    url = f'{INSTANCE_NAME}/api/2.1/unity-catalog/permissions/{securable_type}/{full_name}'
    permissions = { "changes": [ *grants ] }
    response = requests.patch(url, json=permissions, headers=headers)
    return response.json()

# COMMAND ----------

def GetCatalog(namespace):
    prefix = GetCatalogPrefix()
    s = namespace.replace("_", ".").split(".")
    return prefix+s[0]

# COMMAND ----------

def GetSchema(namespace):
    return namespace.replace("_", ".").split(".")[1]

# COMMAND ----------

def GetTable(namespace):
    return namespace.replace(namespace.split( "_" )[:1][0]+"_", "")

# COMMAND ----------

def GenerateRbacCommands():
    _grants = []
    prefix = GetCatalogPrefix()
    groupPrefix = prefix.replace("_", "-")
    for g in CUSTOM_GROUPS:
        groupName = groupPrefix+g["Name"]
        parentGroup = g.get("ParentGroup")
        level = 0 if g.get("Level") is None else int(g.get("Level"))
        aadGroup = g.get("AADGroups")
        users = g.get("Users")
        tableFilter = g.get("TableFilter")
        otherCommands = g.get("OtherCommands")
        
        # CATALOG
        _grants.extend([{ "Group" : groupName, "Child" : i, "Type" : "CATALOG", "Command" : "USE_CATALOG", "Operation" : "add" } for i in set([GetCatalog(i) for i in tableFilter])]) if tableFilter is not None and (level or 1) == 1 else ()
        # SCHEMA
        _grants.extend([{ "Group" : groupName, "Child" : i, "Type" : "SCHEMA", "Command" : "USE_SCHEMA", "Operation" : "add" } for i in set([ GetCatalog(i)+"."+GetSchema(i) for i in tableFilter ])]) if tableFilter is not None and (level or 1) == 1 else ()

        if tableFilter is not None:
            for t in tableFilter:
                catalog = GetCatalog(t)
                schema = GetSchema(t)
                table = GetTable(t)

                # TABLE
                _grants.extend([{ "Group" : groupName, "Child" : f"{catalog}.{schema}.{i.tableName}",  "Type" : "TABLE", "Command" : "SELECT", "Operation" : "add" } for i in spark.sql(f"SHOW TABLES FROM {catalog}.{schema} LIKE '{table}'").collect()])

                # REMOVE L1 GRANT
                if level > 1: 
                    # REMOVE
                    [_grants.remove(i) for i in [{ "Group" : f"{groupPrefix}L1-Official", "Child" : f"{catalog}.{schema}.{i.tableName}",  "Type" : "TABLE", "Command" : "SELECT", "Operation" : "add" } for i in spark.sql(f"SHOW TABLES FROM {catalog}.{schema} LIKE '{table}'").collect()] if i in _grants ]
                    # REVOKE
                    _grants.extend([{ "Group" : f"{groupPrefix}L1-Official", "Child" : f"{catalog}.{schema}.{i.tableName}",  "Type" : "TABLE", "Command" : "SELECT", "Operation" : "remove" } for i in spark.sql(f"SHOW TABLES FROM {catalog}.{schema} LIKE '{table}'").collect()] ) 
    for g in _grants:
        print(g)
        #UpdatePermission(g["Type"], g["Child"], [ {"principal" : g["Group"], g["Operation"] : [ g["Command"] ]} ])
        UpdatePermission(g["Type"], g["Child"], [ {"principal" : g["Group"], "remove" : [ g["Command"] ]} ])
GenerateRbacCommands()

# COMMAND ----------


