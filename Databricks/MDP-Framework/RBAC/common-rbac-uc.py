# Databricks notebook source
# MAGIC %run ../Common/common-workspace

# COMMAND ----------

# MAGIC %run ../Common/common-unity-catalog-helper

# COMMAND ----------

CUSTOM_GROUPS = []
RBAC_TABLE = "edp.f_rbac_commands"

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
            ,"cleansed.iicats_groups"
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
            ,"curated.dimcontract"
        ]
        ,"Users" : []
        ,"AADGroups" : [ "cleansed-sensitive" ]
    },
    {
        "Name" : "L3-Sensitive-Other"
        ,"Level" : 3
        ,"ParentGroup" : "L2-Sensitive"
        ,"TableFilter" : [
            "cleansed.maximo_workorder"
        ]
        ,"Users" : []
        ,"AADGroups" : [ "sensitive-others" ]
    }
]

# COMMAND ----------

def GetDistinctCatalogs(customGroups):
    list = []
    for i in [ i.get("TableFilter") for i in customGroups if i.get("Level") is not None ]:
        for j in i:
            list.append(GetCatalog(j))
    return set(list)

# COMMAND ----------

def GetDistinctCatalogSchemas(customGroups):
    list = []
    for i in [ i.get("TableFilter") for i in customGroups if i.get("Level") is not None ]:
        for j in i:
            list.append(GetCatalog(j) + "." + GetSchema(j))
    return set(list)

# COMMAND ----------

def GenerateRbacCommands(customGroups):
    _grants = []
    prefix = GetPrefix()
    groupPrefix = GetPrefix("-")

    # CATALOG
    _grants.extend([{ "Group" : f"{groupPrefix}L1-Official", "Child" : i, "Type" : "CATALOG", "Command" : "USE_CATALOG", "Operation" : "add" } for i in GetDistinctCatalogs(customGroups)])
    # SCHEMA
    _grants.extend([{ "Group" : f"{groupPrefix}L1-Official", "Child" : i, "Type" : "SCHEMA", "Command" : "USE_SCHEMA", "Operation" : "add" } for i in GetDistinctCatalogSchemas(customGroups)])

    for g in customGroups:
        groupName = groupPrefix+g["Name"]
        parentGroup = g.get("ParentGroup")
        level = 0 if g.get("Level") is None else int(g.get("Level"))
        aadGroup = g.get("AADGroups")
        users = g.get("Users")
        tableFilter = g.get("TableFilter")
        otherCommands = g.get("OtherCommands")
        
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
        print(UpdatePermission(g["Type"], g["Child"], [ {"principal" : g["Group"], g["Operation"] : [ g["Command"] ]} ]))
        #UpdatePermission(g["Type"], g["Child"], [ {"principal" : g["Group"], "remove" : [ g["Command"] ]} ])
GenerateRbacCommands(CUSTOM_GROUPS)

# COMMAND ----------


