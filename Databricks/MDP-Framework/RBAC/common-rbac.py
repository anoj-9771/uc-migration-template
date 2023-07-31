# Databricks notebook source
# MAGIC %run ../Common/common-workspace

# COMMAND ----------

# MAGIC %run ../Common/common-unity-catalog-helper

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
            j = ConvertTableName(j)
            list.append(GetCatalog(j) + "." + GetSchema(j))
    return set(list)

# COMMAND ----------

def GenerateRbacCommands(customGroups, preview=False, raiseError=False):
    _grants = []
    prefix = GetPrefix()
    groupPrefix = GetPrefix("-")

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
                t = prefix+t
                if "curated" in t or len(t.split(".")) == 3:
                    _grants.extend([{ "Group" : groupName, "Child" : t,  "Type" : "TABLE", "Command" : "SELECT", "Operation" : "add" } ])
                    continue
                t = ConvertTableName(t)
                catalog = GetCatalog(t)
                schema = GetSchema(t)
                table = GetTable(t)

                # TABLE
                if len(t.split(".")) == 3:
                    _grants.extend([{ "Group" : groupName, "Child" : f"{catalog}.{schema}.{table}", "Type" : "TABLE", "Command" : "SELECT", "Operation" : "add" }])
                else:
                    _grants.extend([{ "Group" : groupName, "Child" : f"{catalog}.{schema}.{i.tableName}",  "Type" : "TABLE", "Command" : "SELECT", "Operation" : "add" } for i in spark.sql(f"SHOW TABLES FROM {catalog}.{schema} LIKE '{table}'").collect()])

    for g in _grants:
        print(g)
        if preview:
            continue
        child = g["Child"]
        try:
            print(UpdatePermission(g["Type"], g["Child"], [ {"principal" : g["Group"], g["Operation"] : [ g["Command"] ]} ]))
            spark.sql(f"ALTER TABLE {child} SET OWNER TO `{groupPrefix}G3-Admins`")
        except Exception as e:
            print(f"Failed: {0}", g["Child"])
            if raiseError:
                raise e
