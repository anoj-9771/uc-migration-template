# Databricks notebook source
# MAGIC %run ../Common/common-workspace

# COMMAND ----------

# MAGIC %run ../Common/common-helpers

# COMMAND ----------

def GetEnvironmentTag():
    j = json.loads(spark.conf.get("spark.databricks.clusterUsageTags.clusterAllTags"))
    return [x['value'] for x in j if x['key'] == 'Environment'][0]

# COMMAND ----------

L1_NAME = "L1-Official"
L2_NAME = "L2-Secured"
L3_NAME = "L3-PII"

GROUP_LIST = [
    {
        "Name" : f"{L1_NAME}"
        ,"Systems" : [ "beachwatch", "hydra", "bom" ]
    }
    ,{
        "Name" : f"{L2_NAME}"
        ,"ParentGroup" : f"{L1_NAME}"
        ,"Systems" : [ "maximo", "iicats", "hydsra", "iot", "access", "isu", "crm" ]
    }
    ,{
        "Name" : f"{L3_NAME}"
        ,"ParentGroup" : f"{L2_NAME}"
        ,"Systems" : [ "qualtrics", "sapisu", "sapcrm" ]
    }
]

LEGACY_GROUP_LIST = [ 
    [ "DataAnalystAdvUsr", f"{L2_NAME}" ]
    ,[ "DataAnalystStdUsr", f"{L2_NAME}" ]
]
USER_GROUPS = []
CUSTOM_GROUPS = []

# COMMAND ----------

env = GetEnvironmentTag().lower()
for i in ["", f"-{env}"]:
    try:
        r = dbutils.notebook.run(f"../RBAC/CustomGroups/rbac-groups{i}", 0, {})
        j = json.loads(r.replace("\'", "\""))
        CUSTOM_GROUPS.extend(j["CUSTOM_GROUPS"])
    except:
        pass
print(CUSTOM_GROUPS)

# COMMAND ----------

def LoadCustomGroupsFile():
    # LOADED BY /dbfs/FileStore/rbac.json
    global USER_GROUPS, CUSTOM_GROUPS
    RBAC_FILE_PATH = "/dbfs/FileStore/rbac.json"
    
    USER_GROUPS = LoadJsonFile(f"{RBAC_FILE_PATH}")["UserGroups"]
    CUSTOM_GROUPS = LoadJsonFile(f"{RBAC_FILE_PATH}")["CustomGroups"]
    #AAD_GROUPS = LoadJsonFile(f"{RBAC_FILE_PATH}")["AADGroups"]
    print(USER_GROUPS)
    print(CUSTOM_GROUPS)
    #print(AAD_GROUPS)

# COMMAND ----------

def PersistGroups():
    df = (spark.read
            .schema(StructType([StructField(i,StringType(),True ) for i in ["name", "list"]]))
            .json(sc.parallelize([
                { "name" : "USER_GROUPS", "list" : USER_GROUPS }
                ,{ "name" : "CUSTOM_GROUPS", "list" : CUSTOM_GROUPS }
                ,{ "name" : "AAD_GROUPS", "list" : AAD_GROUPS }
            ]))
            .withColumn("timestamp", expr("CURRENT_TIMESTAMP()"))
            .write.mode("overwrite").saveAsTable("edp.f_rbac_group_definition") 
         )

# COMMAND ----------

def CreateGroups():
    for g in GROUP_LIST:
        groupName = g["Name"]
        if spark.sql(f"SHOW GROUPS LIKE '*{groupName}*'").count() > 0:
            continue
            
        spark.sql(f"CREATE GROUP `{groupName}`")
        parentGroup = g.get("ParentGroup")
        
        if parentGroup is not None:
            AssignGroup(groupName, parentGroup)

# COMMAND ----------

def CreateUserGroups():
    allUsers = GetUsers()
    for ug in USER_GROUPS:
        groupName = ug["Name"]
        parentGroup = ug["Group"]
        if spark.sql(f"SHOW GROUPS LIKE '*{groupName}*'").count() == 0:
            spark.sql(f"CREATE GROUP `{groupName}`")
            AssignGroup(groupName, parentGroup)

        users = ug["Users"]
        list = [f"{u}@sydneywater.com.au" for u in users]
        j = GetGroupByName(groupName)
        groupId = j["Resources"][0]["id"]
        CreateUsers(list, groupId)
        userIds = [ i["id"] for i in allUsers["Resources"] if i["userName"] in list ]
        AddGroupMembers(groupId, userIds)

# COMMAND ----------

def RevokeAllPermissions():
    for g in GROUP_LIST:
        RevokePermissionsForGroup(g["Name"], "cleansed")

# COMMAND ----------

def GrantPermissions():
    list = [
        f"GRANT ALL PRIVILEGES ON SCHEMA datalab TO `{L1_NAME}`;"
        ]

    for g in GROUP_LIST:
        groupName = g["Name"]
        list.append(f"GRANT USAGE ON SCHEMA cleansed TO `{groupName}`;")
        
        for s in g["Systems"]:
            df = spark.sql(f"SHOW TABLES FROM cleansed LIKE '{s}*'")
            list.extend([f"GRANT READ_METADATA, SELECT ON TABLE {i.database}.{i.tableName} TO `{groupName}`;" for i in df.collect()])
    sql = "\n".join(list)

    clusterId = GetFirstAclEnabledCluster()["cluster_id"]
    ExecuteCommand(sql, clusterId)
    return sql

# COMMAND ----------

def CreateCustomGroups():
    clusterId = GetFirstAclEnabledCluster()["cluster_id"]
    allUsers = GetUsers()
    
    for g in CUSTOM_GROUPS:
        groupName = g["Name"]
        users = [f"{u}@sydneywater.com.au" for u in g["Users"]]

        # CREATE GROUP IF IT DOESN'T EXIST
        if spark.sql(f"SHOW GROUPS LIKE '{groupName}'").count() == 0:
            spark.sql(f"CREATE GROUP `{groupName}`")
            
        # USER GROUP ASSIGNMENT
        userIds = [ i["id"] for i in allUsers["Resources"] if i["userName"] in users ]
        AddGroupMembers(GetGroupByName(groupName)["Resources"][0]["id"], userIds)
        
        # GRANT DISTINCT TO SCHEMA/TABLE FIRST
        for schema in set([i.split(".", 1)[0] for i in g["TableFilter"]]):
            #RevokePermissionsForGroup(groupName, schema)
            ExecuteCommand(f"GRANT USAGE ON SCHEMA {schema} TO `{groupName}`;", clusterId)
            
        # GENERATE AND EXECUTE GRANTS TO TABLES
        for t in g["TableFilter"]:
            schema, table = t.split(".", 1)
            df = spark.sql(f"SHOW TABLES FROM {schema} LIKE '{table}'")
            ExecuteCommand("".join([f"GRANT READ_METADATA, SELECT ON TABLE {i.database}.{i.tableName} TO `{groupName}`;" for i in df.collect()]), clusterId)

# COMMAND ----------

def AssignLegacyGroups():
    for l in LEGACY_GROUP_LIST:
        AssignGroup([g["displayName"] for g in ListGroups()["Resources"] if l[0] in g["displayName"]][0], l[1])

# COMMAND ----------

def AADGroupAssignment():
    clusterId = GetFirstAclEnabledCluster()["cluster_id"]
    groups = ListGroups()
    for adg in AAD_GROUPS:
        groupName = [g["displayName"] for g in groups["Resources"] if adg["NameWildcard"] in g["displayName"]][0]
        
        # GRANT DISTINCT TO SCHEMA/TABLE FIRST
        for schema in set([i.split(".", 1)[0] for i in adg["TableFilter"]]):
            #RevokePermissionsForGroup(groupName, schema)
            ExecuteCommand(f"GRANT USAGE ON SCHEMA {schema} TO `{groupName}`;", clusterId)
            
        # GENERATE AND EXECUTE GRANTS TO TABLES
        for t in adg["TableFilter"]:
            schema, table = t.split(".", 1)
            df = spark.sql(f"SHOW TABLES FROM {schema} LIKE '{table}'")
            ExecuteCommand("".join([f"GRANT READ_METADATA, SELECT ON TABLE {i.database}.{i.tableName} TO `{groupName}`;" for i in df.collect()]), clusterId)

# COMMAND ----------

def LegacyCleansedDataAnalystAdvUsrAssignment():
    clusterId = StartFirstAclEnabledCluster()["cluster_id"]
    groupName = g = [g["displayName"] for g in ListGroups()["Resources"] if "DataAnalystAdvUsr" in g["displayName"]][0]
    for db in ["raw", "cleansed"]:
        for s in ["access", "isu", "crm", "hydra"]:
            df = spark.sql(f"SHOW TABLES FROM cleansed LIKE '{s}*'")
            sql = [f"GRANT READ_METADATA, SELECT ON TABLE {i.database}.{i.tableName} TO `{groupName}`;" for i in df.collect()]
            #print("\n".join(sql))
            ExecuteCommand("".join([f"GRANT READ_METADATA, SELECT ON TABLE {i.database}.{i.tableName} TO `{groupName}`;" for i in df.collect()]), clusterId)

# COMMAND ----------

def ApplyGrantForTable(tableFqn):
    clusterId = StartFirstAclEnabledCluster()["cluster_id"]
    lSchema = tableFqn.split(".")[0]
    lTableName = tableFqn.split(".")[1]
    groups = ListGroups()
    sql = ""

    for g in CUSTOM_GROUPS:
        groupName = g["Name"]
        
        # GENERATE AND EXECUTE GRANTS FOR TABLE
        for t in g["TableFilter"]:
            schema, table = t.split(".", 1)
            
            if schema != lSchema:
                continue
            if not(any([i["displayName"] for i in groups["Resources"] if groupName == i["displayName"]])):
                print(f"Warning group \"{groupName}\" doesn't exist! Skipping...")
                #continue
            df = spark.sql(f"SHOW TABLES FROM {schema} LIKE '{table}'").where(f"tableName = '{lTableName}'")

            if df.count() != 1:
                continue
            sql += "\n"+ "".join([f"GRANT READ_METADATA, SELECT ON TABLE {i.database}.{i.tableName} TO `{groupName}`;" for i in df.collect()])
    print(sql)
    ExecuteCommand(sql, clusterId)

# COMMAND ----------

def RevokeByPrefix(prefix):
    clusterId = StartFirstAclEnabledCluster()["cluster_id"]
    for schema in ["raw", "cleansed"]:
        df = spark.sql(f"SHOW TABLES FROM {schema} LIKE '{prefix}*'")
        for group in [r["displayName"] for r in ListGroups()["Resources"] if r["displayName"] not in ['users', 'admins', 'AI-ML']]:
            sql = "\n".join([f"REVOKE ALL PRIVILEGES ON TABLE `{i.database}`.`{i.tableName}` FROM `{group}`;" for i in df.collect()])
            #print(sql)
            ExecuteCommand(sql, clusterId)
