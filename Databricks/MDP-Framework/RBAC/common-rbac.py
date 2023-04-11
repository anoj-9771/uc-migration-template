# Databricks notebook source
# MAGIC %run ../Common/common-workspace

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
            #pass
PopuateGroups()

# COMMAND ----------

def GenerateRbacCommands():
    _grants = []
    for g in CUSTOM_GROUPS:
        groupName = g["Name"]
        parentGroup = g.get("ParentGroup")
        level = 0 if g.get("Level") is None else int(g.get("Level"))
        aadGroup = g.get("AADGroups")
        users = g.get("Users")
        tableFilter = g.get("TableFilter")
        otherCommands = g.get("OtherCommands")
        
        _grants.append({ "Group" : groupName, "Child" : "", "Type" : "SqlCreateGroup", "Command" : f"CREATE GROUP `{groupName}`;"})
        _grants.append({ "Group" : groupName, "Child" : parentGroup, "Type" : "ScimAssignGroup", "Command" : f"AssignGroupWildcard,{parentGroup},{groupName}"}) if parentGroup is not None else ()
        _grants.extend([{ "Group" : groupName, "Child" : i, "Type" : "ScimAssignGroup", "Command" : f"AssignGroupWildcard,{groupName},{i}"} for i in aadGroup]) if aadGroup is not None else ()
        _grants.extend([{ "Group" : groupName, "Child" : i, "Type" : "ScimAddGroup", "Command" : f"AddUsersToGroup,{groupName},{i}@sydneywater.com.au"} for i in users]) if users is not None else ()
        _grants.extend([{ "Group" : groupName, "Child" : i, "Type" : "SqlSchemaGrant", "Command" : f"GRANT USAGE ON SCHEMA `{i}` TO `{groupName}`;"} for i in set([i.split(".")[0] for i in tableFilter])]) if tableFilter is not None else ()
        _grants.extend([{ "Group" : groupName, "Child" : i, "Type" : "SqlOtherCommand", "Command" : i} for i in set([i for i in otherCommands])]) if otherCommands is not None else ()

        if tableFilter is not None:
            for t in tableFilter:
                schema, table = t.split(".", 1)
                
                _grants.extend([{ "Group" : groupName, "Child" : f"`{i.database}`.`{i.tableName}`",  "Type" : "SqlTableGrant", "Command" : f"GRANT READ_METADATA, SELECT ON TABLE `{i.database}`.`{i.tableName}` TO `{groupName}`;"} for i in spark.sql(f"SHOW TABLES FROM {schema} LIKE '{table}'").collect()])

                # REMOVE FROM L1 GRANT
                if level > 1:
                    [_grants.remove(i) for i in _grants if i["Type"] == "SqlTableGrant" and i["Group"] == "L1-Official" if i["Child"] in [i["Child"] for i in _grants if i["Type"] == "SqlTableGrant" and i["Group"] == groupName ]]
                    _grants.extend([{ "Group" : groupName, "Child" : f"`{i.database}`.`{i.tableName}`",  "Type" : "SqlTableGrant", "Command" : f"REVOKE ALL PRIVILEGES ON TABLE `{i.database}`.`{i.tableName}` FROM `L1-Official`;"} for i in spark.sql(f"SHOW TABLES FROM {schema} LIKE '{table}'").collect()])
 
    # PERSIST TABLE
    (spark.read.json(sc.parallelize(_grants))
    .withColumn("CreatedDTS", expr("CURRENT_TIMESTAMP()"))
    ).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(RBAC_TABLE) 
#GenerateRbacCommands()

# COMMAND ----------

def RbacExecuteSqlCommands(list):
    commandCount = len(list)
    if commandCount == 0:
        return
    clusterId = StartFirstAclEnabledCluster()["cluster_id"]
    sql ="\n".join(list)
    #print(sql)
    print(f"SQL Commands: {commandCount}")
    ExecuteCommand(sql, clusterId)
def RbacExecuteScimCommands(list):
    commandCount = len(list)
    if commandCount == 0:
        return
    #print(list)
    print(f"SCIM Commands: {commandCount}")
    for i in list:
        method, arg1, arg2 = i.split(",", 2)
        print(method, arg1, arg2)
        try:
            print(globals()[method](arg1, arg2))
        except Exception as e:
            print(e)
def RbacExecuteCommands():
    # GROUPS
    print("Creating Groups...")
    RbacExecuteSqlCommands([i.Command for i in spark.table(RBAC_TABLE).where("`Type` = 'SqlCreateGroup'").select("Command").collect() if not(any(j.displayName in i.Command for j in JsonToDataFrame(ListGroups()["Resources"]).select("displayName").collect()))])
    
    # SCHEMAS
    print("Schemas Grants...")
    RbacExecuteSqlCommands([i.Command for i in spark.table(RBAC_TABLE).where("`Type` = 'SqlSchemaGrant'").select("Command").collect()])
    
    # GROUP ASSIGNMENT
    print("Group Assignment...")
    RbacExecuteScimCommands([i.Command for i in spark.table(RBAC_TABLE).where("`Type` = 'ScimAssignGroup'").select("Command").collect()])
    
    # TABLE GRANT
    print("Table grants...")
    RbacExecuteSqlCommands([i.Command for i in spark.table(RBAC_TABLE).where("`Type` = 'SqlTableGrant'").select("Command").collect()])
    
    # TABLE DENY
    print("Table deny...")
    RbacExecuteSqlCommands([i.Command for i in spark.table(RBAC_TABLE).where("`Type` = 'SqlTableDeny'").select("Command").collect()])
    
    # OTHER COMMANDS
    print("Other commands...")
    RbacExecuteSqlCommands([i.Command for i in spark.table(RBAC_TABLE).where("`Type` = 'SqlOtherCommand'").select("Command").collect()])

#RbacExecuteCommands()

# COMMAND ----------

def CustomGroupMemberAssignment(customGroups):
    for g in customGroups:
        groupName = g["Name"]

        # CREATE GROUP IF IT DOESN'T EXIST
        if spark.sql(f"SHOW GROUPS LIKE '{groupName}'").count() == 0:
            spark.sql(f"CREATE GROUP `{groupName}`")

        groupId = GetGroupByName(groupName)["Resources"][0]["id"]
        users = [f"{u}@sydneywater.com.au" for u in g["Users"]]
        newUsers = [u for u in users if u not in [i["userName"] for i in GetUsers()["Resources"]] ]

        #CREATE NEW USERS
        for u in newUsers:
            print(CreateUser(u))
            
        # ASSIGN USERS TO GROUP
        AddUsersToGroup(groupName, users)
    
#CustomGroupMemberAssignment(
    #[{ "Name" : "UC1-Training"
            #,"Users" : [ "t1w","2zu","ajp","lpr","ub0","pxz","rn7","fws","i3s","yce","enl","4xd","2bv","ao8","dgf","nnn","lf8","qil","45c" ] }]
#)
    
#CustomGroupMemberAssignment(
    #[{ "Name" : "UAT-L2"
            #,"Users" : [ "s0y","yi1","vnw","ngz","jde","hpr","f1d","0pk","4tm","f94","vds","0ke","hjv","n8m","ulr","w0a","q1g","wpv","lnt","ito","lpv","i2k","nnn","xv5","wiu","tgm","cdv","mqv","rka" ] }]
#)

# COMMAND ----------


