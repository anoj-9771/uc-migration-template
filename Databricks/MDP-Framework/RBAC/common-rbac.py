# Databricks notebook source
# MAGIC %run ../Common/common-workspace

# COMMAND ----------

# MAGIC %run ../Common/common-helpers

# COMMAND ----------

L1_NAME = "L1-Official"
L2_NAME = "L2-Secured"
L3_NAME = "L3-PII"

GROUP_LIST = [
    {
        "Name" : "L1-Official"
        ,"Systems" : [ "beachwatch", "hydra", "bom" ]
    }
    ,{
        "Name" : "L2-Secured"
        ,"ParentGroup" : "L1-Official"
        ,"Systems" : [ "maximo", "iicats", "hydsra", "iot" ]
    }
    ,{
        "Name" : "L3-PII"
        ,"ParentGroup" : "L2-Secured"
        ,"Systems" : [ "qualtrics", "sapisu", "sapcrm" ]
    }
]

# LOADED BY /dbfs/FileStore/rbac.json
USER_GROUPS = LoadJsonFile("/dbfs/FileStore/rbac.json")["UserGroups"]
print(USER_GROUPS)

# COMMAND ----------

def ListUsersInGroup(name):
    df = JsonToDataFrame(GetGroupByName(name)).selectExpr("explode(Resources[0].members) col").select("col.*")
    display(df)

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
    for ug in USER_GROUPS:
        groupName = ug["Name"]
        parentGroup = ug["Group"]
        if spark.sql(f"SHOW GROUPS LIKE '*{groupName}*'").count() == 0:
            spark.sql(f"CREATE GROUP `{groupName}`")
            AssignGroup(groupName, parentGroup)
        #print(ug)

        list = [f"{u}@sydneywater.com.au" for u in ug["Users"]]
        j = GetGroupByName(groupName)
        groupId = j["Resources"][0]["id"]
        CreateUsers(list, groupId)

# COMMAND ----------

def DeleteGroupUsers():
    for ug in USER_GROUPS:
        list = [f"{u}@sydneywater.com.au" for u in ug["Users"]]
        DeleteUsers(list)

# COMMAND ----------

def GenerateGrantPermissions():
    # RUN ON SQL WAREHOUSE
    list = [
        f"GRANT ALL PRIVILEGES ON SCHEMA datalab TO `{L1_NAME}`;"
        ]

    for g in GROUP_LIST:
        groupName = g["Name"]
        list.append(f"GRANT USAGE ON SCHEMA cleansed TO `{groupName}`;")
        
        for s in g["Systems"]:
            df = spark.sql(f"SHOW TABLES FROM cleansed LIKE '{s}*'")
            list.extend([f"GRANT READ_METADATA, SELECT ON TABLE {i.database}.{i.tableName} TO `{groupName}`;" for i in df.rdd.collect()])

    print("\n".join(list))

# COMMAND ----------

def Run():
    CreateGroups()
    CreateUserGroups()
    GenerateGrantPermissions()
Run()

# COMMAND ----------


