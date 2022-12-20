# Databricks notebook source
# MAGIC %run ../Common/common-workspace

# COMMAND ----------

L1_NAME = "L1-Official"
L2_NAME = "L2-Secured"
L3_NAME = "L3-PII"

USER_GROUPS = [
    { "Name" : "UC5-Users", "Users" : ["s0y","yi1","vnw","f1d","0pk","4tm","f94","vds","0ke","hjv","3bj"] }
]

SYSTEM_CODES = [ "maximo", "iicats", "hydsra", "hydra" ]

# COMMAND ----------

def CreateGroups():
    groups = [ L1_NAME, L2_NAME, L3_NAME ]
    groups.extend([i["Name"] for i in USER_GROUPS])
    for group in groups:
        if spark.sql(f"SHOW GROUPS LIKE '*{group}*'").count() > 0:
            continue
        spark.sql(f"CREATE GROUP `{group}`")
    AssignGroup(L2_NAME, L1_NAME)
    AssignGroup(L3_NAME, L2_NAME)

# COMMAND ----------

def CreateUserGroups():
    for ug in USER_GROUPS:
        groupName = ug["Name"]
        AssignGroup(groupName, L2_NAME)
        list = [f"{u}@sydneywater.com.au" for u in ug["Users"]]
        j = GetGroupByName(groupName)
        groupId = j["Resources"][0]["id"]

        #DeleteUsers(list)
        CreateUsers(list, groupId)

# COMMAND ----------

def GenerateGrantPermissions():
    # RUN ON SQL WAREHOUSE
    list = [
        f"GRANT USAGE ON SCHEMA cleansed TO `{L1_NAME}`;"
        ,f"GRANT ALL PRIVILEGES ON SCHEMA datalab TO `{L1_NAME}`;"
        ]

    for s in SYSTEM_CODES:
        df = spark.sql(f"SHOW TABLES FROM cleansed LIKE '{s}*'")
        for i in df.rdd.collect():
            grant = f"GRANT READ_METADATA, SELECT ON TABLE {i.database}.{i.tableName} TO `{L2_NAME}`;"
            list.append(grant)
            spark.sql()

    print("\n".join(list))

# COMMAND ----------

CreateGroups()
CreateUserGroups()
GenerateGrantPermissions()

# COMMAND ----------


