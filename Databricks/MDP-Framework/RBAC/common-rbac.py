# Databricks notebook source
# MAGIC %run ../Common/common-workspace

# COMMAND ----------

groups = [ "L1-Official", "L2-Secured", "L3-PII", "UC5-Users" ]
for group in groups:
    if spark.sql(f"SHOW GROUPS LIKE '*{group}*'").count() > 0:
        continue
    spark.sql(f"CREATE GROUP `{group}`")

# COMMAND ----------

AssignGroup("L2-Secured", "L1-Official")
AssignGroup("L3-PII", "L2-Secured")
AssignGroup("UC5-Users", "L2-Secured")

# COMMAND ----------

list = ["s0y","yi1","vnw","f1d","0pk","4tm","f94","vds","0ke","hjv", "3bj"]
list = [f"{u}@sydneywater.com.au" for u in list]
j = GetGroupByName("UC5-Users")
groupId = j["Resources"][0]["id"]

#DeleteUsers(list)
CreateUsers(list, groupId)

# COMMAND ----------

# RUN ON SQL WAREHOUSE
systemCodes = ["maximo", "iicats", "hydsra", "hydra"]
list = ["GRANT USAGE ON SCHEMA cleansed TO `L1-Official`;"]

for s in systemCodes:
    df = spark.sql(f"SHOW TABLES FROM cleansed LIKE '{s}*'")
    for i in df.rdd.collect():
        grant = f"GRANT READ_METADATA, SELECT ON TABLE {i.database}.{i.tableName} TO `L2-Secured`;"
        list.append(grant)

print("\n".join(list))

# COMMAND ----------


