# Databricks notebook source
# MAGIC %run ../Common/common-workspace

# COMMAND ----------

# MAGIC %run ../Common/common-helpers

# COMMAND ----------

def ListGroups():
    headers = GetAuthenticationHeader()
    url = f'{INSTANCE_NAME}/api/2.0/preview/scim/v2/Groups'
    response = requests.get(url, headers=headers)
    jsonResponse = response.json()
    return jsonResponse
#groups = ListGroups()

# COMMAND ----------

#firstRow = spark.sql("SELECT DATE_FORMAT(CURRENT_TIMESTAMP(), 'yyyy/MM/dd') Path, DATE_FORMAT(CURRENT_TIMESTAMP(), 'yyMMddHHmm') BatchId").rdd.collect()[0]["Path"]
#resultsPath = f"/mnt/datalake-raw/edp/groups/{firstRow}/groups.json"
#print(resultsPath)
#dbutils.fs.put(resultsPath, json.dumps(groups), True)

# COMMAND ----------

df = spark.read.option("multiline", "true").json("/mnt/datalake-raw/edp/groups/*/*/*/*")
df = (df.selectExpr("explode(Resources) r")
        .selectExpr("r.*", "explode(r.members) m")
        .selectExpr("*", "m.*")
        .selectExpr(
            "id groupId"
            ,"displayName groupName"
            ,"value userId"
            ,"display userDisplayName"
        )
        .withColumn("CreatedDTS", expr("CURRENT_TIMESTAMP()"))
     )
#display(df)
#df.write.mode("append").saveAsTable("atf.f_rbac_group_assignment")

# COMMAND ----------


