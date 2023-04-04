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

# COMMAND ----------

def WriteGroups():
    firstRow = spark.sql("SELECT DATE_FORMAT(CURRENT_TIMESTAMP(), 'yyyy/MM/dd') Path, DATE_FORMAT(CURRENT_TIMESTAMP(), 'yyMMddHHmm') BatchId").collect()[0]["Path"]
    resultsPath = f"/mnt/datalake-raw/edp/groups/{firstRow}/groups.json"
    print(resultsPath)
    dbutils.fs.put(resultsPath, json.dumps(groups), True)

# COMMAND ----------

def ReadGroups():
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

def TableAssignment():
    df = spark.sql("SHOW TABLES IN cleansed")#.limit(3)

    resultsDf = spark.createDataFrame([], 
            StructType([
                StructField('Principal', StringType()),
                StructField('ActionType', StringType()),
                StructField('ObjectType', StringType()),
                StructField('ObjectKey', StringType()),
                StructField('Schema', StringType()),
                StructField('Table', StringType()),
            ])
     )

    for i in df.collect():
        sql = f"SHOW GRANTS ON TABLE `{i.database}`.`{i.tableName}`"
        resultsDf = resultsDf.union(
            spark.sql(sql).where(f"ObjectType = 'TABLE' AND ActionType != 'OWN'")
            .withColumn("ObjectKey", expr("replace(ObjectKey, '`', '')"))
            .withColumn("Schema", expr("split(ObjectKey, '[.]', 0)[0]"))
            .withColumn("Table", expr("split(ObjectKey, '[.]', 0)[1]"))
        ) 
    resultsDf = resultsDf.withColumn("CreatedDTS", expr("CURRENT_TIMESTAMP()"))
    resultsDf.display()
    #resultsDf.write.mode("append").saveAsTable("atf.f_rbac_table_assignment")
    #display(resultsDf)
