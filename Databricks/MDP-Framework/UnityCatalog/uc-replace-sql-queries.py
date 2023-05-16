# Databricks notebook source
# MAGIC %run ../Common/common-workspace

# COMMAND ----------

def ListUserSqlQueries():
    headers = GetAuthenticationHeader()
    url = f"{INSTANCE_NAME}/api/2.0/preview/sql/queries?page_size=9999"
    response = requests.get(url, headers=headers)
    return response.json()

# COMMAND ----------

def GetUserSqlQuery(id):
    headers = GetAuthenticationHeader()
    url = f"{INSTANCE_NAME}/api/2.0/preview/sql/queries/{id}"
    response = requests.get(url, headers=headers)
    return response.json()

# COMMAND ----------

def TransferUserSqlQueryOwner(id, new_owner):
    headers = GetAuthenticationHeader()
    url = f"{INSTANCE_NAME}/api/2.0/preview/sql/permissions/query/{id}/transfer"
    data = json.dumps( { 
        "new_owner": new_owner
    })
    response = requests.post(url, headers=headers, data=data)
    return response.json()

# COMMAND ----------

def CreateUserSqlQuery(json):
    headers = GetAuthenticationHeader()
    url = f"{INSTANCE_NAME}/api/2.0/preview/sql/queries"
    response = requests.post(url, headers=headers, data=json)
    return response.json()

# COMMAND ----------

def UpdateUserSqlQuery(id, query):
    headers = GetAuthenticationHeader()
    url = f"{INSTANCE_NAME}/api/2.0/preview/sql/queries/{id}"
    data = json.dumps( { 
        "query": query
    })
    response = requests.post(url, headers=headers, data=data)
    return response.json()

# COMMAND ----------

def GetUserSqlQueryPermission(id):
    headers = GetAuthenticationHeader()
    url = f"{INSTANCE_NAME}/api/2.0/preview/sql/permissions/queries/{id}"
    response = requests.get(url, headers=headers)
    return response.json()

# COMMAND ----------

def SetUserSqlQueryPermission(id, email):
    headers = GetAuthenticationHeader()
    url = f"{INSTANCE_NAME}/api/2.0/preview/sql/permissions/queries/{id}"
    data = json.dumps({ "access_control_list" : [
        { 
        "permission_level": "CAN_MANAGE"
        ,"user_name": email
        }
    ] })
    response = requests.post(url, headers=headers, data=data)
    return response.json()

# COMMAND ----------

def CreateUserQuery(name, query, schema, parent, owner):
    j = CreateUserSqlQuery(
    json.dumps( {
        "parent": parent,
        "name": name,
        "query": query,
        "run_as_role": "viewer",
        "options": {
            "catalog": "hive_metastore",
            "schema": schema,
        },
    })
    )
    queryId = j["id"]
    TransferUserSqlQueryOwner(queryId, owner)

# COMMAND ----------

def DeleteUserSqlQuery(id):
    headers = GetAuthenticationHeader()
    url = f"{INSTANCE_NAME}/api/2.0/preview/sql/queries/{id}"
    response = requests.delete(url, headers=headers)
    return response.json()

# COMMAND ----------

def CreateUserSqlQueryTarget(json):
    headers = GetAuthenticationHeader("daf-target-databricks-token")
    url = f"{INSTANCE_NAME}/api/2.0/preview/sql/queries"
    response = requests.post(url, headers=headers, data=json)
    return response.json()

# COMMAND ----------

def TransferUserSqlQueryOwnerTarget(id, new_owner):
    headers = GetAuthenticationHeader("daf-target-databricks-token")
    url = f"{INSTANCE_NAME}/api/2.0/preview/sql/permissions/query/{id}/transfer"
    data = json.dumps( { 
        "new_owner": new_owner
    })
    response = requests.post(url, headers=headers, data=data)
    return response.json()

# COMMAND ----------

def DeleteUserSqlQueryTarget(id):
    headers = GetAuthenticationHeader("daf-target-databricks-token")
    url = f"{INSTANCE_NAME}/api/2.0/preview/sql/queries/{id}"
    response = requests.delete(url, headers=headers)
    return response.json()

# COMMAND ----------

def ListUserSqlQueriesTarget():
    headers = GetAuthenticationHeader("daf-target-databricks-token")
    url = f"{INSTANCE_NAME}/api/2.0/preview/sql/queries?page_size=9999"
    response = requests.get(url, headers=headers)
    return response.json()

# COMMAND ----------

def GetUserQueriesExploded(oMethod):
    return (
        JsonToDataFrame(oMethod()).selectExpr("explode(results) o").select("o.*")
        .selectExpr(
            "id"
            ,"name"
            ,"query"
            ,"options.parent"
            ,"options.schema"
            ,"user.email"
        )
    )

# COMMAND ----------

def ReplaceUserQueries():
    list = GetUserQueriesExploded(ListUserSqlQueries).where("lower(query) like '%curated_v2%' or options.schema = 'curated_v2'")

    for i in list.collect():
        import re
        break
        # BACKUP
        CreateUserQuery(i.name + "-old", sql, i.schema, i.parent, i.email)
        # REPLACE
        replacedSql = re.compile(re.escape(i.schema), re.IGNORECASE).sub(targetSchema, i.query)
        # DELETE 
        DeleteUserSqlQuery(i.id)
        # UPLOAD
        CreateUserQuery(i.name, replacedSql, targetSchema, i.parent, i.email)
#ReplaceUserQueries()

# COMMAND ----------

def MigrateUserQueries(targetParent):
    list = GetUserQueriesExploded(ListUserSqlQueries).where("lower(user.email) like '%3bj%' and lower(name) not like '%old%'")
    
    for i in list.collect():
        import re

        j = json.dumps( {
                #TODO: FIND HOW TO GET THIS ID
                "parent": targetParent,
                "name": i.name,
                "query": i.query,
                "run_as_role": "viewer",
                "options": {
                    "catalog": "hive_metastore",
                    "schema": i.schema,
                },
            })
        # CREATE
        j = CreateUserSqlQueryTarget(j)
        # TRANSFER OWNER
        TransferUserSqlQueryOwnerTarget(j["id"], i.email)
#MigrateUserQueries("folders/2317656478667114")

# COMMAND ----------

 #GetUserQueriesExploded(ListUserSqlQueries).where("lower(user.email) like '3bj%' and name not like '%-old'").display()
 #GetUserQueriesExploded(ListUserSqlQueriesTarget).where("lower(user.email) like '%3bj%'").display()
