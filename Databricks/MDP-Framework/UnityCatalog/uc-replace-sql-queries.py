# Databricks notebook source
# MAGIC %run ../Common/common-workspace

# COMMAND ----------

# MAGIC %run ../Common/common-unity-catalog-helper

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

def CreateUserQuery(name, query, catalog, schema, parent, owner):
    j = CreateUserSqlQuery(
    json.dumps( {
        "parent": parent,
        "name": name,
        "query": query,
        "run_as_role": "viewer",
        "options": {
            "catalog": catalog,
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
            ,"options.catalog"
            ,"options.schema"
            ,"user.email"
        )
    )

# COMMAND ----------

def ReplaceUserQueries():
    import re
    list = GetUserQueriesExploded(ListUserSqlQueriesTarget).where("options.schema IN ('curated')")
    #list = GetUserQueriesExploded(ListUserSqlQueriesTarget)
    #list = GetUserQueriesExploded(ListUserSqlQueries)
    #tables = []
    for i in list.collect():
        tables = []
        # REPLACE
        repalcedSql = i.query
        for m in re.finditer("(?i)[ |\r|\n](from|join)[ |\r|\n]*[a-zA-Z0-9_.]+", i.query, re.S):
            table = re.split("[ |\r|\n]", m.group(0))[-1:][0]
            if len(table) <= 7 or "datalab" in table:
                continue
            tables.append(table)
            repalcedSql = repalcedSql.replace(table, ConvertTableName(table))

        if len(tables) == 0:
            continue
        targetCatalog = GetCatalogPrefix()+i.schema
        targetSchema = GetSchema(tables[0])

        # BACKUP
        #CreateUserQuery(i.name + "-old", sql, i.schema, i.parent, i.email)
        # DELETE 
        #DeleteUserSqlQuery(i.id)
        # CREATE
        #CreateUserQuery(i.name, repalcedSql, targetCatalog, targetSchema, i.parent, i.email)
        print(f"Creating {i.name}... from {i.email}")
        #CreateUserQuery(i.name, repalcedSql, targetCatalog, targetSchema, "folders/3666611353204640", "o3bj@sydneywater.com.au")
    #for t in sorted(set(tables)):
        #print(t + "," + ConvertTableName(t))
ReplaceUserQueries()

# COMMAND ----------

def MigrateUserQueries(targetParent):
    list = GetUserQueriesExploded(ListUserSqlQueries)
    
    for i in list.collect():
        print(i)
        j = json.dumps( {
                #TODO: FIND HOW TO GET THIS ON TARGET
                "parent": targetParent,
                "name": i.name,
                "query": i.query,
                "run_as_role": "viewer",
                "options": {
                    "catalog": i.catalog,
                    "schema": i.schema,
                },
            })
        # CREATE
        #j = CreateUserSqlQueryTarget(j)
        # TRANSFER OWNER
        #TransferUserSqlQueryOwnerTarget(j["id"], i.email)
#MigrateUserQueries("folders/2317656478667114")
