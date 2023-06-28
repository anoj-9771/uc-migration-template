# Databricks notebook source
import requests, json

# COMMAND ----------

SECRET_SCOPE = "ADS"
account_id = dbutils.secrets.get(scope = SECRET_SCOPE, key = "databricks-account-id")
ACCOUNT_API = f"https://accounts.azuredatabricks.net/api/2.0/accounts/{account_id}"
USERS = {}

# COMMAND ----------

def GetWorkspaceId():
    return spark.conf.get("spark.databricks.clusterUsageTags.clusterOwnerOrgId")

# COMMAND ----------

def OAuthGetBearerToken(asHeader=False):
    tenant_id = dbutils.secrets.get(scope = SECRET_SCOPE, key = "daf-tenant-id")
    app_id = dbutils.secrets.get(scope = SECRET_SCOPE, key = "daf-serviceprincipal-app-id")
    secret = dbutils.secrets.get(scope = SECRET_SCOPE, key = "daf-serviceprincipal-app-secret")
    headers = {
        "content-type" : "application/x-www-form-urlencoded"
    }
    url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    data = {
        "grant_type" : "client_credentials"
        ,"client_secret" : secret
        ,"client_id" : app_id
        ,"scope" : "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default"
    }

    response = requests.post(url, data, headers)
    return response.json()["access_token"] if not(asHeader) else { 'Authorization' : f'Bearer ' + response.json()["access_token"] }

# COMMAND ----------

def ListAccountGroups():
    headers = OAuthGetBearerToken(True)
    url = f"{ACCOUNT_API}/scim/v2/Groups"
    response = requests.get(url, headers=headers)
    return response.json()

# COMMAND ----------

def GetAccountGroupByName(groupName):
    headers = OAuthGetBearerToken(True)
    url = f"{ACCOUNT_API}/scim/v2/Groups?filter=displayName eq {groupName}"
    response = requests.get(url, headers=headers)
    return response.json()

# COMMAND ----------

def CreateAccountGroup(name):
    headers = OAuthGetBearerToken(True)
    url = f"{ACCOUNT_API}/scim/v2/Groups"
    response = requests.post(url, json={ "displayName": name }, headers=headers)
    return response.json()

# COMMAND ----------

def ListServicePrincipals():
    headers = OAuthGetBearerToken(True)
    url = f"{ACCOUNT_API}/scim/v2/ServicePrincipals"
    response = requests.get(url, headers=headers)
    return response.json()

# COMMAND ----------

def ListAccountUsers():
    headers = OAuthGetBearerToken(True)
    url = f"{ACCOUNT_API}/scim/v2/Users"
    response = requests.get(url, headers=headers)
    return response.json()

# COMMAND ----------

def AddAccountUser(id):
    headers = OAuthGetBearerToken(True)
    url = f"{ACCOUNT_API}/scim/v2/Users"
    response = requests.post(url, json={ "displayName": id, "userName" : id }, headers=headers)
    return response.json()

# COMMAND ----------

def GetAccountUsersCached():
    global USERS
    if len(USERS) == 0:
        for i in ListAccountUsers()["Resources"]:
            USERS[i["userName"]] = i["id"]
    return USERS

# COMMAND ----------

def CreateOrUpdatePermissionAssignment(workspace_id, principal_id):
    headers = OAuthGetBearerToken(True)
    url = f"{ACCOUNT_API}/workspaces/{workspace_id}/permissionassignments/principals/{principal_id}"
    
    response = requests.put(url, json={ "permissions": [ "USER" ] }, headers=headers)
    return response.json()

# COMMAND ----------

def UpdateAccountGroupMembers(groupName, members):
    USERS = GetAccountUsersCached()
    group_id = GetAccountGroupByName(groupName)["Resources"][0]["id"]
    headers = OAuthGetBearerToken(True)
    url = f"{ACCOUNT_API}/scim/v2/Groups/{group_id}"
    body = { "Operations" : [
                { "op": "add", "value": { "members": [] } }
            ]}
    for m in members:
        u = USERS.get(f"{m}@sydneywater.com.au")
        
        if u is None:
            print(f"{m} not found!") 
            AddAccountUser(f"{m}@sydneywater.com.au")
        body["Operations"][0]["value"]["members"].append({ "value" : u })

    response = requests.patch(url, json=body, headers=headers)
    return response.json()

# COMMAND ----------

def ListWorkspacePermissions(workspace_id):
    url = f"{ACCOUNT_API}/workspaces/{workspace_id}/permissionassignments"
    headers = OAuthGetBearerToken(True)
    response = requests.get(url, headers=headers)
    return response.json()
