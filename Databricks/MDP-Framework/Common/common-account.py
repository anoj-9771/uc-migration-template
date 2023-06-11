# Databricks notebook source
import requests
import json

# COMMAND ----------

SECRET_SCOPE = "ADS"
DATABRICKS_PAT_SECRET_NAME = "databricks-scim-token"
account_id = dbutils.secrets.get(scope = SECRET_SCOPE, key = "databricks-account-id")
ACCOUNT_API = f"https://accounts.azuredatabricks.net/api/2.0/accounts/{account_id}/scim/v2"
USERS = {}

# COMMAND ----------

def GetAuthenticationHeader(key=DATABRICKS_PAT_SECRET_NAME):
    pat = dbutils.secrets.get(scope = SECRET_SCOPE, key = key)
    headers = { "Authorization" : f"Bearer {pat}" }
    return headers

# COMMAND ----------

def ListAccountGroups():
    headers = GetAuthenticationHeader()
    url = f"{ACCOUNT_API}/Groups"
    response = requests.get(url, headers=headers)
    return response.json()

# COMMAND ----------

def GetAccountGroupByName(groupName):
    headers = GetAuthenticationHeader()
    url = f"{ACCOUNT_API}/Groups?filter=displayName eq {groupName}"
    response = requests.get(url, headers=headers)
    return response.json()

# COMMAND ----------

def ListAccountUsers():
    headers = GetAuthenticationHeader()
    url = f"{ACCOUNT_API}/Users"
    response = requests.get(url, headers=headers)
    return response.json()

# COMMAND ----------

def AddAccountUser(id):
    headers = GetAuthenticationHeader()
    url = f"{ACCOUNT_API}/Users"
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

def UpdateAccountGroupMembers(groupName, members):
    USERS = GetAccountUsersCached()
    group_id = GetAccountGroupByName(groupName)["Resources"][0]["id"]
    headers = GetAuthenticationHeader()
    url = f"{ACCOUNT_API}/Groups/{group_id}"
    body = { "Operations" : [
                { "op": "add", "value": { "members": [] } }
            ]}
    for m in members:
        u = USERS.get(f"{m}@sydneywater.com.au")
        print(f"{m} not found!") if u is None else ()
        body["Operations"][0]["value"]["members"].append({ "value" : u })

    response = requests.patch(url, json=body, headers=headers)
    return response.json()

# COMMAND ----------

#UpdateAccountGroupMembers("ppd-UAT-L2", [ "s0y","yi1","vnw","ngz","jde","hpr","f1d","0pk","4tm","f94","vds","0ke","hjv","n8m","ulr","w0a","q1g","wpv","lnt","ito","lpv","i2k","nnn","xv5","wiu","tgm","cdv","mqv","q0n","rka","lkx","95g","an9","4ed","cqs","72j","boq","iyl","als","x2r","i7w","zg0","p7c","sqn","kfn","mew","rw4","x2r","2to","1tk","x2r","mej","03c","81f","f1d","vmo","dbp","sqn","nnr","hdm","p7c","ooo","1gs","8gj","16o","04e","6n6","cml","d8z","p24","qb6","jsj","int","g8o","bxi","23i","lxc","2eb","iep","shq","yes","bui","cbl","o5x","nq5","ho3","xxz","c2n","s9z","cyx","dc7","yqe","s8l","3gy","uv6","uni","ksd","pc1","lqa","8w6","enl","1wr","lf8","qpa","prj","t1w","2zu","ajp","lpr","ub0","pxz","rn7","fws","i3s","yce","enl","4xd","2bv","ao8","dgf","nnn","lf8","qil","45c","mew","eq2","kfn","03c","0un","6nd","sql","ugq"])
#UpdateAccountGroupMembers("UAT-L2", [ "s0y","yi1","vnw","ngz","jde","hpr","f1d","0pk","4tm","f94","vds","0ke","hjv","w0a","q1g","lnt","lkx","95g","an9","4ed","cqs","72j","boq","iyl","als","x2r","i7w","zg0","p7c","sqn","rw4","x2r","2to"])
#UpdateAccountGroupMembers("ppd-UAT-L3", [ "n8m","ulr","enl","1wr","lf8" ])
#UpdateAccountGroupMembers("UAT-L3", [ "n8m","ulr" ])
