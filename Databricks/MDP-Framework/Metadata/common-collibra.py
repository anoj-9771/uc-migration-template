# Databricks notebook source
import requests
import json

# COMMAND ----------

SECRET_SCOPE = "ADS"
domain = dbutils.secrets.get(scope = SECRET_SCOPE, key = "daf-collibra-api-domain")
user = dbutils.secrets.get(scope = SECRET_SCOPE, key = "daf-collibra-api-user")
secret = dbutils.secrets.get(scope = SECRET_SCOPE, key = "daf-collibra-api-secret")

# COMMAND ----------

def GetDomainByName(name):
    url = f"https://{domain}/rest/2.0/domains?offset=0&limit=0&countLimit=-1&name={name}&nameMatchMode=ANYWHERE&excludeMeta=true&includeSubCommunities=false"
    response = requests.get(url, auth=(f"{user}", f"{secret}"))
    jsonResponse = response.json()
    return jsonResponse

# COMMAND ----------

def GetAssetByName(name, domainId):
    url = f"https://{domain}/rest/2.0/assets?offset=0&limit=0&countLimit=-1&name={name}&nameMatchMode=EXACT&domainId={domainId}&typeInheritance=true&excludeMeta=true&sortField=NAME&sortOrder=ASC"
    response = requests.get(url, auth=(f"{user}", f"{secret}"))
    jsonResponse = response.json()
    return jsonResponse

# COMMAND ----------

def GetAsset(assetId):
    url = f"https://{domain}/rest/2.0/assets/{assetId}"
    response = requests.get(url, auth=(f"{user}", f"{secret}"))
    jsonResponse = response.json()
    return jsonResponse
#print(GetAsset("b7f3a7ad-1cc5-46e4-8a6c-896b51616b0a"))

# COMMAND ----------

def GetTableFqn(fqnTableName):
    if "." not in fqnTableName:
        raise Exception("Should be in . (dot) notation!") 
        return
    schema = fqnTableName.split(".")[0]    
    tableName = fqnTableName.split(".")[0]
    return GetAssetByName(tableName, GetDomainByName(schema)["results"][0]["id"])
#print(GetTableFqn("raw.hydra_tsystemarea"))

# COMMAND ----------

def GetTableAttributes(tableFqn):
    assetId = GetTableFqn(tableFqn)["results"][0]["id"]
    url = f"https://{domain}/rest/2.0/attributes?assetId={assetId}"
    response = requests.get(url, auth=(f"{user}", f"{secret}"))
    jsonResponse = response.json()
    return jsonResponse
#print(GetTableAttributes("raw.hydra_tsystemarea"))

# COMMAND ----------

def SetTableAttributes(tableFqn, description):
    assetId = "84642f6f-9e8a-4ff5-b9fe-36fa35158fb7"
    url = f"https://{domain}/rest/2.0/attributes"
    response = requests.post(url, json={ "assetId": assetId, "typeId" : "00000000-0000-0000-0000-000000003114", "value" : description }, auth=(f"{user}", f"{secret}"))
    return response.json()
#print(SetTableAttributes("raw.hydra_tsystemarea", "kh-test"))

# COMMAND ----------

def SetTableAttributes(tableFqn, description):
    #'POST' \
    #'https://sydneywater-dev.collibra.com/rest/2.0/attributes' \
    #-H 'accept: application/json' \
    #-H 'Content-Type: application/json' \
    #-d '{
    #"assetId": "84642f6f-9e8a-4ff5-b9fe-36fa35158fb7",
    #"typeId": "00000000-0000-0000-0000-000000003114",
    #"value": "test dsc"
    #}'
    return

# COMMAND ----------

def GetAssetType(assetTypeId):
    url = f"https://{domain}/rest/2.0/assetTypes/{assetTypeId}"
    response = requests.get(url, auth=(f"{user}", f"{secret}"))
    jsonResponse = response.json()
    return jsonResponse
#print(GetAssetType("00000000-0000-0000-0001-000500000018"))


# COMMAND ----------

def SetTableMetadata(tableFqn, key, value):
    spark.sql(f"ALTER TABLE {tableFqn} SET TBLPROPERTIES( {key}=\"{value}\" )")
#SetTableMetadata("cleansed.access_facilitytimeslice", "meta", "")

# COMMAND ----------

def GetTableMetadata(tableFqn, key):
    df = spark.sql(f"SHOW TBLPROPERTIES {tableFqn}").where(f"key = '{key}'").select("value")
    if df.count() == 0:
        return None
    return df.collect()[0][0]
#print(GetTableMetadata("cleansed.access_facilitytimeslice", "meta"))

# COMMAND ----------


