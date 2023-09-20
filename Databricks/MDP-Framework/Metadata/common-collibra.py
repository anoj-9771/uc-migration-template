# Databricks notebook source
import requests
import json

# COMMAND ----------

SECRET_SCOPE = "ADS"
domain = dbutils.secrets.get(scope = SECRET_SCOPE, key = "daf-collibra-api-domain")
user = dbutils.secrets.get(scope = SECRET_SCOPE, key = "daf-collibra-api-user")
secret = dbutils.secrets.get(scope = SECRET_SCOPE, key = "daf-collibra-api-secret")
DESCRIPTION_TYPE_ID = "00000000-0000-0000-0000-000000003114"

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
    schema = fqnTableName.split(".")[0]
    tableName = fqnTableName.split(".")[1]
    return GetAssetByName(tableName, GetDomainByName(schema)["results"][0]["id"])
#print(GetTableFqn("raw.hydra_tsystemarea"))
#print(GetTableFqn("raw.access_z309_tdebit"))

# COMMAND ----------

def GetTableAssetId(tableFqn):
    return GetTableFqn(tableFqn)["results"][0]["id"]

# COMMAND ----------

def GetTableAttributes(tableFqn):
    assetId = GetTableAssetId(tableFqn)
    url = f"https://{domain}/rest/2.0/attributes?assetId={assetId}"
    response = requests.get(url, auth=(f"{user}", f"{secret}"))
    jsonResponse = response.json()
    return jsonResponse
#print(GetTableAttributes("raw.hydra_tsystemarea"))

# COMMAND ----------

 def GetTableDescriptionAttribute(tableFqn):
    assetId = GetTableAssetId(tableFqn)
    url = f"https://{domain}/rest/2.0/attributes?offset=0&limit=0&countLimit=-1&typeIds={DESCRIPTION_TYPE_ID}&assetId={assetId}"
    response = requests.get(url, auth=(f"{user}", f"{secret}"))
    jsonResponse = response.json()
    return jsonResponse
#print(GetTableDescriptionAttribute("raw.hydra_tsystemarea"))

# COMMAND ----------

def SetTableDescriptionAttribute(tableFqn, description):
    assetId = GetTableAssetId(tableFqn)
    url = f"https://{domain}/rest/2.0/attributes"
    response = requests.post(url, json={ "assetId": assetId, "typeId" : DESCRIPTION_TYPE_ID, "value" : description }, auth=(f"{user}", f"{secret}"))
    return response.json()
#print(SetTableDescriptionAttribute("raw.hydra_tsystemarea", "kh-test-2"))

# COMMAND ----------

def UpdateTableDescriptionAttribute(tableFqn, description):
    assetId = GetTableDescriptionAttribute(tableFqn)["results"][0]["id"]
    url = f"https://{domain}/rest/2.0/attributes/{assetId}"
    response = requests.patch(url, json={ "value" : description }, auth=(f"{user}", f"{secret}"))
    return response.json()
#print(UpdateTableDescriptionAttribute("raw.hydra_tsystemarea", "kh-update-4"))

# COMMAND ----------

def SetOrUpdateTableDescriptionAttribute(tableFqn, description):
    attributes = GetTableDescriptionAttribute(tableFqn)
    SetTableDescriptionAttribute(tableFqn, description) if attributes["total"] == 0 else UpdateTableDescriptionAttribute(tableFqn, description)
#SetOrUpdateTableDescriptionAttribute("raw.access_z309_tdebit", "kh-update-111")

# COMMAND ----------

 def GetTableDescriptionAttributeByAssetId(assetId):
    url = f"https://{domain}/rest/2.0/attributes?offset=0&limit=0&countLimit=-1&typeIds={DESCRIPTION_TYPE_ID}&assetId={assetId}"
    response = requests.get(url, auth=(f"{user}", f"{secret}"))
    jsonResponse = response.json()
    return jsonResponse

# COMMAND ----------

def GetAssetType(assetTypeId):
    url = f"https://{domain}/rest/2.0/assetTypes/{assetTypeId}"
    response = requests.get(url, auth=(f"{user}", f"{secret}"))
    jsonResponse = response.json()
    return jsonResponse

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
