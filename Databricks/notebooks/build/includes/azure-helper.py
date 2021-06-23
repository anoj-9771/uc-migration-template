# Databricks notebook source
# MAGIC %run ./global-variables-python

# COMMAND ----------

def AzHelperGetAzureAccessToken():
  
  import adal
  import time
  import requests 
  
  CLIENT_ID = dbutils.secrets.get(scope = ADS_KV_ACCOUNT_SCOPE, key = "appjbdsid")
  CLIENT_KEY = dbutils.secrets.get(scope = ADS_KV_ACCOUNT_SCOPE, key = "appjbdssecret")
  DIRECTORY_ID = dbutils.secrets.get(scope = ADS_KV_ACCOUNT_SCOPE, key = "directoryid")

  authentication_endpoint = 'https://login.microsoftonline.com/'
  resource  = 'https://management.core.windows.net/'

  # get an Azure access token using the adal library
  context = adal.AuthenticationContext(authentication_endpoint + DIRECTORY_ID)
  token_response = context.acquire_token_with_client_credentials(resource, CLIENT_ID, CLIENT_KEY)

  #print(token_response)
  access_token = token_response.get('accessToken')
  return access_token

# COMMAND ----------

def AzHelperGetSynapseAPIEndPoint(action, DB_SERVER, DATABASE_NAME):
    SUBSCRIPTION_ID = dbutils.secrets.get(scope = ADS_KV_ACCOUNT_SCOPE, key = "subscriptionid")
    
    API_ENDPOINT = "https://management.azure.com/"
    API_ENDPOINT = API_ENDPOINT + "subscriptions/" + SUBSCRIPTION_ID + "/"
    API_ENDPOINT = API_ENDPOINT + "resourceGroups/" + ADS_RESOURCE_GROUP + "/"
    API_ENDPOINT = API_ENDPOINT + "providers/Microsoft.Sql/servers/" + DB_SERVER + "/"
    API_ENDPOINT = API_ENDPOINT + "databases/" + DATABASE_NAME 
    
    if action =="pause":
      API_ENDPOINT = API_ENDPOINT + "/pause"
    elif action == "resume":
      API_ENDPOINT = API_ENDPOINT + "/resume"
      
    API_ENDPOINT = API_ENDPOINT + "?api-version=2017-10-01-preview"
      
    return API_ENDPOINT
  

# COMMAND ----------

def AzHelperResumeSynapse(DB_SERVER, DATABASE_NAME):
  
  import adal
  import time
  import requests 
  
  access_token = AzHelperGetAzureAccessToken()
  api_endpoint = AzHelperGetSynapseAPIEndPoint("resume", DB_SERVER, DATABASE_NAME)
  
  status = ""
  headers = {"Authorization": 'Bearer ' + access_token}
  r = requests.post(url = api_endpoint, headers = headers)
  print (r.text)
  
  while status != "Online":
    api_endpoint = GetSynapseAPIEndPoint("")
    r = requests.get(url = api_endpoint, headers = headers)
    status = r.json()["properties"]["status"]
    print ("Current status is " + status)
    if status != "Online":
      print("Waiting for 15 seconds before checking the status again.")
      time.sleep(15)
  
  
  

# COMMAND ----------

def AzHelperPauseSynapse(DB_SERVER, DATABASE_NAME):
  
  import adal
  import time
  import requests 
  
  access_token = AzHelperGetAzureAccessToken()
  api_endpoint = AzHelperGetSynapseAPIEndPoint("pause", DB_SERVER, DATABASE_NAME)
  
  headers = {"Authorization": 'Bearer ' + access_token}
  r = requests.post(url = api_endpoint, headers = headers)
  print (r.text)
  


# COMMAND ----------

