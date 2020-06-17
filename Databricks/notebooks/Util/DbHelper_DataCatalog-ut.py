# Databricks notebook source
# DBTITLE 1,Import Libraries
import io
import uuid
import json
import pandas as pd
import requests
from io import StringIO
import os
from requests.auth import HTTPBasicAuth
from pyspark.sql import functions as F
from pyspark.sql.functions import explode, explode_outer, array, col, first, monotonically_increasing_id, isnan, when, count

# COMMAND ----------

# DBTITLE 1,Data Catalog Data Profile Function
def dataCatalogProfile(data_all_df,data_cols, table_name, dataset_id):
    data_df = data_all_df.select(data_cols)
    columns2Bprofiled = data_df.columns
    batchId = parameters["batchId"]
    taskId = parameters["taskId"]
    datasetId = dataset_id
    schemaName = table_name.split('.')[0]
    tableName = table_name.split('.')[1]
    size = source_size
    rowCount = data_df.count()
    dprof_df = pd.DataFrame({'batchId':[batchId] * len(data_df.columns),\
                             'taskId':[taskId] * len(data_df.columns),\
                             'datasetId':[datasetId] * len(data_df.columns),\
                             'schemaName':[schemaName] * len(data_df.columns),\
                             'tableName':[tableName] * len(data_df.columns),\
                             'columnName':data_df.columns,\
                             'rowCount':[rowCount] * len(data_df.columns),\
                             'size':[size] * len(data_df.columns),\
                             'type':[x[1] for x in data_df.dtypes]}) 
    dprof_df = dprof_df[['batchId', 'taskId', 'datasetId', 'schemaName', 'tableName','columnName', 'rowCount', 'size', 'type']]
    dprof_df.set_index('columnName', inplace=False, drop=False)
    # ======================    
    # number of rows with nulls and nans   
    try:
      df_nacounts = data_df.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in data_df.columns \
                                    if data_df.select(c).dtypes[0][1]!='timestamp']).toPandas().transpose()
    except:
      df_nacounts = data_df.select([count(when(col(c).isNull(), c)).alias(c) for c in data_df.columns \
                                    if data_df.select(c).dtypes[0][1]!='timestamp']).toPandas().transpose()
    df_nacounts = df_nacounts.reset_index()  
    df_nacounts.columns = ['columnName','nullCount']
    dprof_df = pd.merge(dprof_df, df_nacounts, on = ['columnName'], how = 'left')
    # =========================
    # using the in built describe() function 
    desc_df = data_df.describe().toPandas().transpose()
    desc_df.columns = ['distinctCount', 'avg', 'stdev', 'min', 'max']
    desc_df = desc_df.iloc[1:,:]  
    desc_df = desc_df.reset_index()  
    desc_df.columns.values[0] = 'columnName'  
    desc_df = desc_df[['columnName','distinctCount', 'avg', 'stdev']] 
    dprof_df = pd.merge(dprof_df, desc_df , on = ['columnName'], how = 'left')
    # ===========================================
    allminvalues = [data_df.select(F.min(x)).limit(1).toPandas().iloc[0][0] for x in columns2Bprofiled]
    allmaxvalues = [data_df.select(F.max(x)).limit(1).toPandas().iloc[0][0] for x in columns2Bprofiled]
    df_counts = dprof_df[['columnName']]
    df_counts.insert(loc=0, column='min', value=allminvalues)
    df_counts.insert(loc=0, column='max', value=allmaxvalues)
    df_counts = df_counts[['columnName','min','max']]
    dprof_df = pd.merge(dprof_df, df_counts , on = ['columnName'], how = 'left')
    # ==========================================
    # number of distinct values in each column
    dprof_df['distinctCount'] = [data_df.select(x).distinct().count() for x in columns2Bprofiled]
    #Recast for data catalog column profile
    dprof_df = dprof_df.astype({'batchId':'int64'})
    dprof_df = dprof_df.astype({'taskId':'int64'})
    dprof_df = dprof_df.astype({'datasetId':'int64'})
    dprof_df = dprof_df.astype({'columnName':'str'})
    dprof_df = dprof_df.astype({'rowCount':'int64'})
    dprof_df = dprof_df.astype({'size':'float64'})
    dprof_df = dprof_df.astype({'type':'str'})
    dprof_df = dprof_df.astype({'min':'str'})
    dprof_df = dprof_df.astype({'max':'str'})
    dprof_df = dprof_df.fillna('0').astype({'stdev':'float64'}).astype({'stdev':'int64'})
    dprof_df = dprof_df.astype({'avg':'float64'}).astype({'avg':'int64'})
    dprof_df = dprof_df.fillna(0).astype({'nullCount':'int64'})
    dprof_df = dprof_df.astype({'distinctCount':'int64'})
    return dprof_df

# COMMAND ----------

# DBTITLE 1,Build Data Source
def buildProperties(source_type, source_name, upn, fromSourceSystem, **kwargs):
    if source_type.lower() in 'sql server':
        return """
            "fromSourceSystem" : {2},
            "name": "{0}",
            "dataSource": {{
                "sourceType": "SQL Server",
                "objectType": "Table"
            }},
            "dsl": {{
              "protocol": "tds",
              "authentication": "protocol",
              "address": {{ 
                  "server": " ",
                  "database": " ",
                  "schema": " ",
                  "object": "{0}"
              }}
            }},
            "lastRegisteredBy": {{
                "upn": "{1}"
            }}""".format(source_name, upn, json.dumps(fromSourceSystem))
    if source_type.lower() in 'oracle':
        return """
            "fromSourceSystem" : {2},
            "name": "{0}",
            "dataSource": {{
                "sourceType": "Oracle Database",
                "objectType": "Table"
            }},
            "dsl": {{
                "protocol": "oracle",
                "authentication": "protocol",
                "address": {{
                    "server": " ",
                    "database": " ",
                    "schema": " ",
                    "object": "{0}"
                }}
            }},
            "lastRegisteredBy": {{
                "upn": "{1}"
            }}""".format(source_name, upn, json.dumps(fromSourceSystem))
    elif source_type.lower() == 'azure storage':
       return """
            "fromSourceSystem" : {3},
            "name": "{1}",
            "dataSource": {{
                "sourceType": "{0}",
                "objectType": "Blob"
            }},
            "dsl": {{
                "protocol": "azure-blobs",
                "authentication": "azure-access-key",
                "address": {{
                    "domain": "blob.core.windows.net/{6}/{7}",
                    "account": "{4}",
                    "container": "{5}"
                }}
            }},
            "lastRegisteredBy": {{
                "upn": "{2}"
            }}""".format(source_type, source_name, upn, json.dumps(fromSourceSystem), kwargs["account"], kwargs["container"], parameters["srcDirectoryName"], parameters["srcBlobName"].split('/')[0] + '/./' + parameters["srcBlobName"].split('/')[0] + parameters["srcFormat"])
    elif source_type.lower() == 'azure data lake store':
       return """
            "fromSourceSystem" : {3},
            "name": "{1}",
            "dataSource": {{
                "sourceType": "{0}",
                "objectType": "Container"
            }},
            "dsl": {{
               "protocol": "webhdfs",
               "authentication": "basic",
               "address": {{
                   "url": "{4}"
               }}
           }},
            "lastRegisteredBy": {{
                "upn": "{2}"
            }}""".format(source_type, source_name, upn, json.dumps(fromSourceSystem), kwargs["url"])


# COMMAND ----------

# DBTITLE 1,Build Table Profile
def buildTableProfile(key, timestamp, fromSourceSystem, **kwargs):
        return """
            "tableDataProfiles": [{{
                  "properties": {{
                    "dataModifiedTime": "{0}",
                    "schemaModifiedTime": "{0}",
                    "size": {1},
                    "numberOfRows": {2},
                    "key": "{3}",
                    "fromSourceSystem": {4}
                  }}
          }}]""".format(timestamp, kwargs["size"], kwargs["numberOfRows"], key, json.dumps(fromSourceSystem))

# COMMAND ----------

# DBTITLE 1,Build Column Profile
def buildColumnProfile(columns, key, fromSourceSystem):
  return"""
  "columnsDataProfiles":[ 
      {{
        "properties": {{
          "columns": {0},
          "key": "{1}",
          "fromSourceSystem": {2}
        }}
      }}]""".format(columns, key, json.dumps(fromSourceSystem))

# COMMAND ----------

# DBTITLE 1,Build Table Preview
def buildTablePreview(preview, key, fromSourceSystem):
  return"""
      "previews": [{{
              "properties": {{
                  "preview": {0},
                  "key": "Test",
                  "fromSourceSystem": {2}
              }}
          }}]""".format(preview, key, json.dumps(fromSourceSystem))

# COMMAND ----------

def buildSchema(columns, fromSourceSystem):
  return"""
      "schema": {{
            "properties" : {{
                "fromSourceSystem" : {1},
                "columns": [{0}]
            }}
      }}""".format(columns, json.dumps(fromSourceSystem))

# COMMAND ----------

def buildExperts(expert_rights, upn, fromSourceSystem, key, timestamp):
  effectiveRights = []
  if 'r' in expert_rights:
      effectiveRights.append("Read")
  if 'd' in expert_rights:
      effectiveRights.append("Delete")
  if 'v' in expert_rights:
      effectiveRights.append("ViewRoles")
  return"""
      "experts":[
          {{
                "effectiveRights": {0},
                "properties": {{
                    "expert": {{
                        "upn": "{1}"
                    }},
                    "fromSourceSystem": {2},
                    "key": "{3}"
                }},
                "timestamp": "{4}"
            }}
        ]""".format(json.dumps(effectiveRights), upn, json.dumps(fromSourceSystem), key, timestamp)

# COMMAND ----------

def buildAnnotation(*annotations):
    output = StringIO()
    print(*annotations, sep=", ", end=" ", file=output)
    return """{{{0}}}""".format(output.getvalue())

# COMMAND ----------

# def buildExperts():
#   return"""
#   {{
#                 "effectiveRights": ["Read", "Delete", "ViewRoles"],
#                 "properties": {{
#                     "expert": {{
#                         "objectId": "874fcdd8-ca17-4047-8448-32d4f3eb5da5",
#                         "upn": "stephen.lundallxx@lundalls.onmicrosoft.com"
#                     }},
#                     "fromSourceSystem": true,
#                     "key": "{4}"
#                 }},
#                 "timestamp": "2020-03-20T15:18:59.8402664Z"
#             }}""".format(effectiveRights,upn, fromSourceSystem)

# COMMAND ----------

# DBTITLE 1,Data Catalog Function Set
global creds
creds = {}

# Convert json response into dataframe
def jsonToDataFrame(json, schema=None):
  reader = spark.read
  if schema:
    reader.schema(schema)
  return reader.json(sc.parallelize([json])) 


def getCreds(kvSecret):
  try:
    connection = dbutils.secrets.get(scope='azr-dp-keyvault',key=kvSecret)
  except:
    connection = kvSecret
  cons = connection.split(';')
  cons = cons
  for con in cons:
      value = con.split('=')
      try:
        creds[value[0]] = value[1]
      except : False
  return creds

def getUPN(catalog_secret):
  creds = getCreds(catalog_secret)
  upn = creds["client_id"] + "@" + creds["tenant_id"]
  return upn

# Get Access Token
def getAccessToken(kvSecret):
  creds = getCreds(kvSecret)
  url_auth = "https://login.microsoftonline.com/%s/oauth2/v2.0/token" % creds["tenant_id"]
  headers = {'Content-Type': "application/x-www-form-urlencoded"}
  payload = {'grant_type' : 'client_credentials', 'scope':'https://datacatalog.azure.com/.default'}
  res = requests.get(url_auth,headers=headers,data=payload,auth=HTTPBasicAuth(creds["client_id"], creds["secret"]))
  accessToken = res.json()['access_token']
  return accessToken

# Register Data Asset
def registerDataAsset(jsonasset,catalog_secret):
    registerUri = "https://api.azuredatacatalog.com/catalogs/{0}/views/tables?api-version=2016-03-30".format(getCreds(catalog_secret)["catalog_name"])
    resp = setRequestAndGetResponse(registerUri, jsonasset)
    dataAssetHeader = resp.headers["Location"]
    return dataAssetHeader

# Dynamic Body format  
def bodyJson(data_asset, catalog_secret, key, timestamp, fromSourceSystem, **kwargs):
    upn = getUPN(catalog_secret)
    annotations = []
    if 'table_preview' in kwargs and kwargs["table_preview"] == True:
        dsamp = df.select("*").limit(10).toPandas()
        preview = dsamp.to_json(orient='records')
        table_preview = buildTablePreview(preview, 'table_preview_' + key, fromSourceSystem) #preview, key, fromSourceSystem
        annotations.append(table_preview)
    if 'table_profile' in kwargs and kwargs["table_profile"] == True:
        row_count = pdDf_prof["rowCount"][0]
        table_size = int(pdDf_prof["size"][0]/ 0.000001)
        table_profile = buildTableProfile('table_profile_' + key, timestamp, fromSourceSystem, size = table_size, numberOfRows = row_count)
        annotations.append(table_profile)
    if data_asset == 'source_system':
        dschm = schm.select(col("Column_Name").alias("name"),col("Column_DataType").alias("type"), F.when(schm.Column_Nullable == "",False).otherwise(True).alias("isNullable")).toPandas()
        build_properties = buildProperties(parameters["prcType"],parameters["dstTableName"], upn, fromSourceSystem, server = parameters["srcAccName"], database = parameters["srcDirectoryName"], schema = parameters["dstTableName"].split('.')[0], table = parameters["dstTableName"].split('.')[1])
    if data_asset == 'source':
        dschm = pd.DataFrame(df.dtypes)
        dschm.columns = ['name','type']
        dschm['isNullable'] = True
        build_properties = buildProperties('azure storage',parameters["dstTableName"].replace('.','_'), upn, fromSourceSystem, account = parameters["srcAccName"], container = parameters["srcContainerName"])
    
    columns = dschm.to_json(orient='records').replace('[','').replace(']','')
    
    if 'column_profile' in kwargs and kwargs["column_profile"] == True:
        dprof =  pdDf_prof[['columnName', 'type', 'min', 'max', 'stdev', 'avg', 'nullCount','distinctCount']]
        profile = dprof.to_json(orient='records')
        column_profile = buildColumnProfile(profile, 'column_profile_' + key, fromSourceSystem)
        annotations.append(column_profile)
    if 'table_expert' in kwargs and kwargs["table_expert"] == True:
        table_expert = buildExperts(expert_rights,upn, timestamp, 'table_expert_' + key, fromSourceSystem)
        annotations.append(table_expert)
    if 'table_schema' in kwargs and kwargs["table_schema"] == True:
        table_schema = buildSchema(columns, fromSourceSystem)
        annotations.append(table_schema)
    return """{{
        "properties" :{{
            {0}
        }},
        "annotations" : {1}        
    }}""".format(build_properties, buildAnnotation(*annotations))
  
# Make request and return response
def setRequestAndGetResponse(url, payload):
    while True:
        http_headers = getHeadersAuth()
        if(payload is None):
            r = requests.get(url, headers=http_headers, allow_redirects=False) 
        else:
            r = requests.post(url, headers=http_headers, data=payload, allow_redirects=False) 
        
        if(r.status_code >= 301 and r.status_code <= 399):
            redirectedUrl = r.headers["Location"]
            url = redirectedUrl
            r = None
        else:
            return r
    print(r)
# Create Header for request          
def getHeadersAuth():
    http_headers = {'Authorization': 'Bearer ' + accessToken,
                    'Content-Type': 'application/json; charset=utf-8'
                    }
    return http_headers

# Search DC for Data Asset
def searchDataAsset(searchTerm, catalog_secret):
    fullUri = "https://api.azuredatacatalog.com/catalogs/{0}/search/search?searchTerms={1}&count=10&api-version=2016-03-30".format(getCreds(catalog_secret)["catalog_name"], searchTerm)
    resp = setRequestAndGetResponse(fullUri, None)
    return resp.text

# Delete Data Asses by Id
def deleteAsset(dataAssetUrl):
    http_headers = getHeadersAuth()
    fullUri = "{0}?api-version=2016-03-30".format(dataAssetUrl)
    r = requests.delete(fullUri, headers=http_headers)
    return str(r.status_code)

