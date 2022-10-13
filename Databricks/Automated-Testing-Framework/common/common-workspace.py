# Databricks notebook source
import requests
import json
from ast import literal_eval
from pyspark.sql.functions import *

# COMMAND ----------

INSTANCE_NAME = "https://australiaeast.azuredatabricks.net"
SECRET_SCOPE = "ADS"
DATABRICKS_PAT_SECRET_NAME = "databricks-token"

# COMMAND ----------

def CurrentNotebookPath():
    return "/".join(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().split("/")[:-1])

# COMMAND ----------

def GetAuthenticationHeader():
    pat = dbutils.secrets.get(scope = SECRET_SCOPE, key = DATABRICKS_PAT_SECRET_NAME)
    headers = {
        'Authorization': f'Bearer {pat}',
    }
    return headers

# COMMAND ----------

def JasonToDataFrame(jsonInput):
    jsonData = json.dumps(jsonInput)
    jsonDataList = []
    jsonDataList.append(jsonData)
    jsonRDD = sc.parallelize(jsonDataList)
    return spark.read.json(jsonRDD)

# COMMAND ----------

def ListClusters():
    headers = GetAuthenticationHeader()
    url = f'{INSTANCE_NAME}/api/2.0/clusters/list'
    response = requests.get(url, headers=headers)
    jsonResponse = response.json()
    return JasonToDataFrame(jsonResponse)

# COMMAND ----------

def ListWorkspaces(path="/"):
    headers = GetAuthenticationHeader()
    url = f'{INSTANCE_NAME}/api/2.0/workspace/list'
    data_path = '{{"path": "{0}"}}'.format(path)
    response = requests.get(url, headers=headers, data=data_path)
    jsonResponse = response.json()
    return JasonToDataFrame(jsonResponse)

# COMMAND ----------

def GetNotebookName(path):
    list = path.split("/")
    count=len(list)
    return list[count-1]
