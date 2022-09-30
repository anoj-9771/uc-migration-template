# Databricks notebook source
import requests
import json
from ast import literal_eval
from pyspark.sql.functions import *

INSTANCE_NAME = "https://australiaeast.azuredatabricks.net"
SECRET_SCOPE = "ADS"
DATABRICKS_PAT_SECRET_NAME = "databricks-token"

def CurrentNotebookPath():
    return "/".join(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().split("/")[:-1])

def GetAuthenticationHeader():
    pat = dbutils.secrets.get(scope = SECRET_SCOPE, key = DATABRICKS_PAT_SECRET_NAME)
    headers = {
        'Authorization': f'Bearer {pat}',
    }
    return headers
def JasonToDataFrame(jsonInput):
    jsonData = json.dumps(jsonInput)
    jsonDataList = []
    jsonDataList.append(jsonData)
    jsonRDD = sc.parallelize(jsonDataList)
    return spark.read.json(jsonRDD)
def ListClusters():
    headers = GetAuthenticationHeader()
    url = f'{INSTANCE_NAME}/api/2.0/clusters/list'
    response = requests.get(url, headers=headers)
    jsonResponse = response.json()
    return JasonToDataFrame(jsonResponse)
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

# COMMAND ----------

firstRow = spark.sql("SELECT DATE_FORMAT(CURRENT_TIMESTAMP(), 'yyyy/MM/dd') Path, DATE_FORMAT(CURRENT_TIMESTAMP(), 'yyMMddHHmm') BatchId").rdd.collect()[0]
rawPath = firstRow.Path
batchId = firstRow.BatchId

for p in ["/tests", "/tests/Dimension", "/tests/Fact"]:
    df = ListWorkspaces(CurrentNotebookPath() + p)
    df = df.selectExpr(f"explode({df.columns[0]}) o").where("o.object_type != 'DIRECTORY'")
    
    for i in df.rdd.collect():
        path = i.o.path
        r = dbutils.notebook.run(path, 0, { "DEFAULT_BATCH_ID" : batchId })
        j = json.loads(r)
        #print(j["results"])
        notebook = GetNotebookName(path)
        #Databricks/Automated-Testing-Framework/tests/Dimension/dimlo2
        resultsPath = f"/mnt/datalake-raw/atf/{rawPath}/{batchId}/{notebook}.json"
        #print(resultsPath)
        dbutils.fs.put(resultsPath, r, True)
        
        #break
    #break

# COMMAND ----------


