# Databricks notebook source
# MAGIC %run "/build/Util/DbHelper_jdbc-ut.py"

# COMMAND ----------

import requests
from requests.auth import HTTPBasicAuth
from pyspark.sql.types import *
import json
from pyspark.sql.functions import explode, explode_outer, array, col, first, monotonically_increasing_id, split, posexplode

dbutils.widgets.text('parameters','','')
parameters = json.loads(dbutils.widgets.get('parameters'))


global creds
creds = {}

def getRequest(kvSecret, query):
  connection = dbutils.secrets.get(scope='vwazr-dp-keyvault',key=kvSecret)
  cons = connection.split(';')
  cons = cons
  for con in cons:
      value = con.split('=')
      try:
        creds[value[0]] = value[1]
      except : False
  url_auth = "%s/token" % creds["endpoint"]
  headers = {'Content-Type': "application/x-www-form-urlencoded"}
  payload = {'grant_type' : 'client_credentials', 'scope':'https://api.loganalytics.io/.default'}
  res = requests.get(url_auth,headers=headers,data=payload,auth=HTTPBasicAuth(creds["client_id"], creds["secret"]))
  token = res.json()['access_token']
  data = {'query':"ADFActivityRun |project TimeGenerated,Status,Start,ActivityName,ActivityRunId,PipelineRunId,ActivityType,LinkedServiceName,End,FailureType,PipelineName,Input,Output,ErrorMessage,Error,Type | where End != todatetime('1601-01-01 00:00:00.0000000') and TimeGenerated > ago(2d)| sort by TimeGenerated desc"}
  url_token = query
  headers = {'Content-Type': "application/json",'Authorization': "Bearer {0}".format(token)}
  return requests.post(url_token,headers=headers,json=data)#,auth=HTTPBasicAuth(creds["client_id"], creds["secret"]))
# Convert response text from json to df
def jsonToDf(json, schema=None):
  reader = spark.read
  if schema:
    reader.schema(schema)
  return reader.json(sc.parallelize([json]))
# This section is completely dependent on the output of you json response...This is just a demo
def getDf(response):
    # Convert response into dataframe
    df0 = jsonToDf(response.text)
    # Explode value array and rename as data
    # Use explode to flatten an array. If it is a Struct, just use '.' to extract nested items
    df1 = df0.select(explode_outer(df0.tables).alias('tables'))
    df2= df1.select(explode_outer(df1.tables.rows).alias('rows'))
    return df2

# COMMAND ----------

resp = getRequest(parameters["srcKvSecret"],parameters["query"])
df = getDf(resp)

# COMMAND ----------

# split_col = split(df['rows'], ',')
df1 = (df.withColumn("TimeGenerated", df["rows"].getItem(0).cast(TimestampType()))
  .withColumn("Status", df["rows"].getItem(1))
  .withColumn("Start", df["rows"].getItem(2).cast(TimestampType()))
  .withColumn("ActivityName", df["rows"].getItem(3))
  .withColumn("ActivityRunId", df["rows"].getItem(4))
  .withColumn("PipelineRunId", df["rows"].getItem(5))
  .withColumn("ActivityType", df["rows"].getItem(6))
  .withColumn("LinkedServiceName", df["rows"].getItem(7))
  .withColumn("End", df["rows"].getItem(8).cast(TimestampType()))
  .withColumn("FailureType", df["rows"].getItem(9))
  .withColumn("PipelineName", df["rows"].getItem(10))
  .withColumn("Input", df["rows"].getItem(11))
  .withColumn("Output", df["rows"].getItem(12))
  .withColumn("ErrorMessage", df["rows"].getItem(13))
  .withColumn("Error", df["rows"].getItem(14))
  .withColumn("Type", df["rows"].getItem(15))
).drop("rows")

# COMMAND ----------

ctlKvSecret = 'ADF-LS-AzureSql-ControlDB'

# Write source profile to Control DB
DatabaseConn_analytics = DbHelper(ctlKvSecret)
DatabaseConn_analytics.overwrite_table("stg.ADFActivityRun",df1)
status = 'Succeeded'

# COMMAND ----------

df1.count()
