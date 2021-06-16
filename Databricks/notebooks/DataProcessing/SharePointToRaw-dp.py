# Databricks notebook source
dbutils.widgets.text('srcKvSecret','','')
dbutils.widgets.text('dstKvSecret','','')
dbutils.widgets.text('srcAccount','','')
dbutils.widgets.text('dstAccount','','')
dbutils.widgets.text('srcContainerName','','')
dbutils.widgets.text('srcDirectoryName','','')
dbutils.widgets.text('srcType','','')
dbutils.widgets.text('dstContainerName','','')
dbutils.widgets.text('dstDirectoryName','','')
dbutils.widgets.text('srcBlobName','','')
dbutils.widgets.text('dstBlobName','','')
dbutils.widgets.text('dstTableName','','')
dbutils.widgets.text('srcFormat','','')
dbutils.widgets.text('dstFormat','','')
dbutils.widgets.text('prcType','','')
dbutils.widgets.text('query','','')



parameters = dict(
    srcKvSecret = dbutils.widgets.get('srcKvSecret')
    ,srcAccName = dbutils.widgets.get('srcAccount')
    ,srcContainerName = dbutils.widgets.get('srcContainerName')
    ,srcDirectoryName = dbutils.widgets.get('srcDirectoryName')
    ,srcType = dbutils.widgets.get('srcType')
    ,dstKvSecret = dbutils.widgets.get('dstKvSecret')
    ,dstAccName = dbutils.widgets.get('dstAccount')
    ,dstContainerName = dbutils.widgets.get('dstContainerName')
    ,dstDirectoryName = dbutils.widgets.get('dstDirectoryName')
    ,srcBlobName = dbutils.widgets.get('srcBlobName')
    ,dstBlobName = dbutils.widgets.get('dstBlobName')
    ,dstTableName = dbutils.widgets.get('dstTableName')
    ,srcFormat = dbutils.widgets.get('srcFormat')
    ,dstFormat = dbutils.widgets.get('dstFormat')
    ,prcType = dbutils.widgets.get('prcType')
    ,query = dbutils.widgets.get('query')
)
  

# COMMAND ----------

# MAGIC %run "/build/Util/BlobHelper-ut.py"

# COMMAND ----------

dstAccountName = BlobStoreAccount(parameters["dstKvSecret"])

dstBlobPath = GetBlobStoreFiles(parameters["dstKvSecret"],dstAccountName,\
                                parameters["dstContainerName"],parameters["dstDirectoryName"],parameters["dstBlobName"], "wasbs")

dstPath = GetBlobStoreFiles(parameters["dstKvSecret"],dstAccountName,\
                                parameters["dstContainerName"],parameters["dstDirectoryName"],'', "wasbs")

# COMMAND ----------

auth_payload = dbutils.secrets.get(scope='vwazr-dp-keyvault',key=parameters["srcKvSecret"])

# COMMAND ----------

import requests
from requests.auth import HTTPBasicAuth
import json
from pyspark.sql.functions import explode, explode_outer, array, col, first, monotonically_increasing_id


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
  payload = {'grant_type' : 'client_credentials', 'scope':'https://graph.microsoft.com/.default'}
  res = requests.get(url_auth,headers=headers,data=payload,auth=HTTPBasicAuth(creds["client_id"], creds["secret"]))
  token = res.json()['access_token']
  url_token = query
  headers = {'Content-Type': "application/json",'Authorization': "Bearer {0}".format(token)}
  return requests.get(url_token,headers=headers)#,auth=HTTPBasicAuth(creds["client_id"], creds["secret"]))

# COMMAND ----------

res = getRequest(parameters["srcKvSecret"], parameters["query"])
data = json.loads(res.text)["value"]
df = sc.parallelize(data).toDF()
df0 = df.withColumn("row_id", monotonically_increasing_id())

df1 = df0.select( \
                    "row_id",\
                    col("createdDateTime").alias("CreatedDateTime"), \
                    col("createdBy.user.email").alias("CreateByUserEmail"),\
                    col("createdBy.user.displayName").alias("CreateByUserName"),\
                    col("lastModifiedDateTime").alias("lastModifiedDateTime"),\
                    col("lastModifiedBy.user.email").alias("ModifiedByUserEmail"),\
                    col("lastModifiedBy.user.displayName").alias("ModifiedByUserName"),\
                    explode(df0.fields)
                    )
df2 = df1.select("row_id", "CreatedDateTime","CreateByUserEmail","CreateByUserName","lastModifiedDateTime","ModifiedByUserEmail","ModifiedByUserName", "key", "value") \
        .groupBy("row_id", "CreatedDateTime","CreateByUserEmail","CreateByUserName","lastModifiedDateTime","ModifiedByUserEmail","ModifiedByUserName")  \
        .pivot("key") \
        .agg(first("value"))
df2 = df2.drop("@odata.etag")
#df = df2.select(<cleaned up columns... to be decided>)
MakeDirectory(parameters["dstKvSecret"],parameters["dstAccName"],parameters["dstContainerName"],dstPath)
df2.write.mode('overwrite') \
  .option("codec", "org.apache.hadoop.io.compress.GzipCodec") \
  .option("header","true") \
  .option("delimiter","|") \
  .csv(dstBlobPath + parameters["dstFormat"]) 
#wait 2 mins 
import time
time.sleep(120) 
