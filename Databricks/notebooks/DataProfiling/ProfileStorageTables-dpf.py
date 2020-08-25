# Databricks notebook source
# DBTITLE 1,Parameters
import json

dbutils.widgets.text('parameters','','')
parameters = json.loads(dbutils.widgets.get('parameters'))
  

# COMMAND ----------

# DBTITLE 1,Blob Library
# MAGIC %run "/build/Util/BlobHelper-ut.py"

# COMMAND ----------

# DBTITLE 1,Schema Create Library
# MAGIC %run "/build/Util/DbHelperCreateTableSQL-ut.py"

# COMMAND ----------

# MAGIC %run "/build/Util/DbHelper_jdbc-ut.py"

# COMMAND ----------

# %run "/build/Util/pyodbcHelper-ut.py"

# COMMAND ----------

# MAGIC %run "/build/Util/Helper_getDataFrame-ut.py"

# COMMAND ----------

# DBTITLE 1,Data Catalog
# MAGIC %run "/build/Util/DbHelper_DataCatalog-ut.py"

# COMMAND ----------

srcBlobPath = GetBlobStoreFiles(parameters["srcKvSecret"],parameters["srcAccName"],\
                                parameters["srcContainerName"],parameters["srcDirectoryName"],parameters["srcBlobName"], parameters["srcType"].split('_')[0].lower())
try:
  dbutils.fs.ls(srcBlobPath + parameters["srcFormat"])
  df_source = CreateDataFrame(srcBlobPath, parameters["srcFormat"], parameters["query"], parameters["srcBlobName"])
except:
  x = srcBlobPath.rfind('/',0,len(srcBlobPath))
  print(x)
  srcBlobPath = srcBlobPath[0:x+1] + '*/*'
  df_source = spark.read.format("csv").options(header='true',quote='"', escape='\'').load(srcBlobPath + parameters["srcFormat"])
finally:
  print(srcBlobPath)

# COMMAND ----------

# x = srcBlobPath.rfind('/',0,len(srcBlobPath))
# #print(x)
# srcBlobPath_2 = srcBlobPath[0:x+1] + '*/*'
# print(srcBlobPath_2)
# df_source = spark.read.format("csv").options(header='true',quote='"', escape='\'').load(srcBlobPath_2 + + parameters["srcFormat"])

# COMMAND ----------

# DBTITLE 1,Create and Write Data Frame
from pyspark.sql import functions as F
from datetime import datetime

try: 
    source_size = spark.createDataFrame(dbutils.fs.ls(srcBlobPath + parameters["srcFormat"])).select("size").groupBy().sum().collect()[0][0] * 0.000001

    df_prof = dataCatalogProfile(df_source, df_source.columns, parameters["srcContainerName"] + '.' + parameters["srcBlobName"], parameters["srcId"])

    ctlKvSecret = 'ADF-LS-AzureSql-ControlDB'

    # Write source profile to Control DB
    DatabaseConn_analytics = DbHelper(ctlKvSecret)
    DatabaseConn_analytics.append_table("dbo.DataProfile",df_prof)
    status = 'Succeeded'
except: status = 'Failed'
  


# COMMAND ----------

# import json
# paramters_json = json.dumps(parameters)
# test = json.loads(paramters_json)
# print(paramters_json)

# COMMAND ----------

# DBTITLE 1,Catalog Source System Data Set
# accessToken = getAccessToken(parameters["catalogSecret"])
# srcSys_dc = bodyJson('source_system',parameters["catalogSecret"], parameters["dstTableName"], timestamp, True, table_preview = True, table_profile = True, table_schema = True)
# id = registerDataAsset(srcSys_dc, parameters["catalogSecret"])
# print("REG:" + id)

# COMMAND ----------

# DBTITLE 1,Catalog Raw Data Frame
# raw_dc = bodyJson('source',parameters["catalogSecret"], parameters["srcContainerName"] + '.' + parameters["srcBlobName"], timestamp, True, table_preview = True, table_profile = True, column_profile = True, table_schema = True)
# id = registerDataAsset(raw_dc, parameters["catalogSecret"])
# print("REG:" + id)

# COMMAND ----------

# #Search
# src_searchJson = searchDataAsset(parameters["dstTableName"])
# print("SER:" + src_searchJson)

# COMMAND ----------

# dataProfile(df, df.columns).head(20)
