# Databricks notebook source
# DBTITLE 1,Parameters
dbutils.widgets.text('srcKvSecret','','')
dbutils.widgets.text('dstKvSecret','','')
dbutils.widgets.text('srcAccount','','')
dbutils.widgets.text('dstAccount','','')
dbutils.widgets.text('srcContainerName','','')
dbutils.widgets.text('srcDirectoryName','','')
dbutils.widgets.text('srcType','','')
dbutils.widgets.text('dstType','','')
dbutils.widgets.text('dstContainerName','','')
dbutils.widgets.text('dstDirectoryName','','')
dbutils.widgets.text('srcBlobName','','')
dbutils.widgets.text('dstBlobName','','')
dbutils.widgets.text('dstTableName','','')
dbutils.widgets.text('srcFormat','','')
dbutils.widgets.text('dstFormat','','')
dbutils.widgets.text('prcType','','')
dbutils.widgets.text('query','','')
dbutils.widgets.text('catalogSecret','','')
dbutils.widgets.text('batchId','','')
dbutils.widgets.text('sourceId','','')
dbutils.widgets.text('targetId','','')
dbutils.widgets.text('taskId','','')



parameters = dict(
    srcKvSecret = dbutils.widgets.get('srcKvSecret')
    ,srcAccName = dbutils.widgets.get('srcAccount')
    ,srcContainerName = dbutils.widgets.get('srcContainerName')
    ,srcDirectoryName = dbutils.widgets.get('srcDirectoryName')
    ,srcType = dbutils.widgets.get('srcType')
    ,dstType = dbutils.widgets.get('dstType')
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
    ,catalogSecret = dbutils.widgets.get('catalogSecret')
    ,batchId = dbutils.widgets.get('batchId')
    ,srcId = dbutils.widgets.get('sourceId')
    ,dstId = dbutils.widgets.get('targetId')
    ,taskId = dbutils.widgets.get('taskId')
)
  

# COMMAND ----------

# DBTITLE 1,Blob Library
# MAGIC %run "/build/Util/BlobHelper-ut.py"

# COMMAND ----------

# DBTITLE 1,Schema Create Library
# MAGIC %run "/build/Util/DbHelperCreateTableSQL-ut.py"

# COMMAND ----------

# MAGIC %run "/build/Util/DbHelper_jdbc-ut.py"

# COMMAND ----------

# MAGIC %run "/build/Util/pyodbcHelper-ut.py"

# COMMAND ----------

# MAGIC %run "/build/Util/Helper_getDataFrame-ut.py"

# COMMAND ----------

# DBTITLE 1,Data Catalog
# MAGIC %run "/build/Util/DbHelper_DataCatalog-ut.py"

# COMMAND ----------

srcBlobPath = GetBlobStoreFiles(parameters["srcKvSecret"],parameters["srcAccName"],\
                                parameters["srcContainerName"],parameters["srcDirectoryName"],parameters["srcBlobName"], parameters["srcType"].split('_')[0].lower())

dstBlobPath = GetBlobStoreFiles(parameters["dstKvSecret"],parameters["dstAccName"],\
                                parameters["dstContainerName"],parameters["dstDirectoryName"],parameters["dstBlobName"], parameters["dstType"].split('_')[0].lower())

dstPath = GetBlobStoreFiles(parameters["dstKvSecret"],parameters["dstAccName"],\
                                parameters["dstContainerName"],parameters["dstDirectoryName"],'', parameters["dstType"].split('_')[0].lower())

# COMMAND ----------

print(srcBlobPath)
print(dstBlobPath)
print(dstPath)

# COMMAND ----------

# DBTITLE 1,Create and Write Data Frame
from pyspark.sql import functions as F
from datetime import datetime

date_time = datetime.now()
timestamp = date_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

df_source = CreateDataFrame(srcBlobPath, parameters["srcFormat"], parameters["query"], parameters["srcBlobName"])
df = df_source

MakeDirectory(parameters["dstKvSecret"],parameters["dstAccName"],parameters["dstContainerName"],dstPath)
df.write.mode('overwrite').parquet(dstBlobPath + parameters["dstFormat"])

dstSchemaName = parameters["dstTableName"].split('.')[0]
dstTableName = parameters["dstTableName"].split('.')[1]

##Added in if statement to move schema blob from raw to stage
if parameters["prcType"] in ['SQL Server']:#, 'Oracle']:
  schm = spark.read.format('csv').options(sep='|', header='true',quote='"', escape='\'').option("inferSchema", "true").load(srcBlobPath + '_schema' + parameters["srcFormat"])
  if len(schm.head(1)) == 0:
    schm = createSchemaDataFrame(df, dstSchemaName, dstTableName)
  schm.write.mode('overwrite').parquet(dstBlobPath + '_schema' + parameters["dstFormat"])
# elif parameters["prcType"] in ['Oracle']:
#   None
else:
  schm = createSchemaDataFrame(df, dstSchemaName, dstTableName)
  schm.write.mode('overwrite').parquet(dstBlobPath + '_schema' + parameters["dstFormat"])

# COMMAND ----------

source_size = spark.createDataFrame(dbutils.fs.ls(srcBlobPath + parameters["srcFormat"])).select("size").groupBy().sum().collect()[0][0] * 0.000001
pdDf_prof = dataCatalogProfile(df_source, df_source.columns, parameters["srcContainerName"] + '.' + parameters["srcBlobName"], parameters["srcId"])
  
# Convert to Pyspark DF
df_prof = spark.createDataFrame(pdDf_prof)

ctlKvSecret = 'ADF-LS-AzureSql-ControlDB'

# Write source profile to Control DB
DatabaseConn_analytics = DbHelper(ctlKvSecret)
DatabaseConn_analytics.append_table("dbo.DataProfile",df_prof)

# COMMAND ----------

# DBTITLE 1,Catalog Source System Data Set
accessToken = getAccessToken(parameters["catalogSecret"])
srcSys_dc = bodyJson('source_system',parameters["catalogSecret"], parameters["dstTableName"], timestamp, True, table_preview = True, table_profile = True, table_schema = True)
id = registerDataAsset(srcSys_dc, parameters["catalogSecret"])
print("REG:" + id)

# COMMAND ----------

# DBTITLE 1,Catalog Raw Data Frame
raw_dc = bodyJson('source',parameters["catalogSecret"], parameters["srcContainerName"] + '.' + parameters["srcBlobName"], timestamp, True, table_preview = True, table_profile = True, column_profile = True, table_schema = True)
id = registerDataAsset(raw_dc, parameters["catalogSecret"])
print("REG:" + id)

# COMMAND ----------

# #Search
# src_searchJson = searchDataAsset(parameters["dstTableName"])
# print("SER:" + src_searchJson)

# COMMAND ----------

# dataProfile(df, df.columns).head(20)
