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
    ,batchId = int(dbutils.widgets.get('batchId'))
    ,srcId = int(dbutils.widgets.get('sourceId'))
    ,dstId = int(dbutils.widgets.get('targetId'))
    ,taskId = int(dbutils.widgets.get('taskId'))
)
  

# COMMAND ----------

# DBTITLE 0,Blob Library
# MAGIC %run "/build/Util/BlobHelper-ut.py"

# COMMAND ----------

# DBTITLE 0,Schema Create Library
# MAGIC %run "/build/Util/DbHelperCreateTableSQL-ut.py"

# COMMAND ----------

# MAGIC %run "/build/Util/DbHelper_jdbc-ut.py"

# COMMAND ----------

# %run "/build/Util/pyodbcHelper-ut.py"

# COMMAND ----------

# MAGIC %run "/build/Util/Helper_getDataFrame-ut.py"

# COMMAND ----------

# MAGIC %run "/build/Util/JobHelper-ut.py"

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

MakeDirectory(parameters["dstKvSecret"],parameters["dstAccName"],parameters["dstContainerName"],dstPath)
df_source.write.mode('overwrite').parquet(dstBlobPath + parameters["dstFormat"])

dstSchemaName = parameters["dstTableName"].split('.')[0]
dstTableName = parameters["dstTableName"].split('.')[1]

##Added in if statement to move schema blob from raw to stage
if parameters["prcType"] in ['SQL Server']:#, 'Oracle']:
  schm = spark.read.format('csv').options(sep='|', header='true',quote='"', escape='\'').option("inferSchema", "true").load(srcBlobPath + '_schema' + parameters["srcFormat"])
  if len(schm.head(1)) == 0:
    schm = createSchemaDataFrame(df_source, dstSchemaName, dstTableName)
  schm.write.mode('overwrite').parquet(dstBlobPath + '_schema' + parameters["dstFormat"])
# elif parameters["prcType"] in ['Oracle']:
#   None
else:
  schm = createSchemaDataFrame(df_source, dstSchemaName, dstTableName)
  schm.write.mode('overwrite').parquet(dstBlobPath + '_schema' + parameters["dstFormat"])

# COMMAND ----------

# DBTITLE 1,Create and Load Data Profile
runJob(49997)
