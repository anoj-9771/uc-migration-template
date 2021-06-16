# Databricks notebook source
# DBTITLE 1,Parameters
import json
# dbutils.widgets.removeAll()
dbutils.widgets.text('item','','')
dbutils.widgets.text('execTime','','')
items = json.loads(dbutils.widgets.get('item'))

parameters = dict(
    srcKvSecret = items['SourceConnection'] #.split('_')[0]
    ,srcAccName = items['SourceDataStoreName'].split('_')[0].lower()
    ,srcContainerName = items['SourceLocation'].split('/')[0].lower()
    ,srcDirectoryName = items['SourceLocation'].split('/')[1]
    ,srcType = items['SourceType']
    ,dstType = items['TargetType']
    ,dstKvSecret = items['TargetConnection']# .split('_')[0]
    ,dstAccName = items['TargetDataStoreName'].split('_')[0].lower()
    ,dstContainerName = items['TargetLocation']
    ,srcBlobName = items['SourceBlobName']
    ,dstBlobName = items['TargetName']
    ,dstTableName = items['DestinationTable']
    ,srcFormat = items['SourceFormat']
    ,dstFormat = items['TargetFormat']
    ,prcType = items['ProcessType']
    ,query = items['SourceQuery']
    ,watermark = items['Watermark']
    ,execTime = dbutils.widgets.get('execTime')
    ,batchId = int(items['BatchId'])
    ,srcId = int(items['SourceId'])
    ,dstId = int(items['TargetId'])
    ,taskId = int(items['TaskId'])
)  


# COMMAND ----------

print(items)
print("\n")
print(parameters)


# COMMAND ----------

# DBTITLE 1,Blob Library
# MAGIC %run "/build/Util/BlobHelper-ut.py"

# COMMAND ----------

# DBTITLE 1,DB Library
# MAGIC %run "/build/Util/DbHelper_jdbc-ut.py"

# COMMAND ----------

# MAGIC %run "/build/Util/DbHelperCreateTableSQL-ut.py"

# COMMAND ----------

# MAGIC %run "/build/Util/Helper_getDataFrame-ut.py"

# COMMAND ----------

print(parameters["srcKvSecret"])

# COMMAND ----------

# DBTITLE 1,Connect to Azure Storage and DB
DatabaseConn_analytics = DbHelper(parameters["dstKvSecret"])

srcBlobPath = GetBlobStoreFiles(parameters["srcKvSecret"],parameters["srcAccName"],\
                                parameters["srcContainerName"],parameters["srcDirectoryName"],parameters["srcBlobName"], parameters["srcType"].split('_')[0].lower())
tmpBlobPath = GetBlobStoreFiles(parameters["srcKvSecret"],parameters["srcAccName"],\
                                "transient","tempDir","", parameters["srcType"].split('_')[0].lower())

# COMMAND ----------

print(srcBlobPath)
print(tmpBlobPath)

# COMMAND ----------

from delta.tables import *
from pyspark.sql import Row
from pyspark.sql.functions import to_date, date_format, lit
from collections import OrderedDict
import json

DatabaseConn_analytics=DbHelper(parameters["dstKvSecret"])
url = DatabaseConn_analytics.connect_db()

# Ingest Curated Delta table into dataframe
df = DeltaTable.forPath(spark, srcBlobPath + parameters["srcFormat"]).toDF()

# Write Curated table to EDW for rows where IS_CURRENT_DeltaLake = TRUE
(df.where((col("IS_CURRENT_DeltaLake") == True)).write
  .format("com.databricks.spark.sqldw")
  .option("url", url)
  .option("useAzureMSI", "true")
  .option("dbTable", parameters["dstTableName"])
  .option("tempDir", tmpBlobPath)
  .mode("overwrite")
  .save())
