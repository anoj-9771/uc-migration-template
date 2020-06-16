# Databricks notebook source
# DBTITLE 1,Parameters
dbutils.widgets.text('srcKvSecret','','')
dbutils.widgets.text('dstKvSecret','','')
dbutils.widgets.text('srcAccount','','')
dbutils.widgets.text('dstAccount','','')
dbutils.widgets.text('srcContainerName','','')
dbutils.widgets.text('srcDirectoryName','','')
dbutils.widgets.text('dstContainerName','','')
dbutils.widgets.text('dstDirectoryName','','')
dbutils.widgets.text('blobName','','')
dbutils.widgets.text('dstTableName','','')



parameters = dict(
    srcKvSecret = dbutils.widgets.get('srcKvSecret')
    ,srcAccName = dbutils.widgets.get('srcAccount')
    ,srcContainerName = dbutils.widgets.get('srcContainerName')
    ,srcDirectoryName = dbutils.widgets.get('srcDirectoryName')
    ,dstKvSecret = dbutils.widgets.get('dstKvSecret')
    ,dstAccName = dbutils.widgets.get('dstAccount')
    ,dstContainerName = dbutils.widgets.get('dstContainerName')
    ,dstDirectoryName = dbutils.widgets.get('dstDirectoryName')
    ,blobName = dbutils.widgets.get('blobName')
    ,dstTableName = dbutils.widgets.get('dstTableName')
)
  

# COMMAND ----------

# DBTITLE 1,Blob Library
# MAGIC %run "/build/Util/BlobHelper-ut.py"

# COMMAND ----------

# DBTITLE 1,DB Library
# MAGIC %run "/build/Util/DbHelper_jdbc-ut.py"

# COMMAND ----------

# DBTITLE 1,Connect to Azure Storage and DB
DatabaseConn_analytics = DbHelper(parameters["dstKvSecret"])

srcBlobPath = GetBlobStoreFiles(parameters["srcKvSecret"],parameters["srcAccName"],\
                                parameters["srcContainerName"],parameters["srcDirectoryName"],parameters["blobName"])

# COMMAND ----------

from pyspark.sql import Row
from collections import OrderedDict
import json

df = sqlContext.read.parquet(srcBlobPath)

#write to db using jdbc
DatabaseConn_analytics.overwrite_table(parameters["dstDirectoryName"],df)
#DatabaseConn_analytics.append_table(parameters["dstTableName"],df)

# COMMAND ----------

# DBTITLE 1,Write to DB using Spark SQL Connector
#%run "/Util/DbHelper_scala"

# COMMAND ----------

# #convert python dictionary into Dataframe
# dictionary=[json.dumps(parameters)]
# jsonRDD = sc.parallelize(dictionary)
# param = spark.read.json(jsonRDD)

# #convert pyspark dataframe to scala dataframe
# df.registerTempTable("df_s")
# param.registerTempTable("param_s")

# COMMAND ----------

# %scala
# val df = table("df_s")
# val param_s = table("param_s")
# val params = param_s.collect.map(p => Map(param_s.columns.zip(p.toSeq):_*))
# val parameters = params(0)

# //Write to DB
# writeToDB(parameters("dstkvSecret"), parameters("dstDirectoryName"), df)


