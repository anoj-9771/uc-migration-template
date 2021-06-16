# Databricks notebook source
# DBTITLE 1,Parameters
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

# DBTITLE 1,Blob Library
# MAGIC %run "/build/Util/BlobHelper-ut.py"

# COMMAND ----------

# DBTITLE 1,DB Library
# MAGIC %run "/build/Util/DbHelper_jdbc-ut.py"

# COMMAND ----------

# DBTITLE 1,Connect to Azure Storage and DB
DatabaseConn_analytics = DbHelper(parameters["dstKvSecret"])

srcAccountName = BlobStoreAccount(parameters["srcKvSecret"])

srcBlobPath = GetBlobStoreFiles(parameters["srcKvSecret"],srcAccountName,\
                                parameters["srcContainerName"],parameters["srcDirectoryName"],parameters["srcBlobName"], "wasbs")


# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.functions import to_date, date_format, lit, unix_timestamp, from_utc_timestamp, current_timestamp
from collections import OrderedDict
import json, time, datetime
timestamp = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')

df =  (spark.read.parquet(srcBlobPath))
#Add timestamp column to base dataframe structure
df2 = df.withColumn('DSS_UPDATE_TIME',from_utc_timestamp(current_timestamp(),'Australia/Perth'))
#df.withColumn('DSS_UPDATE_TIME',date_format(unix_timestamp(lit(timestamp),'yyyy-MM-dd HH:mm:ss').cast("timestamp"),'yyyy-MM-dd HH:mm:ss'))
#display(df2)


#write to db using jdbc
DatabaseConn_analytics.overwrite_table(parameters["dstDirectoryName"],df2)
#DatabaseConn_analytics.append_table(parameters["dstTableName"],df)

# COMMAND ----------

# DBTITLE 1,Write to DB using Spark SQL Connector
#%run "/build/Util/DbHelper_scala2-ut"

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
