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

# DBTITLE 1,JDBC DB Library
# MAGIC %run "/build/Util/DbHelper_jdbc-ut.py"

# COMMAND ----------

# DBTITLE 1,Connect to Azure Storage and DB
DatabaseConn_analytics = DbHelper(parameters["dstKvSecret"])

srcAccountName = BlobStoreAccount(parameters["srcKvSecret"])

srcBlobPath = GetBlobStoreFiles(parameters["srcKvSecret"],srcAccountName,\
                                parameters["srcContainerName"],parameters["srcDirectoryName"],parameters["blobName"])


# COMMAND ----------

# DBTITLE 1,Read Parquet and Create SQL Table
from pyspark.sql import Row
from pyspark.sql.functions import to_date, date_format, lit, unix_timestamp,from_utc_timestamp, current_timestamp
from collections import OrderedDict
import json, time, datetime
timestamp = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')

df =  (spark.read.parquet(srcBlobPath))
#Add timestamp column to base dataframe structure
newUpdRecs = df.withColumn('DSS_UPDATE_TIME',from_utc_timestamp(current_timestamp(),'Australia/Perth'))
#(date_format(unix_timestamp(lit(timestamp),'yyyy-MM-dd HH:mm:ss').cast("timestamp"),'yyyy-MM-dd HH:mm:ss')),'')
#display(newUpdRecs.limit(1))
newUpdRecs.createOrReplaceTempView("newUpdRecs")


#write first row to db using jdbc to create the table based on inferred schema
DatabaseConn_analytics.overwrite_table(parameters["dstDirectoryName"],newUpdRecs.limit(1))


# COMMAND ----------

# DBTITLE 1,Spark SQL DB Library
# MAGIC %run "/build/Util/DbHelper_scala-ut.scala"

# COMMAND ----------

# DBTITLE 1,Write to DB using Spark SQL Connector
# MAGIC %scala
# MAGIC 
# MAGIC 
# MAGIC val df = spark.table("newUpdRecs")
# MAGIC val arr = dbutils.widgets.get("dstDirectoryName").split('.').map(_.trim)
# MAGIC var TargetSQLTableName = arr(1)
# MAGIC var schemaName = arr(0)
# MAGIC var kvSecret = dbutils.widgets.get("dstKvSecret")
# MAGIC 
# MAGIC //print ("Schema: " + schemaName)
# MAGIC 
# MAGIC // try{
# MAGIC //     bulkInsertIterativeLoad(kvSecret, df, schemaName, TargetSQLTableName)
# MAGIC // }
# MAGIC // catch{
# MAGIC //   case e: Throwable => println("Error: " + e)
# MAGIC //   dbutils.notebook.exit("message")
# MAGIC // }
# MAGIC 
# MAGIC 
# MAGIC //bulkInsertIterativeLoad(kvSecret, df, schemaName, TargetSQLTableName)
# MAGIC bulkInsertFullLoad(kvSecret, df, schemaName, TargetSQLTableName)

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

