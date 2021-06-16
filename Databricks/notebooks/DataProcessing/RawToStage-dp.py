# Databricks notebook source
import json
# dbutils.widgets.removeAll()
dbutils.widgets.text('item','','')
dbutils.widgets.text('execTime','','')
items = json.loads(dbutils.widgets.get('item'))

# COMMAND ----------

# MAGIC %md
# MAGIC This notebook is used to create Delta tables with a common format. Common columns include HASH_CODE, IS_CURRENT, VALID_FROM, VALID_TO, BK.
# MAGIC HASH_CODE: Used to track row level changes and is made up of all columns excluding natural keys.
# MAGIC IS_CURRENT: Used to identify the current version of a record.
# MAGIC VALID_FROM: The date a record was inserted, updated or deleted.
# MAGIC VALID_TO: The date a version of a record was no longer current.
# MAGIC BK(keys):  The natural key (business key) used to identify a records uniquness. This can be 1 or many columns and if none exists, this will default to the HASH_CODE. This can be passed in via the query parameter.
# MAGIC Watermark: A column that has the last updated time stamp or an incrementing key.
# MAGIC 
# MAGIC # Logic
# MAGIC ##### 1. If keys is null and watermark is null, then perform an Insert and Delete
# MAGIC ##### 2. If keys is null and watermark is not null, then perform an Insert
# MAGIC ##### 3. If keys is not null and watermark is null, then perform an Insert, Update and Delete
# MAGIC ##### 4. If keys is not null and watermark is not null, then perform an Insert and Update

# COMMAND ----------

# DBTITLE 1,Parameters Mapping
parameters = dict(
    srcKvSecret = items['SourceConnection'].split('_')[0]
    ,srcAccName = items['SourceDataStoreName'].split('_')[0].lower()
    ,srcContainerName = items['SourceLocation'].split('/')[0].lower()
    ,srcDirectoryName = items['SourceLocation'].split('/')[1]
    ,srcType = items['SourceType']
    ,dstType = items['TargetType']
    ,dstKvSecret = items['TargetConnection'].split('_')[0]
    ,dstAccName = items['TargetDataStoreName'].split('_')[0].lower()
    ,dstContainerName = items['TargetLocation'].split('/')[0].lower()
    ,dstDirectoryName = items['TargetLocation'].split('/')[1]
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

print(dbutils.widgets.get('item'))
print("\r\n")
print(parameters)

# COMMAND ----------

# DBTITLE 0,Blob Library
# MAGIC %run "/build/Util/BlobHelper-ut.py"

# COMMAND ----------

# DBTITLE 0,Schema Create Library
# MAGIC %run "/build/Util/DbHelperCreateTableSQL-ut.py"

# COMMAND ----------

# MAGIC %run "/build/Util/DbHelper_jdbc-ut.py"

# COMMAND ----------

# MAGIC %run "/build/Util/Helper_getDataFrame-ut.py"

# COMMAND ----------

# MAGIC %run "/build/Util/CreateDeltaTableHelper-ut.py"

# COMMAND ----------

# storage_conn = dict(
#     srcBlobPath = GetBlobStoreFiles(parameters["srcKvSecret"],parameters["srcAccName"],\
#                                     parameters["srcContainerName"],parameters["srcDirectoryName"],parameters["srcBlobName"], parameters["srcType"].split('_')[0].lower())

#     dstBlobPath = GetBlobStoreFiles(parameters["dstKvSecret"],parameters["dstAccName"],\
#                                     parameters["dstContainerName"],parameters["dstDirectoryName"],parameters["dstBlobName"], parameters["dstType"].split('_')[0].lower())

#     dstPath = GetBlobStoreFiles(parameters["dstKvSecret"],parameters["dstAccName"],\
#                                     parameters["dstContainerName"],parameters["dstDirectoryName"],'', parameters["dstType"].split('_')[0].lower())

#     trnsBlobPath = GetBlobStoreFiles(parameters["dstKvSecret"],parameters["dstAccName"],\
#                                 "transient",parameters["dstDirectoryName"],parameters["dstBlobName"], parameters["dstType"].split('_')[0].lower())
# )

# COMMAND ----------

srcBlobPath = GetBlobStoreFiles(parameters["srcKvSecret"],parameters["srcAccName"],\
                                parameters["srcContainerName"],parameters["srcDirectoryName"],parameters["srcBlobName"], parameters["srcType"].split('_')[0].lower())

dstBlobPath = GetBlobStoreFiles(parameters["dstKvSecret"],parameters["dstAccName"],\
                                parameters["dstContainerName"],parameters["dstDirectoryName"],parameters["dstBlobName"], parameters["dstType"].split('_')[0].lower())

dstPath = GetBlobStoreFiles(parameters["dstKvSecret"],parameters["dstAccName"],\
                                parameters["dstContainerName"],parameters["dstDirectoryName"],'', parameters["dstType"].split('_')[0].lower())

trnsBlobPath = GetBlobStoreFiles(parameters["dstKvSecret"],parameters["dstAccName"],\
                                "transient",parameters["dstDirectoryName"],parameters["dstBlobName"], parameters["dstType"].split('_')[0].lower())

# COMMAND ----------

print(srcBlobPath)
print(dstBlobPath)
print(dstPath)

# COMMAND ----------

# dbutils.fs.ls('abfss://transient@adlsdevaueanalytics.dfs.core.windows.net/')

# COMMAND ----------

from delta.tables import *
from datetime import datetime, timedelta
from pyspark.sql.functions import *
from pyspark.sql.functions import sha2, concat_ws

# Ingest raw dataframe from source location
df = CreateDataFrame(srcBlobPath, parameters["srcFormat"], parameters["query"], parameters["srcBlobName"])

# Write data to synapse
DatabaseConn_analytics=DbHelper("BI-Reload2-connectionString")
url = DatabaseConn_analytics.connect_db()

(df.write
  .format("com.databricks.spark.sqldw")
  .option("url", url)
  .option("useAzureMSI", "true")
#   .option("forwardSparkAzureStorageCredentials", "true")
  .option("dbTable", parameters["srcDirectoryName"] + '.' + parameters["dstTableName"])
  .option("tempDir", trnsBlobPath)
  .mode("overwrite")
  .save())

# COMMAND ----------



# Extract dataframe's infered schema
schm_ = df.schema

# Set BK and/or HASH_CODE column names
cols_ = df.columns
if parameters["query"]:
  keys = parameters["query"].split(',')
  check_sum = list(set(cols_) - set (keys))
else:
  keys = check_sum = cols_

# Create correct dataframe structure based on keys:
#     1. If keys exists, then BK must be tracked for changes e.g. for full load, 
#        deletes and uppdates must be tracked.
schm_delta = (df
      .withColumn("HASH_CODE_DeltaLake", lit(" "))
      .withColumn("LOAD_ID_DeltaLake", lit(datetime.today()))
      .withColumn("IS_CURRENT_DeltaLake", lit(True))
      .withColumn("VALID_FROM_DeltaLake", lit(datetime.today()))
      .withColumn("VALID_TO_DeltaLake", lit(datetime.today()))
      .withColumn("BK_DeltaLake", lit(" ")).schema)

# Create Delta table if none exists and retain ACID transactions for 7 days
try: 
    # Create empty datafram
    df_delta = spark.createDataFrame(spark.sparkContext.emptyRDD(),schm_delta)
    cols_delta = df_delta.columns
    # Create and write Delta table if noe exists
    MakeDirectory(parameters["dstKvSecret"],parameters["dstAccName"],parameters["dstContainerName"],dstPath)
    df_delta.write.format("delta").save(dstBlobPath + parameters["dstFormat"])
    spark.sql("VACUUM delta.`" + dstBlobPath + parameters['dstFormat'] +"` RETAIN 168 HOURS")
except: 
    print("Delta table %s already exists!"%(dstBlobPath))
finally:
    deltaTable = DeltaTable.forPath(spark, dstBlobPath + parameters["dstFormat"])

# Write to tranisent layer
buildDeltaTable(df,parameters["watermark"]).write.format("delta").mode("overwrite").save(trnsBlobPath + parameters["dstFormat"])
#spark.sql("VACUUM delta.`" + trnsBlobPath + parameters['dstFormat'] +"` RETAIN 24 HOURS")
df_trans = DeltaTable.forPath(spark, trnsBlobPath + parameters["dstFormat"])

# Merge source dataframe with corresponding Delta table    
perform_upsert(df_trans.toDF(), parameters["watermark"])
deltaTable.toDF().count()

# COMMAND ----------

perform_upsert(df_trans.toDF(), parameters["watermark"])

# COMMAND ----------

deltaTable.toDF().createOrReplaceTempView("temps")
df_trans.toDF().createOrReplaceTempView("temps_")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from (select DDADMNO, sum(cast(IS_CURRENT_DeltaLake as int)) as ics from temps
# MAGIC group by DDADMNO) a
# MAGIC where ics = 0
# MAGIC and DDADMNO = 59630005

# COMMAND ----------

# MAGIC %sql
# MAGIC update temps
# MAGIC set IS_CURRENT_DeltaLake =  1 where DDADMNO = 59630005
