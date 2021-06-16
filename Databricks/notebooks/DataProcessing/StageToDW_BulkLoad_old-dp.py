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

# MAGIC %run "/build/Util/Helper_getDataFrame-ut.py"

# COMMAND ----------

# MAGIC %run "/build/Util/DbHelper_jdbc-ut.py"

# COMMAND ----------

# MAGIC %run "/build/Util/pyodbcHelper-ut.py"

# COMMAND ----------

# DBTITLE 1,Data Catalog
# MAGIC %run "/build/Util/DbHelper_DataCatalog-ut.py"

# COMMAND ----------

# MAGIC %run "/build/Util/DbHelper_scala-ut.scala"

# COMMAND ----------

DatabaseConn_analytics = DbHelper(parameters["dstKvSecret"])

srcBlobPath = GetBlobStoreFiles(parameters["srcKvSecret"],parameters["srcAccName"],\
                                parameters["srcContainerName"],parameters["srcDirectoryName"],parameters["srcBlobName"], parameters["srcType"].split('_')[0].lower())

# COMMAND ----------

# DBTITLE 1,Extract Dataset
#extracts dataframe for csv, json and excel from blob storage

# COMMAND ----------

# DBTITLE 1,Create and Write Data Frame
from pyspark.sql import Row
from pyspark.sql.functions import col
from pyspark.sql.functions import to_date, date_format, lit, unix_timestamp,from_utc_timestamp, current_timestamp
from collections import OrderedDict
import json, time
from datetime import datetime

date_time = datetime.now()
timestamp = date_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

df_source =  (spark.read.parquet(srcBlobPath + parameters["srcFormat"]))
df = df_source.na.drop(how="all")
if parameters["prcType"] in ['SQL Server', 'Excel', 'API']:
  df_schema = spark.read.parquet(srcBlobPath + '_schema' + parameters["srcFormat"])
  if parameters["prcType"] in ['SQL Server']:
    df = recastTable(df, df_schema)

# df = df.select([col(c).cast("string") for c in df.columns])

#Add timestamp column to base dataframe structure
newUpdRecs = df.withColumn('DSS_UPDATE_TIME',from_utc_timestamp(current_timestamp(),'Australia/Perth'))
#(date_format(unix_timestamp(lit(timestamp),'yyyy-MM-dd HH:mm:ss').cast("timestamp"),'yyyy-MM-dd HH:mm:ss')),'')
#display(newUpdRecs.limit(1))
newUpdRecs.createOrReplaceTempView("newUpdRecs")


#write first row to db using jdbc to create the table based on inferred schema
#DatabaseConn_analytics.overwrite_table(parameters["dstDirectoryName"],newUpdRecs.limit(1))
if parameters["prcType"] in ['SQL Server', 'API']:#, 'Excel']:
  create_table = createTable(df_schema, parameters["dstTableName"], parameters["prcType"])
  spark.createDataFrame([create_table],StringType()).registerTempTable("create_table")
else:
  DatabaseConn_analytics.overwrite_table(parameters["dstTableName"],newUpdRecs.limit(1))

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC val df = spark.table("newUpdRecs")
# MAGIC val arr = dbutils.widgets.get("dstTableName").split('.').map(_.trim)
# MAGIC var TargetSQLTableName = arr(1)
# MAGIC var schemaName = arr(0)
# MAGIC var kvSecret = dbutils.widgets.get("dstKvSecret")
# MAGIC var schema_query = "select count(*) Counts from sys.schemas s where s.name = '" + schemaName + "'"
# MAGIC var schema_count = readQueryFromDB(kvSecret,schema_query).as[Integer].collect()(0)
# MAGIC var create_schema = "create schema " + schemaName
# MAGIC var table_query = "select count(*) Counts from sys.tables t join sys.schemas s on t.schema_id = s.schema_id where s.name = '" + schemaName + "' and t.name = '" + TargetSQLTableName + "'"
# MAGIC var table_count = readQueryFromDB(kvSecret, table_query).as[Integer].collect()(0)
# MAGIC val prc_type = dbutils.widgets.get("prcType")
# MAGIC //print ("Schema: " + schemaName)
# MAGIC 
# MAGIC // try{
# MAGIC //     bulkInsertIterativeLoad(kvSecret, df, schemaName, TargetSQLTableName)
# MAGIC // }
# MAGIC // catch{
# MAGIC //   case e: Throwable => println("Error: " + e)
# MAGIC //   dbutils.notebook.exit("message")
# MAGIC // }
# MAGIC //executeSQL(kvSecret, "drop table CAMMS.Incident")
# MAGIC if ((prc_type == "SQL Server")||(prc_type == "API")){
# MAGIC   var create_table = table("create_table").as[String].collect()(0)
# MAGIC   
# MAGIC   //Create destination table
# MAGIC   if(table_count == 0){
# MAGIC     executeSQL(kvSecret, create_table)
# MAGIC   }
# MAGIC }
# MAGIC //Create schema if none exists
# MAGIC if(schema_count == 0){
# MAGIC executeSQL(kvSecret, create_schema)
# MAGIC }
# MAGIC //bulkInsertIterativeLoad(kvSecret, df, schemaName, TargetSQLTableName)
# MAGIC bulkInsertFullLoad(kvSecret, df, schemaName, TargetSQLTableName)

# COMMAND ----------

# DBTITLE 1,Build Source Data Profile table and write to ControlDB
source_size = spark.createDataFrame(dbutils.fs.ls(srcBlobPath + parameters["srcFormat"])).select("size").groupBy().sum().collect()[0][0] * 0.000001
pdDf_prof = dataCatalogProfile(df_source,df_source.columns, parameters["srcContainerName"] + '.' + parameters["srcBlobName"], parameters["srcId"])
  
# Convert to Pyspark DF
df_prof = spark.createDataFrame(pdDf_prof)

ctlKvSecret = 'ADF-LS-AzureSql-ControlDB'

# Write source profile to Control DB
DatabaseConn_analytics = DbHelper(ctlKvSecret)
DatabaseConn_analytics.append_table("dbo.DataProfile",df_prof)

# COMMAND ----------

# DBTITLE 1,Catalog Source 
accessToken = getAccessToken(parameters["catalogSecret"])
raw_dc = bodyJson('source',parameters["catalogSecret"], parameters["srcContainerName"] + '/' + parameters["srcBlobName"], timestamp, True, table_preview = True, table_profile = True, column_profile = True, table_schema = True)
id = registerDataAsset(raw_dc, parameters["catalogSecret"])
print("REG:" + id)

# COMMAND ----------

# DBTITLE 1,Build EDW Data Profile table and write to ControlDB
df_prof = get_data_profile(parameters["dstKvSecret"], parameters["dstId"])
ctlKvSecret = 'ADF-LS-AzureSql-ControlDB'

DatabaseConn_analytics = DbHelper(ctlKvSecret)
DatabaseConn_analytics.append_table("dbo.DataProfile",df_prof)

# COMMAND ----------

# DBTITLE 1,Catalog EDW
accessToken = getAccessToken(parameters["catalogSecret"])
raw_dc = bodyJson('source',parameters["catalogSecret"], parameters["dstTableName"], timestamp, True, table_preview = True, table_profile = True, column_profile = True, table_schema = True)
id = registerDataAsset(raw_dc, parameters["catalogSecret"])
print("REG:" + id)

# COMMAND ----------

# #Search
# src_searchJson = searchDataAsset(parameters["dstTableName"])
# print("SER:" + src_searchJson)
