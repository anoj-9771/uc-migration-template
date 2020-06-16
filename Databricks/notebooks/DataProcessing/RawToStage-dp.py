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

# MAGIC %run "/build/Util/BlobHelper-ut.py"

# COMMAND ----------

srcAccountName = BlobStoreAccount(parameters["srcKvSecret"])

srcBlobPath = GetBlobStoreFiles(parameters["srcKvSecret"],srcAccountName,\
                                parameters["srcContainerName"],parameters["srcDirectoryName"],parameters["blobName"])

dstAccountName = BlobStoreAccount(parameters["dstKvSecret"])

dstBlobPath = GetBlobStoreFiles(parameters["dstKvSecret"],dstAccountName,\
                                parameters["dstContainerName"],parameters["dstDirectoryName"],parameters["blobName"])

dstPath = GetBlobStoreFiles(parameters["dstKvSecret"],dstAccountName,\
                                parameters["dstContainerName"],parameters["dstDirectoryName"],'')

# COMMAND ----------

df = spark.read.format('csv').options(sep='|', header='true',quote='"', escape='\'').option("inferSchema", "true").load(srcBlobPath)
MakeDirectory(parameters["dstKvSecret"],parameters["dstAccName"],parameters["dstContainerName"],dstPath)
df.write.mode('overwrite').parquet(dstBlobPath)

