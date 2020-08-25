# Databricks notebook source
dbutils.widgets.text('srcKvSecret','','')
dbutils.widgets.text('query','','')



parameters = dict(
    srcKvSecret = dbutils.widgets.get('srcKvSecret')
    ,query = dbutils.widgets.get('query')
)
  

# COMMAND ----------

# MAGIC %run "/build/Util/BlobHelper-ut.py"

# COMMAND ----------

# MAGIC %run "/build/Util/DbHelper_jdbc-ut.py"

# COMMAND ----------

# MAGIC %run "/build/Util/ParallelNotebookHelper-ut.py"

# COMMAND ----------

# dstAccountName = BlobStoreAccount(parameters["dstKvSecret"])

# dstBlobPath = GetBlobStoreFiles(parameters["dstKvSecret"],dstAccountName,\
#                                 parameters["dstContainerName"],parameters["dstDirectoryName"],parameters["dstBlobName"], "wasbs")

# dstPath = GetBlobStoreFiles(parameters["dstKvSecret"],dstAccountName,\
#                                 parameters["dstContainerName"],parameters["dstDirectoryName"],'', "wasbs")

# COMMAND ----------

auth_payload = dbutils.secrets.get(scope='vwazr-dp-keyvault',key=parameters["srcKvSecret"])

# COMMAND ----------

import json
params = json.dumps(parameters)
notebooks = [
 NotebookData("/build/DataProcessing/ADFPipelineRun-dp.py",300,{"parameters" : params}),
 NotebookData("/build/DataProcessing/ADFActivityRun-dp.py",300,{"parameters" : params})
]
res = parallelNotebooks(notebooks, 2)
result = [f.result(timeout=3600) for f in res] # This is a blocking call.
print(result)

