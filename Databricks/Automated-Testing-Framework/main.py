# Databricks notebook source
# MAGIC %run ./common/common-workspace

# COMMAND ----------

# MAGIC %run ./common/common-atf-output

# COMMAND ----------

def RunTests():
    firstRow = spark.sql("SELECT DATE_FORMAT(CURRENT_TIMESTAMP(), 'yyyy/MM/dd') Path, DATE_FORMAT(CURRENT_TIMESTAMP(), 'yyMMddHHmm') BatchId").collect()[0]
    rawPath = firstRow.Path
    batchId = firstRow.BatchId

    for p in ["/tests/cleansed", "/tests/curated", "/tests/curated_v2"]:
        df = ListWorkspaces(CurrentNotebookPath() + p)
        df = df.selectExpr(f"explode({df.columns[0]}) o").where("o.object_type != 'DIRECTORY'")

        for i in df.collect():
            path = i.o.path
            r = dbutils.notebook.run(path, 0, { "DEFAULT_BATCH_ID" : batchId })
            j = json.loads(r)
            notebook = GetNotebookName(path)
            resultsPath = f"/mnt/datalake-raw/atf/{rawPath}/{batchId}/{notebook}.json"
            dbutils.fs.put(resultsPath, r, True)
    PopulateTestResults()
RunTests()

# COMMAND ----------


