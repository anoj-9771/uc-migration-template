# Databricks notebook source
# MAGIC %run ../Common/common-workspace

# COMMAND ----------

def RunAllConfigs():
    df = ListWorkspaces(CurrentNotebookPath() + "/ExtractLoadManifest")
    df = df.selectExpr(f"explode({df.columns[0]}) o").where("o.object_type != 'DIRECTORY'")
    
    for i in df.rdd.collect():
        path = i.o.path
        try:
            r = dbutils.notebook.run(path, 0, {})
        except:
            print(f"Notebook path \"{path}\" failed!")

RunAllConfigs()

# COMMAND ----------


