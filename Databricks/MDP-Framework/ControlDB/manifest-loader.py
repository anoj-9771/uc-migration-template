# Databricks notebook source
# MAGIC %run ../Common/common-workspace

# COMMAND ----------

def RunAllConfigs():
    df = ListWorkspaces(CurrentNotebookPath() + "/ExtractLoadManifest")
    df = df.selectExpr(f"explode({df.columns[0]}) o").where("o.object_type != 'DIRECTORY'")
    
    for i in df.collect():
        path = i.o.path
        try:
            print(f"Running: \"{path}\"")
            r = dbutils.notebook.run(path, 300, {})
        except Exception as e: 
            print(f"Notebook path \"{path}\" failed! - {str(e)}")

RunAllConfigs()

# COMMAND ----------


