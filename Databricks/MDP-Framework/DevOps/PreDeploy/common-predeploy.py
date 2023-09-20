# Databricks notebook source
# MAGIC %run ../../Common/common-workspace

# COMMAND ----------

def GetEnvironmentTag():
    j = json.loads(spark.conf.get("spark.databricks.clusterUsageTags.clusterAllTags"))
    return [x['value'] for x in j if x['key'] == 'Environment'][0]

# COMMAND ----------

def Run():
    env = GetEnvironmentTag().lower()
    df = JsonToDataFrame(ListWorkspaces(CurrentNotebookPath() + f"/predeploy-{env}"))
    df = df.selectExpr(f"explode({df.columns[0]}) o").where("o.object_type != 'DIRECTORY'")
    
    for i in df.collect():
        path = i.o.path
        try:
            r = dbutils.notebook.run(path, 0, {})
        except:
            print(f"Notebook path \"{path}\" failed!")

Run()
