# Databricks notebook source
# MAGIC %run ./rbac-other-grants

# COMMAND ----------

for i in ListWorkspaces("/".join([*CurrentNotebookPath().split("/"), "GrantList"]))["objects"]:
    path = i["path"]
    dbutils.notebook.run(path, 0, {})

# COMMAND ----------

#AssignAllCatalogOwner("G3-Admins")
