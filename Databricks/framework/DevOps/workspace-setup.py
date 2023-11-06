# Databricks notebook source
# MAGIC %run ../Common/common-workspace

# COMMAND ----------

# MAGIC %run ../Common/common-mount

# COMMAND ----------

# MAGIC %run ../Common/common-unity-catalog-helper

# COMMAND ----------

def RunAllInPath(path):
    fullPath = f"{CurrentNotebookPath()}/{path}"
    list = ListWorkspaces(fullPath).get("objects")
    if list is None:
        print(f"Path not found! [{fullPath}]")
        return
    
    for i in list:
        notebookPath = i["path"]
        try:
            r = dbutils.notebook.run(notebookPath, 0, {})
        except:
            print(f"Notebook path \"{notebookPath}\" failed!")

# COMMAND ----------

def AssignWorkspaceCatalogs():
    for i in GetCommonCatalogs():
        catalog = f"{GetPrefix()}{i}"
        print(catalog)
        print(UpdateCatalogWorkspaceBindings(catalog, [ GetWorkspaceId() ]))
AssignWorkspaceCatalogs()

# COMMAND ----------

RunAllInPath("Pools")

# COMMAND ----------

RunAllInPath("Compute")

# COMMAND ----------

RunAllInPath("Workflows")

# COMMAND ----------

for blob in [ 
                "sewercctvimages"
                ,"sewercctvmodel"
                ,"sewercctvvideos"
                ,"urbanplunge"
                ,"kaltura"
                ,"iotsewertelemetrydata"
                ,"iotswtelemetryalarmdata"
            ]:
    try:
        MountBlobContainer(blob)
    except Exception as e:
        print(e.message)
        break

# COMMAND ----------

for dls in [ "raw", "landing" ]:
    try:
        MountContainer(dls)
    except Exception as e:
        print(e.message)
        break

# COMMAND ----------


