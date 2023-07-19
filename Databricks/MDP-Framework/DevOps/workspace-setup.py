# Databricks notebook source
# MAGIC %run ../Common/common-workspace

# COMMAND ----------

# MAGIC %run ../Common/common-unity-catalog-helper

# COMMAND ----------

def AssignWorkspaceCatalogs():
    for i in GetCommonCatalogs():
        catalog = f"{GetPrefix()}{i}"
        print(catalog)
        print(UpdateCatalogWorkspaceBindings(catalog, [ GetWorkspaceId() ]))
