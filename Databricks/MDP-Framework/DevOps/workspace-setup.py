# Databricks notebook source
# MAGIC %run ../Common/common-workspace

# COMMAND ----------

# MAGIC %run ../Common/common-mount

# COMMAND ----------

# MAGIC %run ../Common/common-unity-catalog-helper

# COMMAND ----------

def AssignWorkspaceCatalogs():
    for i in GetCommonCatalogs():
        catalog = f"{GetPrefix()}{i}"
        print(catalog)
        print(UpdateCatalogWorkspaceBindings(catalog, [ GetWorkspaceId() ]))

# COMMAND ----------

# MAGIC %run ./Pools/SCCTV_POOL

# COMMAND ----------

# MAGIC %run ./Pools/RIVERWATCH_POOL

# COMMAND ----------

# MAGIC %run ./Compute/devops

# COMMAND ----------

# MAGIC %run ./Compute/sewer-cctv

# COMMAND ----------

# MAGIC %run ./Compute/riverwatch

# COMMAND ----------

# MAGIC %run ./Workflows/sewer-cctv-pipeline

# COMMAND ----------

# MAGIC %run ./Workflows/riverwatch-pipeline

# COMMAND ----------

MountBlobContainer(containerName="urbanplunge")
MountBlobContainer(containerName="iotsewertelemetrydata")
MountBlobContainer(containerName="iotswtelemetryalarmdata")
MountContainer(containerName="raw")
