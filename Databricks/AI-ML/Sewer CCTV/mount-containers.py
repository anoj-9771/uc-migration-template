# Databricks notebook source
def MountExists(containerName):
    return any(containerName in mount.mountPoint for mount in dbutils.fs.mounts())

# COMMAND ----------

def MountContainer(containerName, storageSecretName="daf-lake-fqn", createDatabase=True):
    if MountExists(containerName):
        return
    ADS_KV_ACCOUNT_SCOPE  = "ADS"
    dlFqn = dbutils.secrets.get(scope = ADS_KV_ACCOUNT_SCOPE, key = storageSecretName)
    EXTRA_CONFIG = {f"fs.azure.sas.{containerName}.{dlFqn}":dbutils.secrets.get(scope = ADS_KV_ACCOUNT_SCOPE, key = "daf-sa-blob-sastoken")}

    DATA_LAKE_SOURCE = f"wasbs://{containerName}@{dlFqn}/"
    MOUNT_POINT = f"/mnt/blob-{containerName}"
    dbutils.fs.mount(
        source = DATA_LAKE_SOURCE,
        mount_point = MOUNT_POINT,
        extra_configs = EXTRA_CONFIG)
    
    if createDatabase:
        spark.sql(f"CREATE DATABASE {containerName}")

# COMMAND ----------

MountContainer(containerName="kaltura", storageSecretName="daf-blob-fqn", createDatabase=False)

# COMMAND ----------

MountContainer(containerName="sewercctvvideos", storageSecretName="daf-blob-fqn", createDatabase=False)

# COMMAND ----------

MountContainer(containerName="sewercctvmodel", storageSecretName="daf-blob-fqn", createDatabase=False)

# COMMAND ----------

MountContainer(containerName="sewercctvimages", storageSecretName="daf-blob-fqn", createDatabase=False)
