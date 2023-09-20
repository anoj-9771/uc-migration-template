# Databricks notebook source
def MountExists(containerName):
    return any(containerName in mount.mountPoint for mount in dbutils.fs.mounts())

# COMMAND ----------

#TODO: BELOW IS DEPRECATED, DO THIS INSTEAD => https://learn.microsoft.com/en-us/azure/databricks/security/credential-passthrough/adls-passthrough#python-3
def MountContainer(containerName, storageSecretName="daf-lake-fqn"):
    if MountExists(containerName):
        return
    SECRET_SCOPE = "ADS"
    APP_ID = dbutils.secrets.get(scope = SECRET_SCOPE, key = "daf-serviceprincipal-app-id")
    SECRET = dbutils.secrets.get(scope = SECRET_SCOPE, key = "daf-serviceprincipal-app-secret")
    DIRECTORY_ID = dbutils.secrets.get(scope = SECRET_SCOPE, key = "daf-tenant-id")
    configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": APP_ID,
           "fs.azure.account.oauth2.client.secret": SECRET,
           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/{0}/oauth2/token".format(DIRECTORY_ID)}
    dlFqn = dbutils.secrets.get(scope = SECRET_SCOPE, key = storageSecretName)

    DATA_LAKE_SOURCE = f"abfss://{containerName}@{dlFqn}/"
    MOUNT_POINT = f"/mnt/datalake-{containerName}"
    dbutils.fs.mount(
        source = DATA_LAKE_SOURCE,
        mount_point = MOUNT_POINT,
        extra_configs = configs)

# COMMAND ----------

def MountBlobContainer(containerName, storageSecretName="daf-blob-fqn"):
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

# COMMAND ----------

def AddSparkStorageCredentails():
    SECRET_SCOPE = "ADS"
    APP_ID = dbutils.secrets.get(scope = SECRET_SCOPE, key = "daf-serviceprincipal-app-id")
    SECRET = dbutils.secrets.get(scope = SECRET_SCOPE, key = "daf-serviceprincipal-app-secret")
    DIRECTORY_ID = dbutils.secrets.get(scope = SECRET_SCOPE, key = "daf-tenant-id")
    dlFqn = dbutils.secrets.get(scope = SECRET_SCOPE, key = storageSecretName)

    spark.conf.set(f"fs.azure.account.auth.type.{dlFqn}", "OAuth")
    spark.conf.set(f"fs.azure.account.oauth.provider.type.{dlFqn}", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
    spark.conf.set(f"fs.azure.account.oauth2.client.id.{dlFqn}", APP_ID)
    spark.conf.set(f"fs.azure.account.oauth2.client.secret.{dlFqn}", SECRET)
    spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{dlFqn}", f"https://login.microsoftonline.com/{DIRECTORY_ID}/oauth2/token")

# COMMAND ----------


