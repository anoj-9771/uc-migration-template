# Databricks notebook source
# MAGIC %run ./global-variables-python

# COMMAND ----------

# MAGIC %run ./util-log

# COMMAND ----------

def DataLakeUnmountPoint(datalakezone):
  #Define a Mount Point.
  MOUNT_POINT = "/mnt/datalake-" + datalakezone

  LogEtl("Unmounting " + MOUNT_POINT)
  dbutils.fs.unmount(MOUNT_POINT)

# COMMAND ----------

def DataLakeGetMountPoint(datalakezone):
  LogEtl ("Starting : DataLakeGetMountPoint for zone : " + datalakezone)

  #Define a Mount Point.
  MOUNT_POINT = "/mnt/datalake-" + datalakezone
  
  #Create the path for the Data Lake along with the Container
  DATA_LAKE_SOURCE = "abfss://{container}@{datalakeaccount}.dfs.core.windows.net/".format(container = datalakezone, datalakeaccount = ADS_DATA_LAKE_ACCOUNT)
  
  #Get Secrets from the Key Vault using the databricks defined scope
  CLIENT_ID = dbutils.secrets.get(scope = ADS_KV_ACCOUNT_SCOPE, key = ADS_SECRET_APP_ID)
  CLIENT_KEY = dbutils.secrets.get(scope = ADS_KV_ACCOUNT_SCOPE, key = ADS_SECRET_APP_SECRET)
  DIRECTORY_ID = dbutils.secrets.get(scope = ADS_KV_ACCOUNT_SCOPE, key = ADS_TENANT_ID)

  #Standard Configurations for connecting to Data Lake
  configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": CLIENT_ID,
           "fs.azure.account.oauth2.client.secret": CLIENT_KEY,
           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/{0}/oauth2/token".format(DIRECTORY_ID)}

  #Mount the data lake container
  #Log(configs)
  LogEtl("Path : {} | MountPoint : {} | Data Lake : {}".format(DATA_LAKE_SOURCE, MOUNT_POINT, ADS_DATA_LAKE_ACCOUNT))
  
  try:
    #LogEtl(dbutils.fs.ls(MOUNT_POINT))
    fileList = dbutils.fs.ls(MOUNT_POINT)
    return MOUNT_POINT
  except Exception as e:
    LogEtl("Directory not already mounted.")

  
  
  LogEtl("Starting to mount...")
  try:
    dbutils.fs.mount(
      source = DATA_LAKE_SOURCE,
      mount_point = MOUNT_POINT,
      extra_configs = configs)
    LogEtl("{0} Directory Mounted".format(MOUNT_POINT))
    LogEtl(dbutils.fs.ls(MOUNT_POINT))
    return MOUNT_POINT
  except Exception as e:
    if "Directory already mounted" in str(e):
      #Ignore error if the container is already mounted
      LogEtl("{0} already mounted".format(MOUNT_POINT))
      return MOUNT_POINT
    else:
      #Else raise a error. Something's gone wrong
      raise e
    

# COMMAND ----------

def BlobGetMountPoint(blobcontainer):
    LogEtl ("Starting : BlobGetMountPoint for zone : " + blobcontainer)
    #Define a Mount Point.  
    MOUNT_POINT = "/mnt/blob-" + blobcontainer
    storageAccount = ADS_BLOB_STORAGE_ACCOUNT
    container = blobcontainer
  
    #Create the path for the Data Lake along with the Container
    BLOB_SOURCE = f"wasbs://{container}@{storageAccount}.blob.core.windows.net/"
    EXTRA_CONFIG = {f"fs.azure.sas.{container}.{storageAccount}.blob.core.windows.net":dbutils.secrets.get(scope = ADS_KV_ACCOUNT_SCOPE, key = "daf-sa-blob-sastoken")}
    
    #Mount the blob container
    #Log(configs)
    LogEtl("Path : {} | MountPoint : {} | Data Lake : {}".format(BLOB_SOURCE, MOUNT_POINT, ADS_BLOB_STORAGE_ACCOUNT))
  
    try:
        #LogEtl(dbutils.fs.ls(MOUNT_POINT))
        fileList = dbutils.fs.ls(MOUNT_POINT)
        return MOUNT_POINT
    except Exception as e:
        LogEtl("Directory not already mounted.")  
  
    LogEtl("Starting to mount...")
    try:
        dbutils.fs.mount(
        source = BLOB_SOURCE ,
        mount_point = MOUNT_POINT,
        extra_configs = EXTRA_CONFIG)
        LogEtl("{0} Directory Mounted".format(MOUNT_POINT))
        LogEtl(dbutils.fs.ls(MOUNT_POINT))
        return MOUNT_POINT
    except Exception as e:
        if "Directory already mounted" in str(e):
            #Ignore error if the container is already mounted
            LogEtl("{0} already mounted".format(MOUNT_POINT))
            return MOUNT_POINT
    else:
        #Else raise a error. Something's gone wrong
        raise e
