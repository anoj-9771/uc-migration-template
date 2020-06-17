# Databricks notebook source
def BlobStoreSASToken(kvSecret):
    return dbutils.secrets.get(scope='azr-dp-keyvault',key=kvSecret)

def BlobStorePath(account,container, connectionType):
    if connectionType == 'wasbs':
        return "wasbs://{0}@{1}.blob.core.windows.net".format(container, account)  
    elif connectionType == 'abfss':
        return "abfss://{0}@{1}.dfs.core.windows.net".format(container, account)  
        
def GetBlobStoreFiles(kvSecret,account,container,directory, file, connectionType):
    #key = BlobStoreSASToken(kvSecret)
    if connectionType == 'azure storage':
        spark.conf.set("fs.azure.sas.{0}.{1}.blob.core.windows.net".format(container, account),BlobStoreSASToken(kvSecret))
        return "%s/%s/%s"%(BlobStorePath(account,container, 'wasbs'),directory,file)   
    elif connectionType == 'azure data lake store':
        spark.conf.set("fs.azure.account.key.{0}.dfs.core.windows.net".format(account),BlobStoreSASToken(kvSecret))
        return "%s/%s/%s"%(BlobStorePath(account,container, 'abfss'),directory,file)   
    elif connectionType == 'mnt':
        fullFilePath = "/mnt/%s/%s/"%(container,directory)
        spark.conf.set("fs.azure.sas.{0}.{1}.blob.core.windows.net".format(container, account, directory),BlobStoreSASToken(kvSecret))
        dbutils.fs.mount(source = "%s/%s"%(BlobStorePath(account,container, 'mnt'), directory),\
               mount_point  = fullFilePath, \
               extra_configs = {"fs.azure.sas.{0}.{1}.blob.core.windows.net".format(container, account): \
                                BlobStoreSASToken(kvSecret)})
        return fullFilePath

def MakeDirectory(kvSecret,account,container,blobPath):
    spark.conf.set("fs.azure.sas.{0}.{1}.blob.core.windows.net".format(container, account),BlobStoreSASToken(kvSecret))
    try: 
        dbutils.fs.mkdirs(blobPath)
        return True
    except: False
