# Databricks notebook source
def BlobStoreSASToken(kvSecret):
    return dbutils.secrets.get(scope='cbhazr-inf-keyvault',key=kvSecret)
  
def BlobStoreAccount(kvSecret):
    import re
    val = dbutils.secrets.get(scope='cbhazr-inf-keyvault',key=kvSecret)
    result = re.search('https://(.*).blob.core.windows.net*', val)
    return result.group(1)

def BlobStoreUrl(account,container):
    return "wasbs://{0}@{1}.blob.core.windows.net".format(container, account)


def GetBlobStoreFiles(kvSecret,account,container,directory, file):
    #key = BlobStoreSASToken(kvSecret)
    spark.conf.set("fs.azure.sas.{0}.{1}.blob.core.windows.net".format(container, account),BlobStoreSASToken(kvSecret))
    fullFilePath = BlobStoreUrl(account,container) + '/' + directory + '/' + file
    return fullFilePath

def MakeDirectory(kvSecret,account,container,blobPath):
    spark.conf.set("fs.azure.sas.{0}.{1}.blob.core.windows.net".format(container, account),BlobStoreSASToken(kvSecret))
    try: 
        dbutils.fs.mkdirs(blobPath)
        return True
    except: False
    
