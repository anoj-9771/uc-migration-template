# Databricks notebook source
# Extract Account key from connection string or SAS token from Key Vault secret
def BlobStoreToken(kvSecret):
    if '-connectionstring' in kvSecret.lower(): 
        secret = dbutils.secrets.get(scope='KeyVault',key=kvSecret)
        con_str = {}
        cons = secret.split(';')
        cons = cons
        for con in cons:
            value = con.replace('==','<eqalequal>').split('=')
            try:
              con_str[value[0]] = value[1]
            except : False
        return(con_str['AccountKey'].replace('<eqalequal>','=='))
    elif '-sastoken' in kvSecret.lower():
        return dbutils.secrets.get(scope='KeyVault',key=kvSecret)
    else: print("Please check secret name")
    
    
def BlobStorePath(account,container, connectionType):
    if connectionType == 'wasbs':
        return "wasbs://{0}@{1}.blob.core.windows.net".format(container, account)  
    elif connectionType == 'abfss':
        return "abfss://{0}@{1}.dfs.core.windows.net".format(container, account)  
        
def GetBlobStoreFiles(kvSecret,account,container,directory, file, connectionType):
    if connectionType == 'azure storage':
        spark.conf.set("fs.azure.sas.{0}.{1}.blob.core.windows.net".format(container, account),BlobStoreToken(kvSecret))
        return "%s/%s/%s"%(BlobStorePath(account,container, 'wasbs'),directory,file)   
    elif connectionType == 'azure data lake store':
        spark.conf.set("fs.azure.account.key.{0}.dfs.core.windows.net".format(account),BlobStoreToken(kvSecret))
        return "%s/%s/%s"%(BlobStorePath(account,container, 'abfss'),directory,file)   
    elif connectionType == 'dbfs':
        fullFilePath = "/mnt/%s/%s/"%(container,directory)
        spark.conf.set("fs.azure.sas.{0}.{1}.blob.core.windows.net".format(container, account, directory),BlobStoreToken(kvSecret))
        dbutils.fs.mount(source = "%s/%s"%(BlobStorePath(account,container, 'wasbs'), directory),\
               mount_point  = fullFilePath, \
               extra_configs = {"fs.azure.sas.{0}.{1}.blob.core.windows.net".format(container, account): \
                                BlobStoreToken(kvSecret)})
        return fullFilePath

def MakeDirectory(kvSecret,account,container,blobPath):
    spark.conf.set("fs.azure.sas.{0}.{1}.blob.core.windows.net".format(container, account),BlobStoreToken(kvSecret))
    try: 
        dbutils.fs.mkdirs(blobPath)
        return True
    except: False
      
def RemoveDirectory(kvSecret,account,container,blobPath):
    spark.conf.set("fs.azure.sas.{0}.{1}.blob.core.windows.net".format(container, account),BlobStoreToken(kvSecret))
    try: 
        dbutils.fs.rmdirs(blobPath,True)
        return True
    except: False
