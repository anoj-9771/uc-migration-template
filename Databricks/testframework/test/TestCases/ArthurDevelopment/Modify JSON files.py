# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType
import pandas as pd

environment = 'test'
storage_account_name = "sablobdaftest01"
storage_account_access_key = dbutils.secrets.get(scope="TestScope",key="test-sablob-key")
containerName = "isudata"
fileName = '0UCCONTRACT_ATTR_2_20211026182923.json'
#fileName = '0UC_INSTFACTS_20211027190332.json'
firstField = 'EXTRACT_RUN_ID'
#firstField = 'DI_SEQUENCE_NUMBER'

# COMMAND ----------

def MountDataLakeContainer(datalakeAccountName, datalakeAccountKey, containerName, mountPointName):
    dataLakeAccountDNS = datalakeAccountName + ".blob.core.windows.net"
    
    dataLakeAuthTypeConfig = "fs.azure.account.auth.type." + dataLakeAccountDNS
    dataLakeKeyConfig = "fs.azure.account.key." + dataLakeAccountDNS
    
    dataLakeExtraConfig = {dataLakeAuthTypeConfig:"SharedKey"
                           , dataLakeKeyConfig:datalakeAccountKey}
    
    containerUri = "wasbs://" + containerName + "@" + dataLakeAccountDNS
    mountPointFolder = "/mnt/" + mountPointName

    try:
        dbutils.fs.mount(source = containerUri, mount_point = mountPointFolder, extra_configs = dataLakeExtraConfig)
        print(f'Mount point {mountPointFolder} linked to {containerName}.')
    except Exception as e:
        #if already mounted check if it is to the same mount point as requested
        if "Directory already mounted" in str(e):
            mountPoints = dbutils.fs.mounts()
            for mountPoint in mountPoints:
                if mountPoint[0] == mountPointFolder:
                    parts = mountPoint[1].split('@')[0].split('//')

                    if parts[1] == containerName:
                        print(f'Mount point {mountPointFolder} linked to {containerName}')
                    else:
                        print(f'WARNING: Mount point {mountPointFolder} is already in use and is currently linked to: {parts[1]}')
                        print('\tChoose a different mount name or use dbutils.fs.unmount to unmount the mount point first')
            pass # Ignore error if already mounted.
        elif 'No such file or directory' in str(e):
            print(f'ERROR: You must create a storage container with name {containerName} first!')
            raise
        else:
            raise e
    return containerUri

# COMMAND ----------

MountDataLakeContainer(storage_account_name, storage_account_access_key, containerName, 'landing/'+containerName)

# COMMAND ----------

sorted(dbutils.fs.mounts())

# COMMAND ----------

dbutils.fs.cp('wasbs://isudata@sablobdaftest01.blob.core.windows.net/0UCCONTRACT_ATTR_2_20211026182923.json','/tmp/arthur_in.json')

# COMMAND ----------

# MAGIC %sh
# MAGIC sed 's/},{/\},\
# MAGIC {/g' < /dbfs/tmp/arthur_in.json > /dbfs/tmp/arthur_out.json

# COMMAND ----------

# MAGIC %sh
# MAGIC head /dbfs/tmp/arthur_out.json

# COMMAND ----------

df = spark.read.option("multiline", "true").json("/tmp/arthur_out.json")
# display(df)


# COMMAND ----------

df.count()

# COMMAND ----------

try:
    dbutils.fs.rm('/mnt/landing/isudata/0UCCONTRACT_ATTR_2_20211026182923.json')
except FileNotFoundError:
    print('not found')
    pass

#df.write.json('wasbs://isudata@sablobdaftest01.blob.core.windows.net/0UCCONTRACT_ATTR_2_20211026182923.json')
df.write.format('json').save('/mnt/landing/isudata/0UCCONTRACT_ATTR_2_20211026182923.json') #coalesce(1).

# COMMAND ----------

for thefile in dbutils.fs.ls('/mnt/landing/isudata/0UCCONTRACT_ATTR_2_20211026182923.json'):
    print(thefile)
    if thefile.name.endswith('.json'):
        print('found json')
        dbutils.fs.cp(thefile.path,'/mnt/landing/isudata/0UCCONTRACT_ATTR_2_copy.json')
    
    #dbutils.fs.rm(thefile.path)
    
# dbutils.fs.cp(myFile + '/*.json',myFile)

#dbutils.fs.rm(fileprefix+".tmp",recurse=true)

# COMMAND ----------

dbutils.fs.cp('/tmp/arthur_in.json','/mnt/landing/isudata/0UCCONTRACT_ATTR_2_20211026182923.json')

# COMMAND ----------

sorted(dbutils.fs.ls('/tmp'))

# COMMAND ----------

df2 = spark.read.json('/mnt/landing/isudata/0UCCONTRACT_ATTR_2_copy.json')
display(df2)
print(df2.count())

# COMMAND ----------

dbutils.fs.rm('/mnt/landing/isudata/0UCCONTRACT_ATTR_2_20211026182923.json',recurse=True)

# COMMAND ----------

dbutils.fs.mv('/mnt/landing/isudata/0UCCONTRACT_ATTR_2_copy.json','/mnt/landing/isudata/0UCCONTRACT_ATTR_2_20211026182923.json')

# COMMAND ----------


