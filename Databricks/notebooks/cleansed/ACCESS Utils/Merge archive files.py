# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType
import glob
import os, sys

environment = 'test'
storage_account_name = "sablobdafdev01"
#storage_account_name = dbutils.secrets.get(scope="TestScope",key="daf-sa-blob-connectionstring")
#storage_account_name = dbutils.secrets.get(scope="DevScope",key="daf-sa-blob-key1")
storage_account_access_key = 'cRh/J3cT9DfAFGScnmUqAq1fOIbdydLr0HR/dBU3NYTUe3rKBr1zru7jsOWP7gM5r6WFTH07Y/lyzZNys25Vfw==' #dbutils.secrets.get(scope="TestScope",key="sadaftest01-raw-sastoken")
#storage_account_access_key = dbutils.secrets.get(scope="DevScope",key="daf-sa-blob-sastoken")
containerData = "accessdata"
containerStage = "accessstage"

fileMask = 'Z309_TMETEREADING_'
outFile = fileMask + 'HISTORY'

# COMMAND ----------

fileLocationData = f"wasbs://{containerData}@{storage_account_name}.blob.core.windows.net/"
fileLocationStage = f"wasbs://{containerStage}@{storage_account_name}.blob.core.windows.net/"
# print(storage_account_name)
spark.conf.set("fs.azure.account.key."+storage_account_name+".blob.core.windows.net",storage_account_access_key) 

# COMMAND ----------

# MAGIC %run ../../includes/include-all-util

# COMMAND ----------

DATA_LAKE_MOUNT_POINT = BlobGetMountPoint('accessdata')
print(DATA_LAKE_MOUNT_POINT)
DATA_LAKE_MOUNT_POINT = BlobGetMountPoint('accessstage')
print(DATA_LAKE_MOUNT_POINT)

# COMMAND ----------

# dbfsLocation = '/dbfs/mnt/blob-accessdata/'
# listOfFiles = glob.glob(f'{dbfsLocation}/{fileMask}*.csv')
# for file in listOfFiles:
#     dbutils.fs.cp(f"/{'/'.join(file.split('/')[2:])}",f"/mnt/blob-accessstage/{file.split('/')[-1]}")
    
# for file in listOfFiles:
#     dbutils.fs.rm(f"/{'/'.join(file.split('/')[2:])}")

# COMMAND ----------

sorted(dbutils.fs.mounts())

# COMMAND ----------

# DBTITLE 1,Read pattern matched files into a dataframe, verify schemas are all equal
files = dbutils.fs.ls(f'{fileLocationStage}')
saveSchema = ''
fileCount = 0
rowCount = 0
runningTotal = 0
#make sure all files are the same format
for file in files:
    if not file.name.startswith(fileMask):
        continue
        
    print(file)
    dfArch = spark.read.csv(file.path,sep='|',header=True)
    rowCount = dfArch.count()
    runningTotal += rowCount
    print(f'Record Count: {rowCount:,}, running total: {runningTotal:,}')
    
    if saveSchema == '':
        dfEverything = dfArch
    elif dfArch.schema != saveSchema:
        print(f'Schema mismatch at {file.name}')
        print(saveSchema)
        print(dfArch.schema)
    else:
        dfEverything = dfEverything.union(dfArch)
        
    saveSchema = dfArch.schema
    
print(f'Total records read into dataframe: {dfEverything.count():,}')
dfEverything.write.option('header',True).option('delimiter','|').mode('overwrite').csv(f'/mnt/blob-accessdata/{outFile}')

# COMMAND ----------

# DBTITLE 1,Merge all the fragmented files into one
#This code works but is slow. 22 minutes for meter reading history. Left the code for educational purposes only
# dfEverything.coalesce(1).write.option('header',True).option('delimiter','|').csv(f'{fileLocation}/landing/accessdata/{outFile}',sep='|',header=True)
# #the above wrote a single partition file to a folder with the desired file name. So annoying. The next bit of code will write the proper file name in the desired location
# listOfFiles = glob.glob(f'/dbfs/mnt/datalake-raw/landing/accessdata/{outFile}/*.csv')
# # print(listOfFiles)
# #Copy the csv file to the proper location. Strip the /dbfs bit off the string (required for dbutils functions)
# dbutils.fs.cp(listOfFiles[0][5:],f'/mnt/datalake-raw/landing/accessdata/{outFile}.csv')

#the code below does the same thing but runs in half the time.... :)
dbfsLocation = '/dbfs/mnt/blob-accessstage'
listOfFiles = glob.glob(f'{dbfsLocation}/{fileMask}*.csv')

firstFile = True
recCount = 0
fileCount = 0

with open(f'/dbfs/mnt/blob-accessdata/{outFile}.csv','w', newline='\n') as outfile:
    for file in listOfFiles:
        fileCount += 1
        
        with open(file,'r', newline='\n') as infile:
            record = infile.readline()
            
            if firstFile:
                firstFile = False
            else:
                #skip header for subsequent file
                record = infile.readline()
                
            while len(record) > 0:
                recCount += 1
                outfile.write(record)
                record = infile.readline()
                

print(f'File Count: {fileCount:,}')            
print(f'Record count: {recCount-1:,}')            #remove header record from count
#remove the folder with the partition file, recurse=True because all folder contents need to be deleted
#dbutils.fs.rm(f'/mnt/datalake-raw/landing/accessdata/{outFile}', recurse=True)

# COMMAND ----------

# DBTITLE 1,Read file for visual verification
dfArch = spark.read.csv(f'/mnt/blob-accessdata/{outFile}.csv',sep='|',header=True)
print(f'Total records read from merged file: {dfArch.count():,}')
display(dfArch)

# COMMAND ----------

dbutils.fs.rm(f'/mnt/blob-accessdata/{outFile}',recurse=True)

# COMMAND ----------


