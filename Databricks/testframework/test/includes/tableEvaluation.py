# Databricks notebook source
tableName = f'{source.lower()}_{table}'
print(tableName)
spark.conf.set('vars.table',tableName)
spark.conf.set('vars.environment',environment)

# COMMAND ----------

# DBTITLE 0,Writing Count Result in Database
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS ${vars.environment}

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists ${vars.environment}.${vars.table}

# COMMAND ----------

# DBTITLE 1,[Config] Connection Setup
from datetime import datetime

global fileCount

fileLocation = f"wasbs://{containerName}@{storage_account_name}.blob.core.windows.net/"
fileType = 'json'
# print(storage_account_name)
spark.conf.set("fs.azure.account.key."+storage_account_name+".blob.core.windows.net",storage_account_access_key) 

# COMMAND ----------

def listDetails(inFile):
    global fileCount
    dfs[fileCount] = spark.read.format(fileType).option("inferSchema", "true").load(inFile.path)
    print(f'Results for {inFile.name.strip("/")}')
    dfs[fileCount].printSchema()
    tmpTable = f'{inFile.name.split(".")[0]}_file{str(fileCount)}'
    dfs[fileCount].createOrReplaceTempView(tmpTable)
    display(spark.sql(f'select * from {tmpTable}'))
    testdf = spark.sql(f'select * from {tmpTable}')
    testdf.write.format(fileType).mode("append").saveAsTable("test" + "." + source + "_" + table)
   

# COMMAND ----------

# DBTITLE 1,List folders in fileLocation
folders = dbutils.fs.ls(fileLocation)
fileNames = []
fileCount = 0
dfs = {}

assert source in ['CRM','ISU'], 'source variable has unexpected value'

for folder in folders:
    if folder.path not in [f'{fileLocation}{source.lower()}data/',f'{fileLocation}{source.lower()}ref/']:
        print(f'{fileLocation}{source.lower()}data/')
        continue
        
    try:
#         print('Level 1')
        dateFolders = dbutils.fs.ls(folder.path)
        prntDate = False
        for dateFolder in dateFolders:
#             print('Level 2')
#             print(dateFolder.path)
            tsFolders = dbutils.fs.ls(dateFolder.path)
            prntTime = False
            
            for tsFolder in tsFolders:
#                 print('Level 3')
#                 print(tsFolder.path)
                files = dbutils.fs.ls(tsFolder.path)
                prntTime = False
                for myFile in files:
#                    if myFile.name[:3] != '0BP':
#                       continue
#                     print('Level 4')
#                     print(myFile)
#                     print(myFile.name[len(table):len(table) + 3])
                    if myFile.name[:len(table)] == table and myFile.name[len(table):len(table) + 3] == '_20':
                        fileCount += 1
                        if not prntDate:
                            print(f'{datetime.strftime(datetime.strptime(myFile.path.split("/")[4],"%Y%m%d"),"%Y-%m-%d")}')
                            printDate = True

                        if not prntTime:
                            print(f'\t{tsFolder.name.split("_")[-1].strip("/")}')
                            prntTime = True

                        print(f'\t\t{myFile.name.strip("/")}\t{myFile.size}')

                        if myFile.size > 2:
                            fileNames.append(myFile)
    except:
        print(f'Invalid folder name: {folder.name.strip("/")}')

for myFile in fileNames: 
    print(f'Processing {myFile.name}')
    listDetails(myFile)

# COMMAND ----------

print('Data from Test environment')
sourcedf = spark.sql(f"select * from {environment}.{tableName}")
display(sourcedf)

# COMMAND ----------

print('Data from Cleansed environment')
targetdf = spark.sql(f"select * from cleansed.{tableName}")
display(targetdf)

# COMMAND ----------

# DBTITLE 1,[Source] Schema check
print('Schema in Test environment')
sourcedf.printSchema()

# COMMAND ----------

# DBTITLE 1,[Target] Schema Check
print('Schema in Cleansed environment')
targetdf.printSchema()
print('/nDistinct extract timestamps report')

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct EXTRACT_DATETIME from ${vars.environment}.${vars.table} order by EXTRACT_DATETIME desc
