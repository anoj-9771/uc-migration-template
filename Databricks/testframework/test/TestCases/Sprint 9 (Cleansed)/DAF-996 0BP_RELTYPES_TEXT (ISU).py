# Databricks notebook source
table = '0BP_RELTYPES_TEXT'
source = 'ISU' #either CRM or ISU
table1 = table.lower()
print(table1)

# COMMAND ----------

# DBTITLE 0,Writing Count Result in Database
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS test

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists test.isu_0BP_RELTYPES_TEXT

# COMMAND ----------

# DBTITLE 1,[Config] Connection Setup
from datetime import datetime

global fileCount

storage_account_name = "sablobdaftest01"
storage_account_access_key = dbutils.secrets.get(scope="TestScope",key="test-sablob-key")
container_name = "archive"
fileLocation = "wasbs://archive@sablobdaftest01.blob.core.windows.net/"
fileType = 'json'
print(storage_account_name)
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

assert source in ('CRM','ISU'), 'source variable has unexpected value'

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

                        if myFile.size > 0:
                            fileNames.append(myFile)
    except:
        print(f'Invalid folder name: {folder.name.strip("/")}')

for myFile in fileNames: 
   listDetails(myFile)

# COMMAND ----------

sourcedf = spark.sql(f"select * from test." + "isu_" + table)
display(sourcedf)

# COMMAND ----------

targetdf = spark.sql(f"select * from cleansed." + "isu_" + table)
display(targetdf)

# COMMAND ----------

# DBTITLE 1,[Source] Schema check
sourcedf.printSchema()

# COMMAND ----------

# DBTITLE 1,[Target] Schema Check
targetdf.printSchema()

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct EXTRACT_DATETIME from test.isu_0bp_reltypes_text order by EXTRACT_DATETIME desc

# COMMAND ----------

# DBTITLE 1,[Source] with mapping
# MAGIC %sql
# MAGIC select 
# MAGIC relationshipDirection,
# MAGIC relationshipTypeCode,
# MAGIC relationshipType
# MAGIC from (
# MAGIC select
# MAGIC RELDIR as relationshipDirection
# MAGIC ,RELTYP as relationshipTypeCode
# MAGIC ,TXTLG as relationshipType
# MAGIC ,row_number() over (partition by RELDIR, RELTYP order by extract_datetime) rn
# MAGIC from test.isu_0bp_reltypes_text) where rn = 1

# COMMAND ----------

# DBTITLE 1,[Verification] Count
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from cleansed.isu_0bp_reltypes_text
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from (select 
# MAGIC relationshipDirection,
# MAGIC relationshipTypeCode,
# MAGIC relationshipType
# MAGIC from (
# MAGIC select
# MAGIC RELDIR as relationshipDirection
# MAGIC ,RELTYP as relationshipTypeCode
# MAGIC ,TXTLG as relationshipType
# MAGIC ,row_number() over (partition by RELDIR, RELTYP order by extract_datetime) rn
# MAGIC from test.isu_0bp_reltypes_text) where rn = 1)

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT relationshipDirection,relationshipTypeCode, COUNT (*) as count
# MAGIC FROM cleansed.isu_0bp_reltypes_text
# MAGIC GROUP BY relationshipDirection,relationshipTypeCode
# MAGIC HAVING COUNT (*) > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT * FROM (
# MAGIC SELECT
# MAGIC *,
# MAGIC row_number() OVER(PARTITION BY relationshipDirection,relationshipTypeCode order by relationshipDirection,relationshipTypeCode) as rn
# MAGIC FROM  cleansed.isu_0bp_reltypes_text
# MAGIC )a where a.rn > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Source and Target Data
# MAGIC %sql
# MAGIC select 
# MAGIC relationshipDirection,
# MAGIC relationshipTypeCode,
# MAGIC relationshipType
# MAGIC from (
# MAGIC select
# MAGIC RELDIR as relationshipDirection
# MAGIC ,RELTYP as relationshipTypeCode
# MAGIC ,TXTLG as relationshipType
# MAGIC ,row_number() over (partition by RELDIR, RELTYP order by extract_datetime) rn
# MAGIC from test.isu_0bp_reltypes_text) where rn = 1
# MAGIC except
# MAGIC select
# MAGIC relationshipDirection,
# MAGIC relationshipTypeCode,
# MAGIC relationshipType
# MAGIC from
# MAGIC cleansed.isu_0bp_reltypes_text

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Target and Source Data
# MAGIC %sql
# MAGIC select
# MAGIC relationshipDirection,
# MAGIC relationshipTypeCode,
# MAGIC relationshipType
# MAGIC from
# MAGIC cleansed.isu_0bp_reltypes_text
# MAGIC except
# MAGIC select 
# MAGIC relationshipDirection,
# MAGIC relationshipTypeCode,
# MAGIC relationshipType
# MAGIC from (
# MAGIC select
# MAGIC RELDIR as relationshipDirection
# MAGIC ,RELTYP as relationshipTypeCode
# MAGIC ,TXTLG as relationshipType
# MAGIC ,row_number() over (partition by RELDIR, RELTYP order by extract_datetime) rn
# MAGIC from test.isu_0bp_reltypes_text) where rn = 1