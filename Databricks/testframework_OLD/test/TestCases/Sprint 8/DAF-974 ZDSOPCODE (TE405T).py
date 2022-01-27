# Databricks notebook source
table = 'ZDSOPCODE'
table1 = table.lower()
print(table1)

# COMMAND ----------

# DBTITLE 0,Writing Count Result in Database
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS test

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists test.table1

# COMMAND ----------

# DBTITLE 1,[Config] Connection Setup
from datetime import datetime

global fileCount

storage_account_name = "sablobdaftest01"
storage_account_access_key = dbutils.secrets.get(scope="TestScope",key="test-sablob-key")
container_name = "archive"
fileLocation = "wasbs://archive@saswcnonprod01landingtst.blob.core.windows.net/sapisu/"
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
    testdf.write.format(fileType).mode("append").saveAsTable("test" + "." + table)
   

# COMMAND ----------

# DBTITLE 1,List folders in fileLocation
folders = dbutils.fs.ls(fileLocation)
fileNames = []
fileCount = 0
dfs = {}

for folder in folders:
    try:
        subfolders = dbutils.fs.ls(folder.path)
        prntDate = False
        for subfolder in subfolders:
            files = dbutils.fs.ls(subfolder.path)
            prntTime = False
            for myFile in files:
                if myFile.name[:len(table)] == table:
                    fileCount += 1
                    if not prntDate:
                        print(f'{datetime.strftime(datetime.strptime(folder.name.strip("/"),"%Y%m%d"),"%Y-%m-%d")}')
                        printDate = True
                    
                    if not prntTime:
                        print(f'\t{subfolder.name.split("_")[-1].strip("/")}')
                        prntTime = True
                        
                    print(f'\t\t{myFile.name.strip("/")}\t{myFile.size}')
                    
                    if myFile.size > 0:
                        fileNames.append(myFile)
    except:
        print(f'Invalid folder name: {folder.name.strip("/")}')

for myFile in fileNames:
    listDetails(myFile)

# COMMAND ----------

sourcedf = spark.sql(f"select * from test.{table1}")
display(sourcedf)

# COMMAND ----------

# DBTITLE 1,Source schema check
sourcedf.printSchema()

# COMMAND ----------

sourcedf.createOrReplaceTempView("Source")

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct EXTRACT_DATETIME from Source order by EXTRACT_DATETIME desc

# COMMAND ----------

# DBTITLE 1,[Source with mapping]
# MAGIC %sql
# MAGIC select
# MAGIC operationCode
# MAGIC ,operationDescription
# MAGIC from(SELECT
# MAGIC OPCODE as operationCode
# MAGIC ,OPCODETXT as operationDescription
# MAGIC ,row_number() over (partition by operationCode order by EXTRACT_DATETIME desc) as rn
# MAGIC from source
# MAGIC WHERE SPRAS = 'E') a where a.rn = 1

# COMMAND ----------

lakedf = spark.sql("select * from cleansed.t_sapisu_ZDSOPCODE")

# COMMAND ----------

# DBTITLE 1,[Target] Schema Check
lakedf.printSchema()

# COMMAND ----------

# DBTITLE 1,[Verification] count
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from cleansed.t_sapisu_ZDSOPCODE
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from Source

# COMMAND ----------

# DBTITLE 1,[Duplicate checks]
# MAGIC %sql
# MAGIC SELECT * FROM (
# MAGIC SELECT
# MAGIC *,
# MAGIC row_number() OVER(PARTITION BY operationCode order by validFromDate) as rn
# MAGIC FROM  cleansed.t_sapisu_ZDSOPCODE
# MAGIC )a where a.rn > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Source and Target Data
# MAGIC %sql
# MAGIC select
# MAGIC operationCode
# MAGIC ,operationDescription
# MAGIC from(SELECT
# MAGIC OPCODE as operationCode
# MAGIC ,OPCODETXT as operationDescription
# MAGIC ,row_number() over (partition by operationCode order by EXTRACT_DATETIME desc) as rn
# MAGIC from source
# MAGIC WHERE SPRAS = 'E') a where a.rn = 1
# MAGIC except
# MAGIC select
# MAGIC operationCode
# MAGIC ,operationDescription
# MAGIC from
# MAGIC cleansed.t_sapisu_ZDSOPCODE

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Target and Source Data
# MAGIC %sql
# MAGIC select
# MAGIC operationCode
# MAGIC ,operationDescription
# MAGIC from
# MAGIC cleansed.t_sapisu_ZDSOPCODE
# MAGIC except
# MAGIC select
# MAGIC operationCode
# MAGIC ,operationDescription
# MAGIC from(SELECT
# MAGIC OPCODE as operationCode
# MAGIC ,OPCODETXT as operationDescription
# MAGIC ,row_number() over (partition by operationCode order by EXTRACT_DATETIME desc) as rn
# MAGIC from source
# MAGIC WHERE SPRAS = 'E') a where a.rn = 1
