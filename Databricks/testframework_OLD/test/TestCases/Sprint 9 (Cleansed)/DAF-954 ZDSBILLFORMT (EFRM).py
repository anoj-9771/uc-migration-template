# Databricks notebook source
table = 'EFRM'
source = 'ISU' #either CRM or ISU

# COMMAND ----------

# DBTITLE 0,Writing Count Result in Database
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS test

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists test.isu_efrm

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

# DBTITLE 1,Source schema check
sourcedf.printSchema()

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct EXTRACT_DATETIME from test.isu_efrm order by EXTRACT_DATETIME desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC  applicationArea
# MAGIC ,mainTransactionLineItemCode
# MAGIC ,subtransactionLineItemCode
# MAGIC ,subtransaction from (select
# MAGIC APPLK as applicationArea
# MAGIC ,HVORG as mainTransactionLineItemCode
# MAGIC ,TVORG as subtransactionLineItemCode
# MAGIC ,TXT30 as subtransaction
# MAGIC ,row_number() over (partition by APPLK,HVORG,TVORG order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.isu_efrm)
# MAGIC where rn = 1

# COMMAND ----------

# DBTITLE 1,[Source with mapping]
# MAGIC %sql
# MAGIC select
# MAGIC applicationForm
# MAGIC ,formClass
# MAGIC ,formName
# MAGIC ,userExitInclude
# MAGIC ,userTopInclude
# MAGIC ,originalSystem
# MAGIC ,createdDate
# MAGIC ,createdBy
# MAGIC ,createdBySAPAction
# MAGIC ,lastChangedDate
# MAGIC ,lastChangedBy
# MAGIC ,lastChangedTime
# MAGIC ,lastChangedBySAPAction
# MAGIC ,applicationFormDescription
# MAGIC ,userExitBeforeHierarchyInterpretation
# MAGIC ,userExitAfterHierarchyInterpretation
# MAGIC , userExitForDataDispatch
# MAGIC ,formType
# MAGIC ,smartForm
# MAGIC ,genFormGUID
# MAGIC , applicationFormGUID
# MAGIC ,functionName
# MAGIC ,functionGroup
# MAGIC ,crossFormClassCollection
# MAGIC ,PDFFormName
# MAGIC ,dataDispatchMode
# MAGIC ,dynamicPDFForm from
# MAGIC (SELECT
# MAGIC FORMKEY as applicationForm
# MAGIC ,FORMCLASS as formClass
# MAGIC ,TDFORM as formName
# MAGIC ,EXIT_BIBL as userExitInclude
# MAGIC ,USER_TOP as userTopInclude
# MAGIC ,ORIG_SYST as originalSystem
# MAGIC ,ERDAT as createdDate
# MAGIC ,ERNAM as createdBy
# MAGIC ,ERSAP as createdBySAPAction
# MAGIC ,AEDAT as lastChangedDate
# MAGIC ,AENAM as lastChangedBy
# MAGIC ,AEUZEIT as lastChangedTime
# MAGIC ,AESAP as lastChangedBySAPAction
# MAGIC ,DESCRIPT as applicationFormDescription
# MAGIC ,EXIT_INIT as userExitBeforeHierarchyInterpretation
# MAGIC ,EXIT_CLOSE as userExitAfterHierarchyInterpretation
# MAGIC ,EXIT_DISPATCH as userExitForDataDispatch
# MAGIC ,FORMTYPE as formType
# MAGIC ,SMARTFORM as smartForm
# MAGIC ,GENGUID as genFormGUID
# MAGIC ,FORMGUID as applicationFormGUID
# MAGIC ,FUNC_NAME as functionName
# MAGIC ,FUNC_POOL as functionGroup
# MAGIC ,CROSSFCLASS as crossFormClassCollection
# MAGIC ,ADFORM as PDFFormName
# MAGIC ,DATADISPATCH_MOD as dataDispatchMode
# MAGIC ,PDF_DYNAMIC as dynamicPDFForm
# MAGIC ,row_number() over (partition by FORMKEY order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.isu_efrm ) WHERE rn = 1

# COMMAND ----------

lakedf = spark.sql("select * from cleansed.isu_efrm")

# COMMAND ----------

# DBTITLE 1,[Target] Schema Check
lakedf.printSchema()

# COMMAND ----------

# DBTITLE 1,[Verification] count
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from cleansed.isu_efrm
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from (select
# MAGIC applicationForm
# MAGIC ,formClass
# MAGIC ,formName
# MAGIC ,userExitInclude
# MAGIC ,userTopInclude
# MAGIC ,originalSystem
# MAGIC ,createdDate
# MAGIC ,createdBy
# MAGIC ,createdBySAPAction
# MAGIC ,lastChangedDate
# MAGIC ,lastChangedBy
# MAGIC ,lastChangedTime
# MAGIC ,lastChangedBySAPAction
# MAGIC ,applicationFormDescription
# MAGIC ,userExitBeforeHierarchyInterpretation
# MAGIC ,userExitAfterHierarchyInterpretation
# MAGIC , userExitForDataDispatch
# MAGIC ,formType
# MAGIC ,smartForm
# MAGIC ,genFormGUID
# MAGIC , applicationFormGUID
# MAGIC ,functionName
# MAGIC ,functionGroup
# MAGIC ,crossFormClassCollection
# MAGIC ,PDFFormName
# MAGIC ,dataDispatchMode
# MAGIC ,dynamicPDFForm from
# MAGIC (SELECT
# MAGIC FORMKEY as applicationForm
# MAGIC ,FORMCLASS as formClass
# MAGIC ,TDFORM as formName
# MAGIC ,EXIT_BIBL as userExitInclude
# MAGIC ,USER_TOP as userTopInclude
# MAGIC ,ORIG_SYST as originalSystem
# MAGIC ,ERDAT as createdDate
# MAGIC ,ERNAM as createdBy
# MAGIC ,ERSAP as createdBySAPAction
# MAGIC ,AEDAT as lastChangedDate
# MAGIC ,AENAM as lastChangedBy
# MAGIC ,AEUZEIT as lastChangedTime
# MAGIC ,AESAP as lastChangedBySAPAction
# MAGIC ,DESCRIPT as applicationFormDescription
# MAGIC ,EXIT_INIT as userExitBeforeHierarchyInterpretation
# MAGIC ,EXIT_CLOSE as userExitAfterHierarchyInterpretation
# MAGIC ,EXIT_DISPATCH as userExitForDataDispatch
# MAGIC ,FORMTYPE as formType
# MAGIC ,SMARTFORM as smartForm
# MAGIC ,GENGUID as genFormGUID
# MAGIC ,FORMGUID as applicationFormGUID
# MAGIC ,FUNC_NAME as functionName
# MAGIC ,FUNC_POOL as functionGroup
# MAGIC ,CROSSFCLASS as crossFormClassCollection
# MAGIC ,ADFORM as PDFFormName
# MAGIC ,DATADISPATCH_MOD as dataDispatchMode
# MAGIC ,PDF_DYNAMIC as dynamicPDFForm
# MAGIC ,row_number() over (partition by FORMKEY order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.isu_efrm ) WHERE rn = 1)

# COMMAND ----------

# DBTITLE 1,[Duplicate checks]
# MAGIC %sql
# MAGIC SELECT * FROM (
# MAGIC SELECT
# MAGIC *,
# MAGIC row_number() OVER(PARTITION BY applicationForm order by applicationForm) as rn
# MAGIC FROM  cleansed.isu_efrm
# MAGIC ) where rn > 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT applicationForm, COUNT (*) as count
# MAGIC FROM cleansed.isu_efrm
# MAGIC GROUP BY applicationForm
# MAGIC HAVING COUNT (*) > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Source and Target Data
# MAGIC %sql
# MAGIC select
# MAGIC applicationForm
# MAGIC ,formClass
# MAGIC ,formName
# MAGIC ,userExitInclude
# MAGIC ,userTopInclude
# MAGIC ,originalSystem
# MAGIC ,createdDate
# MAGIC ,createdBy
# MAGIC ,createdBySAPAction
# MAGIC ,lastChangedDate
# MAGIC ,lastChangedBy
# MAGIC ,lastChangedTime
# MAGIC ,lastChangedBySAPAction
# MAGIC ,applicationFormDescription
# MAGIC ,userExitBeforeHierarchyInterpretation
# MAGIC ,userExitAfterHierarchyInterpretation
# MAGIC , userExitForDataDispatch
# MAGIC ,formType
# MAGIC ,smartForm
# MAGIC ,genFormGUID
# MAGIC , applicationFormGUID
# MAGIC ,functionName
# MAGIC ,functionGroup
# MAGIC ,crossFormClassCollection
# MAGIC ,PDFFormName
# MAGIC ,dataDispatchMode
# MAGIC ,dynamicPDFForm from
# MAGIC (SELECT
# MAGIC FORMKEY as applicationForm
# MAGIC ,FORMCLASS as formClass
# MAGIC ,TDFORM as formName
# MAGIC ,EXIT_BIBL as userExitInclude
# MAGIC ,USER_TOP as userTopInclude
# MAGIC ,ORIG_SYST as originalSystem
# MAGIC ,ERDAT as createdDate
# MAGIC ,ERNAM as createdBy
# MAGIC ,ERSAP as createdBySAPAction
# MAGIC ,AEDAT as lastChangedDate
# MAGIC ,AENAM as lastChangedBy
# MAGIC ,AEUZEIT as lastChangedTime
# MAGIC ,AESAP as lastChangedBySAPAction
# MAGIC ,DESCRIPT as applicationFormDescription
# MAGIC ,EXIT_INIT as userExitBeforeHierarchyInterpretation
# MAGIC ,EXIT_CLOSE as userExitAfterHierarchyInterpretation
# MAGIC ,EXIT_DISPATCH as userExitForDataDispatch
# MAGIC ,FORMTYPE as formType
# MAGIC ,SMARTFORM as smartForm
# MAGIC ,GENGUID as genFormGUID
# MAGIC ,FORMGUID as applicationFormGUID
# MAGIC ,FUNC_NAME as functionName
# MAGIC ,FUNC_POOL as functionGroup
# MAGIC ,CROSSFCLASS as crossFormClassCollection
# MAGIC ,ADFORM as PDFFormName
# MAGIC ,DATADISPATCH_MOD as dataDispatchMode
# MAGIC ,PDF_DYNAMIC as dynamicPDFForm
# MAGIC ,row_number() over (partition by FORMKEY order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.isu_efrm ) WHERE rn = 1
# MAGIC except
# MAGIC select
# MAGIC applicationForm
# MAGIC ,formClass
# MAGIC ,formName
# MAGIC ,userExitInclude
# MAGIC ,userTopInclude
# MAGIC ,originalSystem
# MAGIC ,createdDate
# MAGIC ,createdBy
# MAGIC ,createdBySAPAction
# MAGIC ,lastChangedDate
# MAGIC ,lastChangedBy
# MAGIC ,lastChangedTime
# MAGIC ,lastChangedBySAPAction
# MAGIC ,applicationFormDescription
# MAGIC ,userExitBeforeHierarchyInterpretation
# MAGIC ,userExitAfterHierarchyInterpretation
# MAGIC , userExitForDataDispatch
# MAGIC ,formType
# MAGIC ,smartForm
# MAGIC ,genFormGUID
# MAGIC , applicationFormGUID
# MAGIC ,functionName
# MAGIC ,functionGroup
# MAGIC ,crossFormClassCollection
# MAGIC ,PDFFormName
# MAGIC ,dataDispatchMode
# MAGIC ,dynamicPDFForm
# MAGIC from
# MAGIC cleansed.isu_efrm

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Target and Source Data
# MAGIC %sql
# MAGIC select
# MAGIC applicationForm
# MAGIC ,formClass
# MAGIC ,formName
# MAGIC ,userExitInclude
# MAGIC ,userTopInclude
# MAGIC ,originalSystem
# MAGIC ,createdDate
# MAGIC ,createdBy
# MAGIC ,createdBySAPAction
# MAGIC ,lastChangedDate
# MAGIC ,lastChangedBy
# MAGIC ,lastChangedTime
# MAGIC ,lastChangedBySAPAction
# MAGIC ,applicationFormDescription
# MAGIC ,userExitBeforeHierarchyInterpretation
# MAGIC ,userExitAfterHierarchyInterpretation
# MAGIC , userExitForDataDispatch
# MAGIC ,formType
# MAGIC ,smartForm
# MAGIC ,genFormGUID
# MAGIC , applicationFormGUID
# MAGIC ,functionName
# MAGIC ,functionGroup
# MAGIC ,crossFormClassCollection
# MAGIC ,PDFFormName
# MAGIC ,dataDispatchMode
# MAGIC ,dynamicPDFForm
# MAGIC from
# MAGIC cleansed.isu_efrm
# MAGIC except
# MAGIC select
# MAGIC applicationForm
# MAGIC ,formClass
# MAGIC ,formName
# MAGIC ,userExitInclude
# MAGIC ,userTopInclude
# MAGIC ,originalSystem
# MAGIC ,createdDate
# MAGIC ,createdBy
# MAGIC ,createdBySAPAction
# MAGIC ,lastChangedDate
# MAGIC ,lastChangedBy
# MAGIC ,lastChangedTime
# MAGIC ,lastChangedBySAPAction
# MAGIC ,applicationFormDescription
# MAGIC ,userExitBeforeHierarchyInterpretation
# MAGIC ,userExitAfterHierarchyInterpretation
# MAGIC , userExitForDataDispatch
# MAGIC ,formType
# MAGIC ,smartForm
# MAGIC ,genFormGUID
# MAGIC , applicationFormGUID
# MAGIC ,functionName
# MAGIC ,functionGroup
# MAGIC ,crossFormClassCollection
# MAGIC ,PDFFormName
# MAGIC ,dataDispatchMode
# MAGIC ,dynamicPDFForm from
# MAGIC (SELECT
# MAGIC FORMKEY as applicationForm
# MAGIC ,FORMCLASS as formClass
# MAGIC ,TDFORM as formName
# MAGIC ,EXIT_BIBL as userExitInclude
# MAGIC ,USER_TOP as userTopInclude
# MAGIC ,ORIG_SYST as originalSystem
# MAGIC ,ERDAT as createdDate
# MAGIC ,ERNAM as createdBy
# MAGIC ,ERSAP as createdBySAPAction
# MAGIC ,AEDAT as lastChangedDate
# MAGIC ,AENAM as lastChangedBy
# MAGIC ,AEUZEIT as lastChangedTime
# MAGIC ,AESAP as lastChangedBySAPAction
# MAGIC ,DESCRIPT as applicationFormDescription
# MAGIC ,EXIT_INIT as userExitBeforeHierarchyInterpretation
# MAGIC ,EXIT_CLOSE as userExitAfterHierarchyInterpretation
# MAGIC ,EXIT_DISPATCH as userExitForDataDispatch
# MAGIC ,FORMTYPE as formType
# MAGIC ,SMARTFORM as smartForm
# MAGIC ,GENGUID as genFormGUID
# MAGIC ,FORMGUID as applicationFormGUID
# MAGIC ,FUNC_NAME as functionName
# MAGIC ,FUNC_POOL as functionGroup
# MAGIC ,CROSSFCLASS as crossFormClassCollection
# MAGIC ,ADFORM as PDFFormName
# MAGIC ,DATADISPATCH_MOD as dataDispatchMode
# MAGIC ,PDF_DYNAMIC as dynamicPDFForm
# MAGIC ,row_number() over (partition by FORMKEY order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.isu_efrm ) WHERE rn = 1
