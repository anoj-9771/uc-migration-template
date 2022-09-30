# Databricks notebook source
# MAGIC %run ../../atf-common

# COMMAND ----------

businessColumns = 'contractId,validToDate'

# COMMAND ----------

def SimpleTestCase1():
    Assert("1","1")

# COMMAND ----------

def TestSql():
    table = f"curated.{GetNotebookName()}"
    df = spark.sql(f"SELECT * FROM {table}")
    Assert(table,table)

# COMMAND ----------

def SimpleTestCase2():
    Assert("a","b")

# COMMAND ----------

def Jira_1234():
    Assert("a","b")

# COMMAND ----------

def CheckNullBusinessColumns():
    df = spark.sql("select count(*) as recCount from curated.dimContract where (ContractID is null or ContractID = '' or ContractID = ' ')  \
                or (VAlidToDate is NULL or ContractID ='' or ContractID=' ')")
    count = df.select("recCount").collect()[0][0]
    Assert(count,0)
    #assert 0==count, 'FAIL - Null Validation for business columns'

# COMMAND ----------

def DuplicateBusinessColumnsCurrentRecords():
    table = GetNotebookName()
    df = spark.sql(f"select count(*) as recCount from curated.{table} where _RecordCurrent=1 and _RecordDeleted=0  \
                GROUP BY {businessColumns} HAVING COUNT (*) > 1")
    #count = df.select("recCount").collect()[0][0]
    count = df.count()
    Assert(count,0)

# COMMAND ----------

def DuplicateBusinessColumnsAllRecords():
    table = GetNotebookName()
    df = spark.sql(f"select count(*) as recCount from curated.{table} \
                GROUP BY {businessColumns} HAVING COUNT (*) > 1")
    count = df.count()
    Assert(count,0)

# COMMAND ----------

src=spark.sql("""Select 
md5(concat_ws("|",contractId,validToDate) ) as ContractSK,
contractId,
validFromDate,
validToDate,
sourceSystemCode,
contractStartDate,
contractEndDate,
invoiceJointlyFlag,
moveInDate,
moveOutDate,
contractAccountNumber,
contractAccountCategory,
applicationArea,
installationId
from
(select
c.contractId as contractId
,coalesce(ch.validFromDate,to_date('1900-01-01','yyyy-MM-dd')) as validFromDate
,coalesce(ch.validToDate,to_date('9999-12-31','yyyy-MM-dd')) as validToDate
,'ISU' as sourceSystemCode
,case
when (ch.validFromDate < c.createdDate and ch.validFromDate is not null) then ch.validFromDate
else c.createdDate end as contractStartDate
,ch.validToDate as contractEndDate
,c.invoiceContractsJointly as invoiceJointlyFlag
,c.moveInDate as moveInDate
,c.moveOutDate as moveOutDate
,ca.contractAccountNumber as contractAccountNumber
,ca.contractAccountCategory as contractAccountCategory
,ca.applicationArea as applicationArea
,c.installationId
from cleansed.isu_0UCCONTRACT_ATTR_2 c
Left join cleansed.isu_0UCCONTRACTH_ATTR_2 ch 
on c.contractId = ch.contractId and ch.deletedindicator is null and ch._RecordDeleted <> 1 AND ch._RecordCurrent = 1
Left join cleansed.isu_0CACONT_ACC_ATTR_2 ca
on c.contractAccountNumber = ca.contractAccountNumber
where c._RecordDeleted <> 1 AND c._RecordCurrent = 1

union all
select * from(
select 
'-1' as contractId,
'1900-01-01' as validFromDate,
'9999-12-31' as validToDate,
null as sourceSystemCode,
null as contractStartDate,
null as contractEndDate,
null as invoiceJointlyFlag,
null as moveInDate,
null as moveOutDate,
null as contractAccountNumber,
null as contractAccountCategory,
null as applicationArea,
null as installationId
from  cleansed.isu_0UCCONTRACT_ATTR_2 limit 1)b


)
""")

# COMMAND ----------

tgt=spark.sql("""select ContractSK,
contractId,
validFromDate,
validToDate,
sourceSystemCode,
contractStartDate,
contractEndDate,
invoiceJointlyFlag,
moveInDate,
moveOutDate,
contractAccountNumber,
contractAccountCategory,
applicationArea,
installationId
from curated.dimContract""")

# COMMAND ----------

def CountSourceTarget():
    srcCount = src.count()
    tgtCount = tgt.count()
    Assert(srcCount, tgtCount, 'Equals', 'Count mismatch between source and target') 

# COMMAND ----------

def CompareSourceTarget():
    srcTgtCount = src.subtract(tgt).count()
    tgtSrcCount = tgt.subtract(src).count()
    
    Assert(srcTgtCount, tgtSrcCount, 'Equals', 'Data mismatch between source and target')

# COMMAND ----------

#ALWAYS RUN THIS AT THE END
RunTests()

# COMMAND ----------


