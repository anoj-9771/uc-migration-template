# Databricks notebook source
# MAGIC %run ../../common/common-atf

# COMMAND ----------

businessColumns = 'contractId,validToDate'

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

#ALWAYS RUN THIS AT THE END
RunTests()

# COMMAND ----------


