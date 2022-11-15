# Databricks notebook source
# MAGIC %run ../../atf-common

# COMMAND ----------

# DBTITLE 1,SQL Query logic -Source
src=spark.sql("""
select 
'ISU' as sourceSystemCode,
a.installationNumber as installationNumber,
a.operandCode as operandCode,
a.validFromDate as validFromDate,
a.consecutiveDaysFromDate as consecutiveDaysFromDate,
a.validToDate as validToDate,
a.billingDocumentNumber as billingDocumentNumber,
a.mBillingDocumentNumber as mBillingDocumentNumber,
a.moveOutFlag as moveOutFlag,
a.expiryDate as expiryDate,
a.inactiveFlag as inactiveFlag,
a.manualChangeFlag as manualChangeFlag,
a.rateTypeCode as rateTypeCode,
a.rateType as rateType,
a.rateFactGroupCode as rateFactGroupCode,
a.rateFactGroup as rateFactGroup,
a.entryValue as entryValue,
a.valueToBeBilled as valueToBeBilled,
a.operandValue1 as operandValue1,
a.operandValue3Flag as operandValue3Flag,
a.amount as amount,
a.currencyKey as currencyKey
,_RecordCurrent
,_recordDeleted
,a._DLCleansedZoneTimeStamp
from cleansed.isu_ettifn as a  
""")
src.createOrReplaceTempView("src")
#display(src)
#src.count()

src_a=spark.sql("Select * except(_RecordCurrent,_recordDeleted,_DLCleansedZoneTimeStamp) from src where _RecordCurrent=1 and _recordDeleted=0 ")
src_d=spark.sql("Select * except(_RecordCurrent,_recordDeleted,_DLCleansedZoneTimeStamp) from src where _RecordCurrent=0 and _recordDeleted=1 and _DLCleansedZoneTimeStamp > (select min(_DLCuratedZoneTimeStamp) from  curated_v2.dimInstallationFacts) ")
src_a.createOrReplaceTempView("src_a")
src_d.createOrReplaceTempView("src_d")

# COMMAND ----------

# DBTITLE 1,Define fields and table names
keyColumns =  'installationNumber,operandCode,validFromDate,consecutiveDaysFromDate'
mandatoryColumns = 'installationNumber,operandCode,validFromDate,consecutiveDaysFromDate'

columns = ("""sourceSystemCode,
installationNumber,
operandCode,
validFromDate,
consecutiveDaysFromDate,
validToDate,
billingDocumentNumber,
mBillingDocumentNumber,
moveOutflag,
expiryDate,
inactiveflag,
manualChangeflag,
rateTypeCode,
rateType,
rateFactGroupCode,
rateFactGroup,
entryValue,
valueToBeBilled,
operandValue1,
operandValue3Flag,
amount,
currencyKey
""")

source_a = spark.sql(f"""
Select {columns}
From src_a
""")

source_d = spark.sql(f"""
Select {columns}
From src_d
""")



# COMMAND ----------

def ManualDateCheck_1():
    df = spark.sql(f"SELECT * from {GetSelfFqn()} \
                where ValidFromDate > ValidToDate")
    count = df.count()
    #display(df)
    Assert(count, 0)
#ManualDupeCheck()

# COMMAND ----------

def ManualDateCheck_2():
    df = spark.sql(f"SELECT * from {GetSelfFqn()} \
                where (ValidFromDate <> date(_recordStart)) or (ValidToDate <>  date(_recordEnd))")
    count = df.count()
    #display(df)
    Assert(count, 0)
#ManualDupeCheck()

# COMMAND ----------

#ALWAYS RUN THIS AT THE END
RunTests()

# COMMAND ----------

# DBTITLE 1,Schema and Data Validation
tgt=spark.sql("Select * from curated_v2.dimInstallationFacts")
tgt1=spark.sql("Select * from curated_v2.dimInstallationFacts where sourceSystemCode is NULL")
tgt.printSchema()
display(tgt)
display(tgt1)


# COMMAND ----------

a1=spark.sql("Select moveOutFlag,count(*) from curated_v2.dimInstallationFacts group by 1")
a2=spark.sql("Select rateTypeCode,rateType,count(*) from curated_v2.dimInstallationFacts group by 1,2")
a3=spark.sql("Select rateFactGroupCode,rateFactGroup,count(*) from curated_v2.dimInstallationFacts group by 1,2")
a4=spark.sql("Select inactiveFlag,count(*) from curated_v2.dimInstallationFacts group by 1")
a5=spark.sql("Select manualChangeFlag,count(*) from curated_v2.dimInstallationFacts group by 1")
a8=spark.sql("Select operandValue3Flag,count(*) from curated_v2.dimInstallationFacts group by 1")
a6=spark.sql("Select currencyKey,count(*) from curated_v2.dimInstallationFacts group by 1")
a7=spark.sql("Select operandValue1,count(*) from curated_v2.dimInstallationFacts group by 1")


display(a2)
display(a3)
display(a1)
display(a4)
display(a5)
display(a8)
display(a6)
display(a7)


# COMMAND ----------

tgt=spark.sql("Select * from curated_v2.dimInstallationFacts")
from pyspark.sql.functions import col
for name in tgt.columns: 
    rec=tgt.filter(col(name).isNull())
    print("column "+name+" count is: ", rec.count())

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cleansed.isu_ettifn limit 10
