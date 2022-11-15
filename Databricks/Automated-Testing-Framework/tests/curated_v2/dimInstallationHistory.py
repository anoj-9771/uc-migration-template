# Databricks notebook source
# MAGIC %run ../../atf-common

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from curated_v2.dimInstallationHistory where --_recordCurrent=0
# MAGIC installationNumber in (4103465084,4103451058)
# MAGIC order by installationNumber,_recordstart
# MAGIC --Select _RecordStart,_RecordEnd from curated_v2.dimBusinessPartner 

# COMMAND ----------

# DBTITLE 1,SQL Query logic -Source
src=spark.sql("""
select 
'ISU' as sourceSystemCode
,a.installationNumber as installationNumber
,a.validFromDate as validFromDate
,a.validToDate as validToDate
,a.rateCategoryCode as rateCategoryCode
,a.rateCategory as rateCategory
,b.portionNumber as portionNumber
,b.portionText as portionText
,a.industrySystemCode as industrySystemCode
,a.industrySystem as industrySystem
,a.industryCode as industryCode
,a.industry as industry
,a.billingClassCode as billingClassCode
,a.billingClass as billingClass
,a.meterReadingUnit as meterReadingUnit
,a._recordCurrent
,a._recordDeleted
,a._DLCleansedZoneTimeStamp
from cleansed.isu_0ucinstallah_attr_2 a
left outer join cleansed.isu_0ucmtrdunit_attr b
on a.meterReadingUnit = b.meterReadingUnit
and b._recordCurrent=1 and b._recordDeleted=0
""")
src.createOrReplaceTempView("src")
#display(src)
#src.count()

src_a=spark.sql("Select * except(_RecordCurrent,_recordDeleted,_DLCleansedZoneTimeStamp) from src where _RecordCurrent=1 and _recordDeleted=0 ")
src_d=spark.sql("Select * except(_RecordCurrent,_recordDeleted,_DLCleansedZoneTimeStamp) from src where _RecordCurrent=0 and _recordDeleted=1 and _DLCleansedZoneTimeStamp > (select min(_DLCuratedZoneTimeStamp) from  curated_v2.dimInstallationHistory) ")

src_a.createOrReplaceTempView("src_a")
src_d.createOrReplaceTempView("src_d")

# COMMAND ----------

# DBTITLE 1,Define fields and table names
keyColumns =  'installationNumber,validToDate'
mandatoryColumns = 'installationNumber,validFromDate,validToDate'

columns = ("""sourceSystemCode,
installationNumber,
validFromDate,
validToDate,
rateCategoryCode,
rateCategory,
portionNumber,
portionText,
industrySystemCode,
IndustrySystem,
industryCode,
industry,
billingClassCode,
billingClass,
meterReadingUnit
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

tgt=spark.sql("Select * from curated_v2.dimInstallationHistory")
tgt1=spark.sql("Select * from curated_v2.dimInstallationHistory where sourceSystemCode is NULL")
tgt.printSchema()
display(tgt)
display(tgt1)

# COMMAND ----------

a1=spark.sql("Select rateCategoryCode,rateCategory,count(*) from curated_v2.dimInstallationHistory group by 1,2")
a2=spark.sql("Select portionNumber,portionText,count(*) from curated_v2.dimInstallationHistory group by 1,2")
a3=spark.sql("Select industrySystemCode,IndustrySystem,count(*) from curated_v2.dimInstallationHistory group by 1,2")
a4=spark.sql("Select industryCode,industry,count(*) from curated_v2.dimInstallationHistory group by 1,2")
a5=spark.sql("Select billingClassCode,billingClass,count(*) from curated_v2.dimInstallationHistory group by 1,2")
display(a1)
display(a2)
display(a3)
display(a4)
display(a5)


# COMMAND ----------

tgt=spark.sql("Select * from curated_v2.dimInstallationHistory")
from pyspark.sql.functions import col
for name in tgt.columns: 
    rec=tgt.filter(col(name).isNull())
    print("column "+name+" count is: ", rec.count())
