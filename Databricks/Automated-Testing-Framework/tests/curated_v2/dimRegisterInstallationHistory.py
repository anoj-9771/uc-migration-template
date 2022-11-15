# Databricks notebook source
# MAGIC %run ../../atf-common

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from curated_v2.dimRegisterInstallationHistory

# COMMAND ----------

target_df = spark.sql("select * from curated_v2.dimRegisterInstallationHistory")
target_df.printSchema()

# COMMAND ----------

source_isu=spark.sql("""
select 
'ISU' as sourceSystemCode
, a.logicalRegisterNumber as logicalRegisterNumber
, a.installationNumber as installationNumber
, a.validToDate as validToDate
, a.validFromDate as validFromDate
, a.operationCode as operationCode
, a.operationDescription as operationDescription
, a.rateTypeCode as rateTypeCode
, a.rateType as rateType
, a.registerNotRelevantToBilling as registerNotRelevantToBilling
, a.rateFactGroupCode as rateFactGroupCode
,a.rateFactGroup as rateFactGroup
, a.validFromDate as _RecordStart
, a.validToDate as _RecordEnd
, a._RecordDeleted as _RecordDeleted
, a._RecordCurrent as _RecordCurrent 
from
cleansed.isu_0uc_reginst_str_attr a
where _RecordCurrent not in ('0') and _RecordDeleted not in ('1')
""")
source_isu.createOrReplaceTempView("source_view")
src_a = spark.sql("Select * except(_RecordCurrent,_recordDeleted) from source_view where _RecordCurrent=1 and _recordDeleted=0 ")
src_d = spark.sql("Select * except(_RecordCurrent,_recordDeleted) from source_view where _RecordCurrent=0 and _recordDeleted=1 ")
src_a.createOrReplaceTempView("src_a")
src_d.createOrReplaceTempView("src_d")

# COMMAND ----------

keyColumns = 'logicalRegisterNumber,installationNumber,validToDate'
mandatoryColumns = 'logicalRegisterNumber,installationNumber,validToDate'
columns = ("""
sourceSystemCode
, logicalRegisterNumber
, installationNumber
, validToDate
, validFromDate
, operationCode
, operationDescription
, rateTypeCode
, rateType
, registerNotRelevantToBilling
, rateFactGroupCode
, rateFactGroup
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

#ALWAYS RUN THIS AT THE END
RunTests()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from curated_v2.dimRegisterInstallationHistory where installationNumber = '4103046277'
# MAGIC order by logicalRegisterNumber desc, validFromDate desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from 
# MAGIC cleansed.isu_0uc_reginst_str_attr where installationNumber = '4103046277'where installationNumber = '4101482909'
