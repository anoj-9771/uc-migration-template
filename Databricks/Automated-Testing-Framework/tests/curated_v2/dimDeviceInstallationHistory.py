# Databricks notebook source
# MAGIC %run /build/includes/util-general

# COMMAND ----------

# MAGIC %run ../../atf-common

# COMMAND ----------

target_df = spark.sql("select * from curated_v2.dimDeviceInstallationHistory")
target_df.printSchema()

# COMMAND ----------

source_isu=spark.sql("""
select
'ISU' as sourceSystemCode
, a.installationNumber as installationNumber
, a.logicalDeviceNumber as logicalDeviceNumber
, ToValidDate(a.validToDate) as validToDate
, ToValidDate(a.validFromDate) as validFromDate
, a.priceClassCode as priceClassCode
, a.priceClass as priceClass
, a.rateTypeCode as rateTypeCode
, a.rateType as rateType
, a.payRentalPrice as payRentalPrice
, ToValidDate(a.validFromDate) as _RecordStart
, ToValidDate(a.validToDate) as _RecordEnd
, a._RecordDeleted as _RecordDeleted
, a._RecordCurrent as _RecordCurrent
from
cleansed.isu_0uc_devinst_attr a
where _RecordDeleted not in ('1') and _RecordCurrent not in ('0') 
""")
source_isu.createOrReplaceTempView("source_view")
src_a = spark.sql("Select * except(_RecordCurrent,_recordDeleted) from source_view where _RecordCurrent=1 and _recordDeleted=0 ")
src_d = spark.sql("Select * except(_RecordCurrent,_recordDeleted) from source_view where _RecordCurrent=0 and _recordDeleted=1 ")
src_a.createOrReplaceTempView("src_a")
src_d.createOrReplaceTempView("src_d")

# COMMAND ----------

keyColumns = 'installationNumber,logicalDeviceNumber,validToDate'
mandatoryColumns = 'installationNumber,logicalDeviceNumber,validToDate'
columns = ("""
sourceSystemCode
, installationNumber
, logicalDeviceNumber
, validToDate
, validFromDate
, priceClassCode
, priceClass
, rateTypeCode
, rateType
, payRentalPrice
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
# MAGIC select * from curated_v2.dimDeviceInstallationHistory --where installationNumber = '4101482909' ---_RecordDeleted = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from 
# MAGIC cleansed.isu_0uc_devinst_attr --where installationNumber = '4101482909'

# COMMAND ----------

# DBTITLE 1,_RecordStart Date Validation
# MAGIC %sql
# MAGIC select * from curated_v2.dimDeviceInstallationHistory where _RecordStart < '1900-01-01'

# COMMAND ----------

source_isu=spark.sql("""
select
'ISU' as sourceSystemCode
, a.installationNumber as installationNumber
, a.logicalDeviceNumber as logicalDeviceNumber
, ToValidDate(a.validToDate) as validToDate
, ToValidDate(a.validToDate) as validFromDate
, a.priceClassCode as priceClassCode
, a.priceClass as priceClass
, a.rateTypeCode as rateTypeCode
, a.rateType as rateType
, a.payRentalPrice as payRentalPrice
, ToValidDate(a.validFromDate) as _RecordStart
, ToValidDate(a.validToDate) as _RecordEnd
, case when to_date(current_date, 'yyyy-MM-dd') between a.validFromDate and a.validToDate then '1' else '0' end as _RecordCurrent
, a._RecordDeleted as _RecordDeleted
from
cleansed.isu_0uc_devinst_attr a
""")
source_isu.createOrReplaceTempView("source_view")
src_a = spark.sql("Select * except(_RecordCurrent,_recordDeleted) from source_view where _RecordCurrent=1 and _recordDeleted=0 ")
src_d = spark.sql("Select * except(_RecordCurrent,_recordDeleted) from source_view where _RecordCurrent=0 and _recordDeleted=1 ")
src_a.createOrReplaceTempView("src_a")
src_d.createOrReplaceTempView("src_d")

# COMMAND ----------

# MAGIC %sql
# MAGIC select installationNumber,_RecordStart,_RecordEnd,_RecordCurrent,_RecordDeleted
# MAGIC from
# MAGIC source_view
# MAGIC except
# MAGIC select installationNumber,_RecordStart,_RecordEnd,_RecordCurrent,_RecordDeleted
# MAGIC from
# MAGIC curated_v2.dimDeviceInstallationHistory

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from curated_v2.dimDeviceInstallationHistory where _RecordStart = _DLCuratedZoneTimeStamp and _RecordEnd = '9999-12-31T00:00:00.000+1100' and _RecordCurrent  = 1
