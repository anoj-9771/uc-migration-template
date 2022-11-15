# Databricks notebook source
# MAGIC %run ../../atf-common

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct(doNotReadIndicator) from curated_v2.dimRegisterHistory 

# COMMAND ----------

aaa=spark.sql("select doNotReadIndicator from curated_v2.dimRegisterHistory where doNotReadIndicator='X'")
aaa.createOrReplaceTempView("aaa")
display(aaa)

# COMMAND ----------

# DBTITLE 1,[Target] Schema Check
target_df = spark.sql("select * from curated_v2.dimRegisterHistory")
target_df.printSchema()

# COMMAND ----------

# DBTITLE 1,Source with mapping for active and deleted records
source_isu = spark.sql("""
select
'ISU' as sourceSystemCode
, a.registerNumber  as registerNumber
, a.equipmentNumber as deviceNumber
, a.validToDate as validToDate
, a.validFromDate as validFromDate
, a.logicalRegisterNumber as logicalRegisterNumber
, a.divisionCategoryCode as divisionCategoryCode
, a.divisionCategory as divisionCategory
, a.registerIdCode as registerIdCode
, a.registerId as registerId
, a.registerTypeCode as registerTypeCode
, a.registerType as registerType
, a.registerCategoryCode as registerCategoryCode
, a.registerCategory as registerCategory
, a.reactiveApparentOrActiveRegister as reactiveApparentOrActiveRegisterCode
, a.reactiveApparentOrActiveRegisterTxt as reactiveApparentOrActiveRegister
, a.unitOfMeasurementMeterReading as unitOfMeasurementMeterReading
, a.doNotReadIndicator as doNotReadIndicator
, a._RecordStart as _RecordStart
, a._RecordEnd as _RecordEnd
, a._RecordCurrent as _RecordCurrent
, a._RecordDeleted as _RecordDeleted
From
cleansed.isu_0uc_regist_attr a
""")
source_isu.createOrReplaceTempView("source_view")
#display(source_isu)
#source_isu.count()

src_a = spark.sql("Select * except(_RecordCurrent,_recordDeleted) from source_view where _RecordCurrent=1 and _recordDeleted=0 ")
src_d = spark.sql("Select * except(_RecordCurrent,_recordDeleted) from source_view where _RecordCurrent=0 and _recordDeleted=1 ")
src_a.createOrReplaceTempView("src_a")
src_d.createOrReplaceTempView("src_d")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cleansed.isu_0uc_regist_attr a

# COMMAND ----------

# DBTITLE 1,Define Variables for ATF
keyColumns = 'registerNumber, deviceNumber, validToDate'
mandatoryColumns = 'registerNumber, deviceNumber, validToDate'

columns = ("""
sourceSystemCode,
registerNumber, deviceNumber, validToDate
, validFromDate
, logicalRegisterNumber
, divisionCategoryCode
, divisionCategory
, registerIdCode
, registerId
, registerTypeCode
, registerType
, registerCategoryCode
, registerCategory
, reactiveApparentOrActiveRegisterCode
, reactiveApparentOrActiveRegister
, unitOfMeasurementMeterReading
, doNotReadIndicator
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

# MAGIC %md
# MAGIC #Investigation for failed tests

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from curated_v2.dimRegisterHistory where deviceNumber=-1
