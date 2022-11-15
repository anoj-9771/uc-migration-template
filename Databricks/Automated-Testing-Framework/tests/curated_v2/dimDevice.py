# Databricks notebook source
# MAGIC %run ../../atf-common

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from curated_v2.dimDevice

# COMMAND ----------

# DBTITLE 1,[Target] Schema Check
target_df = spark.sql("select * from curated_v2.dimDevice")
target_df.printSchema()

# COMMAND ----------

# DBTITLE 1,Source with mapping for active and deleted records
source_isu=spark.sql("""
select 
'ISU' as sourceSystemCode
, a.equipmentNumber as deviceNumber
, a.materialNumber as materialNumber
, a.deviceNumber as deviceID
, a.inspectionRelevanceIndicator as inspectionRelevanceIndicator
, a.deviceSize as deviceSize
, a.assetManufacturerName as assetManufacturerName
, a.manufacturerSerialNumber as manufacturerSerialNumber
, a.manufacturerModelNumber as manufacturerModelNumber
, a.objectNumber as objectNumber
, b.functionClassCode as functionClassCode
, b.functionClass as functionClass
, b.constructionClassCode as constructionClassCode
, b.constructionClass as constructionClass
, b.deviceCategoryName as deviceCategoryName
, b.deviceCategoryDescription as deviceCategoryDescription
, b.ptiNumber as ptiNumber
, b.ggwaNumber as ggwaNumber
, b.certificationRequirementType as certificationRequirementType
, a._RecordStart as _RecordStart
, a._RecordEnd as _RecordEnd
, a._RecordCurrent as _RecordCurrent
, a._RecordDeleted as _RecordDeleted
from
cleansed.isu_0uc_device_attr a
left outer join cleansed.isu_0uc_devcat_attr b
on a.materialNumber = b.materialNumber
""")
source_isu.createOrReplaceTempView("source_view")
#display(source_isu)
#source_isu.count()

src_a = spark.sql("Select * except(_RecordCurrent,_recordDeleted) from source_view where _RecordCurrent=1 and _recordDeleted=0 ")
src_d = spark.sql("Select * except(_RecordCurrent,_recordDeleted) from source_view where _RecordCurrent=0 and _recordDeleted=1 ")
src_a.createOrReplaceTempView("src_a")
src_d.createOrReplaceTempView("src_d")


# COMMAND ----------

# DBTITLE 1,Define Variables for ATF
keyColumns = 'deviceNumber'
mandatoryColumns = 'deviceNumber'
columns = ("""
sourceSystemCode,
deviceNumber,
materialNumber,
inspectionRelevanceIndicator,
deviceSize,
assetManufacturerName,
manufacturerSerialNumber,
manufacturerModelNumber,
objectNumber,
functionClassCode,
functionClass,
constructionClassCode,
constructionClass,
deviceCategoryName,
deviceCategoryDescription,
ptiNumber,
ggwaNumber,
certificationRequirementType
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

aaa=spark.sql("select inspectionRelevanceIndicator from curated_v2.dimDevice where inspectionRelevanceIndicator='X'")
aaa.createOrReplaceTempView("aaa")
display(aaa)

# COMMAND ----------

aa=spark.sql("select * from curated_v2.dimDevice ")

# COMMAND ----------

states2=aa.rdd.map(lambda x: x.inspectionRelevanceIndicator).collect()


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from curated_v2.dimDevice where deviceNumber = '000000000011439775'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from curated_v2.dimSewerNetwork where _recordDeleted= '0' and _RecordCurrent = '1' --and _RecordStart = _DLCuratedZoneTimeStamp --group by scamp having count(*)>1

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from 
# MAGIC cleansed.hydra_tsystemarea where _recordDeleted= '0' and _RecordCurrent = '1' 

# COMMAND ----------

# DBTITLE 1,To Find New Record/Update Record
# MAGIC %sql
# MAGIC select * from curated_v2.dimDevice where _RecordStart = _DLCuratedZoneTimeStamp and _RecordEnd = '9999-12-31T00:00:00.000+1100' and _RecordCurrent  = 1
# MAGIC ----_RecordDelete is directly coming from Cleansed table, what if Record_Delete is '1' in cleansed table - i think it is wrong

# COMMAND ----------

# DBTITLE 1,To Find Delete Record
# MAGIC %sql
# MAGIC select * from curated_v2.dimDevice where _RecordStart = _DLCuratedZoneTimeStamp and _RecordEnd = '9999-12-31T00:00:00.000+1100' and _RecordCurrent  = 1 and _RecordDelete  = 1
