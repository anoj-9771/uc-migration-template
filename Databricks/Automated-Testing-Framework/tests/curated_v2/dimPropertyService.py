# Databricks notebook source
# MAGIC %run ../../atf-common

# COMMAND ----------

target_df = spark.sql("select * from curated_v2.dimPropertyService")
target_df.printSchema()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from curated_v2.dimPropertyService

# COMMAND ----------

# DBTITLE 1,Source with mapping
vc = spark.sql("select * from cleansed.isu_vibdcharact")
vc.createOrReplaceTempView("vc_view")
vc_active = spark.sql("Select * except(_RecordCurrent,_recordDeleted) from vc_view where _RecordCurrent=1 and _recordDeleted=0 ")
vc_deleted = spark.sql("Select * except(_RecordCurrent,_recordDeleted) from vc_view where _RecordCurrent=0 and _recordDeleted=1 ")
vc_active.createOrReplaceTempView("vc_active_view")
vc_deleted.createOrReplaceTempView("vc_deleted_view")

vn = spark.sql("select * from cleansed.isu_vibdnode")
vn.createOrReplaceTempView("vn_view")
vn_active = spark.sql("Select * except(_RecordCurrent,_recordDeleted) from vn_view where _RecordCurrent=1 and _recordDeleted=0 ")
vn_deleted = spark.sql("Select * except(_RecordCurrent,_recordDeleted) from vn_view where _RecordCurrent=0 and _recordDeleted=1 ")
vn_active.createOrReplaceTempView("vn_active_view")
vn_deleted.createOrReplaceTempView("vn_deleted_view")

# Source Query for active recs
src_isu_a = spark.sql("""
select
'ISU' as sourceSystemCode
, propertyNumber
, architecturalObjectInternalId
, validToDate
, validFromDate
, fixtureAndFittingCharacteristicCode
, fixtureAndFittingCharacteristic
, supplementInfo
from(
select
vn.architecturalObjectNumber as propertyNumber
, vc.architecturalObjectInternalId as architecturalObjectInternalId
, vc.validToDate as validToDate
, vc.validFromDate as validFromDate
, vc.fixtureAndFittingCharacteristicCode as fixtureAndFittingCharacteristicCode
, vc.fixtureAndFittingCharacteristic as fixtureAndFittingCharacteristic
, vc.supplementInfo as supplementInfo
from vc_active_view vc
inner join vn_active_view vn 
on vc.architecturalObjectInternalId = vn.architecturalObjectInternalId)
""")


# Source Query for deleted recs
src_isu_d = spark.sql("""
select
'ISU' as sourceSystemCode
, propertyNumber
, architecturalObjectInternalId
, validToDate
, validFromDate
, fixtureAndFittingCharacteristicCode
, fixtureAndFittingCharacteristic
, supplementInfo
from(
select
vn.architecturalObjectNumber as propertyNumber
, vc.architecturalObjectInternalId as architecturalObjectInternalId
, vc.validToDate as validToDate
, vc.validFromDate as validFromDate
, vc.fixtureAndFittingCharacteristicCode as fixtureAndFittingCharacteristicCode
, vc.fixtureAndFittingCharacteristic as fixtureAndFittingCharacteristic
, vc.supplementInfo as supplementInfo 
from vc_deleted_view vc
inner join vn_deleted_view vn 
on vc.architecturalObjectInternalId = vn.architecturalObjectInternalId)
""")

src_isu_a.createOrReplaceTempView("src_isu_a")
src_isu_d.createOrReplaceTempView("src_isu_d")

# COMMAND ----------

# DBTITLE 1,Define Variables for ATF
keyColumns = 'propertyNumber, architecturalObjectInternalId, validToDate, fixtureAndFittingCharacteristicCode'
mandatoryColumns = 'propertyNumber, architecturalObjectInternalId, validToDate, fixtureAndFittingCharacteristicCode'

columns = ("""
sourceSystemCode
, propertyNumber
, architecturalObjectInternalId
, validToDate
, validFromDate
, fixtureAndFittingCharacteristicCode
, fixtureAndFittingCharacteristic
, supplementInfo
""")

source_a = spark.sql(f"""
Select {columns}
From src_isu_a
""")

source_d = spark.sql(f"""
Select {columns}
From src_isu_d
""")

# COMMAND ----------

#ALWAYS RUN THIS AT THE END
RunTests()

# COMMAND ----------

# DBTITLE 1,Check if dummy record is populated
df = spark.sql(" select * from curated_v2.dimPropertyService where sourceSystemCode is null ")
display(df)

# COMMAND ----------

# DBTITLE 1,Date validation for valid(To/From)Date
# MAGIC %sql
# MAGIC SELECT * FROM curated_v2.dimPropertyService
# MAGIC WHERE validFromDate > validToDate

# COMMAND ----------

# DBTITLE 1,Check 'Code' columns have matching 'Description' Columns - fixtureAndFittingCharacteristic
# MAGIC %sql
# MAGIC select distinct fixtureAndFittingCharacteristicCode, fixtureAndFittingCharacteristic 
# MAGIC from curated_v2.dimPropertyService

# COMMAND ----------

# MAGIC %sql
# MAGIC select propertyNumber, architecturalObjectInternalId, validToDate, fixtureAndFittingCharacteristicCode, count(*)
# MAGIC from src_isu_a
# MAGIC group by 1, 2, 3, 4
# MAGIC having count(*) > 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cleansed.isu_vibdcharact
# MAGIC where architecturalObjectInternalId = 'I000100007863'
# MAGIC and fixtureAndFittingCharacteristicCode = 'ZWW3'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cleansed.isu_vibdnode
# MAGIC where architecturalObjectInternalId = 'I000100007863'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from curated_v2.dimPropertyService
# MAGIC where fixtureAndFittingCharacteristicCode = 'ZWW3'

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from src_isu_a
# MAGIC where fixtureAndFittingCharacteristicCode = 'ZWW3'

# COMMAND ----------

# DBTITLE 1,Source with mapping for active and deleted records
source_isu = spark.sql("""
select
'ISU' as sourceSystemCode
, propertyNumber
, architecturalObjectInternalId
, validToDate
, validFromDate
, fixtureAndFittingCharacteristicCode
, fixtureAndFittingCharacteristic
, supplementInfo
,_RecordCurrent
,_RecordDeleted
from(
select
vn.architecturalObjectNumber as propertyNumber
, vc.architecturalObjectInternalId as architecturalObjectInternalId
, vc.validToDate as validToDate
, vc.validFromDate as validFromDate
, vc.fixtureAndFittingCharacteristicCode as fixtureAndFittingCharacteristicCode
, vc.fixtureAndFittingCharacteristic as fixtureAndFittingCharacteristic
, vc.supplementInfo as supplementInfo
, vc._RecordCurrent as _RecordCurrent
, vc._RecordDeleted as _RecordDeleted   
from cleansed.isu_vibdcharact vc
inner join cleansed.isu_vibdnode vn 
on vc.architecturalObjectInternalId = vn.architecturalObjectInternalId)
""")

source_isu.createOrReplaceTempView("source_view")
#display(source)
#source.count()

src_a = spark.sql("Select * except(_RecordCurrent,_recordDeleted) from source_view where _RecordCurrent=1 and _recordDeleted=0 ")
src_d = spark.sql("Select * except(_RecordCurrent,_recordDeleted) from source_view where _RecordCurrent=0 and _recordDeleted=1 ")
src_a.createOrReplaceTempView("src_a")
src_d.createOrReplaceTempView("src_d")

# COMMAND ----------

vc = spark.sql("select * from cleansed.isu_vibdcharact")
vc.createOrReplaceTempView("vc_view")
vc_active = spark.sql("Select * except(_RecordCurrent,_recordDeleted) from vc_view where _RecordCurrent=1 and _recordDeleted=0 ")
vc_deleted = spark.sql("Select * except(_RecordCurrent,_recordDeleted) from vc_view where _RecordCurrent=0 and _recordDeleted=1 ")
vc_active.createOrReplaceTempView("vc_active_view")
vc_deleted.createOrReplaceTempView("vc_deleted_view")

vn = spark.sql("select * from cleansed.isu_vibdnode")
vn.createOrReplaceTempView("vn_view")
vn_active = spark.sql("Select * except(_RecordCurrent,_recordDeleted) from vn_view where _RecordCurrent=1 and _recordDeleted=0 ")
vn_deleted = spark.sql("Select * except(_RecordCurrent,_recordDeleted) from vn_view where _RecordCurrent=0 and _recordDeleted=1 ")
vn_active.createOrReplaceTempView("vn_active_view")
vn_deleted.createOrReplaceTempView("vn_deleted_view")

# Source Query for active recs
src_isu_a = spark.sql("""
select
'ISU' as sourceSystemCode
, propertyNumber
, architecturalObjectInternalId
, validToDate
, validFromDate
, fixtureAndFittingCharacteristicCode
, fixtureAndFittingCharacteristic
, supplementInfo
,_RecordCurrent
,_RecordDeleted
from(
select
vn.architecturalObjectNumber as propertyNumber
, vc.architecturalObjectInternalId as architecturalObjectInternalId
, vc.validToDate as validToDate
, vc.validFromDate as validFromDate
, vc.fixtureAndFittingCharacteristicCode as fixtureAndFittingCharacteristicCode
, vc.fixtureAndFittingCharacteristic as fixtureAndFittingCharacteristic
, vc.supplementInfo as supplementInfo
, vc._RecordCurrent as _RecordCurrent
, vc._RecordDeleted as _RecordDeleted   
from vc_active_view vc
inner join vn_active_view vn 
on vc.architecturalObjectInternalId = vn.architecturalObjectInternalId)
""")


# Source Query for deleted recs
src_isu_d = spark.sql("""
select
'ISU' as sourceSystemCode
, propertyNumber
, architecturalObjectInternalId
, validToDate
, validFromDate
, fixtureAndFittingCharacteristicCode
, fixtureAndFittingCharacteristic
, supplementInfo
,_RecordCurrent
,_RecordDeleted
from(
select
vn.architecturalObjectNumber as propertyNumber
, vc.architecturalObjectInternalId as architecturalObjectInternalId
, vc.validToDate as validToDate
, vc.validFromDate as validFromDate
, vc.fixtureAndFittingCharacteristicCode as fixtureAndFittingCharacteristicCode
, vc.fixtureAndFittingCharacteristic as fixtureAndFittingCharacteristic
, vc.supplementInfo as supplementInfo
, vc._RecordCurrent as _RecordCurrent
, vc._RecordDeleted as _RecordDeleted   
from vc_deleted_view vc
inner join vn_deleted_view vn 
on vc.architecturalObjectInternalId = vn.architecturalObjectInternalId)
""")

src_isu_a.createOrReplaceTempView("src_isu_a")
src_isu_d.createOrReplaceTempView("src_isu_d")

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from cleansed.isu_vibdnode

# COMMAND ----------

# DBTITLE 1,Check if dummy record is populated
df = spark.sql(""" select * from curated_v2.dimPropertyService
                   where sourceSystemCode is null
               """)
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from curated_v2.dimPropertyService where sourceSystemCode is null

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from curated_v2.dimPropertyRelation where sourceSystemCode is null

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from curated_v2.dimPropertyTypeHistory where sourceSystemCode is null

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from curated_v2.dimPropertyLot where sourceSystemCode is null
