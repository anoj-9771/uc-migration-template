# Databricks notebook source
# MAGIC %run /build/includes/util-general

# COMMAND ----------

# MAGIC %run ../../atf-common

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from curated_v2.dimDeviceHistory

# COMMAND ----------

# DBTITLE 1,[Target] Schema Check
target_df = spark.sql("select * from curated_v2.dimDeviceHistory")
target_df.printSchema()

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Source with mapping for active and deleted records
source_isu=spark.sql("""
select 
'ISU' as sourceSystemCode
, a.equipmentNumber as deviceNumber
, ToValidDate(a.validToDate) as validToDate
, ToValidDate(a.validFromDate) as validFromDate
, a.logicalDeviceNumber as logicalDeviceNumber
, a.deviceLocation as deviceLocation
, a.deviceCategoryCombination as deviceCategoryCombination
, a.registerGroupCode as registerGroupCode
, a.registerGroup as registerGroup
, ToValidDate(a.installationDate) as installationDate
, ToValidDate(a.deviceRemovalDate) as deviceRemovalDate
, a.activityReasonCode as activityReasonCode
, a.activityReason as activityReason
, a.windingGroup as windingGroup
, a.advancedMeterCapabilityGroup as advancedMeterCapabilityGroup
, a.messageAttributeId as messageAttributeId
, min(a.installationDate) over (partition by a.equipmentNumber) as firstInstallationDate
, case
when (a.validFromDate <= current_date and a.validToDate >= current_date) then a.deviceRemovalDate
else null
END as lastDeviceRemovalDate
,coalesce(ToValidDate(a.validFromDate), '1900-01-01') as _RecordStart
, ToValidDate(a.validToDate) as _RecordEnd
, a._RecordDeleted as _RecordDeleted
, a._RecordCurrent as _RecordCurrent
from
cleansed.isu_0uc_deviceh_attr a 
where _RecordDeleted not in ('1') and _RecordCurrent not in ('0') 
""")
source_isu.createOrReplaceTempView("source_view")
src_a = spark.sql("Select * except(_RecordCurrent,_recordDeleted) from source_view where _RecordCurrent=1 and _recordDeleted=0 ")
src_d = spark.sql("Select * except(_RecordCurrent,_recordDeleted) from source_view where _RecordCurrent=0 and _recordDeleted=1 ")
src_a.createOrReplaceTempView("src_a")
src_d.createOrReplaceTempView("src_d")


# COMMAND ----------

# DBTITLE 1,Define Variables for ATF
keyColumns = 'deviceNumber,validToDate'
mandatoryColumns = 'deviceNumber,validToDate'
columns = ("""
sourceSystemCode,
deviceNumber,
validToDate,
validFromDate,
logicalDeviceNumber,
deviceLocation,
deviceCategoryCombination,
registerGroupCode,
registerGroup,
installationDate,
deviceRemovalDate,
activityReasonCode,
activityReason,
windingGroup,
advancedMeterCapabilityGroup,
messageAttributeId,
firstInstallationDate,
lastDeviceRemovalDate
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
# MAGIC select count(*) from curated_v2.dimDeviceHistory

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from source_view

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from curated_v2.dimDeviceHistory  where _RecordCurrent=0 and _recordDeleted=0
