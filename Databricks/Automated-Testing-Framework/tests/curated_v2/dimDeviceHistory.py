# Databricks notebook source
# MAGIC %run ../../atf-common

# COMMAND ----------

keyColumns = 'deviceNumber, validToDate'
businessKey = '_BusinessKey'
businessColumns = 'sourceSystemCode'

# COMMAND ----------

sourceColumns = ("""sourceSystemCode
, deviceNumber
, validToDate
, validFromDate
, logicalDeviceNumber
, deviceLocation
, deviceCategoryCombination
, registerGroupCode
, registerGroup
, installationDate
, deviceRemovalDate
, activityReasonCode
, activityReason
, windingGroup
, advancedMeterCapabilityGroup
, messageAttributeId
, firstInstallationDate
, lastDeviceRemovalDate
""")

source = spark.sql(f"""
Select {sourceColumns}
,_RecordStart
,_RecordEnd
,_RecordCurrent
,_RecordDeleted
From
(
Select
'ISU' as sourceSystemCode
, a.equipmentNumber as deviceNumber
, a.validToDate as validToDate
, a.validFromDate as validFromDate
, a.logicalDeviceNumber as logicalDeviceNumber
, a.deviceLocation as deviceLocation
, a.deviceCategoryCombination as deviceCategoryCombination
, a.registerGroupCode as registerGroupCode
, a.registerGroup as registerGroup
, a.installationDate as installationDate
, a.deviceRemovalDate as deviceRemovalDate
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
, a._RecordStart as _RecordStart
, a._RecordEnd as _RecordEnd
, a._RecordCurrent as _RecordCurrent
, a._RecordDeleted as _RecordDeleted
From
cleansed.isu_0uc_deviceh_attr a
)

""")
#display(source)
#source.count()

# COMMAND ----------

# only include the columns you wish to compare with the source table columns
targetColumns = ("""
sourceSystemCode
, deviceNumber
, validToDate
, validFromDate
, logicalDeviceNumber
, deviceLocation
, deviceCategoryCombination
, registerGroupCode
, registerGroup
, installationDate
, deviceRemovalDate
, activityReasonCode
, activityReason
, windingGroup
, advancedMeterCapabilityGroup
, messageAttributeId
, firstInstallationDate
, lastDeviceRemovalDate
""")

target = spark.sql(f"""
select {targetColumns}
,_RecordStart
,_RecordEnd
,_RecordCurrent
,_RecordDeleted
from 
curated_v2.dimDeviceHistory
""")
#display(target)
#target.count()

# COMMAND ----------

def ManualDupeCheck():
    df = spark.sql(f"SELECT {keyColumns}, COUNT(*) as recCount from curated_v2.dimDeviceHistory \
                GROUP BY {keyColumns} HAVING COUNT(*) > 1")
    count = df.count()
    #display(df)
    Assert(count, 0)
#ManualDupeCheck()

# COMMAND ----------

#ALWAYS RUN THIS AT THE END
RunTests()
