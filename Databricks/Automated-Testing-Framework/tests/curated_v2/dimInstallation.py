# Databricks notebook source
# MAGIC %run ../../atf-common

# COMMAND ----------

# DBTITLE 1,SQL Query logic -Source
src=spark.sql("""
select 
'ISU' as sourceSystemCode
,installationNumber as installationNumber
,divisionCode as divisionCode
,division as division
,propertyNumber as propertyNumber
,premise as Premise
,meterReadingBlockedReason as meterReadingBlockedReason
,basePeriodCategory as basePeriodCategory
,installationType as installationType
,meterReadingControlCode as meterReadingControlCode
,meterReadingControl as meterReadingControl
,reference as reference
,authorizationGroupCode as authorizationGroupCode
,guaranteedSupplyReason as guaranteedSupplyReason
,serviceTypeCode as serviceTypeCode
,serviceType as serviceType
,deregulationStatus as deregulationStatus
,createdDate as createdDate
,createdBy as createdBy
,lastChangedDate as lastChangedDate
,lastChangedBy as lastChangedBy
--,c.portionNumber as portionNumber
--,c.portionText as portionText
,_RecordCurrent
,_recordDeleted
,_DLCleansedZoneTimeStamp
from cleansed.isu_0ucinstalla_attr_2
""")
src.createOrReplaceTempView("src")
#display(src)
#src.count()

src_a=spark.sql("Select * except(_RecordCurrent,_recordDeleted,_DLCleansedZoneTimeStamp) from src where _RecordCurrent=1 and _recordDeleted=0 ")
src_d=spark.sql("Select * except(_RecordCurrent,_recordDeleted,_DLCleansedZoneTimeStamp) from src where _RecordCurrent=0 and _recordDeleted=1 and _DLCleansedZoneTimeStamp > (select min(_DLCuratedZoneTimeStamp) from  curated_v2.dimInstallation) ")
src_a.createOrReplaceTempView("src_a")
src_d.createOrReplaceTempView("src_d")

# COMMAND ----------

# DBTITLE 1,Define fields and table names
keyColumns =  'installationNumber'
mandatoryColumns = 'installationNumber'

columns = ("""sourceSystemCode,
installationNumber,
divisionCode,
division,
propertyNumber,
Premise,
meterReadingBlockedReason,
basePeriodCategory,
installationType,
meterReadingControlCode,
meterReadingControl,
reference,
authorizationGroupCode,
guaranteedSupplyReason,
serviceTypeCode,
serviceType,
deregulationStatus,
createdDate,
createdBy,
lastChangedDate,
lastChangedBy
""")

source_a = spark.sql(f"""
Select {columns}
From
src_a
""")
source_d = spark.sql(f"""
Select {columns}
From
src_d
""")
#display(source)
#source.count()


# COMMAND ----------

#ALWAYS RUN THIS AT THE END
RunTests()

# COMMAND ----------

tgt=spark.sql("Select * from curated_v2.dimInstallation")
tgt1=spark.sql("Select * from curated_v2.dimInstallation where sourceSystemCode is NULL")
tgt.printSchema()
display(tgt)
display(tgt1)

# COMMAND ----------

a1=spark.sql("Select divisionCode,division,count(*) from curated_v2.dimInstallation group by 1,2")
a2=spark.sql("Select meterReadingControlCode,meterReadingControl,count(*) from curated_v2.dimInstallation group by 1,2")
a3=spark.sql("Select serviceTypeCode,serviceType,count(*) from curated_v2.dimInstallation group by 1,2")

display(a1)
display(a2)
display(a3)


# COMMAND ----------

print(tgt.count())
from pyspark.sql.functions import col
#fld=['identificationTypeCode','identificationType','validFromDate','validToDate','entryDate','institute','stateCode','countryShortName']
for name in tgt.columns: 
#fld:
    rec=tgt.filter(col(name).isNull())
    print("column "+name+" count is: ", rec.count())
