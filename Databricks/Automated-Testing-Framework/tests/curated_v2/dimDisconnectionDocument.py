# Databricks notebook source
# MAGIC %run ../../atf-common

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,SQL Query logic -Source
src=spark.sql("""
select 
'ISU' as sourceSystemCode,
a.disconnectionDocumentNumber as disconnectionDocumentNumber,
a.disconnectionObjectNumber as disconnectionObjectNumber,
a.disconnectionActivityPeriod as disconnectionActivityPeriod,
a.disconnectionDate as disconnectionDate,
a.validFromDate as validFromDate,
a.validToDate as validToDate,
a.disconnectionActivityTypeCode as disconnectionActivityTypeCode,
a.disconnectionActivityType as disconnectionActivityType,
a.disconnectionObjectTypeCode as disconnectionObjectTypeCode,
a.disconnectionReasonCode as disconnectionReasonCode,
a.disconnectionReason as disconnectionReason,
a.processingVariantCode as processingVariantCode,
a.processingVariant as processingVariant,
a.disconnectionReconnectionStatusCode as disconnectionReconnectionStatusCode,
a.disconnectionReconnectionStatus as disconnectionReconnectionStatus,
a.disconnectionDocumentStatusCode as disconnectionDocumentStatusCode,
a.disconnectionDocumentStatus as disconnectionDocumentStatus,
a.installationNumber as installationNumber,
a.equipmentNumber as equipmentNumber,
a.propertyNumber as propertyNumber,
a.referenceObjectTypeCode as referenceObjectTypeCode
,_RecordCurrent
,_recordDeleted
,a._DLCleansedZoneTimeStamp
from cleansed.isu_0uc_isu_32 as a  
""")
src.createOrReplaceTempView("src")
#display(src)
#src.count()

src_a=spark.sql("Select * except(_RecordCurrent,_recordDeleted,_DLCleansedZoneTimeStamp) from src where _RecordCurrent=1 and _recordDeleted=0 ")
src_d=spark.sql("Select * except(_RecordCurrent,_recordDeleted,_DLCleansedZoneTimeStamp) from src where _RecordCurrent=0 and _recordDeleted=1 and _DLCleansedZoneTimeStamp > (select min(_DLCuratedZoneTimeStamp) from  curated_v2.dimDisconnectionDocument) ")
src_a.createOrReplaceTempView("src_a")
src_d.createOrReplaceTempView("src_d")

# COMMAND ----------

# DBTITLE 1,Define fields and table names
keyColumns =  'disconnectionDocumentNumber,disconnectionObjectNumber,disconnectionActivityPeriod'
mandatoryColumns = 'disconnectionDocumentNumber,disconnectionObjectNumber,disconnectionActivityPeriod'

columns = ("""sourceSystemCode,
disconnectionDocumentNumber,
disconnectionObjectNumber,
disconnectionActivityPeriod,
disconnectionDate,
validFromDate,
validToDate,
disconnectionActivityTypeCode,
disconnectionActivityType,
disconnectionObjectTypeCode,
disconnectionReasonCode,
disconnectionReason,
processingVariantCode,
processingVariant,
disconnectionReconnectionStatusCode,
disconnectionReconnectionStatus,
disconnectionDocumentStatusCode,
disconnectionDocumentStatus,
installationNumber,
equipmentNumber,
propertyNumber,
referenceObjectTypeCode
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

tgt=spark.sql("Select * from curated_v2.dimDisconnectionDocument")
tgt1=spark.sql("Select * from curated_v2.dimDisconnectionDocument where sourceSystemCode is NULL")
tgt.printSchema()
display(tgt)
display(tgt1)


# COMMAND ----------

a1=spark.sql("Select disconnectionReasonCode,disconnectionReason,count(*) from curated_v2.dimDisconnectionDocument group by 1,2")
a2=spark.sql("Select disconnectionActivityTypeCode,disconnectionActivityType,count(*) from curated_v2.dimDisconnectionDocument group by 1,2")
a3=spark.sql("Select processingVariantCode,processingVariant,count(*) from curated_v2.dimDisconnectionDocument group by 1,2")
a4=spark.sql("Select disconnectionReconnectionStatusCode,disconnectionReconnectionStatus,count(*) from curated_v2.dimDisconnectionDocument group by 1,2")
a5=spark.sql("Select disconnectionDocumentStatusCode,disconnectionDocumentStatus,count(*) from curated_v2.dimDisconnectionDocument group by 1,2")

display(a1)
display(a2)
display(a3)
display(a4)
display(a5)


# COMMAND ----------

from pyspark.sql.functions import col
for name in tgt.columns: 
    rec=tgt.filter(col(name).isNull())
    print("column "+name+" count is: ", rec.count())
