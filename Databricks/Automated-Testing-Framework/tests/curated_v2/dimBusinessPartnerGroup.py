# Databricks notebook source
# MAGIC %run ../../atf-common

# COMMAND ----------

# DBTITLE 1,SQL Query logic -Source
#fetching data from isu_0bp_def_address_attr for active and inactive records 

bpg_i=spark.sql("""select 

'ISU' as sourceSystemCode,
businessPartnerNumber as businessPartnerGroupNumber 
,businessPartnerGroupCode
,businessPartnerGroup
,businessPartnerCategoryCode
,businessPartnerCategory
,businessPartnerTypeCode
,businessPartnerType
,externalBusinessPartnerNumber
,businessPartnerGUID
,nameGroup1
,nameGroup2
,createdBy,
createdDateTime,
lastUpdatedBy, 
lastUpdatedDateTime, 
validFromDate,
validToDate
,_RecordCurrent,_recordDeleted
,_DLCleansedZoneTimeStamp
from cleansed.isu_0bpartner_attr
where businessPartnerCategoryCode = '3'""")

bpg_i.createOrReplaceTempView('bpg_i')
bpg_ia=spark.sql("Select * except(_RecordCurrent,_recordDeleted,_DLCleansedZoneTimeStamp) from bpg_i where _RecordCurrent=1 and _recordDeleted=0 ")
bpg_id=spark.sql("""Select * except(_RecordCurrent,_recordDeleted,_DLCleansedZoneTimeStamp) from bpg_i where _RecordCurrent=0 and _recordDeleted=1 and _DLCleansedZoneTimeStamp > (select min(_DLCuratedZoneTimeStamp) from  curated_v2.dimbusinesspartnergroup)""")
bpg_ia.createOrReplaceTempView("bpg_ia")
bpg_id.createOrReplaceTempView("bpg_id")

#############################################################

#fetching data from crm_0bp_def_address_attr for active and inactive records 
bpg_c=spark.sql("""select 
'CRM' as sourceSystemCode,
businessPartnerNumber as businessPartnerGroupNumber,
businessPartnerGroupCode,
businessPartnerGroup,
businessPartnerCategoryCode,
businessPartnerCategory,
businessPartnerTypeCode,
businessPartnerType,
externalBusinessPartnerNumber,
businessPartnerGUID,
nameGroup1,
nameGroup2,
paymentAssistSchemeFlag,
billAssistFlag,
consent1Indicator,
warWidowFlag,
userId,
createdDate,
kidneyDialysisFlag,
patientUnit,
patientTitleCode,
patientTitle,
patientFirstName,
patientSurname,
patientAreaCode,
patientPhoneNumber,
hospitalCode,
hospitalName,
patientMachineTypeCode,
patientMachineType,
machineTypeValidFromDate,
machineTypeValidToDate,
machineOffReasonCode,
machineOffReason,
createdBy,
createdDateTime,
lastUpdatedBy,
lastUpdatedDateTime,
validFromDate,
validToDate
,_RecordCurrent,_recordDeleted
,_DLCleansedZoneTimeStamp
from cleansed.crm_0bpartner_attr
where businessPartnerCategoryCode = '3'
""")
bpg_c.createOrReplaceTempView('bpg_c')
#display(bpg_c)

bpg_ca=spark.sql("Select * except(_RecordCurrent,_recordDeleted,_DLCleansedZoneTimeStamp) from bpg_c where _RecordCurrent=1 and _recordDeleted=0 ")
bpg_cd=spark.sql("""Select * except(_RecordCurrent,_recordDeleted,_DLCleansedZoneTimeStamp) from bpg_c where _RecordCurrent=0 and _recordDeleted=1 and _DLCleansedZoneTimeStamp > (select min(_DLCuratedZoneTimeStamp) from  curated_v2.dimbusinesspartnergroup)""")

bpg_ca.createOrReplaceTempView("bpg_ca")
bpg_cd.createOrReplaceTempView("bpg_cd")

#fetching data from isu data by performing inner join to fetch the common data for active records only . 
src_a1 = spark.sql(""" 
select a.sourceSystemCode  as sourceSystemCode,
a.businessPartnerGroupNumber as businessPartnerGroupNumber,
a.businessPartnerGroupCode as businessPartnerGroupCode,
a.businessPartnerGroup as businessPartnerGroup,
a.businessPartnerCategoryCode as businessPartnerCategoryCode,
a.businessPartnerCategory as businessPartnerCategory,
a.businessPartnerTypeCode as businessPartnerTypeCode,
a.businessPartnerType as businessPartnerType,
a.externalBusinessPartnerNumber as externalNumber,
a.businessPartnerGUID as businessPartnerGUID,
a.nameGroup1 as businessPartnerGroupName1,
a.nameGroup2 as businessPartnerGroupName2,
b.paymentAssistSchemeFlag as paymentAssistSchemeFlag,
b.billAssistFlag as billAssistFlag,
b.consent1Indicator as consent1Indicator,
b.warWidowFlag as warWidowFlag,
b.userId as indicatorCreatedUserId,
b.createdDate as indicatorCreatedDate,
b.kidneyDialysisFlag as kidneyDialysisFlag,
b.patientUnit as patientUnit,
b.patientTitleCode as patientTitleCode,
b.patientTitle as patientTitle,
b.patientFirstName as patientFirstName,
b.patientSurname as patientSurname,
b.patientAreaCode as patientAreaCode,
b.patientPhoneNumber as patientPhoneNumber,
b.hospitalCode as hospitalCode,
b.hospitalName as hospitalName,
b.patientMachineTypeCode as patientMachineTypeCode,
b.patientMachineType as patientMachineType,
b.machineTypeValidFromDate as machineTypeValidFromDate,
b.machineTypeValidToDate as machineTypeValidToDate,
b.machineOffReasonCode as machineOffReasonCode,
b.machineOffReason as machineOffReason,
a.createdBy as createdBy,
a.createdDateTime as createdDateTime,
a.lastUpdatedBy as lastUpdatedBy,
a.lastUpdatedDateTime as lastUpdatedDateTime,
a.validFromDate as validFromDate,
a.validToDate as validToDate
from bpg_ia as a
inner join bpg_ca as b
on a.businessPartnerGroupNumber = b.businessPartnerGroupNumber
""")
src_a1.createOrReplaceTempView('src_a1')
#display(src_a1)

src_a2 = spark.sql(""" 
select 
coalesce( a.sourceSystemCode , b.sourceSystemCode) as sourceSystemCode,
coalesce( a.businessPartnerGroupNumber , b.businessPartnerGroupNumber) as businessPartnerGroupNumber,
coalesce( a.businessPartnerGroupCode , b.businessPartnerGroupCode) as businessPartnerGroupCode,
coalesce( a.businessPartnerGroup , b.businessPartnerGroup) as businessPartnerGroup,
coalesce( a.businessPartnerCategoryCode , b.businessPartnerCategoryCode) as businessPartnerCategoryCode,
coalesce( a.businessPartnerCategory , b.businessPartnerCategory) as businessPartnerCategory,
coalesce( a.businessPartnerTypeCode , b.businessPartnerTypeCode) as businessPartnerTypeCode,
coalesce( a.businessPartnerType , b.businessPartnerType) as businessPartnerType,
coalesce( a.externalBusinessPartnerNumber , b.externalBusinessPartnerNumber) as externalNumber,
coalesce( a.businessPartnerGUID , b.businessPartnerGUID) as businessPartnerGUID,
coalesce( a.nameGroup1 , b.nameGroup1) as businessPartnerGroupName1,
coalesce( a.nameGroup2 , b.nameGroup2) as businessPartnerGroupName2,
b.paymentAssistSchemeFlag as paymentAssistSchemeFlag,
b.billAssistFlag as billAssistFlag,
b.consent1Indicator as consent1Indicator,
b.warWidowflag as warWidowFlag,
b.userId as indicatorCreatedUserId,
b.createdDate as indicatorCreatedDate,
b.kidneyDialysisFlag as kidneyDialysisFlag,
b.patientUnit as patientUnit,
b.patientTitleCode as patientTitleCode,
b.patientTitle as patientTitle,
b.patientFirstName as patientFirstName,
b.patientSurname as patientSurname,
b.patientAreaCode as patientAreaCode,
b.patientPhoneNumber as patientPhoneNumber,
b.hospitalCode as hospitalCode,
b.hospitalName as hospitalName,
b.patientMachineTypeCode as patientMachineTypeCode,
b.patientMachineType as patientMachineType,
b.machineTypeValidFromDate as machineTypeValidFromDate,
b.machineTypeValidToDate as machineTypeValidToDate,
b.machineOffReasonCode as machineOffReasonCode,
b.machineOffReason as machineOffReason,
coalesce( a.createdBy , b.createdBy) as createdBy,
coalesce( a.createdDateTime , b.createdDateTime) as createdDateTime,
coalesce( a.lastUpdatedBy , b.lastUpdatedBy) as lastUpdatedBy,
coalesce( a.lastUpdatedDateTime , b.lastUpdatedDateTime) as lastUpdatedDateTime,
coalesce( a.validFromDate , b.validFromDate) as validFromDate,
coalesce( a.validToDate , b.validToDate) as validToDate
from bpg_ia as a
full outer join bpg_ca as b
on a.businessPartnerGroupNumber = b.businessPartnerGroupNumber
where a.businessPartnerGroupNumber is NULL or b.businessPartnerGroupNumber is NULL
""")
src_a2.createOrReplaceTempView('src_a2')
#display(src_a2)

src_a=spark.sql("""
select * from src_a1
union all 
select * from src_a2
""")
src_a.createOrReplaceTempView('src_a')
#display(src_a)
#print(src_a.count())


#fetching data from isu data by performing inner join to fetch the common data for deleted records only . 
src_d1 = spark.sql(""" 
select a.sourceSystemCode  as sourceSystemCode,
a.businessPartnerGroupNumber as businessPartnerGroupNumber,
a.businessPartnerGroupCode as businessPartnerGroupCode,
a.businessPartnerGroup as businessPartnerGroup,
a.businessPartnerCategoryCode as businessPartnerCategoryCode,
a.businessPartnerCategory as businessPartnerCategory,
a.businessPartnerTypeCode as businessPartnerTypeCode,
a.businessPartnerType as businessPartnerType,
a.externalBusinessPartnerNumber as externalNumber,
a.businessPartnerGUID as businessPartnerGUID,
a.nameGroup1 as businessPartnerGroupName1,
a.nameGroup2 as businessPartnerGroupName2,
b.paymentAssistSchemeFlag as paymentAssistSchemeFlag,
b.billAssistFlag as billAssistFlag,
b.consent1Indicator as consent1Indicator,
b.warWidowFlag as warWidowFlag,
b.userId as indicatorCreatedUserId,
b.createdDate as indicatorCreatedDate,
b.kidneyDialysisFlag as kidneyDialysisFlag,
b.patientUnit as patientUnit,
b.patientTitleCode as patientTitleCode,
b.patientTitle as patientTitle,
b.patientFirstName as patientFirstName,
b.patientSurname as patientSurname,
b.patientAreaCode as patientAreaCode,
b.patientPhoneNumber as patientPhoneNumber,
b.hospitalCode as hospitalCode,
b.hospitalName as hospitalName,
b.patientMachineTypeCode as patientMachineTypeCode,
b.patientMachineType as patientMachineType,
b.machineTypeValidFromDate as machineTypeValidFromDate,
b.machineTypeValidToDate as machineTypeValidToDate,
b.machineOffReasonCode as machineOffReasonCode,
b.machineOffReason as machineOffReason,
a.createdBy as createdBy,
a.createdDateTime as createdDateTime,
a.lastUpdatedBy as lastUpdatedBy,
a.lastUpdatedDateTime as lastUpdatedDateTime,
a.validFromDate as validFromDate,
a.validToDate as validToDate
from bpg_id as a
inner join bpg_cd as b
on a.businessPartnerGroupNumber = b.businessPartnerGroupNumber
""")
src_d1.createOrReplaceTempView('src_d1')
#display(src_d1)

src_d2 = spark.sql(""" 
select 
coalesce( a.sourceSystemCode , b.sourceSystemCode) as sourceSystemCode,
coalesce( a.businessPartnerGroupNumber , b.businessPartnerGroupNumber) as businessPartnerGroupNumber,
coalesce( a.businessPartnerGroupCode , b.businessPartnerGroupCode) as businessPartnerGroupCode,
coalesce( a.businessPartnerGroup , b.businessPartnerGroup) as businessPartnerGroup,
coalesce( a.businessPartnerCategoryCode , b.businessPartnerCategoryCode) as businessPartnerCategoryCode,
coalesce( a.businessPartnerCategory , b.businessPartnerCategory) as businessPartnerCategory,
coalesce( a.businessPartnerTypeCode , b.businessPartnerTypeCode) as businessPartnerTypeCode,
coalesce( a.businessPartnerType , b.businessPartnerType) as businessPartnerType,
coalesce( a.externalBusinessPartnerNumber , b.externalBusinessPartnerNumber) as externalNumber,
coalesce( a.businessPartnerGUID , b.businessPartnerGUID) as businessPartnerGUID,
coalesce( a.nameGroup1 , b.nameGroup1) as businessPartnerGroupName1,
coalesce( a.nameGroup2 , b.nameGroup2) as businessPartnerGroupName2,
b.paymentAssistSchemeFlag as paymentAssistSchemeFlag,
b.billAssistFlag as billAssistFlag,
b.consent1Indicator as consent1Indicator,
b.warWidowflag as warWidowFlag,
b.userId as indicatorCreatedUserId,
b.createdDate as indicatorCreatedDate,
b.kidneyDialysisFlag as kidneyDialysisFlag,
b.patientUnit as patientUnit,
b.patientTitleCode as patientTitleCode,
b.patientTitle as patientTitle,
b.patientFirstName as patientFirstName,
b.patientSurname as patientSurname,
b.patientAreaCode as patientAreaCode,
b.patientPhoneNumber as patientPhoneNumber,
b.hospitalCode as hospitalCode,
b.hospitalName as hospitalName,
b.patientMachineTypeCode as patientMachineTypeCode,
b.patientMachineType as patientMachineType,
b.machineTypeValidFromDate as machineTypeValidFromDate,
b.machineTypeValidToDate as machineTypeValidToDate,
b.machineOffReasonCode as machineOffReasonCode,
b.machineOffReason as machineOffReason,
coalesce( a.createdBy , b.createdBy) as createdBy,
coalesce( a.createdDateTime , b.createdDateTime) as createdDateTime,
coalesce( a.lastUpdatedBy , b.lastUpdatedBy) as lastUpdatedBy,
coalesce( a.lastUpdatedDateTime , b.lastUpdatedDateTime) as lastUpdatedDateTime,
coalesce( a.validFromDate , b.validFromDate) as validFromDate,
coalesce( a.validToDate , b.validToDate) as validToDate
from bpg_id as a
full outer join bpg_cd as b
on a.businessPartnerGroupNumber = b.businessPartnerGroupNumber
where a.businessPartnerGroupNumber is NULL or b.businessPartnerGroupNumber is NULL
""")
src_d2.createOrReplaceTempView('src_d2')
#display(src_d2)

src_d=spark.sql("""
select * from src_d1
union all 
select * from src_d2
""")
src_d.createOrReplaceTempView('src_d')

# COMMAND ----------

# DBTITLE 1,Define fields and table names
keyColumns =  'businessPartnerGroupNumber'
mandatoryColumns = 'businessPartnerGroupNumber'

columns = ("""sourceSystemCode,
businessPartnerGroupNumber,
businessPartnerGroupCode,
businessPartnerGroup,
businessPartnerCategoryCode,
businessPartnerCategory,
businessPartnerTypeCode,
businessPartnerType,
externalNumber,
businessPartnerGUID,
businessPartnerGroupName1,
businessPartnerGroupName2,
paymentAssistSchemeFlag,
billAssistFlag,
consent1Indicator,
warWidowFlag,
indicatorCreatedUserId,
indicatorCreatedDate,
kidneyDialysisFlag,
patientUnit,
patientTitleCode,
patientTitle,
patientFirstName,
patientSurname,
patientAreaCode,
patientPhoneNumber,
hospitalCode,
hospitalName,
patientMachineTypeCode,
patientMachineType,
machineTypeValidFromDate,
machineTypeValidToDate,
machineOffReasonCode,
machineOffReason,
createdBy,
createdDateTime,
lastUpdatedBy,
lastUpdatedDateTime,
validFromDate,
validToDate
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




# COMMAND ----------

#ALWAYS RUN THIS AT THE END
RunTests()

# COMMAND ----------

tgt=spark.sql("Select * from curated_v2.dimBusinessPartnerGroup")
tgt1=spark.sql("Select * from curated_v2.dimBusinessPartnerGroup where sourceSystemCode is NULL ")
tgt.printSchema()
display(tgt)
display(tgt1)

# COMMAND ----------

a1=spark.sql("Select businessPartnerGroupCode,businessPartnerGroup,count(*) from curated_v2.dimBusinessPartnerGroup group by 1,2")
a2=spark.sql("Select businessPartnerCategoryCode,businessPartnerCategory,count(*) from curated_v2.dimBusinessPartnerGroup group by 1,2")
a3=spark.sql("Select businessPartnerTypeCode,businessPartnerType,count(*) from curated_v2.dimBusinessPartnerGroup group by 1,2")
a4=spark.sql("Select patientTitleCode,patientTitle,count(*) from curated_v2.dimBusinessPartnerGroup group by 1,2")
a5=spark.sql("Select hospitalCode,hospitalName,count(*) from curated_v2.dimBusinessPartnerGroup group by 1,2")
a6=spark.sql("Select patientMachineTypeCode,patientMachineType,count(*) from curated_v2.dimBusinessPartnerGroup group by 1,2")
a7=spark.sql("Select machineOffReasonCode,machineOffReason,count(*) from curated_v2.dimBusinessPartnerGroup group by 1,2")

display(a1)
display(a2)
display(a3)
display(a4)
display(a5)
display(a6)
display(a7)



a8=spark.sql("Select paymentAssistSchemeFlag,count(*) from curated_v2.dimBusinessPartnerGroup group by 1")
a9=spark.sql("Select billAssistFlag,count(*) from curated_v2.dimBusinessPartnerGroup group by 1")
a10=spark.sql("Select consent1Indicator,count(*) from curated_v2.dimBusinessPartnerGroup group by 1")
a11=spark.sql("Select warWidowFlag,count(*) from curated_v2.dimBusinessPartnerGroup group by 1")
a12=spark.sql("Select kidneyDialysisFlag,count(*) from curated_v2.dimBusinessPartnerGroup group by 1")
display(a8)
display(a9)
display(a10)
display(a11)
display(a12)



# COMMAND ----------

from pyspark.sql.functions import col
for name in tgt.columns: 
    rec=tgt.filter(col(name).isNull())
    print("column "+name+" count is: ", rec.count())
