# Databricks notebook source
# MAGIC %run ../../atf-common

# COMMAND ----------

# DBTITLE 1,SQL Query logic -Source
#fetching data from isu_0bp_def_address_attr for active and inactive records 
bp_i= spark.sql("""
Select 'ISU' as sourceSystemCode,
businessPartnerNumber,
businessPartnerCategoryCode,
businessPartnerCategory,
businessPartnerTypeCode,
businessPartnerType,
businessPartnerGroupCode,
businessPartnerGroup,
externalBusinessPartnerNumber,
businessPartnerGUID,
firstName,
lastName,
middleName,
nickName,
titleCode,
title,
dateOfBirth,
dateOfDeath,
validFromDate,
validToDate,
naturalPersonflag, -- name is changed as per the mapping
personNumber,
personnelNumber,
organizationName1,
organizationName2,
organizationName3,
organizationName,
organizationFoundedDate,
createdDateTime,
createdBy,
lastUpdatedDateTime, -- changedDateTime,
lastUpdatedBy --changedBy

,_recordCurrent
,_recordDeleted
,_recordStart
,_recordEnd
from cleansed.isu_0bpartner_attr
""")
bp_i.createOrReplaceTempView("bp_i")
bp_ia=spark.sql("Select * except(_RecordCurrent,_recordDeleted) from bp_i where _RecordCurrent=1 and _recordDeleted=0 ")
bp_id=spark.sql("Select * except(_RecordCurrent,_recordDeleted) from bp_i where _RecordCurrent=0 and _recordDeleted=1 ")
bp_ia.createOrReplaceTempView("bp_ia")
bp_id.createOrReplaceTempView("bp_id")
#############################################################
#fetching data from crm_0bp_def_address_attr for active and inactive records 
bp_c= spark.sql("""
Select 'CRM' as sourceSystemCode,
businessPartnerNumber,
businessPartnerCategoryCode,
businessPartnerCategory,
businessPartnerTypeCode,
businessPartnerType,
businessPartnerGroupCode,
businessPartnerGroup,
externalBusinessPartnerNumber,
businessPartnerGUID,
firstName,
lastName,
middleName,
nickName,
titleCode,
title,
dateOfBirth,
dateOfDeath,
validFromDate,
validToDate,
naturalPersonflag, -- change in mapping
personNumber,
personnelNumber,
organizationName1,
organizationName2,
organizationName3,
organizationName,
organizationFoundedDate,
createdDateTime,
createdBy
,warWidowflag
,deceasedflag
,disabilityflag
,goldCardHolderflag
,consent1Indicator
,consent2Indicator
,eligibilityflag
,paymentAssistSchemeflag
,plannedChangeDocument
,paymentStartDate
,dateOfCheck
,pensionConcessionCardflag
,pensionType
,createdDateTime
,createdBy
,lastUpdatedDateTime
,lastUpdatedBy
,_recordCurrent
,_recordDeleted
,_recordStart
,_recordEnd
from cleansed.crm_0bpartner_attr
""")
bp_c.createOrReplaceTempView("bp_c")
bp_ca=spark.sql("Select * except(_RecordCurrent,_recordDeleted) from bp_c where _RecordCurrent=1 and _recordDeleted=0 ")
bp_cd=spark.sql("Select * except(_RecordCurrent,_recordDeleted) from bp_c where _RecordCurrent=0 and _recordDeleted=1 ")
bp_ca.createOrReplaceTempView("bp_ca")
bp_cd.createOrReplaceTempView("bp_cd")

#fetching data from isu data by performing inner join to fetch the common data for active records only . 
src_a1=spark.sql("""
Select 
a.sourceSystemCode as sourceSystemCode,
a.businessPartnerNumber as businessPartnerNumber,
a.businessPartnerCategoryCode as businessPartnerCategoryCode,
a.businessPartnerCategory as businessPartnerCategory,
a.businessPartnerTypeCode as businessPartnerTypeCode,
a.businessPartnerType as businessPartnerType,
a.businessPartnerGroupCode as businessPartnerGroupCode,
a.businessPartnerGroup as businessPartnerGroup,
a.externalBusinessPartnerNumber as externalNumber,
a.businessPartnerGUID as businessPartnerGUID,
a.firstName as firstName,
a.lastName as lastName,
a.middleName as middleName,
a.nickName as nickName,
a.titleCode as titleCode,
a.title as title,
a.dateOfBirth as dateOfBirth,
a.dateOfDeath as dateOfDeath,
a.validFromDate as validFromDate,
a.validToDate as validToDate,
b.warWidowflag as warWidowflag,
b.deceasedflag as deceasedflag,
b.disabilityflag as disabilityflag,
b.goldCardHolderflag as goldCardHolderflag,
coalesce(a.naturalPersonflag,b.naturalPersonflag ) as naturalPersonflag,
b.consent1Indicator as consent1Indicator,
b.consent2Indicator as consent2Indicator,
b.eligibilityflag as eligibilityflag,
b.paymentAssistSchemeflag as paymentAssistSchemeflag,
b.plannedChangeDocument as plannedChangeDocument,
b.paymentStartDate as paymentStartDate,
b.dateOfCheck as dateOfCheck,
b.pensionConcessionCardflag as pensionConcessionCardflag,
b.pensionType as pensionType,
a.personNumber as personNumber,
a.personnelNumber as personnelNumber,
-- case when a.businessPartnerCategoryCode = '2' then trim(concat_ws(' ',a.organizationName1,a.organizationName2,a.organizationName3) ) else a.organizationName1 end as organizationName
--,case when  a.businessPartnerCategoryCode= '2' then a.organizationFoundedDate else null end as organizationFoundedDate,
trim(a.organizationName) as organizationName,
a.organizationFoundedDate,
a.createdDateTime as createdDateTime,
a.createdBy as createdBy,
a.lastUpdatedDateTime as lastUpdatedDateTime,
a.lastUpdatedBy as lastUpdatedBy
from bp_ia as a 
inner join bp_ca as b
on a.businessPartnerNumber=b.businessPartnerNumber
""")
src_a1.createOrReplaceTempView("src_a1")


#fetching data uncommon data from both ISU and CRM table for active records only . 
src_a2=spark.sql("""
Select 
coalesce(a.sourceSystemCode,b.sourceSystemCode ) as sourceSystemCode,
coalesce(a.businessPartnerNumber,b.businessPartnerNumber ) as businessPartnerNumber,
coalesce(a.businessPartnerCategoryCode,b.businessPartnerCategoryCode ) as businessPartnerCategoryCode,
coalesce(a.businessPartnerCategory,b.businessPartnerCategory ) as businessPartnerCategory,
coalesce(a.businessPartnerTypeCode,b.businessPartnerTypeCode ) as businessPartnerTypeCode,
coalesce(a.businessPartnerType,b.businessPartnerType ) as businessPartnerType,
coalesce(a.businessPartnerGroupCode,b.businessPartnerGroupCode ) as businessPartnerGroupCode,
coalesce(a.businessPartnerGroup,b.businessPartnerGroup ) as businessPartnerGroup,
coalesce(a.externalBusinessPartnerNumber,b.externalBusinessPartnerNumber ) as externalNumber,
coalesce(a.businessPartnerGUID,b.businessPartnerGUID ) as businessPartnerGUID,
coalesce(a.firstName,b.firstName ) as firstName,
coalesce(a.lastName,b.lastName ) as lastName,
coalesce(a.middleName,b.middleName ) as middleName,
coalesce(a.nickName,b.nickName ) as nickName,
coalesce(a.titleCode,b.titleCode ) as titleCode,
coalesce(a.title,b.title ) as title,
coalesce(a.dateOfBirth,b.dateOfBirth ) as dateOfBirth,
coalesce(a.dateOfDeath,b.dateOfDeath ) as dateOfDeath,
coalesce(a.validFromDate,b.validFromDate ) as validFromDate,
coalesce(a.validToDate,b.validToDate ) as validToDate,
b.warWidowflag as warWidowflag,
b.deceasedflag as deceasedflag,
b.disabilityflag as disabilityflag,
b.goldCardHolderflag as goldCardHolderflag,
coalesce(a.naturalPersonflag,b.naturalPersonflag ) as naturalPersonflag,
b.consent1Indicator as consent1Indicator,
b.consent2Indicator as consent2Indicator,
b.eligibilityflag as eligibilityflag,
b.paymentAssistSchemeflag as paymentAssistSchemeflag,
b.plannedChangeDocument as plannedChangeDocument,
b.paymentStartDate as paymentStartDate,
b.dateOfCheck as dateOfCheck,
b.pensionConcessionCardflag as pensionConcessionCardflag,
b.pensionType as pensionType,
coalesce(a.personNumber,b.personNumber) as personNumber,
coalesce(a.personnelNumber,b.personnelNumber) as personnelNumber,
--case when coalesce(a.businessPartnerCategoryCode,b.businessPartnerCategoryCode ) = '2' then trim(concat_ws(' ',coalesce(a.organizationName1,b.organizationName1),coalesce(a.organizationName2,b.organizationName2), coalesce(a.organizationName3,b.organizationName3)) ) else coalesce(a.organizationName1,b.organizationName1) end as organizationName
--,case when  coalesce(a.businessPartnerCategoryCode,b.businessPartnerCategoryCode ) = '2' then coalesce(a.organizationFoundedDate,b.organizationFoundedDate) else null end as organizationFoundedDate,

coalesce(trim(a.organizationName),trim(b.organizationName)) as organizationName,
coalesce(a.organizationFoundedDate,b.organizationFoundedDate) as organizationFoundedDate,

coalesce(a.createdDateTime,b.createdDateTime ) as createdDateTime,
coalesce(a.createdBy,b.createdBy ) as createdBy,
coalesce(a.lastUpdatedDateTime,b.lastUpdatedDateTime ) as lastUpdatedDateTime,
coalesce(a.lastUpdatedBy,b.lastUpdatedBy ) as lastUpdatedBy
from bp_ia as a 
full outer join bp_ca as b
on a.businessPartnerNumber=b.businessPartnerNumber
where (a.businessPartnerNumber is NULL or b.businessPartnerNumber is NULL)

""")
src_a2.createOrReplaceTempView("src_a2")


#union for both active records.
src_a=spark.sql("""
Select * from src_a1 where businessPartnerCategoryCode <> 3
union all 
select * from src_a2 where businessPartnerCategoryCode <> 3
""")
src_a.createOrReplaceTempView("src_a")
#display(src_a)
#print(src_a.count())


#fetching data from isu data by performing inner join to fetch the common data for deleted records only . 
src_d1=spark.sql("""
Select 
a.sourceSystemCode as sourceSystemCode,
a.businessPartnerNumber as businessPartnerNumber,
a.businessPartnerCategoryCode as businessPartnerCategoryCode,
a.businessPartnerCategory as businessPartnerCategory,
a.businessPartnerTypeCode as businessPartnerTypeCode,
a.businessPartnerType as businessPartnerType,
a.businessPartnerGroupCode as businessPartnerGroupCode,
a.businessPartnerGroup as businessPartnerGroup,
a.externalBusinessPartnerNumber as externalNumber,
a.businessPartnerGUID as businessPartnerGUID,
a.firstName as firstName,
a.lastName as lastName,
a.middleName as middleName,
a.nickName as nickName,
a.titleCode as titleCode,
a.title as title,
a.dateOfBirth as dateOfBirth,
a.dateOfDeath as dateOfDeath,
a.validFromDate as validFromDate,
a.validToDate as validToDate,
b.warWidowflag as warWidowflag,
b.deceasedflag as deceasedflag,
b.disabilityflag as disabilityflag,
b.goldCardHolderflag as goldCardHolderflag,
coalesce(a.naturalPersonflag,b.naturalPersonflag ) as naturalPersonflag,
b.consent1Indicator as consent1Indicator,
b.consent2Indicator as consent2Indicator,
b.eligibilityflag as eligibilityflag,
b.paymentAssistSchemeflag as paymentAssistSchemeflag,
b.plannedChangeDocument as plannedChangeDocument,
b.paymentStartDate as paymentStartDate,
b.dateOfCheck as dateOfCheck,
b.pensionConcessionCardflag as pensionConcessionCardflag,
b.pensionType as pensionType,
a.personNumber as personNumber,
a.personnelNumber as personnelNumber,
-- case when a.businessPartnerCategoryCode = '2' then trim(concat_ws(' ',a.organizationName1,a.organizationName2,a.organizationName3) ) else a.organizationName1 end as organizationName
--,case when  a.businessPartnerCategoryCode= '2' then a.organizationFoundedDate else null end as organizationFoundedDate,
trim(a.organizationName) as organizationName,
a.organizationFoundedDate,
a.createdDateTime as createdDateTime,
a.createdBy as createdBy,
a.lastUpdatedDateTime as lastUpdatedDateTime,
a.lastUpdatedBy as lastUpdatedBy
from bp_id as a 
inner join bp_cd as b
on a.businessPartnerNumber=b.businessPartnerNumber
""")
src_d1.createOrReplaceTempView("src_d1")
#display(src_d1)
#print(src_d1.count())

#fetching data uncommon data from both ISU and CRM table for active records only . 
src_d2=spark.sql("""
Select 
coalesce(a.sourceSystemCode,b.sourceSystemCode ) as sourceSystemCode,
coalesce(a.businessPartnerNumber,b.businessPartnerNumber ) as businessPartnerNumber,
coalesce(a.businessPartnerCategoryCode,b.businessPartnerCategoryCode ) as businessPartnerCategoryCode,
coalesce(a.businessPartnerCategory,b.businessPartnerCategory ) as businessPartnerCategory,
coalesce(a.businessPartnerTypeCode,b.businessPartnerTypeCode ) as businessPartnerTypeCode,
coalesce(a.businessPartnerType,b.businessPartnerType ) as businessPartnerType,
coalesce(a.businessPartnerGroupCode,b.businessPartnerGroupCode ) as businessPartnerGroupCode,
coalesce(a.businessPartnerGroup,b.businessPartnerGroup ) as businessPartnerGroup,
coalesce(a.externalBusinessPartnerNumber,b.externalBusinessPartnerNumber ) as externalNumber,
coalesce(a.businessPartnerGUID,b.businessPartnerGUID ) as businessPartnerGUID,
coalesce(a.firstName,b.firstName ) as firstName,
coalesce(a.lastName,b.lastName ) as lastName,
coalesce(a.middleName,b.middleName ) as middleName,
coalesce(a.nickName,b.nickName ) as nickName,
coalesce(a.titleCode,b.titleCode ) as titleCode,
coalesce(a.title,b.title ) as title,
coalesce(a.dateOfBirth,b.dateOfBirth ) as dateOfBirth,
coalesce(a.dateOfDeath,b.dateOfDeath ) as dateOfDeath,
coalesce(a.validFromDate,b.validFromDate ) as validFromDate,
coalesce(a.validToDate,b.validToDate ) as validToDate,
b.warWidowflag as warWidowflag,
b.deceasedflag as deceasedflag,
b.disabilityflag as disabilityflag,
b.goldCardHolderflag as goldCardHolderflag,
coalesce(a.naturalPersonflag,b.naturalPersonflag ) as naturalPersonflag,
b.consent1Indicator as consent1Indicator,
b.consent2Indicator as consent2Indicator,
b.eligibilityflag as eligibilityflag,
b.paymentAssistSchemeflag as paymentAssistSchemeflag,
b.plannedChangeDocument as plannedChangeDocument,
b.paymentStartDate as paymentStartDate,
b.dateOfCheck as dateOfCheck,
b.pensionConcessionCardflag as pensionConcessionCardflag,
b.pensionType as pensionType,
coalesce(a.personNumber,b.personNumber) as personNumber,
coalesce(a.personnelNumber,b.personnelNumber) as personnelNumber,
--case when coalesce(a.businessPartnerCategoryCode,b.businessPartnerCategoryCode ) = '2' then trim(concat_ws(' ',coalesce(a.organizationName1,b.organizationName1),coalesce(a.organizationName2,b.organizationName2), coalesce(a.organizationName3,b.organizationName3)) ) else coalesce(a.organizationName1,b.organizationName1) end as organizationName
--,case when  coalesce(a.businessPartnerCategoryCode,b.businessPartnerCategoryCode ) = '2' then coalesce(a.organizationFoundedDate,b.organizationFoundedDate) else null end as organizationFoundedDate,

coalesce(trim(a.organizationName),trim(b.organizationName)) as organizationName,
coalesce(a.organizationFoundedDate,b.organizationFoundedDate) as organizationFoundedDate,

coalesce(a.createdDateTime,b.createdDateTime ) as createdDateTime,
coalesce(a.createdBy,b.createdBy ) as createdBy,
coalesce(a.lastUpdatedDateTime,b.lastUpdatedDateTime ) as lastUpdatedDateTime,
coalesce(a.lastUpdatedBy,b.lastUpdatedBy ) as lastUpdatedBy
from bp_id as a 
full outer join bp_cd as b
on a.businessPartnerNumber=b.businessPartnerNumber
where (a.businessPartnerNumber is NULL or b.businessPartnerNumber is NULL)


""")
src_d2.createOrReplaceTempView("src_d2")
#display(src_d2)
#print(src_d2.count())

#union for both active records.
src_d=spark.sql("""
Select * from src_d1 where businessPartnerCategoryCode <> 3
union all 
select * from src_d2 where businessPartnerCategoryCode <> 3
""")
src_d.createOrReplaceTempView("src_d")
#display(src_d)
#print(src_d.count())

# COMMAND ----------

# DBTITLE 1,Define fields and table names
keyColumns =  'businessPartnerNumber'
mandatoryColumns = 'businessPartnerNumber'


columns = ("""sourceSystemCode,
businessPartnerNumber,
businessPartnerCategoryCode,
businessPartnerCategory,
businessPartnerTypeCode,
businessPartnerType,
businessPartnerGroupCode,
businessPartnerGroup,
externalNumber,
businessPartnerGUID,
firstName,
lastName,
middleName,
nickName,
titleCode,
title,
dateOfBirth,
dateOfDeath,
validFromDate,
validToDate,
warWidowFlag,
deceasedFlag,
disabilityFlag,
goldCardHolderFlag,
naturalPersonFlag,
consent1Indicator,
consent2Indicator,
eligibilityFlag,
paymentAssistSchemeFlag,
plannedChangeDocument,
paymentStartDate,
dateOfCheck,
pensionConcessionCardFlag,
pensionType,
personNumber,
personnelNumber,
organizationName,
organizationFoundedDate,
createdDateTime,
createdBy,
lastUpdatedDateTime,
lastUpdatedBy
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

tgt=spark.sql("Select * from curated_v2.dimBusinessPartner")
tgt1=spark.sql("Select * from curated_v2.dimBusinessPartner where sourceSystemCode is NULL")
tgt.printSchema()
display(tgt)
display(tgt1)

# COMMAND ----------

a1=spark.sql("Select businessPartnerCategoryCode,businessPartnerCategory,count(*) from curated_v2.dimBusinessPartner group by 1,2")
a2=spark.sql("Select businessPartnerTypeCode,businessPartnerType,count(*) from curated_v2.dimBusinessPartner group by 1,2")
a3=spark.sql("Select businessPartnerGroupCode,businessPartnerGroup,count(*) from curated_v2.dimBusinessPartner group by 1,2")
a4=spark.sql("Select titleCode,title,count(*) from curated_v2.dimBusinessPartner group by 1,2")
a5=spark.sql("Select warWidowFlag,count(*) from curated_v2.dimBusinessPartner group by 1")
a6=spark.sql("Select deceasedFlag,count(*) from curated_v2.dimBusinessPartner group by 1")
a7=spark.sql("Select disabilityFlag,count(*) from curated_v2.dimBusinessPartner group by 1")
a8=spark.sql("Select goldCardHolderFlag,count(*) from curated_v2.dimBusinessPartner group by 1")
a9=spark.sql("Select naturalPersonFlag,count(*) from curated_v2.dimBusinessPartner group by 1")
a10=spark.sql("Select consent1Indicator,count(*) from curated_v2.dimBusinessPartner group by 1")
a11=spark.sql("Select consent2Indicator,count(*) from curated_v2.dimBusinessPartner group by 1")
a12=spark.sql("Select eligibilityFlag,count(*) from curated_v2.dimBusinessPartner group by 1")
a13=spark.sql("Select paymentAssistSchemeFlag,count(*) from curated_v2.dimBusinessPartner group by 1")
a14=spark.sql("Select pensionType,count(*) from curated_v2.dimBusinessPartner group by 1")

display(a1)
display(a2)
display(a3)
display(a4)
display(a5)
display(a6)
display(a7)
display(a8)
display(a9)
display(a10)
display(a11)
display(a12)
display(a13)
display(a14)


# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from curated_v2.dimBusinessPartner
# MAGIC where length(businessPartnerNumber)=8

# COMMAND ----------

tgt=spark.sql("Select * from curated_v2.dimBusinessPartner")
from pyspark.sql.functions import col
for name in tgt.columns: 
    rec=tgt.filter(col(name).isNull())
    print("column "+name+" count is: ", rec.count())

# COMMAND ----------

dimBusinssPArtner()
1. define Key columns
2. Mandatory Columns
3. Source query 

