# Databricks notebook source
# MAGIC %run ../../atf-common

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct businessPartnerIdNumber,businessPartnerNumber from bpi_c
# MAGIC where trim(businessPartnerIdNumber) <> businessPartnerIdNumber

# COMMAND ----------

# DBTITLE 1,SQL Query logic -Source
#fetching data from isu_0bp_def_address_attr for active and inactive records 
bpi_i= spark.sql("""
Select 'ISU' as sourceSystemCode,
businessPartnerIdNumber,
businessPartnerNumber,
identificationTypeCode,
identificationType,
validFromDate,
validToDate,
entryDate,
institute
--stateCode,
--countryShortName
,_recordCurrent
,_recordDeleted
,_recordStart
,_recordEnd
,_DLCleansedZoneTimeStamp
from cleansed.isu_0bp_id_attr
""")
bpi_i.createOrReplaceTempView("bpi_i")

bpi_ia=spark.sql("Select * except(_RecordCurrent,_recordDeleted,_DLCleansedZoneTimeStamp) from bpi_i where _RecordCurrent=1 and _recordDeleted=0 ")
bpi_id=spark.sql("""Select * except(_RecordCurrent,_recordDeleted,_DLCleansedZoneTimeStamp) from bpi_i where _RecordCurrent=0 and _recordDeleted=1 and _DLCleansedZoneTimeStamp > (select min(_DLCuratedZoneTimeStamp) from  curated_v2.dimBusinessPartnerIdentification)""")
bpi_ia.createOrReplaceTempView("bpi_ia")
bpi_id.createOrReplaceTempView("bpi_id")

#############################################################

#fetching data from crm_0bp_def_address_attr for active and inactive records 
bpi_c= spark.sql("""
Select 'CRM' as sourceSystemCode,
businessPartnerIdNumber,
businessPartnerNumber,
identificationTypeCode,
identificationType,
validFromDate,
validToDate,
entryDate,
institute
--stateCode,
--countryShortName
,_recordCurrent
,_recordDeleted
,_recordStart
,_recordEnd
,_DLCleansedZoneTimeStamp
from cleansed.crm_0bp_id_attr
""")
bpi_c.createOrReplaceTempView("bpi_c")

bpi_ca=spark.sql("Select * except(_RecordCurrent,_recordDeleted,_DLCleansedZoneTimeStamp) from bpi_c where _RecordCurrent=1 and _recordDeleted=0 ")
bpi_cd=spark.sql("""Select * except(_RecordCurrent,_recordDeleted,_DLCleansedZoneTimeStamp) from bpi_c where _RecordCurrent=0 and _recordDeleted=1 and _DLCleansedZoneTimeStamp > (select min(_DLCuratedZoneTimeStamp) from  curated_v2.dimbusinesspartnergroup)""")
bpi_ca.createOrReplaceTempView("bpi_ca")
bpi_cd.createOrReplaceTempView("bpi_cd")

#fetching data from isu data by performing inner join to fetch the common data for active records only . 
src_a1=spark.sql("""
Select 
a.sourceSystemCode  as sourceSystemCode ,
a.businessPartnerIdNumber as businessPartnerIdNumber ,
a.businessPartnerNumber  as businessPartnerNumber ,
a.identificationTypeCode  as identificationTypeCode ,
a.identificationType  as identificationType ,
a.validFromDate as validFromDate ,
a.validToDate  as validToDate ,
a.entryDate  as entryDate ,
a.institute as institute 
--a.stateCode  as stateCode ,
--a.countryShortName  as countryShortName 
from bpi_ia as a 
inner join bpi_ca as b
on a.businessPartnerNumber=b.businessPartnerNumber
and a.businessPartnerIdNumber=b.businessPartnerIdNumber
and a.identificationTypeCode=b.identificationTypeCode

""")
src_a1.createOrReplaceTempView("src_a1")

#considering both crm and ISU date using full outer join to fetch uncommon data out.
src_a2=spark.sql("""
Select 
coalesce( a.sourceSystemCode , b.sourceSystemCode ) as sourceSystemCode ,
coalesce( a.businessPartnerIdNumber , b.businessPartnerIdNumber ) as businessPartnerIdNumber ,
coalesce( a.businessPartnerNumber , b.businessPartnerNumber ) as businessPartnerNumber ,
coalesce( a.identificationTypeCode , b.identificationTypeCode ) as identificationTypeCode ,
coalesce( a.identificationType , b.identificationType ) as identificationType ,
coalesce( a.validFromDate , b.validFromDate ) as validFromDate ,
coalesce( a.validToDate , b.validToDate ) as validToDate ,
coalesce( a.entryDate , b.entryDate ) as entryDate ,
coalesce( a.institute , b.institute ) as institute 
--coalesce( a.stateCode , b.stateCode ) as stateCode ,
--coalesce( a.countryShortName , b.countryShortName ) as countryShortName 
from bpi_ia as a 
full outer join bpi_ca as b
on a.businessPartnerNumber=b.businessPartnerNumber
and a.businessPartnerIdNumber=b.businessPartnerIdNumber
and a.identificationTypeCode=b.identificationTypeCode
where (a.businessPartnerNumber is NULL or b.businessPartnerNumber is NULL) and (a.businessPartnerIdNumber is NULL or b.businessPartnerIdNumber is NULL) and (a.identificationTypeCode is NULL or b.identificationTypeCode is NULL)

""")
src_a2.createOrReplaceTempView("src_a2")

src_a=spark.sql("""
select * from src_a1
union all 
select * from src_a2
""")
src_a.createOrReplaceTempView('src_a')
#display(src_a)
#print(src_a.count())


#fetching data from isu data by performing inner join to fetch the common data for deleted records only . 
src_d1=spark.sql("""
Select 
a.sourceSystemCode  as sourceSystemCode ,
a.businessPartnerIdNumber as businessPartnerIdNumber ,
a.businessPartnerNumber  as businessPartnerNumber ,
a.identificationTypeCode  as identificationTypeCode ,
a.identificationType  as identificationType ,
a.validFromDate as validFromDate ,
a.validToDate  as validToDate ,
a.entryDate  as entryDate ,
a.institute as institute 
--a.stateCode  as stateCode ,
--a.countryShortName  as countryShortName 
from bpi_id as a 
inner join bpi_cd as b
on a.businessPartnerNumber=b.businessPartnerNumber
and a.businessPartnerIdNumber=b.businessPartnerIdNumber
and a.identificationTypeCode=b.identificationTypeCode

""")
src_d1.createOrReplaceTempView("src_d1")

#considering both crm and ISU date using full outer join to fetch uncommon data out.
src_d2=spark.sql("""
Select 
coalesce( a.sourceSystemCode , b.sourceSystemCode ) as sourceSystemCode ,
coalesce( a.businessPartnerIdNumber , b.businessPartnerIdNumber ) as businessPartnerIdNumber ,
coalesce( a.businessPartnerNumber , b.businessPartnerNumber ) as businessPartnerNumber ,
coalesce( a.identificationTypeCode , b.identificationTypeCode ) as identificationTypeCode ,
coalesce( a.identificationType , b.identificationType ) as identificationType ,
coalesce( a.validFromDate , b.validFromDate ) as validFromDate ,
coalesce( a.validToDate , b.validToDate ) as validToDate ,
coalesce( a.entryDate , b.entryDate ) as entryDate ,
coalesce( a.institute , b.institute ) as institute 
--coalesce( a.stateCode , b.stateCode ) as stateCode ,
--coalesce( a.countryShortName , b.countryShortName ) as countryShortName 
from bpi_id as a 
full outer join bpi_cd as b
on a.businessPartnerNumber=b.businessPartnerNumber
and a.businessPartnerIdNumber=b.businessPartnerIdNumber
and a.identificationTypeCode=b.identificationTypeCode
where (a.businessPartnerNumber is NULL or b.businessPartnerNumber is NULL) and (a.businessPartnerIdNumber is NULL or b.businessPartnerIdNumber is NULL) and (a.identificationTypeCode is NULL or b.identificationTypeCode is NULL)

""")
src_d2.createOrReplaceTempView("src_d2")



src_d=spark.sql("""
select * from src_d1
union all 
select * from src_d2
""")
src_d.createOrReplaceTempView('src_d')

# COMMAND ----------

# DBTITLE 1,Define fields and table names
keyColumns =  'businessPartnerIdNumber,businessPartnerNumber,identificationTypeCode'
mandatoryColumns = 'businessPartnerIdNumber,businessPartnerNumber,identificationTypeCode'


columns = ("""sourceSystemCode,
businessPartnerIdNumber,
businessPartnerNumber,
identificationTypeCode,
identificationType,
validFromDate,
validToDate,
entryDate,
institute
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

# MAGIC %sql
# MAGIC Select * from curated_v2.dimBusinessPartnerIdentification 
# MAGIC where BusinessPartnerNumber in ('0004356165','0004410860','0010232229','0011384713')
# MAGIC order by BusinessPartnerNumber,BusinessPartnerIdNumber

# COMMAND ----------

a11=spark.sql(f"Select {columns} from src_a where BusinessPartnerNumber in ('0010185382','0010203918','0010806918','0010080205','0001225588','0001225589','0011509873') order by BusinessPartnerNumber ")
a12=spark.sql(f"Select {columns} from curated_v2.dimBusinessPartnerIdentification where BusinessPartnerNumber in ('0010185382','0010203918','0010806918','0010080205','0001225588','0001225589','0011509873') order by BusinessPartnerNumber")

display(a11)
display(a12)

# COMMAND ----------

source_a = spark.sql(f"""
Select businessPartnerIdNumber,businessPartnerNumber,identificationTypeCode
From
src_a
""")
tgt2=spark.sql(f"Select businessPartnerIdNumber,businessPartnerNumber,identificationTypeCode from curated_v2.dimBusinessPartnerIdentification where _recordDeleted=0")
print(source_a.count())
print(tgt2.count())
display(source_a.subtract(tgt2))
display(tgt2.subtract(source_a))


# COMMAND ----------

# DBTITLE 1,Schema and Data Validation
tgt=spark.sql("Select * from curated_v2.dimBusinessPartnerIdentification")
tgt1=spark.sql("Select * from curated_v2.dimBusinessPartnerIdentification where sourceSystemCode is NULL ")
tgt.printSchema()
display(tgt)
display(tgt1)


# COMMAND ----------

a1=spark.sql("Select identificationTypeCode,identificationType,count(*) from curated_v2.dimBusinessPartnerIdentification group by 1,2")
display(a1)


