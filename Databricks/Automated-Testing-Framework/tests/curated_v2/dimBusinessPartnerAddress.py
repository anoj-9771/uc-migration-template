# Databricks notebook source
# MAGIC %run ../../atf-common

# COMMAND ----------

# DBTITLE 1,SQL Query logic -Source
#fetching data from isu_0bp_def_address_attr for active and inactive records 
bpa_i= spark.sql("""
Select 'ISU' as sourceSystemCode,
addressNumber as businessPartnerAddressNumber,
businessPartnerNumber as businessPartnerNumber,
validFromDate as addressValidFromDate,
validToDate as addressValidToDate,
phoneNumber as phoneNumber,
phoneExtension as phoneExtension, 
faxNumber as faxNumber,
faxExtension  as faxExtension, 
emailAddress as emailAddress,
personalAddressFlag as personalAddressFlag,
coName as coName,
shortFormattedAddress2 as shortFormattedAddress2,
streetLine5 as streetLine5,
building as building,
floorNumber as floorNumber,
apartmentNumber as apartmentNumber,
housePrimaryNumber as housePrimaryNumber,
houseSupplementNumber as houseSupplementNumber,
streetName as streetPrimaryName,
streetSupplementName1 as streetSupplementName1, --chk
streetSupplementName2 as streetSupplementName2, --chk
otherLocationName as otherLocationName, --chk
trim(concat_ws(', ',trim(streetLine5),trim(building),trim(floorNumber),trim(apartmentNumber),trim(houseSupplementNumber),trim(housePrimaryNumber))) as houseNumber,
trim(concat_ws(', ',trim(streetName),trim(streetSupplementName1),trim(streetSupplementName2),trim(otherLocationName))) as streetName,
streetCode as streetCode,
cityName as cityName,
cityCode as cityCode,
case when (countryCode = 'AU' and cityCode is null) then '' else stateCode end as stateCode, -- If COUNTRY = 'AU' and CITY1 is null then '' chk
case when (countryCode = 'AU' and cityCode is null) then '' else stateName end as stateName,
postalCode as postalCode,
--case when (countryCode = 'AU' and cityName is null) then '' else countryCode end as countryCode, -- If COUNTRY = 'AU' and CITY1 is null then '' CHK
countryCode as countryCode,
countryName as countryName, --chk
case when (countryCode = 'AU' and cityCode is null) then '' else trim(concat_ws(', ',trim(streetLine5),trim(building),trim(floorNumber),trim(apartmentNumber),trim(houseSupplementNumber),trim(housePrimaryNumber),trim(streetName),trim(streetSupplementName1),trim(streetSupplementName2),trim(otherLocationName),trim(deliveryServiceTypeCode),trim(deliveryServiceNumber),trim(cityName),trim(stateCode),trim(postalCode))) end as addressFullText,
poBoxCode as poBoxCode,
poBoxCity as poBoxCity,
postalCodeExtension as postalCodeExtension,
poBoxExtension as poBoxExtension,
deliveryServiceTypeCode as deliveryServiceTypeCode,
deliveryServiceType as deliveryServiceType,
deliveryServiceNumber as deliveryServiceNumber,
addressTimeZone as addressTimeZone,
communicationAddressNumber as communicationAddressNumber
,_RecordCurrent,_recordDeleted
from cleansed.isu_0bp_def_address_attr
where addressNumber <> ''
""")
bpa_i.createOrReplaceTempView("bpa_i")
bpa_ia=spark.sql("Select * except(_RecordCurrent,_recordDeleted) from bpa_i where _RecordCurrent=1 and _recordDeleted=0 ")
bpa_id=spark.sql("Select * except(_RecordCurrent,_recordDeleted) from bpa_i where _RecordCurrent=0 and _recordDeleted=1 ")
bpa_ia.createOrReplaceTempView("bpa_ia")
bpa_id.createOrReplaceTempView("bpa_id")
#############################################################
#fetching data from crm_0bp_def_address_attr for active and inactive records 
bpa_c= spark.sql("""
Select 'CRM' as sourceSystemCode,
addressNumber as businessPartnerAddressNumber,
businessPartnerNumber as businessPartnerNumber,
validFromDate as addressValidFromDate,
validToDate as addressValidToDate,
phoneNumber as phoneNumber,
phoneExtension as phoneExtension, -- this field is not there in cleansed layer
faxNumber as faxNumber,
faxExtension  as faxExtension, -- this field is not there in cleansed layer
emailAddress as emailAddress,
personalAddressFlag  as personalAddressFlag,
coName as coName,
shortFormattedAddress2 as shortFormattedAddress2,
streetLine5 as streetLine5,
building as building,
floorNumber as floorNumber,
apartmentNumber as apartmentNumber,
housePrimaryNumber as housePrimaryNumber,
houseSupplementNumber as houseSupplementNumber,
streetName as streetPrimaryName,
streetSupplementName1 as streetSupplementName1, --chk
streetSupplementName2 as streetSupplementName2, --chk
otherLocationName as otherLocationName, --chk
trim(concat_ws(', ',trim(streetLine5),trim(building),trim(floorNumber),trim(apartmentNumber),trim(houseSupplementNumber),trim(housePrimaryNumber))) as houseNumber,
trim(concat_ws(', ',trim(streetName),trim(streetSupplementName1),trim(streetSupplementName2),trim(otherLocationName))) as streetName,
streetCode as streetCode,
cityName as cityName,
cityCode as cityCode,
case when (countryCode = 'AU' and cityCode is null) then '' else stateCode end as stateCode, -- If COUNTRY = 'AU' and CITY1 is null then '' chk
case when (countryCode = 'AU' and cityCode is null) then '' else stateName end as stateName,
postalCode as postalCode,
--case when (countryCode = 'AU' and cityName is null) then '' else countryCode end as countryCode, -- If COUNTRY = 'AU' and CITY1 is null then '' CHK
countryCode as countryCode,
countryName as countryName, --chk
case when (countryCode = 'AU' and cityCode is null) then '' else trim(concat_ws(', ',trim(streetLine5),trim(building),trim(floorNumber),trim(apartmentNumber),trim(houseSupplementNumber),trim(housePrimaryNumber),trim(streetName),trim(streetSupplementName1),trim(streetSupplementName2),trim(otherLocationName),trim(cityName),trim(stateCode),trim(postalCode))) end as addressFullText,
poBoxCode as poBoxCode,
poBoxCity as poBoxCity,
NULL  as postalCodeExtension,
NULL as poBoxExtension,
NULL as deliveryServiceTypeCode,
NULL as deliveryServiceType,
NULL as deliveryServiceNumber, 
addressTimeZone as addressTimeZone,
communicationAddressNumber as communicationAddressNumber
,_RecordCurrent,_recordDeleted
from cleansed.crm_0bp_def_address_attr
where addressNumber <> ''
""")
bpa_c.createOrReplaceTempView("bpa_c")
bpa_ca=spark.sql("Select * except(_RecordCurrent,_recordDeleted) from bpa_c where _RecordCurrent=1 and _recordDeleted=0 ")
bpa_cd=spark.sql("Select * except(_RecordCurrent,_recordDeleted) from bpa_c where _RecordCurrent=0 and _recordDeleted=1 ")
bpa_ca.createOrReplaceTempView("bpa_ca")
bpa_cd.createOrReplaceTempView("bpa_cd")

#fetching data from isu data by performing inner join to fetch the common data for active records only . 
src_a1=spark.sql("""
Select a.sourceSystemCode,
a.businessPartnerAddressNumber,
a.businessPartnerNumber,
a.addressValidFromDate,
a.addressValidToDate,
a.phoneNumber,
a.phoneExtension,
a.faxNumber,
a.faxExtension,
a.emailAddress,
a.personalAddressFlag,
a.coName,
a.shortFormattedAddress2,
a.streetLine5,
a.building,
a.floorNumber,
a.apartmentNumber,
a.housePrimaryNumber,
a.houseSupplementNumber,
a.streetPrimaryName,
a.streetSupplementName1,
a.streetSupplementName2,
a.otherLocationName,
a.houseNumber,
a.streetName,
a.streetCode,
a.cityName,
a.cityCode,
a.stateCode,
a.stateName,
a.postalCode,
a.countryCode,
a.countryName,
a.addressFullText,
a.poBoxCode,
a.poBoxCity,
a.postalCodeExtension,
a.poBoxExtension,
a.deliveryServiceTypeCode,
a.deliveryServiceType,
a.deliveryServiceNumber,
a.addressTimeZone,
a.communicationAddressNumber
from bpa_ia as a 
inner join bpa_ca as b 
on  a.businessPartnerNumber=b.businessPartnerNumber
and a.businessPartnerAddressNumber=b.businessPartnerAddressNumber
""")
src_a1.createOrReplaceTempView("src_a1")
#display(src_a1)
print(src_a1.count())

#fetching data uncommon data from both ISU and CRM table for active records only . 
src_a2=spark.sql("""
Select 
coalesce( a.sourceSystemCode , b.sourceSystemCode ) as sourceSystemCode ,
coalesce( a.businessPartnerAddressNumber , b.businessPartnerAddressNumber ) as businessPartnerAddressNumber ,
coalesce( a.businessPartnerNumber , b.businessPartnerNumber ) as businessPartnerNumber ,
coalesce( a.addressValidFromDate , b.addressValidFromDate ) as addressValidFromDate ,
coalesce( a.addressValidToDate , b.addressValidToDate ) as addressValidToDate ,
coalesce( a.phoneNumber , b.phoneNumber ) as phoneNumber ,
coalesce( a.phoneExtension , b.phoneExtension ) as phoneExtension ,
coalesce( a.faxNumber , b.faxNumber ) as faxNumber ,
coalesce( a.faxExtension , b.faxExtension ) as faxExtension ,
coalesce( a.emailAddress , b.emailAddress ) as emailAddress ,
coalesce( a.personalAddressFlag , b.personalAddressFlag ) as personalAddressFlag ,
coalesce( a.coName , b.coName ) as coName ,
coalesce( a.shortFormattedAddress2 , b.shortFormattedAddress2 ) as shortFormattedAddress2 ,
coalesce( a.streetLine5 , b.streetLine5 ) as streetLine5 ,
coalesce( a.building , b.building ) as building ,
coalesce( a.floorNumber , b.floorNumber ) as floorNumber ,
coalesce( a.apartmentNumber , b.apartmentNumber ) as apartmentNumber ,
coalesce( a.housePrimaryNumber , b.housePrimaryNumber ) as housePrimaryNumber ,
coalesce( a.houseSupplementNumber , b.houseSupplementNumber ) as houseSupplementNumber ,
coalesce( a.streetPrimaryName , b.streetPrimaryName ) as streetPrimaryName ,
coalesce( a.streetSupplementName1 , b.streetSupplementName1 ) as streetSupplementName1 ,
coalesce( a.streetSupplementName2 , b.streetSupplementName2 ) as streetSupplementName2 ,
coalesce( a.otherLocationName , b.otherLocationName ) as otherLocationName ,
coalesce( a.houseNumber , b.houseNumber ) as houseNumber ,
coalesce( a.streetName , b.streetName ) as streetName ,
coalesce( a.streetCode , b.streetCode ) as streetCode ,
coalesce( a.cityName , b.cityName ) as cityName ,
coalesce( a.cityCode , b.cityCode ) as cityCode ,
coalesce( a.stateCode , b.stateCode ) as stateCode ,
coalesce( a.stateName , b.stateName ) as stateName ,
coalesce( a.postalCode , b.postalCode ) as postalCode ,
coalesce( a.countryCode , b.countryCode ) as countryCode ,
coalesce( a.countryName , b.countryName ) as countryName ,
coalesce( a.addressFullText , b.addressFullText ) as addressFullText ,
coalesce( a.poBoxCode , b.poBoxCode ) as poBoxCode ,
coalesce( a.poBoxCity , b.poBoxCity ) as poBoxCity ,
coalesce( a.postalCodeExtension , b.postalCodeExtension ) as postalCodeExtension ,
coalesce( a.poBoxExtension , b.poBoxExtension ) as poBoxExtension ,
coalesce( a.deliveryServiceTypeCode , b.deliveryServiceTypeCode ) as deliveryServiceTypeCode ,
coalesce( a.deliveryServiceType , b.deliveryServiceType ) as deliveryServiceType ,
coalesce( a.deliveryServiceNumber , b.deliveryServiceNumber ) as deliveryServiceNumber ,
coalesce( a.addressTimeZone , b.addressTimeZone ) as addressTimeZone ,
coalesce( a.communicationAddressNumber , b.communicationAddressNumber ) as communicationAddressNumber 
from bpa_ia as a 
full outer join bpa_ca as b
on  a.businessPartnerNumber=b.businessPartnerNumber
and a.businessPartnerAddressNumber=b.businessPartnerAddressNumber
where ( a.businessPartnerNumber is NULL or b.businessPartnerNumber is NULL) and ( a.businessPartnerAddressNumber is NULL or b.businessPartnerAddressNumber is NULL)

""")
src_a2.createOrReplaceTempView("src_a2")
#display(src_a2)
print(src_a2.count())

#union for both active records.
src_a=spark.sql("""
Select * from src_a1
union all 
select * from src_a2
""")
src_a.createOrReplaceTempView("src_a")
#display(src_a)
print(src_a.count())


#fetching data from isu data by performing inner join to fetch the common data for deleted records only . 
src_d1=spark.sql("""
Select a.sourceSystemCode,
a.businessPartnerAddressNumber,
a.businessPartnerNumber,
a.addressValidFromDate,
a.addressValidToDate,
a.phoneNumber,
a.phoneExtension,
a.faxNumber,
a.faxExtension,
a.emailAddress,
a.personalAddressFlag,
a.coName,
a.shortFormattedAddress2,
a.streetLine5,
a.building,
a.floorNumber,
a.apartmentNumber,
a.housePrimaryNumber,
a.houseSupplementNumber,
a.streetPrimaryName,
a.streetSupplementName1,
a.streetSupplementName2,
a.otherLocationName,
a.houseNumber,
a.streetName,
a.streetCode,
a.cityName,
a.cityCode,
a.stateCode,
a.stateName,
a.postalCode,
a.countryCode,
a.countryName,
a.addressFullText,
a.poBoxCode,
a.poBoxCity,
a.postalCodeExtension,
a.poBoxExtension,
a.deliveryServiceTypeCode,
a.deliveryServiceType,
a.deliveryServiceNumber,
a.addressTimeZone,
a.communicationAddressNumber
from bpa_id as a 
inner join bpa_cd as b 
on  a.businessPartnerNumber=b.businessPartnerNumber
--and a.businessPartnerAddressNumber=b.businessPartnerAddressNumber
""")
src_d1.createOrReplaceTempView("src_d1")
#display(src_d1)
#print(src_d1.count())

#fetching data uncommon data from both ISU and CRM table for active records only . 
src_d2=spark.sql("""
Select 
coalesce( a.sourceSystemCode , b.sourceSystemCode ) as sourceSystemCode ,
coalesce( a.businessPartnerAddressNumber , b.businessPartnerAddressNumber ) as businessPartnerAddressNumber ,
coalesce( a.businessPartnerNumber , b.businessPartnerNumber ) as businessPartnerNumber ,
coalesce( a.addressValidFromDate , b.addressValidFromDate ) as addressValidFromDate ,
coalesce( a.addressValidToDate , b.addressValidToDate ) as addressValidToDate ,
coalesce( a.phoneNumber , b.phoneNumber ) as phoneNumber ,
coalesce( a.phoneExtension , b.phoneExtension ) as phoneExtension ,
coalesce( a.faxNumber , b.faxNumber ) as faxNumber ,
coalesce( a.faxExtension , b.faxExtension ) as faxExtension ,
coalesce( a.emailAddress , b.emailAddress ) as emailAddress ,
coalesce( a.personalAddressFlag , b.personalAddressFlag ) as personalAddressFlag ,
coalesce( a.coName , b.coName ) as coName ,
coalesce( a.shortFormattedAddress2 , b.shortFormattedAddress2 ) as shortFormattedAddress2 ,
coalesce( a.streetLine5 , b.streetLine5 ) as streetLine5 ,
coalesce( a.building , b.building ) as building ,
coalesce( a.floorNumber , b.floorNumber ) as floorNumber ,
coalesce( a.apartmentNumber , b.apartmentNumber ) as apartmentNumber ,
coalesce( a.housePrimaryNumber , b.housePrimaryNumber ) as housePrimaryNumber ,
coalesce( a.houseSupplementNumber , b.houseSupplementNumber ) as houseSupplementNumber ,
coalesce( a.streetPrimaryName , b.streetPrimaryName ) as streetPrimaryName ,
coalesce( a.streetSupplementName1 , b.streetSupplementName1 ) as streetSupplementName1 ,
coalesce( a.streetSupplementName2 , b.streetSupplementName2 ) as streetSupplementName2 ,
coalesce( a.otherLocationName , b.otherLocationName ) as otherLocationName ,
coalesce( a.houseNumber , b.houseNumber ) as houseNumber ,
coalesce( a.streetName , b.streetName ) as streetName ,
coalesce( a.streetCode , b.streetCode ) as streetCode ,
coalesce( a.cityName , b.cityName ) as cityName ,
coalesce( a.cityCode , b.cityCode ) as cityCode ,
coalesce( a.stateCode , b.stateCode ) as stateCode ,
coalesce( a.stateName , b.stateName ) as stateName ,
coalesce( a.postalCode , b.postalCode ) as postalCode ,
coalesce( a.countryCode , b.countryCode ) as countryCode ,
coalesce( a.countryName , b.countryName ) as countryName ,
coalesce( a.addressFullText , b.addressFullText ) as addressFullText ,
coalesce( a.poBoxCode , b.poBoxCode ) as poBoxCode ,
coalesce( a.poBoxCity , b.poBoxCity ) as poBoxCity ,
coalesce( a.postalCodeExtension , b.postalCodeExtension ) as postalCodeExtension ,
coalesce( a.poBoxExtension , b.poBoxExtension ) as poBoxExtension ,
coalesce( a.deliveryServiceTypeCode , b.deliveryServiceTypeCode ) as deliveryServiceTypeCode ,
coalesce( a.deliveryServiceType , b.deliveryServiceType ) as deliveryServiceType ,
coalesce( a.deliveryServiceNumber , b.deliveryServiceNumber ) as deliveryServiceNumber ,
coalesce( a.addressTimeZone , b.addressTimeZone ) as addressTimeZone ,
coalesce( a.communicationAddressNumber , b.communicationAddressNumber ) as communicationAddressNumber 
from bpa_id as a 
full outer join bpa_cd as b
on  a.businessPartnerNumber=b.businessPartnerNumber
--and a.businessPartnerAddressNumber=b.businessPartnerAddressNumber
where ( a.businessPartnerNumber is NULL or b.businessPartnerNumber is NULL)

""")
src_d2.createOrReplaceTempView("src_d2")
#display(src_d2)
#print(src_d2.count())

#union for both active records.
src_d=spark.sql("""
Select * from src_d1
union all 
select * from src_d2
""")
src_d.createOrReplaceTempView("src_d")
#display(src_d)
#print(src_d.count())

src_a11=spark.sql("""
Select a.sourceSystemCode,
a.businessPartnerAddressNumber,
a.businessPartnerNumber,
a.addressValidFromDate,
a.addressValidToDate,
a.phoneNumber,
a.phoneExtension,
a.faxNumber,
a.faxExtension,
a.emailAddress,
a.personalAddressFlag,
a.coName,
a.shortFormattedAddress2,
a.streetLine5,
a.building,
a.floorNumber,
a.apartmentNumber,
a.housePrimaryNumber,
a.houseSupplementNumber,
a.streetPrimaryName,
a.streetSupplementName1,
a.streetSupplementName2,
a.otherLocationName,
a.houseNumber,
a.streetName,
a.streetCode,
a.cityName,
a.cityCode,
a.stateCode,
a.stateName,
a.postalCode,
a.countryCode,
a.countryName,
a.addressFullText,
a.poBoxCode,
a.poBoxCity,
a.postalCodeExtension,
a.poBoxExtension,
a.deliveryServiceTypeCode,
a.deliveryServiceType,
a.deliveryServiceNumber,
a.addressTimeZone,
a.communicationAddressNumber
from bpa_ia as a 
inner join bpa_ca as b 
on  a.businessPartnerNumber=b.businessPartnerNumber
--and a.businessPartnerAddressNumber=b.businessPartnerAddressNumber
""")
src_a1.createOrReplaceTempView("src_a11")
#display(src_a1)
print(src_a11.count())

# COMMAND ----------

q1=spark.sql("Select * from src_a  where businessPartnerNumber in ('0012471314','0013862247','0001000001','0001000002') order by businessPartnerNumber")
q11=spark.sql("Select * from src_a11  where businessPartnerNumber in ('0012471314','0013862247','0001000001','0001000002') order by businessPartnerNumber")
q2=spark.sql("Select * from bpa_ia  where businessPartnerNumber in ('0012471314','0013862247','0001000001','0001000002') order by businessPartnerNumber")
q3=spark.sql("Select * from bpa_ca  where businessPartnerNumber in ('0012471314','0013862247','0001000001','0001000002') order by businessPartnerNumber")
display(q1)
display(q11)
display(q2)
display(q3)


# COMMAND ----------

# DBTITLE 1,Define fields and table names
#16897542
keyColumns = 'businessPartnerAddressNumber,businessPartnerNumber'
mandatoryColumns = 'businessPartnerAddressNumber,businessPartnerNumber'

columns = ("""sourceSystemCode,
businessPartnerAddressNumber,
businessPartnerNumber,
addressValidFromDate,
addressValidToDate,
phoneNumber,
phoneExtension,
faxNumber,
faxExtension,
emailAddress,
personalAddressFlag,
coName,
shortFormattedAddress2,
streetLine5,
building,
floorNumber,
apartmentNumber,
housePrimaryNumber,
houseSupplementNumber,
streetPrimaryName,
streetSupplementName1,
streetSupplementName2,
otherLocationName,
houseNumber,
streetName,
streetCode,
cityName,
cityCode,
stateCode,
stateName,
postalCode,
countryCode,
countryName,
addressFullText,
poBoxCode,
poBoxCity,
postalCodeExtension,
poBoxExtension,
deliveryServiceTypeCode,
deliveryServiceType,
deliveryServiceNumber,
addressTimeZone,
communicationAddressNumber
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

columns1 = ("""sourceSystemCode,
businessPartnerAddressNumber,
businessPartnerNumber,
addressValidFromDate,
addressValidToDate,
phoneNumber,
phoneExtension,
faxNumber,
faxExtension,
emailAddress,
personalAddressFlag,
coName,
shortFormattedAddress2,
streetLine5,
building,
floorNumber,
apartmentNumber,
housePrimaryNumber,
houseSupplementNumber,
streetPrimaryName,
streetSupplementName1,
streetSupplementName2,
otherLocationName,
houseNumber,
streetName,
streetCode,
cityName,
cityCode,
stateCode,
stateName,
postalCode,
countryName,
addressFullText,
poBoxCode,
poBoxCity,
postalCodeExtension,
poBoxExtension,
deliveryServiceTypeCode,
deliveryServiceType,
deliveryServiceNumber,
addressTimeZone,
communicationAddressNumber
""")

a=spark.sql(f"Select {columns1} from src_a ")
b=spark.sql(f"Select {columns1} from curated_v2.dimBusinessPartnerAddress ")
display(a.subtract(b))
display(b.subtract(a))

# COMMAND ----------

tgt=spark.sql("Select * from curated_v2.dimBusinessPartnerAddress")
tgt1=spark.sql("Select * from curated_v2.dimBusinessPartnerAddress where sourceSystemCode is NULL")
tgt.printSchema()
display(tgt)
display(tgt1)

# COMMAND ----------

a1=spark.sql("Select streetName,streetCode,count(*) from curated_v2.dimBusinessPartnerAddress group by 1,2")
a2=spark.sql("Select cityName,cityCode,count(*) from curated_v2.dimBusinessPartnerAddress group by 1,2")
a3=spark.sql("Select stateCode,stateName,count(*) from curated_v2.dimBusinessPartnerAddress group by 1,2")
a4=spark.sql("Select countryCode,countryName,count(*) from curated_v2.dimBusinessPartnerAddress group by 1,2")
a5=spark.sql("Select poBoxCode,poBoxCity,count(*) from curated_v2.dimBusinessPartnerAddress group by 1,2")
a6=spark.sql("Select deliveryServiceTypeCode,deliveryServiceType,count(*) from curated_v2.dimBusinessPartnerAddress group by 1,2")
a7=spark.sql("Select personalAddressFlag,count(*) from curated_v2.dimBusinessPartnerAddress group by 1")
display(a1)
display(a2)
display(a3)
display(a4)
display(a5)
display(a6)
display(a7)


# COMMAND ----------

#tgt=spark.sql("Select * from curated_v2.dimInstallationHistory")
from pyspark.sql.functions import col
for name in tgt.columns: 
    rec=tgt.filter(col(name).isNull())
    print("column "+name+" count is: ", rec.count())
