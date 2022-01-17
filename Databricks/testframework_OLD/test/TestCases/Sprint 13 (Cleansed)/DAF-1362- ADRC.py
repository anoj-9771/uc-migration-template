# Databricks notebook source
#config parameters
source = 'CRM' #either CRM or ISU
table = 'ADRC'

environment = 'test'
storage_account_name = "sablobdaftest01"
storage_account_access_key = dbutils.secrets.get(scope="TestScope",key="test-sablob-key")
containerName = "archive"


# COMMAND ----------

spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")

# COMMAND ----------

# MAGIC %run ../../includes/tableEvaluation

# COMMAND ----------

lakedf = spark.sql("select * from cleansed.${vars.table}")

# COMMAND ----------

lakedf.printSchema()

# COMMAND ----------

# DBTITLE 1,[Source] with mapping
# MAGIC %sql
# MAGIC select
# MAGIC addressGroup
# MAGIC ,addressNumber
# MAGIC ,building
# MAGIC ,regionalStructureGrouping
# MAGIC ,cityCode
# MAGIC ,cityPoBoxCode
# MAGIC ,cityName
# MAGIC ,countryShortName
# MAGIC ,validFromDate
# MAGIC ,validToDate
# MAGIC ,communicationMethod
# MAGIC ,faxNumber
# MAGIC ,ftpAddressFlag
# MAGIC ,pagerAddressFlag
# MAGIC ,telephoneNumberFlag
# MAGIC ,faxNumberFlag
# MAGIC ,emailAddressFlag
# MAGIC ,floorNumber
# MAGIC ,houseNumber
# MAGIC ,houseNumber2
# MAGIC ,houseNumber3
# MAGIC ,originalAddressRecordCreation
# MAGIC ,streetLine5
# MAGIC ,searchHelpCityName
# MAGIC ,searchHelpLastName
# MAGIC ,searchHelpStreetName
# MAGIC ,coName
# MAGIC ,name1
# MAGIC ,name2
# MAGIC ,name3
# MAGIC ,postalCodeExtension
# MAGIC ,poBoxExtension
# MAGIC ,personalAddressIndicator
# MAGIC ,poBoxCode
# MAGIC ,poBoxCity
# MAGIC ,postalCode
# MAGIC ,poBoxPostalCode
# MAGIC ,companyPostalCode
# MAGIC ,stateCode
# MAGIC ,apartmentNumber
# MAGIC ,searchTerm1
# MAGIC ,searchTerm2
# MAGIC ,streetType
# MAGIC ,streetLine3
# MAGIC ,streetLine4
# MAGIC ,streetName
# MAGIC ,streetAbbreviation
# MAGIC ,streetCode
# MAGIC ,telephoneExtension
# MAGIC ,phoneNumber
# MAGIC ,addressTimeZone
# MAGIC ,titleCode
# MAGIC from
# MAGIC (select
# MAGIC ADDR_GROUP as addressGroup 
# MAGIC ,ADDRNUMBER as addressNumber 
# MAGIC ,BUILDING as building 
# MAGIC ,CHCKSTATUS as regionalStructureGrouping 
# MAGIC ,CITY_CODE as cityCode 
# MAGIC ,CITY_CODE2 as cityPoBoxCode 
# MAGIC ,CITY1 as cityName 
# MAGIC ,COUNTRY as countryShortName 
# MAGIC ,DATE_FROM as validFromDate 
# MAGIC ,DATE_TO as validToDate 
# MAGIC ,DEFLT_COMM as communicationMethod 
# MAGIC ,FAX_NUMBER as faxNumber 
# MAGIC ,FLAGCOMM12 as ftpAddressFlag 
# MAGIC ,FLAGCOMM13 as pagerAddressFlag 
# MAGIC ,FLAGCOMM2 as telephoneNumberFlag 
# MAGIC ,FLAGCOMM3 as faxNumberFlag 
# MAGIC ,FLAGCOMM6 as emailAddressFlag 
# MAGIC ,FLOOR as floorNumber 
# MAGIC ,HOUSE_NUM1 as houseNumber 
# MAGIC ,HOUSE_NUM2 as houseNumber2 
# MAGIC ,HOUSE_NUM3 as houseNumber3 
# MAGIC ,LANGU_CREA as originalAddressRecordCreation 
# MAGIC ,LOCATION as streetLine5 
# MAGIC ,MC_CITY1 as searchHelpCityName 
# MAGIC ,MC_NAME1 as searchHelpLastName 
# MAGIC ,MC_STREET as searchHelpStreetName 
# MAGIC ,NAME_CO as coName 
# MAGIC ,NAME1 as name1 
# MAGIC ,NAME2 as name2 
# MAGIC ,NAME3 as name3 
# MAGIC ,PCODE1_EXT as postalCodeExtension 
# MAGIC ,PCODE2_EXT as poBoxExtension 
# MAGIC ,PERS_ADDR as personalAddressIndicator 
# MAGIC ,PO_BOX as poBoxCode 
# MAGIC ,PO_BOX_LOC as poBoxCity 
# MAGIC ,POST_CODE1 as postalCode 
# MAGIC ,POST_CODE2 as poBoxPostalCode 
# MAGIC ,POST_CODE3 as companyPostalCode 
# MAGIC ,REGION as stateCode 
# MAGIC ,ROOMNUMBER as appartmentNumber 
# MAGIC ,SORT1 as searchTerm1 
# MAGIC ,SORT2 as searchTerm2 
# MAGIC ,STR_SUPPL1 as streetType 
# MAGIC ,STR_SUPPL2 as streetLine3 
# MAGIC ,STR_SUPPL3 as streetLine4 
# MAGIC ,STREET as streetName 
# MAGIC ,STREETABBR as streetAbbreviation 
# MAGIC ,STREETCODE as streetCode 
# MAGIC ,TEL_EXTENS as telephoneExtension 
# MAGIC ,TEL_NUMBER as phoneNumber 
# MAGIC ,TIME_ZONE as addressTimeZone 
# MAGIC ,TITLE as titleCode 
# MAGIC ,row_number() over (partition by addressNumber,validFromDate order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table} )a where  a.rn = 1

# COMMAND ----------

# DBTITLE 1,[Verification] Count Check
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from cleansed.${vars.table}
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from (select
# MAGIC addressGroup
# MAGIC ,addressNumber
# MAGIC ,building
# MAGIC ,regionalStructureGrouping
# MAGIC ,cityCode
# MAGIC ,cityPoBoxCode
# MAGIC ,cityName
# MAGIC ,countryShortName
# MAGIC ,validFromDate
# MAGIC ,validToDate
# MAGIC ,communicationMethod
# MAGIC ,faxNumber
# MAGIC ,ftpAddressFlag
# MAGIC ,pagerAddressFlag
# MAGIC ,telephoneNumberFlag
# MAGIC ,faxNumberFlag
# MAGIC ,emailAddressFlag
# MAGIC ,floorNumber
# MAGIC ,houseNumber
# MAGIC ,houseNumber2
# MAGIC ,houseNumber3
# MAGIC ,originalAddressRecordCreation
# MAGIC ,streetLine5
# MAGIC ,searchHelpCityName
# MAGIC ,searchHelpLastName
# MAGIC ,searchHelpStreetName
# MAGIC ,coName
# MAGIC ,name1
# MAGIC ,name2
# MAGIC ,name3
# MAGIC ,postalCodeExtension
# MAGIC ,poBoxExtension
# MAGIC ,personalAddressIndicator
# MAGIC ,poBoxCode
# MAGIC ,poBoxCity
# MAGIC ,postalCode
# MAGIC ,poBoxPostalCode
# MAGIC ,companyPostalCode
# MAGIC ,stateCode
# MAGIC ,apartmentNumber
# MAGIC ,searchTerm1
# MAGIC ,searchTerm2
# MAGIC ,streetType
# MAGIC ,streetLine3
# MAGIC ,streetLine4
# MAGIC ,streetName
# MAGIC ,streetAbbreviation
# MAGIC ,streetCode
# MAGIC ,telephoneExtension
# MAGIC ,phoneNumber
# MAGIC ,addressTimeZone
# MAGIC ,titleCode
# MAGIC from
# MAGIC (select
# MAGIC ADDR_GROUP as addressGroup 
# MAGIC ,ADDRNUMBER as addressNumber 
# MAGIC ,BUILDING as building 
# MAGIC ,CHCKSTATUS as regionalStructureGrouping 
# MAGIC ,CITY_CODE as cityCode 
# MAGIC ,CITY_CODE2 as cityPoBoxCode 
# MAGIC ,CITY1 as cityName 
# MAGIC ,COUNTRY as countryShortName 
# MAGIC ,DATE_FROM as validFromDate 
# MAGIC ,DATE_TO as validToDate 
# MAGIC ,DEFLT_COMM as communicationMethod 
# MAGIC ,FAX_NUMBER as faxNumber 
# MAGIC ,FLAGCOMM12 as ftpAddressFlag 
# MAGIC ,FLAGCOMM13 as pagerAddressFlag 
# MAGIC ,FLAGCOMM2 as telephoneNumberFlag 
# MAGIC ,FLAGCOMM3 as faxNumberFlag 
# MAGIC ,FLAGCOMM6 as emailAddressFlag 
# MAGIC ,FLOOR as floorNumber 
# MAGIC ,HOUSE_NUM1 as houseNumber 
# MAGIC ,HOUSE_NUM2 as houseNumber2 
# MAGIC ,HOUSE_NUM3 as houseNumber3 
# MAGIC ,LANGU_CREA as originalAddressRecordCreation 
# MAGIC ,LOCATION as streetLine5 
# MAGIC ,MC_CITY1 as searchHelpCityName 
# MAGIC ,MC_NAME1 as searchHelpLastName 
# MAGIC ,MC_STREET as searchHelpStreetName 
# MAGIC ,NAME_CO as coName 
# MAGIC ,NAME1 as name1 
# MAGIC ,NAME2 as name2 
# MAGIC ,NAME3 as name3 
# MAGIC ,PCODE1_EXT as postalCodeExtension 
# MAGIC ,PCODE2_EXT as poBoxExtension 
# MAGIC ,PERS_ADDR as personalAddressIndicator 
# MAGIC ,PO_BOX as poBoxCode 
# MAGIC ,PO_BOX_LOC as poBoxCity 
# MAGIC ,POST_CODE1 as postalCode 
# MAGIC ,POST_CODE2 as poBoxPostalCode 
# MAGIC ,POST_CODE3 as companyPostalCode 
# MAGIC ,REGION as stateCode 
# MAGIC ,ROOMNUMBER as appartmentNumber 
# MAGIC ,SORT1 as searchTerm1 
# MAGIC ,SORT2 as searchTerm2 
# MAGIC ,STR_SUPPL1 as streetType 
# MAGIC ,STR_SUPPL2 as streetLine3 
# MAGIC ,STR_SUPPL3 as streetLine4 
# MAGIC ,STREET as streetName 
# MAGIC ,STREETABBR as streetAbbreviation 
# MAGIC ,STREETCODE as streetCode 
# MAGIC ,TEL_EXTENS as telephoneExtension 
# MAGIC ,TEL_NUMBER as phoneNumber 
# MAGIC ,TIME_ZONE as addressTimeZone 
# MAGIC ,TITLE as titleCode 
# MAGIC ,row_number() over (partition by addressNumber,validFromDate order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table} )a where  a.rn = 1)

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT addressNumber,validFromDate
# MAGIC , COUNT (*) as count
# MAGIC FROM cleansed.${vars.table}
# MAGIC GROUP BY addressNumber,validFromDate
# MAGIC HAVING COUNT (*) > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT * FROM (
# MAGIC SELECT
# MAGIC *,
# MAGIC row_number() OVER(PARTITION BY addressNumber,validFromDate  order by addressNumber,validFromDate) as rn
# MAGIC FROM  cleansed.${vars.table}
# MAGIC )a where a.rn > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Source and Target Data
# MAGIC %sql
# MAGIC select
# MAGIC addressGroup
# MAGIC ,addressNumber
# MAGIC ,building
# MAGIC ,regionalStructureGrouping
# MAGIC ,cityCode
# MAGIC ,cityPoBoxCode
# MAGIC ,cityName
# MAGIC ,countryShortName
# MAGIC ,validFromDate
# MAGIC ,validToDate
# MAGIC ,communicationMethod
# MAGIC ,faxNumber
# MAGIC ,ftpAddressFlag
# MAGIC ,pagerAddressFlag
# MAGIC ,telephoneNumberFlag
# MAGIC ,faxNumberFlag
# MAGIC ,emailAddressFlag
# MAGIC ,floorNumber
# MAGIC ,houseNumber
# MAGIC ,houseNumber2
# MAGIC ,houseNumber3
# MAGIC ,originalAddressRecordCreation
# MAGIC ,streetLine5
# MAGIC ,searchHelpCityName
# MAGIC ,searchHelpLastName
# MAGIC ,searchHelpStreetName
# MAGIC ,coName
# MAGIC ,name1
# MAGIC ,name2
# MAGIC ,name3
# MAGIC ,postalCodeExtension
# MAGIC ,poBoxExtension
# MAGIC ,personalAddressIndicator
# MAGIC ,poBoxCode
# MAGIC ,poBoxCity
# MAGIC ,postalCode
# MAGIC ,poBoxPostalCode
# MAGIC ,companyPostalCode
# MAGIC ,stateCode
# MAGIC ,apartmentNumber
# MAGIC ,searchTerm1
# MAGIC ,searchTerm2
# MAGIC ,streetType
# MAGIC ,streetLine3
# MAGIC ,streetLine4
# MAGIC ,streetName
# MAGIC ,streetAbbreviation
# MAGIC ,streetCode
# MAGIC ,telephoneExtension
# MAGIC ,phoneNumber
# MAGIC ,addressTimeZone
# MAGIC ,titleCode
# MAGIC from
# MAGIC (select
# MAGIC ADDR_GROUP as addressGroup 
# MAGIC ,ADDRNUMBER as addressNumber 
# MAGIC ,BUILDING as building 
# MAGIC ,CHCKSTATUS as regionalStructureGrouping 
# MAGIC ,CITY_CODE as cityCode 
# MAGIC ,CITY_CODE2 as cityPoBoxCode 
# MAGIC ,CITY1 as cityName 
# MAGIC ,COUNTRY as countryShortName 
# MAGIC ,DATE_FROM as validFromDate 
# MAGIC ,DATE_TO as validToDate 
# MAGIC ,DEFLT_COMM as communicationMethod 
# MAGIC ,FAX_NUMBER as faxNumber 
# MAGIC ,FLAGCOMM12 as ftpAddressFlag 
# MAGIC ,FLAGCOMM13 as pagerAddressFlag 
# MAGIC ,FLAGCOMM2 as telephoneNumberFlag 
# MAGIC ,FLAGCOMM3 as faxNumberFlag 
# MAGIC ,FLAGCOMM6 as emailAddressFlag 
# MAGIC ,FLOOR as floorNumber 
# MAGIC ,HOUSE_NUM1 as houseNumber 
# MAGIC ,HOUSE_NUM2 as houseNumber2 
# MAGIC ,HOUSE_NUM3 as houseNumber3 
# MAGIC ,LANGU_CREA as originalAddressRecordCreation 
# MAGIC ,LOCATION as streetLine5 
# MAGIC ,MC_CITY1 as searchHelpCityName 
# MAGIC ,MC_NAME1 as searchHelpLastName 
# MAGIC ,MC_STREET as searchHelpStreetName 
# MAGIC ,NAME_CO as coName 
# MAGIC ,NAME1 as name1 
# MAGIC ,NAME2 as name2 
# MAGIC ,NAME3 as name3 
# MAGIC ,PCODE1_EXT as postalCodeExtension 
# MAGIC ,PCODE2_EXT as poBoxExtension 
# MAGIC ,PERS_ADDR as personalAddressIndicator 
# MAGIC ,PO_BOX as poBoxCode 
# MAGIC ,PO_BOX_LOC as poBoxCity 
# MAGIC ,POST_CODE1 as postalCode 
# MAGIC ,POST_CODE2 as poBoxPostalCode 
# MAGIC ,POST_CODE3 as companyPostalCode 
# MAGIC ,REGION as stateCode 
# MAGIC ,ROOMNUMBER as appartmentNumber 
# MAGIC ,SORT1 as searchTerm1 
# MAGIC ,SORT2 as searchTerm2 
# MAGIC ,STR_SUPPL1 as streetType 
# MAGIC ,STR_SUPPL2 as streetLine3 
# MAGIC ,STR_SUPPL3 as streetLine4 
# MAGIC ,STREET as streetName 
# MAGIC ,STREETABBR as streetAbbreviation 
# MAGIC ,STREETCODE as streetCode 
# MAGIC ,TEL_EXTENS as telephoneExtension 
# MAGIC ,TEL_NUMBER as phoneNumber 
# MAGIC ,TIME_ZONE as addressTimeZone 
# MAGIC ,TITLE as titleCode 
# MAGIC ,row_number() over (partition by addressNumber,validFromDate order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table} )a where  a.rn = 1
# MAGIC except
# MAGIC select
# MAGIC addressGroup
# MAGIC ,addressNumber
# MAGIC ,building
# MAGIC ,regionalStructureGrouping
# MAGIC ,cityCode
# MAGIC ,cityPoBoxCode
# MAGIC ,cityName
# MAGIC ,countryShortName
# MAGIC ,validFromDate
# MAGIC ,validToDate
# MAGIC ,communicationMethod
# MAGIC ,faxNumber
# MAGIC ,ftpAddressFlag
# MAGIC ,pagerAddressFlag
# MAGIC ,telephoneNumberFlag
# MAGIC ,faxNumberFlag
# MAGIC ,emailAddressFlag
# MAGIC ,floorNumber
# MAGIC ,houseNumber
# MAGIC ,houseNumber2
# MAGIC ,houseNumber3
# MAGIC ,originalAddressRecordCreation
# MAGIC ,streetLine5
# MAGIC ,searchHelpCityName
# MAGIC ,searchHelpLastName
# MAGIC ,searchHelpStreetName
# MAGIC ,coName
# MAGIC ,name1
# MAGIC ,name2
# MAGIC ,name3
# MAGIC ,postalCodeExtension
# MAGIC ,poBoxExtension
# MAGIC ,personalAddressIndicator
# MAGIC ,poBoxCode
# MAGIC ,poBoxCity
# MAGIC ,postalCode
# MAGIC ,poBoxPostalCode
# MAGIC ,companyPostalCode
# MAGIC ,stateCode
# MAGIC ,apartmentNumber
# MAGIC ,searchTerm1
# MAGIC ,searchTerm2
# MAGIC ,streetType
# MAGIC ,streetLine3
# MAGIC ,streetLine4
# MAGIC ,streetName
# MAGIC ,streetAbbreviation
# MAGIC ,streetCode
# MAGIC ,telephoneExtension
# MAGIC ,phoneNumber
# MAGIC ,addressTimeZone
# MAGIC ,titleCode
# MAGIC from
# MAGIC cleansed.${vars.table}

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Target and Source Data
# MAGIC %sql
# MAGIC select
# MAGIC addressGroup
# MAGIC ,addressNumber
# MAGIC ,building
# MAGIC ,regionalStructureGrouping
# MAGIC ,cityCode
# MAGIC ,cityPoBoxCode
# MAGIC ,cityName
# MAGIC ,countryShortName
# MAGIC ,validFromDate
# MAGIC ,validToDate
# MAGIC ,communicationMethod
# MAGIC ,faxNumber
# MAGIC ,ftpAddressFlag
# MAGIC ,pagerAddressFlag
# MAGIC ,telephoneNumberFlag
# MAGIC ,faxNumberFlag
# MAGIC ,emailAddressFlag
# MAGIC ,floorNumber
# MAGIC ,houseNumber
# MAGIC ,houseNumber2
# MAGIC ,houseNumber3
# MAGIC ,originalAddressRecordCreation
# MAGIC ,streetLine5
# MAGIC ,searchHelpCityName
# MAGIC ,searchHelpLastName
# MAGIC ,searchHelpStreetName
# MAGIC ,coName
# MAGIC ,name1
# MAGIC ,name2
# MAGIC ,name3
# MAGIC ,postalCodeExtension
# MAGIC ,poBoxExtension
# MAGIC ,personalAddressIndicator
# MAGIC ,poBoxCode
# MAGIC ,poBoxCity
# MAGIC ,postalCode
# MAGIC ,poBoxPostalCode
# MAGIC ,companyPostalCode
# MAGIC ,stateCode
# MAGIC ,apartmentNumber
# MAGIC ,searchTerm1
# MAGIC ,searchTerm2
# MAGIC ,streetType
# MAGIC ,streetLine3
# MAGIC ,streetLine4
# MAGIC ,streetName
# MAGIC ,streetAbbreviation
# MAGIC ,streetCode
# MAGIC ,telephoneExtension
# MAGIC ,phoneNumber
# MAGIC ,addressTimeZone
# MAGIC ,titleCode
# MAGIC from
# MAGIC cleansed.${vars.table}
# MAGIC except
# MAGIC select
# MAGIC addressGroup
# MAGIC ,addressNumber
# MAGIC ,building
# MAGIC ,regionalStructureGrouping
# MAGIC ,cityCode
# MAGIC ,cityPoBoxCode
# MAGIC ,cityName
# MAGIC ,countryShortName
# MAGIC ,validFromDate
# MAGIC ,validToDate
# MAGIC ,communicationMethod
# MAGIC ,faxNumber
# MAGIC ,ftpAddressFlag
# MAGIC ,pagerAddressFlag
# MAGIC ,telephoneNumberFlag
# MAGIC ,faxNumberFlag
# MAGIC ,emailAddressFlag
# MAGIC ,floorNumber
# MAGIC ,houseNumber
# MAGIC ,houseNumber2
# MAGIC ,houseNumber3
# MAGIC ,originalAddressRecordCreation
# MAGIC ,streetLine5
# MAGIC ,searchHelpCityName
# MAGIC ,searchHelpLastName
# MAGIC ,searchHelpStreetName
# MAGIC ,coName
# MAGIC ,name1
# MAGIC ,name2
# MAGIC ,name3
# MAGIC ,postalCodeExtension
# MAGIC ,poBoxExtension
# MAGIC ,personalAddressIndicator
# MAGIC ,poBoxCode
# MAGIC ,poBoxCity
# MAGIC ,postalCode
# MAGIC ,poBoxPostalCode
# MAGIC ,companyPostalCode
# MAGIC ,stateCode
# MAGIC ,apartmentNumber
# MAGIC ,searchTerm1
# MAGIC ,searchTerm2
# MAGIC ,streetType
# MAGIC ,streetLine3
# MAGIC ,streetLine4
# MAGIC ,streetName
# MAGIC ,streetAbbreviation
# MAGIC ,streetCode
# MAGIC ,telephoneExtension
# MAGIC ,phoneNumber
# MAGIC ,addressTimeZone
# MAGIC ,titleCode
# MAGIC from
# MAGIC (select
# MAGIC ADDR_GROUP as addressGroup 
# MAGIC ,ADDRNUMBER as addressNumber 
# MAGIC ,BUILDING as building 
# MAGIC ,CHCKSTATUS as regionalStructureGrouping 
# MAGIC ,CITY_CODE as cityCode 
# MAGIC ,CITY_CODE2 as cityPoBoxCode 
# MAGIC ,CITY1 as cityName 
# MAGIC ,COUNTRY as countryShortName 
# MAGIC ,DATE_FROM as validFromDate 
# MAGIC ,DATE_TO as validToDate 
# MAGIC ,DEFLT_COMM as communicationMethod 
# MAGIC ,FAX_NUMBER as faxNumber 
# MAGIC ,FLAGCOMM12 as ftpAddressFlag 
# MAGIC ,FLAGCOMM13 as pagerAddressFlag 
# MAGIC ,FLAGCOMM2 as telephoneNumberFlag 
# MAGIC ,FLAGCOMM3 as faxNumberFlag 
# MAGIC ,FLAGCOMM6 as emailAddressFlag 
# MAGIC ,FLOOR as floorNumber 
# MAGIC ,HOUSE_NUM1 as houseNumber 
# MAGIC ,HOUSE_NUM2 as houseNumber2 
# MAGIC ,HOUSE_NUM3 as houseNumber3 
# MAGIC ,LANGU_CREA as originalAddressRecordCreation 
# MAGIC ,'LOCATION' as streetLine5 
# MAGIC ,MC_CITY1 as searchHelpCityName 
# MAGIC ,MC_NAME1 as searchHelpLastName 
# MAGIC ,MC_STREET as searchHelpStreetName 
# MAGIC ,NAME_CO as coName 
# MAGIC ,NAME1 as name1 
# MAGIC ,NAME2 as name2 
# MAGIC ,NAME3 as name3 
# MAGIC ,PCODE1_EXT as postalCodeExtension 
# MAGIC ,PCODE2_EXT as poBoxExtension 
# MAGIC ,PERS_ADDR as personalAddressIndicator 
# MAGIC ,PO_BOX as poBoxCode 
# MAGIC ,PO_BOX_LOC as poBoxCity 
# MAGIC ,POST_CODE1 as postalCode 
# MAGIC ,POST_CODE2 as poBoxPostalCode 
# MAGIC ,POST_CODE3 as companyPostalCode 
# MAGIC ,REGION as stateCode 
# MAGIC ,ROOMNUMBER as appartmentNumber 
# MAGIC ,SORT1 as searchTerm1 
# MAGIC ,SORT2 as searchTerm2 
# MAGIC ,STR_SUPPL1 as streetType 
# MAGIC ,STR_SUPPL2 as streetLine3 
# MAGIC ,STR_SUPPL3 as streetLine4 
# MAGIC ,STREET as streetName 
# MAGIC ,STREETABBR as streetAbbreviation 
# MAGIC ,STREETCODE as streetCode 
# MAGIC ,TEL_EXTENS as telephoneExtension 
# MAGIC ,TEL_NUMBER as phoneNumber 
# MAGIC ,TIME_ZONE as addressTimeZone 
# MAGIC ,TITLE as titleCode 
# MAGIC ,row_number() over (partition by addressNumber,validFromDate order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table} )a where  a.rn = 1
