# Databricks notebook source
#config parameters
source = 'CRM' #either CRM or ISU
table = '0BPARTNER_ATTR'

environment = 'test'
storage_account_name = "sablobdaftest01"
storage_account_access_key = dbutils.secrets.get(scope="TestScope",key="test-sablob-key")
containerName = "archive"


# COMMAND ----------

# MAGIC %run ../../includes/tableEvaluation

# COMMAND ----------

# DBTITLE 1,[Source] with mapping
# MAGIC %sql
# MAGIC select
# MAGIC businessPartnerNumber,
# MAGIC businessPartnerCategoryCode,
# MAGIC businessPartnerCategory,
# MAGIC businessPartnerTypeCode,
# MAGIC businessPartnerType,
# MAGIC businessPartnerGroupCode,
# MAGIC businessPartnerGroup,
# MAGIC externalBusinessPartnerNumber,
# MAGIC searchTerm1,
# MAGIC searchTerm2,
# MAGIC titleCode,
# MAGIC title,
# MAGIC deletedIndicator,
# MAGIC centralBlockBusinessPartner,
# MAGIC userId,
# MAGIC paymentAssistSchemeIndicator,
# MAGIC billAssistIndicator,
# MAGIC createdDate,
# MAGIC consent1Indicator,
# MAGIC warWidowIndicator,
# MAGIC disabilityIndicator,
# MAGIC goldCardHolderIndicator,
# MAGIC deceasedIndicator,
# MAGIC pensionConcessionCardIndicator,
# MAGIC eligibilityIndicator,
# MAGIC dateOfCheck,
# MAGIC paymentStartDate,
# MAGIC pensionType,
# MAGIC consent2Indicator,
# MAGIC organizationName1,
# MAGIC organizationName2,
# MAGIC organizationName3,
# MAGIC organizationFoundedDate,
# MAGIC internationalLocationNumber1,
# MAGIC internationalLocationNumber2,
# MAGIC internationalLocationNumber3,
# MAGIC lastName,
# MAGIC firstName,
# MAGIC middleName,
# MAGIC academicTitleCode,
# MAGIC academicTitle,
# MAGIC nickName,
# MAGIC nameInitials,
# MAGIC countryName,
# MAGIC correspondanceLanguage,
# MAGIC nationality,
# MAGIC personNumber,
# MAGIC unknownGenderIndicator,
# MAGIC dateOfBirth,
# MAGIC dateOfDeath,
# MAGIC personnelNumber,
# MAGIC nameGroup1,
# MAGIC nameGroup2,
# MAGIC searchHelpLastName,
# MAGIC searchHelpFirstName,
# MAGIC createdBy,
# MAGIC createdDateTime,
# MAGIC changedBy,
# MAGIC lastChangedDateTime,
# MAGIC businessPartnerGUID,
# MAGIC communicationAddressNumber,
# MAGIC plannedChangeDocument,
# MAGIC validFromDate,
# MAGIC validToDate,
# MAGIC naturalPersonIndicator,
# MAGIC kidneyDialysisIndicator,
# MAGIC patientUnit,
# MAGIC patientTitleCode,
# MAGIC patientTitle,
# MAGIC patientFirstName,
# MAGIC patientSurname,
# MAGIC patientAreaCode,
# MAGIC patientPhoneNumber,
# MAGIC hospitalName,
# MAGIC patientMachineType,
# MAGIC machineTypeValidFromDate,
# MAGIC offReason,
# MAGIC machineTypeValidToDate
# MAGIC from
# MAGIC (
# MAGIC select
# MAGIC PARTNER as businessPartnerNumber
# MAGIC ,TYPE as businessPartnerCategoryCode
# MAGIC ,b.businessPartnerCategory as businessPartnerCategory
# MAGIC ,BPKIND as businessPartnerTypeCode
# MAGIC ,c.businessPartnerType as businessPartnerType
# MAGIC ,BU_GROUP as businessPartnerGroupCode
# MAGIC ,d.businessPartnerGroup as businessPartnerGroup
# MAGIC ,BPEXT as externalBusinessPartnerNumber
# MAGIC ,BU_SORT1 as searchTerm1
# MAGIC ,BU_SORT2 as searchTerm2
# MAGIC ,a.TITLE as titleCode
# MAGIC ,f.title as title
# MAGIC ,XDELE as deletedIndicator
# MAGIC ,XBLCK as centralBlockBusinessPartner
# MAGIC ,ZZUSER as userId
# MAGIC ,ZZPAS_INDICATOR as paymentAssistSchemeIndicator
# MAGIC ,ZZBA_INDICATOR as billAssistIndicator
# MAGIC ,ZZAFLD00001Z as createdOn
# MAGIC ,NAME_ORG1 as organizationName1
# MAGIC ,NAME_ORG2 as organizationName2
# MAGIC ,NAME_ORG3 as organizationName3
# MAGIC ,FOUND_DAT as organizationFoundedDate
# MAGIC ,LOCATION_1 as internationalLocationNumber1
# MAGIC ,LOCATION_2 as internationalLocationNumber2
# MAGIC ,LOCATION_3 as internationalLocationNumber3
# MAGIC ,NAME_LAST as lastName
# MAGIC ,NAME_FIRST as firstName
# MAGIC ,NAME_LAST2 as atBirthName
# MAGIC ,NAMEMIDDLE as middleName
# MAGIC ,TITLE_ACA1 as academicTitle
# MAGIC ,NICKNAME as nickName
# MAGIC ,INITIALS as nameInitials
# MAGIC ,NAMCOUNTRY as countryName
# MAGIC ,LANGU_CORR as correspondanceLanguage
# MAGIC ,NATIO as nationality
# MAGIC ,PERSNUMBER as personNumber
# MAGIC ,XSEXU as unknownGenderIndicator
# MAGIC ,BU_LANGU as language
# MAGIC ,BIRTHDT as dateOfBirth
# MAGIC ,DEATHDT as dateOfDeath
# MAGIC ,PERNO as personnelNumber
# MAGIC ,NAME_GRP1 as nameGroup1
# MAGIC ,NAME_GRP2 as nameGroup2
# MAGIC ,CRUSR as createdBy
# MAGIC ,cast(to_unix_timestamp(CRDAT||' '||CRTIM,'yyyy-MM-dd HH:mm:ss') as timestamp) as createdDateTime
# MAGIC ,CHUSR as changedBy
# MAGIC ,cast(to_unix_timestamp(CHDAT||' '||CHTIM,'yyyy-MM-dd HH:mm:ss') as timestamp) as changedDateTime
# MAGIC ,PARTNER_GUID as businessPartnerGUID
# MAGIC ,ADDRCOMM as addressNumber
# MAGIC ,case when VALID_FROM = '10101000000' then '1900-01-01' else CONCAT(LEFT(VALID_FROM,4),'-',SUBSTRING(VALID_FROM,5,2),'-',SUBSTRING(VALID_FROM,7,2)) end as validFromDate
# MAGIC --,substr(VALID_TO, 1, 8) as validToDate
# MAGIC ,CONCAT(LEFT(VALID_TO,4),'-',SUBSTRING(VALID_TO,5,2),'-',SUBSTRING(VALID_TO,7,2)) as validToDate
# MAGIC --,VALID_FROM as validFromDate
# MAGIC --,VALID_TO as validToDateyes ====!!!! all dates need converting..... :)
# MAGIC ,NATPERS as naturalPersonIndicator
# MAGIC ,row_number() over (partition by RELNR,PARTNER1,PARTNER2,DATE_TO order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table} 
# MAGIC left join cleansed.CRM_0BPARTNER_TEXT b
# MAGIC on a.PARTNER = b.businessPartnerNumber and a.TYPE = b.businessPartnerCategoryCode
# MAGIC LEFT JOIN cleansed.CRM_0BPTYPE_TEXT c
# MAGIC ON a.BPKIND = c.businessPartnerTypeCode --and c.SPRAS = 'E'
# MAGIC LEFT JOIN cleansed.CRM_0BP_GROUP_TEXT d
# MAGIC ON a.BPKIND = d.businessPartnerGroupCode --and d.SPRAS = 'E'
# MAGIC LEFT JOIN cleansed.CRM_ZDSTITLET f
# MAGIC ON a.TITLE = f.TITLEcode --and f.LANGU = 'E'
# MAGIC )a where  a.rn = 1

# COMMAND ----------

# DBTITLE 1,[Verification] Count Check
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from cleansed.${vars.table}
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from (select
# MAGIC businessPartnerRelationshipNumber
# MAGIC ,businessPartnerNumber1
# MAGIC ,businessPartnerNumber2
# MAGIC ,businessPartnerGUID1
# MAGIC ,businessPartnerGUID2
# MAGIC ,relationshipDirection
# MAGIC ,relationshipTypeCode
# MAGIC ,relationshipType
# MAGIC ,validToDate
# MAGIC ,validFromDate
# MAGIC ,countryShortName
# MAGIC ,postalCode
# MAGIC ,cityName
# MAGIC ,streetName
# MAGIC ,houseNumber
# MAGIC ,phoneNumber
# MAGIC ,emailAddress
# MAGIC ,capitalInterestPercentage
# MAGIC ,capitalInterestAmount
# MAGIC ,shortFormattedAddress
# MAGIC ,shortFormattedAddress2
# MAGIC ,addressLine0
# MAGIC ,addressLine1
# MAGIC ,addressLine2
# MAGIC ,addressLine3
# MAGIC ,addressLine4
# MAGIC ,addressLine5
# MAGIC ,addressLine6
# MAGIC ,deletedIndicator
# MAGIC from
# MAGIC (select
# MAGIC RELNR as businessPartnerRelationshipNumber
# MAGIC ,PARTNER1 as businessPartnerNumber1
# MAGIC ,PARTNER2 as businessPartnerNumber2
# MAGIC ,PARTNER1_GUID as businessPartnerGUID1
# MAGIC ,PARTNER2_GUID as businessPartnerGUID2
# MAGIC ,RELDIR as relationshipDirection
# MAGIC ,RELTYP as relationshipTypeCode
# MAGIC ,b.relationshipType as relationshipType
# MAGIC ,DATE_TO as validToDate
# MAGIC ,case
# MAGIC when cast(DATE_FROM as DATE) IS NULL then '1900-01-01'
# MAGIC else DATE_FROM end as validFromDate
# MAGIC ,COUNTRY as countryShortName
# MAGIC ,POST_CODE1 as postalCode
# MAGIC ,CITY1 as cityName
# MAGIC ,STREET as streetName
# MAGIC ,HOUSE_NUM1 as houseNumber
# MAGIC ,TEL_NUMBER as phoneNumber
# MAGIC ,SMTP_ADDR as emailAddress
# MAGIC ,CMPY_PART_PER as capitalInterestPercentage
# MAGIC ,CMPY_PART_AMO as capitalInterestAmount
# MAGIC ,ADDR_SHORT as shortFormattedAddress
# MAGIC ,ADDR_SHORT_S as shortFormattedAddress2
# MAGIC ,LINE0 as addressLine0
# MAGIC ,LINE1 as addressLine1
# MAGIC ,LINE2 as addressLine2
# MAGIC ,LINE3 as addressLine3
# MAGIC ,LINE4 as addressLine4
# MAGIC ,LINE5 as addressLine5
# MAGIC ,LINE6 as addressLine6
# MAGIC ,FLG_DELETED as deletedIndicator
# MAGIC ,row_number() over (partition by RELNR,PARTNER1,PARTNER2,DATE_TO order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table} 
# MAGIC left join cleansed.CRM_0BP_RELTYPES_TEXT b
# MAGIC on RELDIR = b.relationshipDirection and RELTYP = b.relationshipTypeCode 
# MAGIC )a where  a.rn = 1)

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT businessPartnerRelationshipNumber,businessPartnerNumber1,businessPartnerNumber2,businessPartnerGUID1,businessPartnerGUID2,relationshipDirection
# MAGIC ,relationshipTypeCode,relationshipType,validToDate,validFromDate,countryShortName,postalCode,cityName,streetName,houseNumber,phoneNumber,emailAddress
# MAGIC ,capitalInterestPercentage,capitalInterestAmount,shortFormattedAddress,shortFormattedAddress2,addressLine0,addressLine1,addressLine2,addressLine3
# MAGIC ,addressLine4,addressLine5,addressLine6,deletedIndicator
# MAGIC , COUNT (*) as count
# MAGIC FROM cleansed.${vars.table}
# MAGIC GROUP BY businessPartnerRelationshipNumber,businessPartnerNumber1,businessPartnerNumber2,businessPartnerGUID1,businessPartnerGUID2
# MAGIC ,relationshipDirection,relationshipTypeCode,relationshipType,validToDate,validFromDate,countryShortName,postalCode,cityName,streetName
# MAGIC ,houseNumber,phoneNumber,emailAddress,capitalInterestPercentage,capitalInterestAmount,shortFormattedAddress,shortFormattedAddress2
# MAGIC ,addressLine0,addressLine1,addressLine2,addressLine3,addressLine4,addressLine5,addressLine6,deletedIndicator
# MAGIC HAVING COUNT (*) > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT  * FROM  (
# MAGIC     SELECT
# MAGIC       *,row_number() 
# MAGIC       OVER(PARTITION BY businessPartnerRelationshipNumber,businessPartnerNumber1,businessPartnerNumber2,validToDate
# MAGIC         order by
# MAGIC           businessPartnerRelationshipNumber,businessPartnerNumber1,businessPartnerNumber2,validToDate
# MAGIC       ) as rn
# MAGIC     FROM  cleansed.${vars.table}
# MAGIC   ) a where  a.rn > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Source and Target Data
# MAGIC %sql
# MAGIC select
# MAGIC businessPartnerRelationshipNumber
# MAGIC ,businessPartnerNumber1
# MAGIC ,businessPartnerNumber2
# MAGIC ,businessPartnerGUID1
# MAGIC ,businessPartnerGUID2
# MAGIC ,relationshipDirection
# MAGIC ,relationshipTypeCode
# MAGIC ,relationshipType
# MAGIC ,validToDate
# MAGIC ,validFromDate
# MAGIC ,countryShortName
# MAGIC ,postalCode
# MAGIC ,cityName
# MAGIC ,streetName
# MAGIC ,houseNumber
# MAGIC ,phoneNumber
# MAGIC ,emailAddress
# MAGIC ,capitalInterestPercentage
# MAGIC ,capitalInterestAmount
# MAGIC ,shortFormattedAddress
# MAGIC ,shortFormattedAddress2
# MAGIC ,addressLine0
# MAGIC ,addressLine1
# MAGIC ,addressLine2
# MAGIC ,addressLine3
# MAGIC ,addressLine4
# MAGIC ,addressLine5
# MAGIC ,addressLine6
# MAGIC ,deletedIndicator
# MAGIC from
# MAGIC (Select RELNR as businessPartnerRelationshipNumber
# MAGIC ,PARTNER1 as businessPartnerNumber1
# MAGIC ,PARTNER2 as businessPartnerNumber2
# MAGIC ,PARTNER1_GUID as businessPartnerGUID1
# MAGIC ,PARTNER2_GUID as businessPartnerGUID2
# MAGIC ,RELDIR as relationshipDirection
# MAGIC ,RELTYP as relationshipTypeCode
# MAGIC ,b.relationshipType as relationshipType
# MAGIC ,
# MAGIC --case
# MAGIC --when DATE_TO < '1900-01-01' then '2099-12-31'
# MAGIC --else DATE_TO end as validToDate
# MAGIC cast(DATE_TO as DATE) as validToDate
# MAGIC ,case
# MAGIC when DATE_FROM < '1900-01-01' then '1900-01-01'
# MAGIC else DATE_FROM end as validFromDate
# MAGIC --DATE_FROM as validFromDate
# MAGIC ,COUNTRY as countryShortName
# MAGIC ,POST_CODE1 as postalCode
# MAGIC ,CITY1 as cityName
# MAGIC ,STREET as streetName
# MAGIC ,HOUSE_NUM1 as houseNumber
# MAGIC ,TEL_NUMBER as phoneNumber
# MAGIC ,SMTP_ADDR as emailAddress
# MAGIC ,CMPY_PART_PER as capitalInterestPercentage
# MAGIC ,CMPY_PART_AMO as capitalInterestAmount
# MAGIC ,ADDR_SHORT as shortFormattedAddress
# MAGIC ,ADDR_SHORT_S as shortFormattedAddress2
# MAGIC ,LINE0 as addressLine0
# MAGIC ,LINE1 as addressLine1
# MAGIC ,LINE2 as addressLine2
# MAGIC ,LINE3 as addressLine3
# MAGIC ,LINE4 as addressLine4
# MAGIC ,LINE5 as addressLine5
# MAGIC ,LINE6 as addressLine6
# MAGIC ,FLG_DELETED as deletedIndicator
# MAGIC ,row_number() over (partition by RELNR,PARTNER1,PARTNER2,DATE_TO order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table} 
# MAGIC left join cleansed.CRM_0BP_RELTYPES_TEXT b
# MAGIC on RELDIR = b.relationshipDirection and RELTYP = b.relationshipTypeCode 
# MAGIC )a where  a.rn = 1 
# MAGIC except
# MAGIC select
# MAGIC businessPartnerRelationshipNumber
# MAGIC ,businessPartnerNumber1
# MAGIC ,businessPartnerNumber2
# MAGIC ,businessPartnerGUID1
# MAGIC ,businessPartnerGUID2
# MAGIC ,relationshipDirection
# MAGIC ,relationshipTypeCode
# MAGIC ,relationshipType
# MAGIC ,validToDate
# MAGIC ,validFromDate
# MAGIC ,countryShortName
# MAGIC ,postalCode
# MAGIC ,cityName
# MAGIC ,streetName
# MAGIC ,houseNumber
# MAGIC ,phoneNumber
# MAGIC ,emailAddress
# MAGIC ,capitalInterestPercentage
# MAGIC ,capitalInterestAmount
# MAGIC ,shortFormattedAddress
# MAGIC ,shortFormattedAddress2
# MAGIC ,addressLine0
# MAGIC ,addressLine1
# MAGIC ,addressLine2
# MAGIC ,addressLine3
# MAGIC ,addressLine4
# MAGIC ,addressLine5
# MAGIC ,addressLine6
# MAGIC ,deletedIndicator
# MAGIC from
# MAGIC cleansed.${vars.table}

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Target and Source Data
# MAGIC %sql
# MAGIC select
# MAGIC businessPartnerRelationshipNumber
# MAGIC ,businessPartnerNumber1
# MAGIC ,businessPartnerNumber2
# MAGIC ,businessPartnerGUID1
# MAGIC ,businessPartnerGUID2
# MAGIC ,relationshipDirection
# MAGIC ,relationshipTypeCode
# MAGIC ,relationshipType
# MAGIC ,validToDate
# MAGIC ,validFromDate
# MAGIC ,countryShortName
# MAGIC ,postalCode
# MAGIC ,cityName
# MAGIC ,streetName
# MAGIC ,houseNumber
# MAGIC ,phoneNumber
# MAGIC ,emailAddress
# MAGIC ,capitalInterestPercentage
# MAGIC ,capitalInterestAmount
# MAGIC ,shortFormattedAddress
# MAGIC ,shortFormattedAddress2
# MAGIC ,addressLine0
# MAGIC ,addressLine1
# MAGIC ,addressLine2
# MAGIC ,addressLine3
# MAGIC ,addressLine4
# MAGIC ,addressLine5
# MAGIC ,addressLine6
# MAGIC ,deletedIndicator
# MAGIC from
# MAGIC cleansed.${vars.table}
# MAGIC except
# MAGIC select
# MAGIC businessPartnerRelationshipNumber
# MAGIC ,businessPartnerNumber1
# MAGIC ,businessPartnerNumber2
# MAGIC ,businessPartnerGUID1
# MAGIC ,businessPartnerGUID2
# MAGIC ,relationshipDirection
# MAGIC ,relationshipTypeCode
# MAGIC ,relationshipType
# MAGIC ,validToDate
# MAGIC ,validFromDate
# MAGIC ,countryShortName
# MAGIC ,postalCode
# MAGIC ,cityName
# MAGIC ,streetName
# MAGIC ,houseNumber
# MAGIC ,phoneNumber
# MAGIC ,emailAddress
# MAGIC ,capitalInterestPercentage
# MAGIC ,capitalInterestAmount
# MAGIC ,shortFormattedAddress
# MAGIC ,shortFormattedAddress2
# MAGIC ,addressLine0
# MAGIC ,addressLine1
# MAGIC ,addressLine2
# MAGIC ,addressLine3
# MAGIC ,addressLine4
# MAGIC ,addressLine5
# MAGIC ,addressLine6
# MAGIC ,deletedIndicator
# MAGIC from
# MAGIC (Select RELNR as businessPartnerRelationshipNumber
# MAGIC ,PARTNER1 as businessPartnerNumber1
# MAGIC ,PARTNER2 as businessPartnerNumber2
# MAGIC ,PARTNER1_GUID as businessPartnerGUID1
# MAGIC ,PARTNER2_GUID as businessPartnerGUID2
# MAGIC ,RELDIR as relationshipDirection
# MAGIC ,RELTYP as relationshipTypeCode
# MAGIC ,b.relationshipType as relationshipType
# MAGIC ,
# MAGIC --case
# MAGIC --when DATE_TO < '1900-01-01' then '2099-12-31'
# MAGIC --else DATE_TO end as validToDate
# MAGIC cast(DATE_TO as DATE) as validToDate
# MAGIC ,case
# MAGIC when DATE_FROM < '1900-01-01' then '1900-01-01'
# MAGIC else DATE_FROM end as validFromDate
# MAGIC --DATE_FROM as validFromDate
# MAGIC ,COUNTRY as countryShortName
# MAGIC ,POST_CODE1 as postalCode
# MAGIC ,CITY1 as cityName
# MAGIC ,STREET as streetName
# MAGIC ,HOUSE_NUM1 as houseNumber
# MAGIC ,TEL_NUMBER as phoneNumber
# MAGIC ,SMTP_ADDR as emailAddress
# MAGIC ,CMPY_PART_PER as capitalInterestPercentage
# MAGIC ,CMPY_PART_AMO as capitalInterestAmount
# MAGIC ,ADDR_SHORT as shortFormattedAddress
# MAGIC ,ADDR_SHORT_S as shortFormattedAddress2
# MAGIC ,LINE0 as addressLine0
# MAGIC ,LINE1 as addressLine1
# MAGIC ,LINE2 as addressLine2
# MAGIC ,LINE3 as addressLine3
# MAGIC ,LINE4 as addressLine4
# MAGIC ,LINE5 as addressLine5
# MAGIC ,LINE6 as addressLine6
# MAGIC ,FLG_DELETED as deletedIndicator
# MAGIC ,row_number() over (partition by RELNR,PARTNER1,PARTNER2,DATE_TO order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table} 
# MAGIC left join cleansed.CRM_0BP_RELTYPES_TEXT b
# MAGIC on RELDIR = b.relationshipDirection and RELTYP = b.relationshipTypeCode 
# MAGIC )a where  a.rn = 1
