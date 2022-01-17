# Databricks notebook source
#config parameters
source = 'CRM' #either CRM or ISU
table = '0BP_RELATIONS_ATTR'

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
# MAGIC businessPartnerRelationshipNumber,
# MAGIC businessPartnerNumber1,
# MAGIC businessPartnerNumber2,
# MAGIC businessPartnerGUID1,
# MAGIC businessPartnerGUID2,
# MAGIC relationshipDirection,
# MAGIC relationshipTypeCode,
# MAGIC relationshipType,
# MAGIC validToDate,
# MAGIC validFromDate,
# MAGIC countryShortName,
# MAGIC postalCode,
# MAGIC cityName,
# MAGIC streetName,
# MAGIC houseNumber,
# MAGIC phoneNumber,
# MAGIC emailAddress,
# MAGIC capitalInterestPercentage,
# MAGIC capitalInterestAmount,
# MAGIC shortFormattedAddress,
# MAGIC shortFormattedAddress2,
# MAGIC addressLine0,
# MAGIC addressLine1,
# MAGIC addressLine2,
# MAGIC addressLine3,
# MAGIC addressLine4,
# MAGIC addressLine5,
# MAGIC addressLine6,
# MAGIC deletedIndicator
# MAGIC from
# MAGIC (
# MAGIC SELECT
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
# MAGIC left join cleansed.ISU_0BP_RELTYPES_TEXT b
# MAGIC on RELDIR = b.relationshipDirection and RELTYP = b.relationshipTypeCode 
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
