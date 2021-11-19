# Databricks notebook source
# DBTITLE 1,0BP_DEF_ADDRESS_ATTR
# MAGIC %sql
# MAGIC SELECT
# MAGIC PARTNER AS businessPartnerNumber,
# MAGIC ADDRNUMBER AS addressNumber
# MAGIC FROM SOURCE

# COMMAND ----------

# DBTITLE 1,0BP_ID_ATTR
# MAGIC %sql
# MAGIC SELECT
# MAGIC PARTNER AS businessPartnerNumber
# MAGIC ,TYPE AS identificationTypeCode
# MAGIC ,b.TEXT as identificationType
# MAGIC ,IDNUMBER AS businessPartnerIdNumber
# MAGIC ,INSTITUTE   as institute
# MAGIC ENTRY_DATE as entryDate
# MAGIC VALID_DATE_FROM as validFromDate
# MAGIC VALID_DATE_TO as validToDate
# MAGIC COUNTRY as countryShortName
# MAGIC REGION as stateCode
# MAGIC PARTNER_GUID as businessPartnerGUID
# MAGIC FLG_DEL_BW as deletedIndicator
# MAGIC FROM
# MAGIC SOURCE a
# MAGIC left join 0BP_ID_TYPE_TEXT b
# MAGIC on a.TYPE = b.TYPE

# COMMAND ----------

# DBTITLE 1,0BP_ID_TYPE_TEXT
# MAGIC %sql
# MAGIC SELECT
# MAGIC SPRAS as language,
# MAGIC TYPE as identificationTypeCode
# MAGIC FROM
# MAGIC Source

# COMMAND ----------

# DBTITLE 1,0BP_RELATIONS_ATTR
# MAGIC %sql
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
# MAGIC ,DATE_FROM as validFromDate
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
# MAGIC from Source a
# MAGIC left join 0BP_RELTYPES_TEXT b
# MAGIC on a.RELDIR = b.RELDIR and a.RELTYP = b.RELTYP 

# COMMAND ----------

# DBTITLE 1,0BP_RELTYPES_TEXT
# MAGIC %sql
# MAGIC SELECT
# MAGIC LANGU as language
# MAGIC ,RELDIR as relationshipDirection
# MAGIC ,RELTYP as relationshipTypeCode
# MAGIC FROM SOURCE

# COMMAND ----------

# DBTITLE 1,0CACONT_ACC_ATTR_2
# MAGIC %sql
# MAGIC SELECT
# MAGIC MANDT as clientId
# MAGIC VKONT as contractAccountNumber
# MAGIC FROM
# MAGIC SOURCE

# COMMAND ----------

# DBTITLE 1,0FC_ACCTREL_TEXT
# MAGIC %sql
# MAGIC SELECT
# MAGIC SPRAS as language
# MAGIC ,VKPBZ as accountRelationshipCode
# MAGIC FROM
# MAGIC SOURCE

# COMMAND ----------

# DBTITLE 1,0FC_ACCNTBP_ATTR_2
# MAGIC %sql
# MAGIC SELECT
# MAGIC VKONT as contractAccountNumber
# MAGIC ,GPART as businessPartnerGroupNumber
# MAGIC FROM
# MAGIC Source

# COMMAND ----------

# DBTITLE 1,0FL_TYPE_TEXT
# MAGIC %sql
# MAGIC SELECT
# MAGIC SPRAS as language
# MAGIC ,FLTYP as functionalLocationCategoryCode
# MAGIC FROM
# MAGIC Source

# COMMAND ----------

# DBTITLE 1,0FUNCT_LOC_TEXT
# MAGIC %sql
# MAGIC SELECT
# MAGIC TPLNR as functionalLocationNumber
# MAGIC ,TXTMD as functionalLocationDescription
# MAGIC ,ERDAT as createdDate
# MAGIC ,AEDAT as lastChangedDate
# MAGIC FROM
# MAGIC Source

# COMMAND ----------

# DBTITLE 1,0GHO_NETOBJS_TEXT
# MAGIC %sql
# MAGIC SELECT
# MAGIC TPLNR as functionalLocationNumber
# MAGIC ,SPRAS as language
# MAGIC FROM
# MAGIC Source

# COMMAND ----------

# DBTITLE 1,0IND_SECTOR_TEXT
# MAGIC %sql
# MAGIC SELECT
# MAGIC ISTYPE as industrySystem
# MAGIC ,IND_SECTOR as industryCode
# MAGIC ,TEXT as industry
# MAGIC FROM
# MAGIC Source

# COMMAND ----------

# DBTITLE 1,0UC_PORTION_TEXT
# MAGIC %sql
# MAGIC select
# MAGIC TERMSCHL as portion
# MAGIC ,TERMTEXT as scheduleMasterRecord
# MAGIC from
# MAGIC Source

# COMMAND ----------

# DBTITLE 1,0DIVISION_TEXT
# MAGIC %sql
# MAGIC select
# MAGIC SPRAS as language
# MAGIC ,SPART as divisionCode
# MAGIC ,VTEXT as division
# MAGIC from
# MAGIC Source

# COMMAND ----------

# DBTITLE 1,0UC_DEVINST_ATTR
# MAGIC %sql
# MAGIC select
# MAGIC ANLAGE	as	installationId
# MAGIC ,LOGIKNR as	logicalDeviceNumber
# MAGIC ,BIS as	validToDate
# MAGIC ,AB	as	validFromDate
# MAGIC ,PREISKLA as priceClassCode
# MAGIC ,b.PREISKLA as	priceClass
# MAGIC ,GVERRECH as payRentalPrice
# MAGIC ,TARIFART as rateTypeCode
# MAGIC ,c.TARIFART as	rateType
# MAGIC ,LOEVM	as deletedIndicator
# MAGIC ,UPDMOD	as bwDeltaProcess
# MAGIC ,ZOPCODE as	operationCode
# MAGIC from Source a
# MAGIC left join 0UC_PRICCLA_TEXT b
# MAGIC on a.PREISKLA = b.PREISKLA and b.SPRAS='E'
# MAGIC left join 0UC_STATTART_TEXT c
# MAGIC on a.TARIFART = c.TARIFART  and b.SPRAS='E'

# COMMAND ----------

# DBTITLE 1,0UC_DISCREAS_TEXT
# MAGIC %sql
# MAGIC select
# MAGIC KEY1 as disconnecionReasonCode
# MAGIC ,TXTLG as disconnecionReason
# MAGIC from 
# MAGIC source

# COMMAND ----------

# DBTITLE 1,0UC_REGIST_ATTR
# MAGIC %sql
# MAGIC select
# MAGIC EEQUNR as equipmentNumber
# MAGIC ,ZWNUMMER as registerNumber
# MAGIC ,BIS as validToDate
# MAGIC ,AB as validFromDate
# MAGIC ,LOGIKZW as logicalRegisterNumber
# MAGIC ,SPARTYP as divisionCategoryCode
# MAGIC ,b.TXTLG as divisionCategory
# MAGIC ,ZWKENN as registerIdCode
# MAGIC ,c.ZWKTXT as registerId
# MAGIC ,ZWART as registerTypeCode
# MAGIC ,d.ZWARTTXT as registerType
# MAGIC ,ZWTYP as registerCategoryCode
# MAGIC ,f.DDTEXT as registerCategory
# MAGIC ,BLIWIRK as reactiveApparentOrActiveRegister
# MAGIC ,MASSREAD as unitOfMeasurementMeterReading 
# MAGIC ,NABLESEN as doNotReadIndicator
# MAGIC ,HOEKORR as altitudeCorrectionPressure
# MAGIC ,KZAHLE as setGasLawDeviationFactor
# MAGIC ,KZAHLT as actualGasLawDeviationFactor
# MAGIC ,CRGPRESS as gasCorrectionPressure
# MAGIC ,INTSIZEID as intervalLengthId
# MAGIC ,LOEVM as deletedIndicator
# MAGIC from
# MAGIC Source a
# MAGIC left join 0UCDIVISCAT_TEXT b
# MAGIC on a.SPARTYP = b.KEY1  and b.LANGU='E'
# MAGIC left join ZDSREGIDT c
# MAGIC on a.SPARTYP = c.SPARTYP and a.ZWKENN = c.ZWKENN and c.SPRAS ='E'
# MAGIC left join ZDSREGTYPET d
# MAGIC on a.ZWART = d.ZWART and d.SPRAS='E'
# MAGIC left join DD07T f
# MAGIC on a.ZWTYP = f.DOMVALUE_L and f.DDLANGUAGE ='E' and f.DOMNAME ='E_ZWTYP'

# COMMAND ----------

# DBTITLE 1,0UC_SERTYPE_TEXT
# MAGIC %sql
# MAGIC select
# MAGIC SERVICE  serviceTypeCode
# MAGIC ,SERVICETEXT as serviceType
# MAGIC from
# MAGIC Source

# COMMAND ----------

# DBTITLE 1,0UC_STATTART_TEXT 
# MAGIC %sql
# MAGIC select
# MAGIC TARIFART as rateTypeCode
# MAGIC ,TEXT30 as rateType
# MAGIC from source

# COMMAND ----------

# DBTITLE 1,0UC_TARIFNR_TEXT
# MAGIC %sql
# MAGIC select
# MAGIC TARIFNR as rateId
# MAGIC ,TARIFBEZ as rateDescription
# MAGIC from source

# COMMAND ----------

# DBTITLE 1,0UCCONTRACTH_ATTR_2
# MAGIC %sql
# MAGIC select
# MAGIC VERTRAG as contractId
# MAGIC ,BIS  as validToDate
# MAGIC from source

# COMMAND ----------

# DBTITLE 1,0UCDIVISCAT_TEXT
# MAGIC %sql
# MAGIC select
# MAGIC LANGU as language
# MAGIC ,KEY1 as sectorCategoryCode
# MAGIC from source

# COMMAND ----------

# DBTITLE 1,0UCINSTALLA_ATTR_2
# MAGIC %sql
# MAGIC select
# MAGIC ANLAGE as installationId
# MAGIC from source

# COMMAND ----------

# DBTITLE 1,PTAXCATEGORYTU
# MAGIC %sql
# MAGIC select
# MAGIC KALSM as procedure
# MAGIC ,MWSKZ as taxSalesCode
# MAGIC ,SPRAS as language
# MAGIC from Source

# COMMAND ----------

# DBTITLE 1,0UC_ACCNTBP_ATTR_2
# MAGIC %sql
# MAGIC SELECT
# MAGIC GPART as businessPartnerGroupNumber
# MAGIC VKONT as contractAccountNumber
# MAGIC FROM
# MAGIC Source
