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
# MAGIC ,IDNUMBER AS businessPartnerIdNumber
# MAGIC FROM
# MAGIC SOURCE

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
# MAGIC ,DATE_TO as validToDate
# MAGIC FROM
# MAGIC SOURCE

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
# MAGIC LANGU as language
# MAGIC ,TPLNR as functionalLocationNumber
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
# MAGIC SPRAS as language
# MAGIC ,ISTYPE as industrySystem
# MAGIC ,IND_SECTOR as industryCode
# MAGIC FROM
# MAGIC Source

# COMMAND ----------

# DBTITLE 1,0UC_PORTION_TEXT
# MAGIC %sql
# MAGIC select
# MAGIC TERMSCHL as portion
# MAGIC from
# MAGIC Source

# COMMAND ----------

# DBTITLE 1,0DIVISION_TEXT
# MAGIC %sql
# MAGIC select
# MAGIC TERMSCHL as portion
# MAGIC from
# MAGIC Source

# COMMAND ----------

# DBTITLE 1,0UC_DEVINST_ATTR
# MAGIC %sql
# MAGIC select
# MAGIC ANLAGE as installationId
# MAGIC ,LOGIKNR as logicalDeviceNumber
# MAGIC ,BIS as validToDate
# MAGIC from Source

# COMMAND ----------

# DBTITLE 1,0UC_DISCREAS_TEXT
# MAGIC %sql
# MAGIC select
# MAGIC LANGU as language
# MAGIC ,KEY1 as sectorCategoryCode
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
# MAGIC ,SPRAS as language
# MAGIC from
# MAGIC Source

# COMMAND ----------

# DBTITLE 1,0UC_STATTART_TEXT 
# MAGIC %sql
# MAGIC select
# MAGIC SPRAS language
# MAGIC ,TARIFART as rateTypeCode
# MAGIC from source

# COMMAND ----------

# DBTITLE 1,0UC_TARIFNR_TEXT
# MAGIC %sql
# MAGIC select
# MAGIC SPRAS as language
# MAGIC ,TARIFNR as rateId
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
