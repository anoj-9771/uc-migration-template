# Databricks notebook source
#config parameters
source = 'ISU' #either CRM or ISU
table = '0UC_CONNOBJ_ATTR_2'

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
# MAGIC propertyNumber
# MAGIC ,countryShortName
# MAGIC cityCode
# MAGIC ,streetCode
# MAGIC ,a.streetName
# MAGIC ,postCode
# MAGIC ,stateCode
# MAGIC ,regionGroup
# MAGIC ,politicalRegionCode
# MAGIC ,politicalRegion
# MAGIC ,connectionObjectGUID
# MAGIC ,regionGroupPermit
# MAGIC ,planTypeCode
# MAGIC ,planType
# MAGIC ,processTypeCode
# MAGIC ,processType
# MAGIC ,planNumber
# MAGIC ,lotTypeCode
# MAGIC ,lotNumber
# MAGIC ,sectionNumber
# MAGIC ,serviceAgreementIndicator
# MAGIC ,mlimIndicator
# MAGIC ,wicaIndicator
# MAGIC ,sopaIndicator
# MAGIC ,architecturalObjectNumber
# MAGIC ,architecturalObjectTypeCode
# MAGIC ,architecturalObjectType
# MAGIC ,buildingFeeDate
# MAGIC ,cadId
# MAGIC ,pumpWateWaterIndicator
# MAGIC ,fireServiceIndicator
# MAGIC ,architecturalObjectInternalId
# MAGIC ,architecturalObjectId
# MAGIC ,fixtureAndFittingCharacteristic
# MAGIC ,houseAddressNumber
# MAGIC ,WWTP
# MAGIC ,SCAMP
# MAGIC ,houseStreetName
# MAGIC ,houseNumber
# MAGIC ,LGA
# MAGIC ,cityName
# MAGIC ,waterDeliverySystem
# MAGIC ,waterDistributionSystem
# MAGIC ,waterSupplyZone
# MAGIC ,receycleWaterDeliverySystem
# MAGIC ,receycleWaterDistributionSystem
# MAGIC ,recycleWaterSupplyZone
# MAGIC ,superiorPropertyTypeCode
# MAGIC ,superiorPropertyType
# MAGIC ,inferiorPropertyTypeCode
# MAGIC ,inferiorPropertyType
# MAGIC ,objectReferenceIndicator
# MAGIC ,objectNumber
# MAGIC ,flatCount
# MAGIC ,validFromDate
# MAGIC from
# MAGIC (select
# MAGIC HAUS as propertyNumber
# MAGIC ,k.COUNTRY as countryShortName
# MAGIC ,CITY_CODE as cityCode
# MAGIC ,k.STREETCODE as streetCode
# MAGIC ,b.streetName as streetName 
# MAGIC ,POSTALCODE as postCode
# MAGIC ,REGION as stateCode
# MAGIC ,REGIO_GRP as regionGroup
# MAGIC ,k.REGPOLIT as politicalRegionCode
# MAGIC ,c.REGNAME as politicalRegion 
# MAGIC ,wwTP as connectionObjectGUID
# MAGIC ,REGIOGROUP_PERM as regionGroupPermit
# MAGIC ,ZCD_PLAN_TYPE as planTypeCode
# MAGIC ,d.DESCRIPTION as planType
# MAGIC ,ZCD_PROCESS_TYPE as processTypeCode
# MAGIC ,e.DESCRIPTION as processType
# MAGIC ,ZCD_PLAN_NUMBER as planNumber
# MAGIC ,ZCD_LOT_TYPE as lotTypeCode
# MAGIC ,ZCD_LOT_NUMBER as lotNumber
# MAGIC ,ZCD_SECTION_NUMBER as sectionNumber
# MAGIC ,ZCD_IND_SRV_AGR as serviceAgreementIndicator
# MAGIC ,ZCD_IND_MLIM as mlimIndicator
# MAGIC ,ZCD_IND_WICA as wicaIndicator
# MAGIC ,ZCD_IND_SOPA as sopaIndicator
# MAGIC ,ZCD_AONR as architecturalObjectNumber
# MAGIC ,ZCD_AOTYPE as architecturalObjectTypeCode
# MAGIC ,XMAOTYPE as architecturalObjectType
# MAGIC ,ZCD_BLD_FEE_DATE as buildingFeeDate
# MAGIC ,ZCD_CAD_ID as cadId
# MAGIC ,ZZPUMP_WW as pumpWateWaterIndicator
# MAGIC ,ZZFIRE_SERVICE as fireServiceIndicator
# MAGIC ,ZINTRENO as architecturalObjectInternalId
# MAGIC ,ZAOID as architecturalObjectId
# MAGIC ,ZFIXFITCHARACT as fixtureAndFittingCharacteristic
# MAGIC ,HOUSE_ADRNR as houseAddressNumber
# MAGIC ,WWTP as WWTP
# MAGIC ,SCAMP as SCAMP
# MAGIC ,HAUS_STREET as houseStreetName
# MAGIC ,HAUS_NUM1 as houseNumber
# MAGIC ,HAUS_LGA_NAME as LGA
# MAGIC ,HOUS_CITY1 as cityName
# MAGIC ,WATER_DELIVERY_SYSTEM as waterDeliverySystem
# MAGIC ,WATER_DISTRIBUTION_SYSTEM_WATE as waterDistributionSystem
# MAGIC ,WATER_SUPPLY_ZONE as waterSupplyZone
# MAGIC ,RECYCLE_WATER_DELIVERY_SYSTEM as receycleWaterDeliverySystem
# MAGIC ,RECYCLE_WATER_DISTRIBUTION_SYS as receycleWaterDistributionSystem
# MAGIC ,RECYCLE_WATER_SUPPLY_ZONE as recycleWaterSupplyZone
# MAGIC ,ZCD_SUP_PROP_TYPE as superiorPropertyTypeCode
# MAGIC ,ref1.superiorPropertyType as superiorPropertyType
# MAGIC ,ZCD_INF_PROP_TYPE as inferiorPropertyTypeCode
# MAGIC ,ref2.inferiorPropertyType as inferiorPropertyType
# MAGIC ,Z_OWNER as objectReferenceIndicator
# MAGIC ,Z_OBJNR as objectNumber
# MAGIC ,ZCD_NO_OF_FLATS as flatCount
# MAGIC ,DATE_FROM as validFromDate
# MAGIC ,row_number() over (partition by HAUS,k.COUNTRY order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table} k
# MAGIC LEFT JOIN cleansed.isu_0CAM_STREETCODE_TEXT b
# MAGIC  ON k.STREETCODE = b.streetCode
# MAGIC LEFT JOIN cleansed.isu_TE227T c
# MAGIC ON k.REGPOLIT = c.REGPOLIT
# MAGIC LEFT JOIN cleansed.isu_zcd_tplantype_tx d
# MAGIC ON k.ZCD_PLAN_TYPE = d.PLAN_TYPE 
# MAGIC LEFT JOIN cleansed.isu_zcd_tproctype_tx e
# MAGIC ON k.ZCD_PROCESS_TYPE = e.PROCESS_TYPE
# MAGIC LEFT JOIN cleansed.isu_tivbdarobjtypet f
# MAGIC ON k.ZCD_AOTYPE = f.AOTYPE
# MAGIC LEFT JOIN cleansed.isu_zcd_tsupprtyp_tx ref1 ON k.ZCD_SUP_PROP_TYPE = ref1.superiorPropertyTypeCode 
# MAGIC LEFT JOIN  cleansed.isu_zcd_tinfprty_tx ref2 ON k.ZCD_INF_PROP_TYPE = ref2.inferiorPropertyTypeCode
# MAGIC )a where  a.rn = 1

# COMMAND ----------

# DBTITLE 1,[Verification] Count Check
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from cleansed.${vars.table}
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from (select
# MAGIC propertyNumber
# MAGIC ,countryShortName
# MAGIC cityCode
# MAGIC ,streetCode
# MAGIC ,a.streetName
# MAGIC ,postCode
# MAGIC ,stateCode
# MAGIC ,regionGroup
# MAGIC ,politicalRegionCode
# MAGIC ,politicalRegion
# MAGIC ,connectionObjectGUID
# MAGIC ,regionGroupPermit
# MAGIC ,planTypeCode
# MAGIC ,planType
# MAGIC ,processTypeCode
# MAGIC ,processType
# MAGIC ,planNumber
# MAGIC ,lotTypeCode
# MAGIC ,lotNumber
# MAGIC ,sectionNumber
# MAGIC ,serviceAgreementIndicator
# MAGIC ,mlimIndicator
# MAGIC ,wicaIndicator
# MAGIC ,sopaIndicator
# MAGIC ,architecturalObjectNumber
# MAGIC ,architecturalObjectTypeCode
# MAGIC ,architecturalObjectType
# MAGIC ,buildingFeeDate
# MAGIC ,cadId
# MAGIC ,pumpWateWaterIndicator
# MAGIC ,fireServiceIndicator
# MAGIC ,architecturalObjectInternalId
# MAGIC ,architecturalObjectId
# MAGIC ,fixtureAndFittingCharacteristic
# MAGIC ,houseAddressNumber
# MAGIC ,WWTP
# MAGIC ,SCAMP
# MAGIC ,houseStreetName
# MAGIC ,houseNumber
# MAGIC ,LGA
# MAGIC ,cityName
# MAGIC ,waterDeliverySystem
# MAGIC ,waterDistributionSystem
# MAGIC ,waterSupplyZone
# MAGIC ,receycleWaterDeliverySystem
# MAGIC ,receycleWaterDistributionSystem
# MAGIC ,recycleWaterSupplyZone
# MAGIC ,superiorPropertyTypeCode
# MAGIC ,superiorPropertyType
# MAGIC ,inferiorPropertyTypeCode
# MAGIC ,inferiorPropertyType
# MAGIC ,objectReferenceIndicator
# MAGIC ,objectNumber
# MAGIC ,flatCount
# MAGIC ,validFromDate
# MAGIC from
# MAGIC (select
# MAGIC HAUS as propertyNumber
# MAGIC ,k.COUNTRY as countryShortName
# MAGIC ,CITY_CODE as cityCode
# MAGIC ,k.STREETCODE as streetCode
# MAGIC ,b.streetName as streetName 
# MAGIC ,POSTALCODE as postCode
# MAGIC ,REGION as stateCode
# MAGIC ,REGIO_GRP as regionGroup
# MAGIC ,k.REGPOLIT as politicalRegionCode
# MAGIC ,c.REGNAME as politicalRegion 
# MAGIC ,wwTP as connectionObjectGUID
# MAGIC ,REGIOGROUP_PERM as regionGroupPermit
# MAGIC ,ZCD_PLAN_TYPE as planTypeCode
# MAGIC ,d.DESCRIPTION as planType
# MAGIC ,ZCD_PROCESS_TYPE as processTypeCode
# MAGIC ,e.DESCRIPTION as processType
# MAGIC ,ZCD_PLAN_NUMBER as planNumber
# MAGIC ,ZCD_LOT_TYPE as lotTypeCode
# MAGIC ,ZCD_LOT_NUMBER as lotNumber
# MAGIC ,ZCD_SECTION_NUMBER as sectionNumber
# MAGIC ,ZCD_IND_SRV_AGR as serviceAgreementIndicator
# MAGIC ,ZCD_IND_MLIM as mlimIndicator
# MAGIC ,ZCD_IND_WICA as wicaIndicator
# MAGIC ,ZCD_IND_SOPA as sopaIndicator
# MAGIC ,ZCD_AONR as architecturalObjectNumber
# MAGIC ,ZCD_AOTYPE as architecturalObjectTypeCode
# MAGIC ,XMAOTYPE as architecturalObjectType
# MAGIC ,ZCD_BLD_FEE_DATE as buildingFeeDate
# MAGIC ,ZCD_CAD_ID as cadId
# MAGIC ,ZZPUMP_WW as pumpWateWaterIndicator
# MAGIC ,ZZFIRE_SERVICE as fireServiceIndicator
# MAGIC ,ZINTRENO as architecturalObjectInternalId
# MAGIC ,ZAOID as architecturalObjectId
# MAGIC ,ZFIXFITCHARACT as fixtureAndFittingCharacteristic
# MAGIC ,HOUSE_ADRNR as houseAddressNumber
# MAGIC ,WWTP as WWTP
# MAGIC ,SCAMP as SCAMP
# MAGIC ,HAUS_STREET as houseStreetName
# MAGIC ,HAUS_NUM1 as houseNumber
# MAGIC ,HAUS_LGA_NAME as LGA
# MAGIC ,HOUS_CITY1 as cityName
# MAGIC ,WATER_DELIVERY_SYSTEM as waterDeliverySystem
# MAGIC ,WATER_DISTRIBUTION_SYSTEM_WATE as waterDistributionSystem
# MAGIC ,WATER_SUPPLY_ZONE as waterSupplyZone
# MAGIC ,RECYCLE_WATER_DELIVERY_SYSTEM as receycleWaterDeliverySystem
# MAGIC ,RECYCLE_WATER_DISTRIBUTION_SYS as receycleWaterDistributionSystem
# MAGIC ,RECYCLE_WATER_SUPPLY_ZONE as recycleWaterSupplyZone
# MAGIC ,ZCD_SUP_PROP_TYPE as superiorPropertyTypeCode
# MAGIC ,ref1.superiorPropertyType as superiorPropertyType
# MAGIC ,ZCD_INF_PROP_TYPE as inferiorPropertyTypeCode
# MAGIC ,ref2.inferiorPropertyType as inferiorPropertyType
# MAGIC ,Z_OWNER as objectReferenceIndicator
# MAGIC ,Z_OBJNR as objectNumber
# MAGIC ,ZCD_NO_OF_FLATS as flatCount
# MAGIC ,DATE_FROM as validFromDate
# MAGIC ,row_number() over (partition by HAUS,k.COUNTRY order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table} k
# MAGIC LEFT JOIN cleansed.isu_0CAM_STREETCODE_TEXT b
# MAGIC  ON k.STREETCODE = b.streetCode
# MAGIC LEFT JOIN cleansed.isu_TE227T c
# MAGIC ON k.REGPOLIT = c.REGPOLIT
# MAGIC LEFT JOIN cleansed.isu_zcd_tplantype_tx d
# MAGIC ON k.ZCD_PLAN_TYPE = d.PLAN_TYPE 
# MAGIC LEFT JOIN cleansed.isu_zcd_tproctype_tx e
# MAGIC ON k.ZCD_PROCESS_TYPE = e.PROCESS_TYPE
# MAGIC LEFT JOIN cleansed.isu_tivbdarobjtypet f
# MAGIC ON k.ZCD_AOTYPE = f.AOTYPE
# MAGIC LEFT JOIN cleansed.isu_zcd_tsupprtyp_tx ref1 ON k.ZCD_SUP_PROP_TYPE = ref1.superiorPropertyTypeCode 
# MAGIC LEFT JOIN  cleansed.isu_zcd_tinfprty_tx ref2 ON k.ZCD_INF_PROP_TYPE = ref2.inferiorPropertyTypeCode
# MAGIC )a where  a.rn = 1
# MAGIC )

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT propertyNumber,countryShortName
# MAGIC , COUNT (*) as count
# MAGIC FROM cleansed.${vars.table}
# MAGIC GROUP BY propertyNumber,countryShortName
# MAGIC HAVING COUNT (*) > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT * FROM (
# MAGIC SELECT
# MAGIC *,
# MAGIC row_number() OVER(PARTITION BY propertyNumber,countryShortName  order by propertyNumber,countryShortName) as rn
# MAGIC FROM  cleansed.${vars.table}
# MAGIC )a where a.rn > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Source and Target Data
# MAGIC %sql
# MAGIC select
# MAGIC case when propertyNumber is null then '' else propertyNumber end as propertyNumber
# MAGIC ,case when countryShortName is null then '' else countryShortName end as countryShortName
# MAGIC ,cityCode
# MAGIC ,streetCode
# MAGIC ,streetName
# MAGIC ,postCode
# MAGIC ,stateCode
# MAGIC ,regionGroup
# MAGIC ,politicalRegionCode
# MAGIC ,politicalRegion
# MAGIC ,connectionObjectGUID
# MAGIC ,regionGroupPermit
# MAGIC ,planTypeCode
# MAGIC ,planType
# MAGIC ,processTypeCode
# MAGIC ,processType
# MAGIC ,planNumber
# MAGIC ,lotTypeCode
# MAGIC ,lotNumber
# MAGIC ,sectionNumber
# MAGIC ,serviceAgreementIndicator
# MAGIC ,mlimIndicator
# MAGIC ,wicaIndicator
# MAGIC ,sopaIndicator
# MAGIC ,architecturalObjectNumber
# MAGIC ,architecturalObjectTypeCode
# MAGIC ,architecturalObjectType
# MAGIC ,buildingFeeDate
# MAGIC ,cadId
# MAGIC ,pumpWateWaterIndicator
# MAGIC ,fireServiceIndicator
# MAGIC ,architecturalObjectInternalId
# MAGIC ,architecturalObjectId
# MAGIC ,fixtureAndFittingCharacteristic
# MAGIC ,houseAddressNumber
# MAGIC ,WWTP
# MAGIC ,SCAMP
# MAGIC ,houseStreetName
# MAGIC ,houseNumber
# MAGIC ,LGA
# MAGIC ,cityName
# MAGIC ,waterDeliverySystem
# MAGIC ,waterDistributionSystem
# MAGIC ,waterSupplyZone
# MAGIC ,receycleWaterDeliverySystem
# MAGIC ,receycleWaterDistributionSystem
# MAGIC ,recycleWaterSupplyZone
# MAGIC ,superiorPropertyTypeCode
# MAGIC ,superiorPropertyType
# MAGIC ,inferiorPropertyTypeCode
# MAGIC ,inferiorPropertyType
# MAGIC ,objectReferenceIndicator
# MAGIC ,objectNumber
# MAGIC ,flatCount
# MAGIC ,case when validFromDate < '1900-01-01' then '1900-01-01'
# MAGIC when validFromDate is null then '1900-01-01'
# MAGIC else validFromDate end as validFromDate
# MAGIC from
# MAGIC (select
# MAGIC HAUS as propertyNumber
# MAGIC ,k.COUNTRY as countryShortName
# MAGIC ,CITY_CODE as cityCode
# MAGIC ,k.STREETCODE as streetCode
# MAGIC ,b.streetName as streetName 
# MAGIC ,POSTALCODE as postCode
# MAGIC ,REGION as stateCode
# MAGIC ,REGIO_GRP as regionGroup
# MAGIC ,k.REGPOLIT as politicalRegionCode
# MAGIC ,c.REGNAME as politicalRegion 
# MAGIC ,wwTP as connectionObjectGUID
# MAGIC ,REGIOGROUP_PERM as regionGroupPermit
# MAGIC ,ZCD_PLAN_TYPE as planTypeCode
# MAGIC ,d.DESCRIPTION as planType
# MAGIC ,ZCD_PROCESS_TYPE as processTypeCode
# MAGIC ,e.DESCRIPTION as processType
# MAGIC ,ZCD_PLAN_NUMBER as planNumber
# MAGIC ,ZCD_LOT_TYPE as lotTypeCode
# MAGIC ,ZCD_LOT_NUMBER as lotNumber
# MAGIC ,ZCD_SECTION_NUMBER as sectionNumber
# MAGIC ,ZCD_IND_SRV_AGR as serviceAgreementIndicator
# MAGIC ,ZCD_IND_MLIM as mlimIndicator
# MAGIC ,ZCD_IND_WICA as wicaIndicator
# MAGIC ,ZCD_IND_SOPA as sopaIndicator
# MAGIC ,ZCD_AONR as architecturalObjectNumber
# MAGIC ,ZCD_AOTYPE as architecturalObjectTypeCode
# MAGIC ,XMAOTYPE as architecturalObjectType
# MAGIC ,ZCD_BLD_FEE_DATE as buildingFeeDate
# MAGIC ,ZCD_CAD_ID as cadId
# MAGIC ,ZZPUMP_WW as pumpWateWaterIndicator
# MAGIC ,ZZFIRE_SERVICE as fireServiceIndicator
# MAGIC ,ZINTRENO as architecturalObjectInternalId
# MAGIC ,ZAOID as architecturalObjectId
# MAGIC ,ZFIXFITCHARACT as fixtureAndFittingCharacteristic
# MAGIC ,HOUSE_ADRNR as houseAddressNumber
# MAGIC ,WWTP as WWTP
# MAGIC ,SCAMP as SCAMP
# MAGIC ,HAUS_STREET as houseStreetName
# MAGIC ,HAUS_NUM1 as houseNumber
# MAGIC ,HAUS_LGA_NAME as LGA
# MAGIC ,HOUS_CITY1 as cityName
# MAGIC ,WATER_DELIVERY_SYSTEM as waterDeliverySystem
# MAGIC ,WATER_DISTRIBUTION_SYSTEM_WATE as waterDistributionSystem
# MAGIC ,WATER_SUPPLY_ZONE as waterSupplyZone
# MAGIC ,RECYCLE_WATER_DELIVERY_SYSTEM as receycleWaterDeliverySystem
# MAGIC ,RECYCLE_WATER_DISTRIBUTION_SYS as receycleWaterDistributionSystem
# MAGIC ,RECYCLE_WATER_SUPPLY_ZONE as recycleWaterSupplyZone
# MAGIC ,ZCD_SUP_PROP_TYPE as superiorPropertyTypeCode
# MAGIC ,ref1.superiorPropertyType as superiorPropertyType
# MAGIC ,ZCD_INF_PROP_TYPE as inferiorPropertyTypeCode
# MAGIC ,ref2.inferiorPropertyType as inferiorPropertyType
# MAGIC ,Z_OWNER as objectReferenceIndicator
# MAGIC ,Z_OBJNR as objectNumber
# MAGIC ,ZCD_NO_OF_FLATS as flatCount
# MAGIC ,DATE_FROM as validFromDate
# MAGIC ,row_number() over (partition by HAUS,k.COUNTRY order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table} k
# MAGIC LEFT JOIN cleansed.isu_0CAM_STREETCODE_TEXT b
# MAGIC  ON k.STREETCODE = b.streetCode
# MAGIC LEFT JOIN cleansed.isu_TE227T c
# MAGIC ON k.REGPOLIT = c.REGPOLIT
# MAGIC LEFT JOIN cleansed.isu_zcd_tplantype_tx d
# MAGIC ON k.ZCD_PLAN_TYPE = d.PLAN_TYPE 
# MAGIC LEFT JOIN cleansed.isu_zcd_tproctype_tx e
# MAGIC ON k.ZCD_PROCESS_TYPE = e.PROCESS_TYPE
# MAGIC LEFT JOIN cleansed.isu_tivbdarobjtypet f
# MAGIC ON k.ZCD_AOTYPE = f.AOTYPE
# MAGIC LEFT JOIN cleansed.isu_zcd_tsupprtyp_tx ref1 ON k.ZCD_SUP_PROP_TYPE = ref1.superiorPropertyTypeCode 
# MAGIC LEFT JOIN  cleansed.isu_zcd_tinfprty_tx ref2 ON k.ZCD_INF_PROP_TYPE = ref2.inferiorPropertyTypeCode
# MAGIC )a where  a.rn = 1
# MAGIC except
# MAGIC select
# MAGIC propertyNumber
# MAGIC ,countryShortName
# MAGIC ,cityCode
# MAGIC ,streetCode
# MAGIC ,streetName
# MAGIC ,postCode
# MAGIC ,stateCode
# MAGIC ,regionGroup
# MAGIC ,politicalRegionCode
# MAGIC ,politicalRegion
# MAGIC ,connectionObjectGUID
# MAGIC ,regionGroupPermit
# MAGIC ,planTypeCode
# MAGIC ,planType
# MAGIC ,processTypeCode
# MAGIC ,processType
# MAGIC ,planNumber
# MAGIC ,lotTypeCode
# MAGIC ,lotNumber
# MAGIC ,sectionNumber
# MAGIC ,serviceAgreementIndicator
# MAGIC ,mlimIndicator
# MAGIC ,wicaIndicator
# MAGIC ,sopaIndicator
# MAGIC ,architecturalObjectNumber
# MAGIC ,architecturalObjectTypeCode
# MAGIC ,architecturalObjectType
# MAGIC ,buildingFeeDate
# MAGIC ,cadId
# MAGIC ,pumpWateWaterIndicator
# MAGIC ,fireServiceIndicator
# MAGIC ,architecturalObjectInternalId
# MAGIC ,architecturalObjectId
# MAGIC ,fixtureAndFittingCharacteristic
# MAGIC ,houseAddressNumber
# MAGIC ,WWTP
# MAGIC ,SCAMP
# MAGIC ,houseStreetName
# MAGIC ,houseNumber
# MAGIC ,LGA
# MAGIC ,cityName
# MAGIC ,waterDeliverySystem
# MAGIC ,waterDistributionSystem
# MAGIC ,waterSupplyZone
# MAGIC ,receycleWaterDeliverySystem
# MAGIC ,receycleWaterDistributionSystem
# MAGIC ,recycleWaterSupplyZone
# MAGIC ,superiorPropertyTypeCode
# MAGIC ,superiorPropertyType
# MAGIC ,inferiorPropertyTypeCode
# MAGIC ,inferiorPropertyType
# MAGIC ,objectReferenceIndicator
# MAGIC ,objectNumber
# MAGIC ,flatCount
# MAGIC ,validFromDate
# MAGIC from
# MAGIC cleansed.${vars.table}

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Target and Source Data
# MAGIC %sql
# MAGIC select
# MAGIC propertyNumber
# MAGIC ,countryShortName
# MAGIC ,cityCode
# MAGIC ,streetCode
# MAGIC ,streetName
# MAGIC ,postCode
# MAGIC ,stateCode
# MAGIC ,regionGroup
# MAGIC ,politicalRegionCode
# MAGIC ,politicalRegion
# MAGIC ,connectionObjectGUID
# MAGIC ,regionGroupPermit
# MAGIC ,planTypeCode
# MAGIC ,planType
# MAGIC ,processTypeCode
# MAGIC ,processType
# MAGIC ,planNumber
# MAGIC ,lotTypeCode
# MAGIC ,lotNumber
# MAGIC ,sectionNumber
# MAGIC ,serviceAgreementIndicator
# MAGIC ,mlimIndicator
# MAGIC ,wicaIndicator
# MAGIC ,sopaIndicator
# MAGIC ,architecturalObjectNumber
# MAGIC ,architecturalObjectTypeCode
# MAGIC ,architecturalObjectType
# MAGIC ,buildingFeeDate
# MAGIC ,cadId
# MAGIC ,pumpWateWaterIndicator
# MAGIC ,fireServiceIndicator
# MAGIC ,architecturalObjectInternalId
# MAGIC ,architecturalObjectId
# MAGIC ,fixtureAndFittingCharacteristic
# MAGIC ,houseAddressNumber
# MAGIC ,WWTP
# MAGIC ,SCAMP
# MAGIC ,houseStreetName
# MAGIC ,houseNumber
# MAGIC ,LGA
# MAGIC ,cityName
# MAGIC ,waterDeliverySystem
# MAGIC ,waterDistributionSystem
# MAGIC ,waterSupplyZone
# MAGIC ,receycleWaterDeliverySystem
# MAGIC ,receycleWaterDistributionSystem
# MAGIC ,recycleWaterSupplyZone
# MAGIC ,superiorPropertyTypeCode
# MAGIC ,superiorPropertyType
# MAGIC ,inferiorPropertyTypeCode
# MAGIC ,inferiorPropertyType
# MAGIC ,objectReferenceIndicator
# MAGIC ,objectNumber
# MAGIC ,flatCount
# MAGIC ,validFromDate
# MAGIC from
# MAGIC cleansed.${vars.table}
# MAGIC except
# MAGIC select
# MAGIC case when propertyNumber is null then '' else propertyNumber end as propertyNumber
# MAGIC ,case when countryShortName is null then '' else countryShortName end as countryShortName
# MAGIC ,cityCode
# MAGIC ,streetCode
# MAGIC ,streetName
# MAGIC ,postCode
# MAGIC ,stateCode
# MAGIC ,regionGroup
# MAGIC ,politicalRegionCode
# MAGIC ,politicalRegion
# MAGIC ,connectionObjectGUID
# MAGIC ,regionGroupPermit
# MAGIC ,planTypeCode
# MAGIC ,planType
# MAGIC ,processTypeCode
# MAGIC ,processType
# MAGIC ,planNumber
# MAGIC ,lotTypeCode
# MAGIC ,lotNumber
# MAGIC ,sectionNumber
# MAGIC ,serviceAgreementIndicator
# MAGIC ,mlimIndicator
# MAGIC ,wicaIndicator
# MAGIC ,sopaIndicator
# MAGIC ,architecturalObjectNumber
# MAGIC ,architecturalObjectTypeCode
# MAGIC ,architecturalObjectType
# MAGIC ,buildingFeeDate
# MAGIC ,cadId
# MAGIC ,pumpWateWaterIndicator
# MAGIC ,fireServiceIndicator
# MAGIC ,architecturalObjectInternalId
# MAGIC ,architecturalObjectId
# MAGIC ,fixtureAndFittingCharacteristic
# MAGIC ,houseAddressNumber
# MAGIC ,WWTP
# MAGIC ,SCAMP
# MAGIC ,houseStreetName
# MAGIC ,houseNumber
# MAGIC ,LGA
# MAGIC ,cityName
# MAGIC ,waterDeliverySystem
# MAGIC ,waterDistributionSystem
# MAGIC ,waterSupplyZone
# MAGIC ,receycleWaterDeliverySystem
# MAGIC ,receycleWaterDistributionSystem
# MAGIC ,recycleWaterSupplyZone
# MAGIC ,superiorPropertyTypeCode
# MAGIC ,superiorPropertyType
# MAGIC ,inferiorPropertyTypeCode
# MAGIC ,inferiorPropertyType
# MAGIC ,objectReferenceIndicator
# MAGIC ,objectNumber
# MAGIC ,flatCount
# MAGIC ,case when validFromDate < '1900-01-01' then '1900-01-01' 
# MAGIC when validFromDate is null then '1900-01-01'
# MAGIC else validFromDate end as validFromDate
# MAGIC from
# MAGIC (select
# MAGIC HAUS as propertyNumber
# MAGIC ,k.COUNTRY as countryShortName
# MAGIC ,CITY_CODE as cityCode
# MAGIC ,k.STREETCODE as streetCode
# MAGIC ,b.streetName as streetName 
# MAGIC ,POSTALCODE as postCode
# MAGIC ,REGION as stateCode
# MAGIC ,REGIO_GRP as regionGroup
# MAGIC ,k.REGPOLIT as politicalRegionCode
# MAGIC ,c.REGNAME as politicalRegion 
# MAGIC ,wwTP as connectionObjectGUID
# MAGIC ,REGIOGROUP_PERM as regionGroupPermit
# MAGIC ,ZCD_PLAN_TYPE as planTypeCode
# MAGIC ,d.DESCRIPTION as planType
# MAGIC ,ZCD_PROCESS_TYPE as processTypeCode
# MAGIC ,e.DESCRIPTION as processType
# MAGIC ,ZCD_PLAN_NUMBER as planNumber
# MAGIC ,ZCD_LOT_TYPE as lotTypeCode
# MAGIC ,ZCD_LOT_NUMBER as lotNumber
# MAGIC ,ZCD_SECTION_NUMBER as sectionNumber
# MAGIC ,ZCD_IND_SRV_AGR as serviceAgreementIndicator
# MAGIC ,ZCD_IND_MLIM as mlimIndicator
# MAGIC ,ZCD_IND_WICA as wicaIndicator
# MAGIC ,ZCD_IND_SOPA as sopaIndicator
# MAGIC ,ZCD_AONR as architecturalObjectNumber
# MAGIC ,ZCD_AOTYPE as architecturalObjectTypeCode
# MAGIC ,XMAOTYPE as architecturalObjectType
# MAGIC ,ZCD_BLD_FEE_DATE as buildingFeeDate
# MAGIC ,ZCD_CAD_ID as cadId
# MAGIC ,ZZPUMP_WW as pumpWateWaterIndicator
# MAGIC ,ZZFIRE_SERVICE as fireServiceIndicator
# MAGIC ,ZINTRENO as architecturalObjectInternalId
# MAGIC ,ZAOID as architecturalObjectId
# MAGIC ,ZFIXFITCHARACT as fixtureAndFittingCharacteristic
# MAGIC ,HOUSE_ADRNR as houseAddressNumber
# MAGIC ,WWTP as WWTP
# MAGIC ,SCAMP as SCAMP
# MAGIC ,HAUS_STREET as houseStreetName
# MAGIC ,HAUS_NUM1 as houseNumber
# MAGIC ,HAUS_LGA_NAME as LGA
# MAGIC ,HOUS_CITY1 as cityName
# MAGIC ,WATER_DELIVERY_SYSTEM as waterDeliverySystem
# MAGIC ,WATER_DISTRIBUTION_SYSTEM_WATE as waterDistributionSystem
# MAGIC ,WATER_SUPPLY_ZONE as waterSupplyZone
# MAGIC ,RECYCLE_WATER_DELIVERY_SYSTEM as receycleWaterDeliverySystem
# MAGIC ,RECYCLE_WATER_DISTRIBUTION_SYS as receycleWaterDistributionSystem
# MAGIC ,RECYCLE_WATER_SUPPLY_ZONE as recycleWaterSupplyZone
# MAGIC ,ZCD_SUP_PROP_TYPE as superiorPropertyTypeCode
# MAGIC ,ref1.superiorPropertyType as superiorPropertyType
# MAGIC ,ZCD_INF_PROP_TYPE as inferiorPropertyTypeCode
# MAGIC ,ref2.inferiorPropertyType as inferiorPropertyType
# MAGIC ,Z_OWNER as objectReferenceIndicator
# MAGIC ,Z_OBJNR as objectNumber
# MAGIC ,ZCD_NO_OF_FLATS as flatCount
# MAGIC ,DATE_FROM as validFromDate
# MAGIC ,row_number() over (partition by HAUS,k.COUNTRY order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table} k
# MAGIC LEFT JOIN cleansed.isu_0CAM_STREETCODE_TEXT b
# MAGIC  ON k.STREETCODE = b.streetCode
# MAGIC LEFT JOIN cleansed.isu_TE227T c
# MAGIC ON k.REGPOLIT = c.REGPOLIT
# MAGIC LEFT JOIN cleansed.isu_zcd_tplantype_tx d
# MAGIC ON k.ZCD_PLAN_TYPE = d.PLAN_TYPE 
# MAGIC LEFT JOIN cleansed.isu_zcd_tproctype_tx e
# MAGIC ON k.ZCD_PROCESS_TYPE = e.PROCESS_TYPE
# MAGIC LEFT JOIN cleansed.isu_tivbdarobjtypet f
# MAGIC ON k.ZCD_AOTYPE = f.AOTYPE
# MAGIC LEFT JOIN cleansed.isu_zcd_tsupprtyp_tx ref1 ON k.ZCD_SUP_PROP_TYPE = ref1.superiorPropertyTypeCode 
# MAGIC LEFT JOIN  cleansed.isu_zcd_tinfprty_tx ref2 ON k.ZCD_INF_PROP_TYPE = ref2.inferiorPropertyTypeCode
# MAGIC )a where  a.rn = 1
