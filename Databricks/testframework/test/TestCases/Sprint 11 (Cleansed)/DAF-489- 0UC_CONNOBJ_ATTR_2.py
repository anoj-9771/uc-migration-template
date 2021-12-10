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

# MAGIC %sql
# MAGIC SELECT
# MAGIC HAUS as propertyNumber
# MAGIC ,COUNTRY as countryShortName
# MAGIC ,CITY_CODE as cityCode
# MAGIC ,STREETCODE as streetCode
# MAGIC ,b.street as streetName 
# MAGIC ,POSTALCODE as postCode
# MAGIC ,REGION as stateCode
# MAGIC ,REGPOLIT as politicalRegionCode
# MAGIC ,c.REGNAME as politicalRegion 
# MAGIC ,wwTP as connectionObjectGUID
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
# MAGIC ,f.XMAOTYPE as architecturalObjectType
# MAGIC ,ZCD_BLD_FEE_DATE as buildingFeeDate
# MAGIC ,ZZPUMP_WW as pumpWateWaterIndicator
# MAGIC ,ZZFIRE_SERVICE as fireServiceIndicator
# MAGIC ,ZINTRENO as architecturalObjectInternalId
# MAGIC ,ZAOID as architecturalObjectId
# MAGIC ,ZFIXFITCHARACT as fixtureAndFittingCharacteristic
# MAGIC ,WWTP as WWTP
# MAGIC ,SCAMP as SCAMP
# MAGIC ,HAUS_STREET as streetName
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
# MAGIC ,ref1.DESCRIPTION as superiorPropertyType
# MAGIC ,ZCD_INF_PROP_TYPE as inferiorPropertyTypeCode
# MAGIC ,ref2.DESCRIPTION as inferiorPropertyType
# MAGIC ,Z_OWNER as objectReferenceIndicator
# MAGIC ,Z_OBJNR as objectNumber
# MAGIC ,ZCD_NO_OF_FLATS as flatCount
# MAGIC FROM from test.${vars.table}
# MAGIC LEFT JOIN cleansed.isu_0CAM_STREETCODE_TEXT b
# MAGIC  ON STRT_CODE = b.STRT_CODE
# MAGIC LEFT JOIN cleansed.isu_TE227T c
# MAGIC ON REGPOLIT = c.REGPOLIT
# MAGIC LEFT JOIN cleansed.isu_zcd_tplantype_tx d
# MAGIC ON ZCD_PLAN_TYPE = d.PLAN_TYPE 
# MAGIC LEFT JOIN cleansed.isu_zcd_tproctype_tx e
# MAGIC ON ZCD_PROCESS_TYPE = e.PROCESS_TYPE
# MAGIC LEFT JOIN cleansed.isu_tivbdarobjtypet f
# MAGIC ON ZCD_AOTYPE = f.AOTYPE
# MAGIC LEFT JOIN cleansed.isu_zcd_tsupprtyp_tx ref1 ON ref1.SUPERIOR_PROP_TYPE = ZCD_SUP_PROP_TYPE
# MAGIC LEFT JOIN  cleansed.isu_zcd_tinfprty_tx ref2 ON ref2.INFERIOR_PROP_TYPE  = ZCD_SUP_PROP_TYPE
# MAGIC 
# MAGIC --LEFT JOIN (SELECT  ZCD_SUP_PROP_TYPE,  ref1.DESCRIPTION as SUPERIOR_PROP_TYPE
# MAGIC  --   from test.isu_0UC_CONNOBJ_ATTR_2 a
# MAGIC  --   join cleansed.isu_zcd_tsupprtyp_tx ref2 ON ref1.SUPERIOR_PROP_TYPE = a.ZCD_SUP_PROP_TYPE) g
# MAGIC --ON a.ZCD_SUP_PROP_TYPE = G.SUPERIOR_PROP_TYPE
# MAGIC --LEFT JOIN (SELECT  ZCD_INF_PROP_TYPE,  ref2.DESCRIPTION as INFERIOR_PROP_TYPE
# MAGIC ---    from test.isu_0UC_CONNOBJ_ATTR_2 a
# MAGIC --    join cleansed.isu_zcd_tinfprty_tx ref2 ON ref2.INFERIOR_PROP_TYPE = a.ZCD_INF_PROP_TYPE) h
# MAGIC --ON a.ZCD_INF_PROP_TYPE  = h.INFERIOR_PROP_TYPE

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cleansed.isu_zcd_tsupprtyp_tx 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cleansed.isu_zcd_tinfprty_tx 

# COMMAND ----------

# DBTITLE 1,[Source] with mapping
# MAGIC %sql
# MAGIC select
# MAGIC propertyNumber
# MAGIC ,countryShortName
# MAGIC ,cityCode
# MAGIC ,streetCode
# MAGIC ,a.streetName
# MAGIC ,postCode
# MAGIC ,stateCode
# MAGIC ,politicalRegionCode
# MAGIC ,politicalRegion
# MAGIC ,connectionObjectGUID
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
# MAGIC ,pumpWateWaterIndicator
# MAGIC ,fireServiceIndicator
# MAGIC ,architecturalObjectInternalId
# MAGIC ,architecturalObjectId
# MAGIC ,fixtureAndFittingCharacteristic
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
# MAGIC from
# MAGIC (select
# MAGIC HAUS as propertyNumber
# MAGIC ,k.COUNTRY as countryShortName
# MAGIC ,CITY_CODE as cityCode
# MAGIC ,k.STREETCODE as streetCode
# MAGIC ,b.streetName as streetName 
# MAGIC ,POSTALCODE as postCode
# MAGIC ,REGION as stateCode
# MAGIC ,k.REGPOLIT as politicalRegionCode
# MAGIC ,c.REGNAME as politicalRegion 
# MAGIC ,wwTP as connectionObjectGUID
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
# MAGIC ,ZZPUMP_WW as pumpWateWaterIndicator
# MAGIC ,ZZFIRE_SERVICE as fireServiceIndicator
# MAGIC ,ZINTRENO as architecturalObjectInternalId
# MAGIC ,ZAOID as architecturalObjectId
# MAGIC ,ZFIXFITCHARACT as fixtureAndFittingCharacteristic
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

# MAGIC %sql
# MAGIC select * from  cleansed.isu_TE227T 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cleansed.isu_0UC_CONNOBJ_ATTR_2

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from 
# MAGIC cleansed.isu_0CAM_STREETCODE_TEXT

# COMMAND ----------

# DBTITLE 1,[Verification] Count Check
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from cleansed.${vars.table}
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from (select
# MAGIC propertyNumber
# MAGIC ,countryShortName
# MAGIC ,cityCode
# MAGIC ,streetCode
# MAGIC ,streetName
# MAGIC ,postCode
# MAGIC ,stateCode
# MAGIC ,politicalRegionCode
# MAGIC ,politicalRegion
# MAGIC ,connectionObjectGUID
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
# MAGIC ,pumpWateWaterIndicator
# MAGIC ,fireServiceIndicator
# MAGIC ,architecturalObjectInternalId
# MAGIC ,architecturalObjectId
# MAGIC ,fixtureAndFittingCharacteristic
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
# MAGIC from
# MAGIC (select
# MAGIC HAUS as propertyNumber
# MAGIC ,k.COUNTRY as countryShortName
# MAGIC ,CITY_CODE as cityCode
# MAGIC ,k.STREETCODE as streetCode
# MAGIC ,b.streetName as streetName 
# MAGIC ,POSTALCODE as postCode
# MAGIC ,REGION as stateCode
# MAGIC ,k.REGPOLIT as politicalRegionCode
# MAGIC ,c.REGNAME as politicalRegion 
# MAGIC ,wwTP as connectionObjectGUID
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
# MAGIC ,ZZPUMP_WW as pumpWateWaterIndicator
# MAGIC ,ZZFIRE_SERVICE as fireServiceIndicator
# MAGIC ,ZINTRENO as architecturalObjectInternalId
# MAGIC ,ZAOID as architecturalObjectId
# MAGIC ,ZFIXFITCHARACT as fixtureAndFittingCharacteristic
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
# MAGIC )a where  a.rn = 1)

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
# MAGIC ,politicalRegionCode
# MAGIC ,politicalRegion
# MAGIC ,connectionObjectGUID
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
# MAGIC ,pumpWateWaterIndicator
# MAGIC ,fireServiceIndicator
# MAGIC ,architecturalObjectInternalId
# MAGIC ,architecturalObjectId
# MAGIC ,fixtureAndFittingCharacteristic
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
# MAGIC from
# MAGIC (select
# MAGIC HAUS as propertyNumber
# MAGIC ,k.COUNTRY as countryShortName
# MAGIC ,CITY_CODE as cityCode
# MAGIC ,k.STREETCODE as streetCode
# MAGIC ,b.streetName as streetName 
# MAGIC ,POSTALCODE as postCode
# MAGIC ,REGION as stateCode
# MAGIC ,k.REGPOLIT as politicalRegionCode
# MAGIC ,c.REGNAME as politicalRegion 
# MAGIC ,wwTP as connectionObjectGUID
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
# MAGIC ,ZZPUMP_WW as pumpWateWaterIndicator
# MAGIC ,ZZFIRE_SERVICE as fireServiceIndicator
# MAGIC ,ZINTRENO as architecturalObjectInternalId
# MAGIC ,ZAOID as architecturalObjectId
# MAGIC ,ZFIXFITCHARACT as fixtureAndFittingCharacteristic
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
# MAGIC ,politicalRegionCode
# MAGIC ,politicalRegion
# MAGIC ,connectionObjectGUID
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
# MAGIC ,pumpWateWaterIndicator
# MAGIC ,fireServiceIndicator
# MAGIC ,architecturalObjectInternalId
# MAGIC ,architecturalObjectId
# MAGIC ,fixtureAndFittingCharacteristic
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
# MAGIC ,politicalRegionCode
# MAGIC ,politicalRegion
# MAGIC ,connectionObjectGUID
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
# MAGIC ,pumpWateWaterIndicator
# MAGIC ,fireServiceIndicator
# MAGIC ,architecturalObjectInternalId
# MAGIC ,architecturalObjectId
# MAGIC ,fixtureAndFittingCharacteristic
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
# MAGIC ,politicalRegionCode
# MAGIC ,politicalRegion
# MAGIC ,connectionObjectGUID
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
# MAGIC ,pumpWateWaterIndicator
# MAGIC ,fireServiceIndicator
# MAGIC ,architecturalObjectInternalId
# MAGIC ,architecturalObjectId
# MAGIC ,fixtureAndFittingCharacteristic
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
# MAGIC from
# MAGIC (select
# MAGIC HAUS as propertyNumber
# MAGIC ,k.COUNTRY as countryShortName
# MAGIC ,CITY_CODE as cityCode
# MAGIC ,k.STREETCODE as streetCode
# MAGIC ,b.streetName as streetName 
# MAGIC ,POSTALCODE as postCode
# MAGIC ,REGION as stateCode
# MAGIC ,k.REGPOLIT as politicalRegionCode
# MAGIC ,c.REGNAME as politicalRegion 
# MAGIC ,wwTP as connectionObjectGUID
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
# MAGIC ,ZZPUMP_WW as pumpWateWaterIndicator
# MAGIC ,ZZFIRE_SERVICE as fireServiceIndicator
# MAGIC ,ZINTRENO as architecturalObjectInternalId
# MAGIC ,ZAOID as architecturalObjectId
# MAGIC ,ZFIXFITCHARACT as fixtureAndFittingCharacteristic
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

# MAGIC %sql
# MAGIC select * from cleansed.${vars.table}
# MAGIC where propertyNumber = '' and countryShortName = ''

# COMMAND ----------

# DBTITLE 1,case specific query
# MAGIC %sql
# MAGIC select
# MAGIC propertyNumber
# MAGIC ,countryShortName
# MAGIC ,cityCode
# MAGIC ,streetCode
# MAGIC ,streetName
# MAGIC ,postCode
# MAGIC ,stateCode
# MAGIC ,politicalRegionCode
# MAGIC ,politicalRegion
# MAGIC ,connectionObjectGUID
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
# MAGIC ,pumpWateWaterIndicator
# MAGIC ,fireServiceIndicator
# MAGIC ,architecturalObjectInternalId
# MAGIC ,architecturalObjectId
# MAGIC ,fixtureAndFittingCharacteristic
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
# MAGIC from
# MAGIC (select
# MAGIC HAUS as propertyNumber
# MAGIC ,k.COUNTRY as countryShortName
# MAGIC ,CITY_CODE as cityCode
# MAGIC ,k.STREETCODE as streetCode
# MAGIC ,b.streetName as streetName 
# MAGIC ,POSTALCODE as postCode
# MAGIC ,REGION as stateCode
# MAGIC ,k.REGPOLIT as politicalRegionCode
# MAGIC ,c.REGNAME as politicalRegion 
# MAGIC ,wwTP as connectionObjectGUID
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
# MAGIC ,ZZPUMP_WW as pumpWateWaterIndicator
# MAGIC ,ZZFIRE_SERVICE as fireServiceIndicator
# MAGIC ,ZINTRENO as architecturalObjectInternalId
# MAGIC ,ZAOID as architecturalObjectId
# MAGIC ,ZFIXFITCHARACT as fixtureAndFittingCharacteristic
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
# MAGIC where HAUS is null and k.COUNTRY is null
# MAGIC )a where  a.rn = 1 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from test.${vars.table}
# MAGIC where HAUS is null and COUNTRY is null
