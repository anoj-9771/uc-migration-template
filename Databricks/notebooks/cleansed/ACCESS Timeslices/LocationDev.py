# Databricks notebook source
# MAGIC %run ../../includes/include-all-util

# COMMAND ----------

spark.conf.set("c.catalog_name", ADS_DATABASE_CLEANSED)

# COMMAND ----------

# MAGIC %md
# MAGIC access_z309_thpropertyaddress

# COMMAND ----------

# MAGIC %sql
# MAGIC -- access_z309_thpropertyaddress, propertyNumber is PK
# MAGIC -- get unique (propertyNumber, modifiedTimestamp), becasue source has duplicates
# MAGIC 
# MAGIC create or replace temp view thpropertyaddress as
# MAGIC 
# MAGIC with thpropertyaddress_t1 as
# MAGIC (
# MAGIC   select 
# MAGIC   propertyNumber,
# MAGIC   LGACode,
# MAGIC   LGA,
# MAGIC   streetGuideCode,
# MAGIC   floorLevelTypeCode,
# MAGIC   floorLevelType,
# MAGIC   floorLevelNumber,
# MAGIC   flatUnitTypeCode,
# MAGIC   flatUnitType,
# MAGIC   flatUnitNumber,
# MAGIC   houseNumber1,
# MAGIC   houseNumber1Suffix,
# MAGIC   houseNumber2,
# MAGIC   houseNumber2Suffix,
# MAGIC   lotNumber,
# MAGIC   roadSideMailbox,
# MAGIC   --otherAddressInformation,
# MAGIC   --specialDescription,
# MAGIC   buildingName1,
# MAGIC   buildingName2,
# MAGIC   modifiedTimestamp, 
# MAGIC   row_number() over (partition by propertyNumber, modifiedTimestamp order by modifiedTimestamp) as rn 
# MAGIC   from ${c.catalog_name}.access.z309_thpropertyaddress 
# MAGIC ),
# MAGIC 
# MAGIC -- get validFrom, validTo from modifiedTimestamp 
# MAGIC thpropertyaddress_t2 as
# MAGIC (
# MAGIC   select 
# MAGIC   *, 
# MAGIC   modifiedTimestamp as tValidFrom,
# MAGIC   coalesce(lead(modifiedTimestamp - INTERVAL '1' SECOND, 1) over (partition by propertyNumber order by modifiedTimestamp), timestamp('9999-12-31')) as tValidTo,
# MAGIC   row_number() over(order by propertyNumber,modifiedTimestamp asc) as num_row 
# MAGIC   from thpropertyaddress_t1 where rn = 1
# MAGIC ),
# MAGIC 
# MAGIC -- filter the duplicate records
# MAGIC thpropertyaddress_t3 as
# MAGIC (
# MAGIC   select a.*, --b.num_row as b_num_row, b.LGA, 
# MAGIC   if( b.num_row is not null  
# MAGIC       and ((a.LGACode is null and b.LGACode is null) or (a.LGACode = b.LGACode)) --TODO: Add used columns
# MAGIC       and ((a.LGA is null and b.LGA is null) or (a.LGA = b.LGA)) 
# MAGIC       and ((a.streetGuideCode is null and b.streetGuideCode is null) or (a.streetGuideCode = b.streetGuideCode))
# MAGIC       and ((a.floorLevelTypeCode is null and b.floorLevelTypeCode is null) or (a.floorLevelTypeCode = b.floorLevelTypeCode))
# MAGIC       and ((a.floorLevelType is null and b.floorLevelType is null) or (a.floorLevelType = b.floorLevelType))
# MAGIC       and ((a.floorLevelNumber is null and b.floorLevelNumber is null) or (a.floorLevelNumber = b.floorLevelNumber))
# MAGIC       and ((a.flatUnitTypeCode is null and b.flatUnitTypeCode is null) or (a.flatUnitTypeCode = b.flatUnitTypeCode))
# MAGIC       and ((a.flatUnitType is null and b.flatUnitType is null) or (a.flatUnitType = b.flatUnitType))
# MAGIC       and ((a.flatUnitNumber is null and b.flatUnitNumber is null) or (a.flatUnitNumber = b.flatUnitNumber))
# MAGIC       and ((a.houseNumber1 is null and b.houseNumber1 is null) or (a.houseNumber1 = b.houseNumber1))
# MAGIC       and ((a.houseNumber1Suffix is null and b.houseNumber1Suffix is null) or (a.houseNumber1Suffix = b.houseNumber1Suffix))
# MAGIC       and ((a.houseNumber2 is null and b.houseNumber2 is null) or (a.houseNumber2 = b.houseNumber2))
# MAGIC       and ((a.houseNumber2Suffix is null and b.houseNumber2Suffix is null) or (a.houseNumber2Suffix = b.houseNumber2Suffix))
# MAGIC       and ((a.lotNumber is null and b.lotNumber is null) or (a.lotNumber = b.lotNumber))
# MAGIC       and ((a.roadSideMailbox is null and b.roadSideMailbox is null) or (a.roadSideMailbox = b.roadSideMailbox))
# MAGIC       --and ((a.otherAddressInformation is null and b.otherAddressInformation is null) or (a.otherAddressInformation = b.otherAddressInformation))
# MAGIC       --and ((a.specialDescription is null and b.specialDescription is null) or (a.specialDescription = b.specialDescription)) 
# MAGIC       and ((a.buildingName1 is null and b.buildingName1 is null) or (a.buildingName1 = b.buildingName1))
# MAGIC       and ((a.buildingName2 is null and b.buildingName2 is null) or (a.buildingName2 = b.buildingName2)),
# MAGIC       0,1) as tIndicator   
# MAGIC   from thpropertyaddress_t2 a 
# MAGIC   left join thpropertyaddress_t2 b on a.propertyNumber = b.propertyNumber and a.num_row - 1 = b.num_row
# MAGIC ),
# MAGIC 
# MAGIC thpropertyaddress_t4 as
# MAGIC (
# MAGIC   select 
# MAGIC   *,
# MAGIC   sum(tIndicator) over(order by num_row) as tGroup 
# MAGIC   from thpropertyaddress_t3
# MAGIC ),
# MAGIC 
# MAGIC thpropertyaddress_t5 as
# MAGIC (
# MAGIC   select 
# MAGIC   *,
# MAGIC   min(tValidFrom) over(partition by tGroup) as validFrom,
# MAGIC   max(tValidTo) over(partition by tGroup) as validTo 
# MAGIC   from thpropertyaddress_t4  
# MAGIC ),
# MAGIC 
# MAGIC hiyThpropertyaddress as
# MAGIC (
# MAGIC   select 
# MAGIC   propertyNumber, 
# MAGIC   LGACode,
# MAGIC   LGA,
# MAGIC   streetGuideCode,
# MAGIC   floorLevelTypeCode,
# MAGIC   floorLevelType,
# MAGIC   floorLevelNumber,
# MAGIC   flatUnitTypeCode,
# MAGIC   flatUnitType,
# MAGIC   flatUnitNumber,
# MAGIC   houseNumber1,
# MAGIC   houseNumber1Suffix,
# MAGIC   houseNumber2,
# MAGIC   houseNumber2Suffix,
# MAGIC   lotNumber,
# MAGIC   roadSideMailbox,
# MAGIC   --otherAddressInformation,
# MAGIC   --specialDescription,
# MAGIC   buildingName1,
# MAGIC   buildingName2,
# MAGIC   validFrom,
# MAGIC   validTo 
# MAGIC   from thpropertyaddress_t5 where tIndicator = 1 
# MAGIC ),
# MAGIC 
# MAGIC cntPropertyaddress as
# MAGIC (
# MAGIC   select 
# MAGIC   ca.propertyNumber, 
# MAGIC   ca.LGACode,
# MAGIC   null as LGA, --*
# MAGIC   ca.streetGuideCode,
# MAGIC   null as floorLevelTypeCode, --*
# MAGIC   ca.floorLevelType,
# MAGIC   ca.floorLevelNumber,
# MAGIC   null as flatUnitTypeCode, --*
# MAGIC   ca.flatUnitType,
# MAGIC   ca.flatUnitNumber,
# MAGIC   ca.houseNumber1,
# MAGIC   ca.houseNumber1Suffix,
# MAGIC   ca.houseNumber2,
# MAGIC   ca.houseNumber2Suffix,
# MAGIC   ca.lotNumber,
# MAGIC   ca.roadSideMailbox,
# MAGIC   ca.buildingName1,
# MAGIC   ca.buildingName2,
# MAGIC   timestamp(ca.modifiedTimestamp) as validFrom,
# MAGIC   timestamp("9999-12-31") as validTo 
# MAGIC   from ${c.catalog_name}.access.z309_tpropertyaddress ca left join  ${c.catalog_name}.access.z309_thpropertyaddress ha 
# MAGIC   on ca.propertyNumber = ha.propertyNumber 
# MAGIC   where ha.propertyNumber is null 
# MAGIC )
# MAGIC 
# MAGIC select * from hiyThpropertyaddress
# MAGIC union all 
# MAGIC select * from cntPropertyaddress

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC access_z309_thstreetguide

# COMMAND ----------

# MAGIC %sql
# MAGIC -- access_z309_thstreetguide, streetGuideCode is PK
# MAGIC -- get unique (streetGuideCode, rowSupersededTimestamp), becasue source has duplicates
# MAGIC 
# MAGIC create or replace temp view streetGuideCode as
# MAGIC 
# MAGIC with streetGuideCode_t1 as
# MAGIC (
# MAGIC   select 
# MAGIC   streetGuideCode,
# MAGIC   LGACode,
# MAGIC   streetSuffixCode,
# MAGIC   streetName,
# MAGIC   streetTypeCode,
# MAGIC   postCode,
# MAGIC   suburb,
# MAGIC   streetGuideEffectiveDate, 
# MAGIC   row_number() over (partition by streetGuideCode, streetGuideEffectiveDate order by streetGuideEffectiveDate) as rn 
# MAGIC   from ${c.catalog_name}.access.z309_thstreetguide 
# MAGIC ),
# MAGIC 
# MAGIC -- get validFrom, validTo from rowSupersededTimestamp
# MAGIC streetGuideCode_t2 as
# MAGIC (
# MAGIC   select 
# MAGIC   *, 
# MAGIC   streetGuideEffectiveDate as tValidFrom,
# MAGIC   coalesce(lead(streetGuideEffectiveDate - INTERVAL '1' SECOND, 1) over (partition by streetGuideCode order by streetGuideEffectiveDate), timestamp('9999-12-31')) as tValidTo,
# MAGIC   row_number() over(order by streetGuideCode, streetGuideEffectiveDate asc) as num_row 
# MAGIC   from streetGuideCode_t1 where rn = 1
# MAGIC ),
# MAGIC 
# MAGIC -- filter the duplicate records
# MAGIC streetGuideCode_t3 as
# MAGIC (
# MAGIC   select a.*, --b.num_row as b_num_row, b.LGA, 
# MAGIC   if( b.num_row is not null  
# MAGIC       and ((a.LGACode is null and b.LGACode is null) or (a.LGACode = b.LGACode)) --TODO: Add used columns
# MAGIC       and ((a.streetSuffixCode is null and b.streetSuffixCode is null) or (a.streetSuffixCode = b.streetSuffixCode)) 
# MAGIC       and ((a.streetName is null and b.streetName is null) or (a.streetName = b.streetName))
# MAGIC       and ((a.streetTypeCode is null and b.streetTypeCode is null) or (a.streetTypeCode = b.streetTypeCode))
# MAGIC       and ((a.postCode is null and b.postCode is null) or (a.postCode = b.postCode))
# MAGIC       and ((a.suburb is null and b.suburb is null) or (a.suburb = b.suburb)),
# MAGIC       0,1) as tIndicator   
# MAGIC   from streetGuideCode_t2 a 
# MAGIC   left join streetGuideCode_t2 b on a.streetGuideCode = b.streetGuideCode and a.num_row - 1 = b.num_row
# MAGIC ),
# MAGIC 
# MAGIC streetGuideCode_t4 as
# MAGIC (
# MAGIC   select 
# MAGIC   *,
# MAGIC   sum(tIndicator) over(order by num_row) as tGroup 
# MAGIC   from streetGuideCode_t3
# MAGIC ),
# MAGIC 
# MAGIC streetGuideCode_t5 as
# MAGIC (
# MAGIC   select 
# MAGIC   *,
# MAGIC   min(tValidFrom) over(partition by tGroup) as validFrom,
# MAGIC   max(tValidTo) over(partition by tGroup) as validTo 
# MAGIC   from streetGuideCode_t4  
# MAGIC ),
# MAGIC 
# MAGIC hiyStreetGuideCode as
# MAGIC (
# MAGIC   select 
# MAGIC   streetGuideCode,
# MAGIC   LGACode,
# MAGIC   streetSuffixCode,
# MAGIC   streetName,
# MAGIC   streetTypeCode,
# MAGIC   postCode,
# MAGIC   suburb,
# MAGIC   streetGuideEffectiveDate,
# MAGIC   validFrom,
# MAGIC   validTo 
# MAGIC   from streetGuideCode_t5 where tIndicator = 1 
# MAGIC ),
# MAGIC 
# MAGIC cntStreetGuideCode as 
# MAGIC (
# MAGIC   select 
# MAGIC   streetGuideCode,
# MAGIC   LGACode,
# MAGIC   streetSuffix as streetSuffixCode,
# MAGIC   streetName,
# MAGIC   streetType as streetTypeCode,
# MAGIC   postCode,
# MAGIC   suburb,
# MAGIC   streetGuideEffectiveDate,
# MAGIC   timestamp(streetGuideEffectiveDate) as validFrom,
# MAGIC   timestamp('9999-12-31') as validTo 
# MAGIC   from ${c.catalog_name}.access.z309_tstreetguide where streetGuideCode not in (select distinct streetGuideCode from ${c.catalog_name}.access.z309_thstreetguide)
# MAGIC )
# MAGIC 
# MAGIC select * from cntStreetGuideCode
# MAGIC union all
# MAGIC select * from hiyStreetGuideCode
# MAGIC 
# MAGIC --select * from streetGuideCode  where streetGuideCode = 056348 

# COMMAND ----------

# MAGIC %md
# MAGIC access_z309_thproperty

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- access_z309_thproperty, propertyNumber is PK
# MAGIC -- get unique (propertyNumber, rowSupersededTimestamp), becasue source has duplicates
# MAGIC 
# MAGIC create or replace temp view thproperty as
# MAGIC 
# MAGIC with thproperty_t1 as
# MAGIC (
# MAGIC   select 
# MAGIC   propertyNumber,
# MAGIC   LGACode,
# MAGIC   LGA,
# MAGIC   propertyTypeCode,
# MAGIC   propertyType,
# MAGIC   superiorPropertyTypeCode,
# MAGIC   superiorPropertyType,
# MAGIC   rowSupersededTimestamp, 
# MAGIC   row_number() over (partition by propertyNumber, rowSupersededTimestamp order by rowSupersededTimestamp) as rn 
# MAGIC   from ${c.catalog_name}.access.z309_thproperty 
# MAGIC ),
# MAGIC 
# MAGIC -- get validFrom, validTo from rowSupersededTimestamp
# MAGIC thproperty_t2 as
# MAGIC (
# MAGIC   select 
# MAGIC   *, 
# MAGIC   rowSupersededTimestamp as tValidFrom,
# MAGIC   coalesce(lead(rowSupersededTimestamp - INTERVAL '1' SECOND, 1) over (partition by propertyNumber order by rowSupersededTimestamp), timestamp('9999-12-31')) as tValidTo,
# MAGIC   row_number() over(order by propertyNumber,rowSupersededTimestamp asc) as num_row 
# MAGIC   from thproperty_t1 where rn = 1 order by propertyNumber, rowSupersededTimestamp 
# MAGIC ),
# MAGIC 
# MAGIC -- filter the duplicate records
# MAGIC thproperty_t3 as
# MAGIC (
# MAGIC   select a.*, --b.num_row as b_num_row, b.LGA, 
# MAGIC   if( b.num_row is not null  
# MAGIC       and ((a.LGACode is null and b.LGACode is null) or (a.LGACode = b.LGACode)) 
# MAGIC       and ((a.LGA is null and b.LGA is null) or (a.LGA = b.LGA))
# MAGIC       and ((a.propertyTypeCode is null and b.propertyTypeCode is null) or (a.propertyTypeCode = b.propertyTypeCode))
# MAGIC       and ((a.propertyType is null and b.propertyType is null) or (a.propertyType = b.propertyType))
# MAGIC       and ((a.superiorPropertyTypeCode is null and b.superiorPropertyTypeCode is null) or (a.superiorPropertyTypeCode = b.superiorPropertyTypeCode))
# MAGIC       and ((a.superiorPropertyType is null and b.superiorPropertyType is null) or (a.superiorPropertyType = b.superiorPropertyType)),
# MAGIC       0,1) as tIndicator   
# MAGIC   from thproperty_t2 a 
# MAGIC   left join thproperty_t2 b on a.propertyNumber = b.propertyNumber and a.num_row - 1 = b.num_row
# MAGIC ),
# MAGIC 
# MAGIC thproperty_t4 as
# MAGIC (
# MAGIC   select 
# MAGIC   *,
# MAGIC   sum(tIndicator) over(order by num_row) as tGroup 
# MAGIC   from thproperty_t3
# MAGIC ),
# MAGIC 
# MAGIC thproperty_t5 as
# MAGIC (
# MAGIC   select 
# MAGIC   *,
# MAGIC   min(tValidFrom) over(partition by tGroup) as validFrom,
# MAGIC   max(tValidTo) over(partition by tGroup) as validTo 
# MAGIC   from thproperty_t4  
# MAGIC ),
# MAGIC 
# MAGIC thproperty as
# MAGIC (
# MAGIC   select 
# MAGIC   propertyNumber, 
# MAGIC   LGACode,
# MAGIC   LGA,
# MAGIC   propertyTypeCode,
# MAGIC   propertyType,
# MAGIC   superiorPropertyTypeCode,
# MAGIC   superiorPropertyType,
# MAGIC   validFrom,
# MAGIC   validTo 
# MAGIC   from thproperty_t5 where tIndicator = 1 
# MAGIC ),
# MAGIC 
# MAGIC cnProperty as 
# MAGIC (
# MAGIC   select
# MAGIC   cp.propertyNumber, 
# MAGIC   cp.LGACode,
# MAGIC   cp.LGA,
# MAGIC   cp.propertyTypeCode,
# MAGIC   cp.propertyType,
# MAGIC   cp.superiorPropertyTypeCode,
# MAGIC   cp.superiorPropertyType,
# MAGIC   timestamp(cp.propertyUpdatedDate) as validFrom,
# MAGIC   timestamp('9999-12-31') as validTo 
# MAGIC   from ${c.catalog_name}.access.z309_tproperty cp left join  ${c.catalog_name}.access.z309_thproperty hp 
# MAGIC   on cp.propertyNumber = hp.propertyNumber 
# MAGIC   where hp.propertyNumber is null 
# MAGIC )
# MAGIC 
# MAGIC select * from thproperty
# MAGIC union all
# MAGIC select * from cnProperty
# MAGIC 
# MAGIC --select * from thproperty where propertyNumber in(4826357,4947584)

# COMMAND ----------

# MAGIC %md
# MAGIC access_z309_thstrataunits

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- access_z309_thstrataunits, propertyNumber,strataPlanNumber are PK
# MAGIC -- get unique (propertyNumber, rowSupersededTimestamp), becasue source has duplicates
# MAGIC 
# MAGIC create or replace temp view thstrataunits as
# MAGIC 
# MAGIC with thstrataunits_t1 as
# MAGIC (
# MAGIC   select 
# MAGIC   propertyNumber,
# MAGIC   strataPlanNumber,
# MAGIC   rowSupersededTimestamp, 
# MAGIC   row_number() over (partition by propertyNumber, rowSupersededTimestamp order by rowSupersededTimestamp) as rn 
# MAGIC   from ${c.catalog_name}.access.z309_thstrataunits 
# MAGIC ),
# MAGIC 
# MAGIC -- get validFrom, validTo from rowSupersededTimestamp
# MAGIC thstrataunits_t2 as
# MAGIC (
# MAGIC   select 
# MAGIC   *, 
# MAGIC   rowSupersededTimestamp as tValidFrom,
# MAGIC   coalesce(lead(rowSupersededTimestamp - INTERVAL '1' SECOND, 1) over (partition by propertyNumber order by rowSupersededTimestamp), timestamp('9999-12-31')) as tValidTo,
# MAGIC   row_number() over(order by propertyNumber,rowSupersededTimestamp asc) as num_row 
# MAGIC   from thstrataunits_t1 where rn = 1 order by propertyNumber, rowSupersededTimestamp 
# MAGIC ),
# MAGIC 
# MAGIC -- filter the duplicate records
# MAGIC thstrataunits_t3 as
# MAGIC (
# MAGIC   select a.*, --b.num_row as b_num_row, b.LGA, 
# MAGIC   if( b.num_row is not null  
# MAGIC       and ((a.strataPlanNumber is null and b.strataPlanNumber is null) or (a.strataPlanNumber = b.strataPlanNumber)),
# MAGIC       0,1) as tIndicator   
# MAGIC   from thstrataunits_t2 a 
# MAGIC   left join thstrataunits_t2 b on a.propertyNumber = b.propertyNumber and a.num_row - 1 = b.num_row
# MAGIC ),
# MAGIC 
# MAGIC thstrataunits_t4 as
# MAGIC (
# MAGIC   select 
# MAGIC   *,
# MAGIC   sum(tIndicator) over(order by num_row) as tGroup 
# MAGIC   from thstrataunits_t3
# MAGIC ),
# MAGIC 
# MAGIC thstrataunits_t5 as
# MAGIC (
# MAGIC   select 
# MAGIC   *,
# MAGIC   min(tValidFrom) over(partition by tGroup) as validFrom,
# MAGIC   max(tValidTo) over(partition by tGroup) as validTo 
# MAGIC   from thstrataunits_t4  
# MAGIC ),
# MAGIC 
# MAGIC thstrataunits as
# MAGIC (
# MAGIC   select 
# MAGIC   propertyNumber, 
# MAGIC   strataPlanNumber,
# MAGIC   validFrom,
# MAGIC   validTo 
# MAGIC   from thstrataunits_t5 where tIndicator = 1 
# MAGIC ),
# MAGIC 
# MAGIC cnstrataunits as
# MAGIC (
# MAGIC   select 
# MAGIC   cs.propertyNumber, 
# MAGIC   cs.strataPlanNumber,
# MAGIC   timestamp(cs.strataUnitUpdatedDate) as validFrom,
# MAGIC   timestamp('9999-12-31') as validTo 
# MAGIC   from ${c.catalog_name}.access.z309_tstrataunits cs left join  ${c.catalog_name}.access.z309_thstrataunits hs 
# MAGIC   on cs.propertyNumber = hs.propertyNumber 
# MAGIC   where hs.propertyNumber is null 
# MAGIC )
# MAGIC 
# MAGIC select * from thstrataunits 
# MAGIC union all
# MAGIC select * from cnstrataunits
# MAGIC 
# MAGIC --select * from thstrataunits where propertyNumber in(4133607,5315810)

# COMMAND ----------

# MAGIC %md 
# MAGIC access_z309_thmastrataplan

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --access_z309_thmastrataplan
# MAGIC -- get unique (strataPlanNumber, rowSupersededTimestamp), becasue source has duplicates
# MAGIC 
# MAGIC create or replace temp view thmastrataplan as
# MAGIC 
# MAGIC with thmastrataplan_t1 as
# MAGIC (
# MAGIC   select 
# MAGIC   masterPropertyNumber,
# MAGIC   strataPlanNumber,
# MAGIC   masterStrataUpdatedDate, 
# MAGIC   row_number() over (partition by strataPlanNumber,masterStrataUpdatedDate order by masterStrataUpdatedDate) as rn 
# MAGIC   from ${c.catalog_name}.access.z309_thmastrataplan 
# MAGIC ),
# MAGIC 
# MAGIC -- get validFrom, validTo from rowSupersededTimestamp
# MAGIC thmastrataplan_t2 as
# MAGIC (
# MAGIC   select 
# MAGIC   *, 
# MAGIC   masterStrataUpdatedDate as tValidFrom,
# MAGIC   coalesce(lead(masterStrataUpdatedDate - INTERVAL '1' SECOND, 1) over (partition by strataPlanNumber order by masterStrataUpdatedDate), timestamp('9999-12-31')) as tValidTo,
# MAGIC   row_number() over(order by strataPlanNumber,masterStrataUpdatedDate asc) as num_row 
# MAGIC   from thmastrataplan_t1 where rn = 1 order by strataPlanNumber, masterStrataUpdatedDate 
# MAGIC ),
# MAGIC 
# MAGIC -- filter the duplicate records
# MAGIC thmastrataplan_t3 as
# MAGIC (
# MAGIC   select a.*, --b.num_row as b_num_row, b.LGA, 
# MAGIC   if( b.num_row is not null 
# MAGIC     and ((a.masterPropertyNumber is null and b.masterPropertyNumber is null) or (a.masterPropertyNumber = b.masterPropertyNumber))   
# MAGIC     , 0,1) as tIndicator   
# MAGIC   from thmastrataplan_t2 a 
# MAGIC   left join thmastrataplan_t2 b on a.strataPlanNumber = b.strataPlanNumber  
# MAGIC     and a.num_row - 1 = b.num_row
# MAGIC ),
# MAGIC 
# MAGIC thmastrataplan_t4 as
# MAGIC (
# MAGIC   select 
# MAGIC   *,
# MAGIC   sum(tIndicator) over(order by num_row) as tGroup 
# MAGIC   from thmastrataplan_t3
# MAGIC ),
# MAGIC 
# MAGIC thmastrataplan_t5 as
# MAGIC (
# MAGIC   select 
# MAGIC   *,
# MAGIC   min(tValidFrom) over(partition by tGroup) as validFrom,
# MAGIC   max(tValidTo) over(partition by tGroup) as validTo 
# MAGIC   from thmastrataplan_t4  
# MAGIC ),
# MAGIC 
# MAGIC thmastrataplan as
# MAGIC (
# MAGIC   select 
# MAGIC   masterPropertyNumber,
# MAGIC   strataPlanNumber,
# MAGIC   validFrom,
# MAGIC   validTo 
# MAGIC   from thmastrataplan_t5 where tIndicator = 1 
# MAGIC ),
# MAGIC 
# MAGIC cntmastrataplan as 
# MAGIC (
# MAGIC   select 
# MAGIC   cm.masterPropertyNumber,
# MAGIC   cm.strataPlanNumber,
# MAGIC   timestamp(cm.masterStrataUpdatedDate) as validFrom,
# MAGIC   timestamp("9999-12-31") as validTo 
# MAGIC   from ${c.catalog_name}.access.z309_tmastrataplan cm left join  ${c.catalog_name}.access.z309_thmastrataplan hm 
# MAGIC   on cm.strataPlanNumber = hm.strataPlanNumber 
# MAGIC   where hm.strataPlanNumber is null 
# MAGIC )
# MAGIC 
# MAGIC select * from thmastrataplan
# MAGIC union all
# MAGIC select * from cntmastrataplan
# MAGIC 
# MAGIC --select * from thmastrataplan where masterPropertyNumber in(3282649)

# COMMAND ----------

# MAGIC %md
# MAGIC threlatedProps

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --access_z309_threlatedProps
# MAGIC 
# MAGIC create or replace temp view threlatedProps as
# MAGIC 
# MAGIC with threlatedProps_t1 as
# MAGIC (
# MAGIC   select 
# MAGIC   propertyNumber,
# MAGIC   relatedPropertyNumber,
# MAGIC   relationshipTypeCode,
# MAGIC   relationshipType,
# MAGIC   if(relationshipUpdatedDate="9999-12-31",rowSupersededTimestamp,relationshipUpdatedDate) as relationshipUpdatedDate, 
# MAGIC   rowSupersededTimestamp, 
# MAGIC   row_number() over (partition by propertyNumber,
# MAGIC     coalesce(if(relationshipUpdatedDate="9999-12-31",rowSupersededTimestamp,relationshipUpdatedDate),rowSupersededTimestamp) 
# MAGIC     order by coalesce(if(relationshipUpdatedDate="9999-12-31",rowSupersededTimestamp,relationshipUpdatedDate),rowSupersededTimestamp), rowSupersededTimestamp desc) as rn 
# MAGIC   from ${c.catalog_name}.access.z309_threlatedProps 
# MAGIC ),
# MAGIC 
# MAGIC -- get validFrom, validTo from rowSupersededTimestamp
# MAGIC threlatedProps_t2 as
# MAGIC (
# MAGIC   select 
# MAGIC   *, 
# MAGIC   coalesce(relationshipUpdatedDate,rowSupersededTimestamp) as tValidFrom,
# MAGIC   coalesce(lead(coalesce(relationshipUpdatedDate,rowSupersededTimestamp) - INTERVAL '1' SECOND, 1) over (partition by propertyNumber order by coalesce(relationshipUpdatedDate,rowSupersededTimestamp)), timestamp('9999-12-31')) as tValidTo,
# MAGIC   row_number() over(order by propertyNumber, coalesce(relationshipUpdatedDate,rowSupersededTimestamp) asc) as num_row 
# MAGIC   from threlatedProps_t1 where rn = 1 order by propertyNumber, coalesce(relationshipUpdatedDate,rowSupersededTimestamp) 
# MAGIC ),
# MAGIC 
# MAGIC -- filter the duplicate records
# MAGIC threlatedProps_t3 as
# MAGIC (
# MAGIC   select a.*, --b.num_row as b_num_row, b.LGA, 
# MAGIC   if( b.num_row is not null 
# MAGIC     --and ((a.relationshipTypeCode is null and b.relationshipTypeCode is null) or (a.relationshipTypeCode = b.relationshipTypeCode)) 
# MAGIC     --and ((a.relationshipType is null and b.relationshipType is null) or (a.relationshipType = b.relationshipType))
# MAGIC     and ((a.relatedPropertyNumber is null and b.relatedPropertyNumber is null) or (a.relatedPropertyNumber = b.relatedPropertyNumber))
# MAGIC     , 0,1) as tIndicator   
# MAGIC   from threlatedProps_t2 a 
# MAGIC   left join threlatedProps_t2 b on a.propertyNumber = b.propertyNumber   
# MAGIC     and a.num_row - 1 = b.num_row
# MAGIC ),
# MAGIC 
# MAGIC threlatedProps_t4 as
# MAGIC (
# MAGIC   select 
# MAGIC   *,
# MAGIC   sum(tIndicator) over(order by num_row) as tGroup 
# MAGIC   from threlatedProps_t3
# MAGIC ),
# MAGIC 
# MAGIC threlatedProps_t5 as
# MAGIC (
# MAGIC   select 
# MAGIC   *,
# MAGIC   min(tValidFrom) over(partition by tGroup) as validFrom,
# MAGIC   max(tValidTo) over(partition by tGroup) as validTo 
# MAGIC   from threlatedProps_t4  
# MAGIC ),
# MAGIC 
# MAGIC threlatedProps as
# MAGIC (
# MAGIC   select 
# MAGIC   propertyNumber,
# MAGIC   relatedPropertyNumber,
# MAGIC   relationshipTypeCode,
# MAGIC   relationshipType,
# MAGIC   validFrom,
# MAGIC   validTo 
# MAGIC   from threlatedProps_t5 where tIndicator = 1 
# MAGIC ),
# MAGIC 
# MAGIC cntrelatedProps as
# MAGIC (
# MAGIC   select 
# MAGIC   cr.propertyNumber,
# MAGIC   cr.relatedPropertyNumber,
# MAGIC   cr.relationshipTypeCode,
# MAGIC   cr.relationshipType,
# MAGIC   timestamp(cr.relationshipUpdatedDate) as validFrom,
# MAGIC   timestamp("9999-12-31")as validTo 
# MAGIC   from ${c.catalog_name}.access.z309_trelatedProps cr left join  ${c.catalog_name}.access.z309_threlatedProps hr 
# MAGIC   on cr.propertyNumber = hr.propertyNumber 
# MAGIC   where hr.propertyNumber is null
# MAGIC )
# MAGIC 
# MAGIC select * from threlatedProps 
# MAGIC union all
# MAGIC select * from cntrelatedProps 
# MAGIC 
# MAGIC --select * from threlatedProps where propertyNumber = 4688991 and relatedPropertyNumber=4443029

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC create or replace temp view strataProps as
# MAGIC 
# MAGIC select su.propertyNumber, 
# MAGIC        ms.masterPropertyNumber as parentPropertyNumber,
# MAGIC        'Child of Master Strata' as relationshipType,
# MAGIC        case 
# MAGIC          when (su.validFrom < ms.validFrom and su.validTo > ms.validFrom and su.validTo < ms.validTo) then ms.validFrom
# MAGIC          when (su.validFrom > ms.validFrom and su.validTo <= ms.validTo) then su.validFrom
# MAGIC          when (su.validFrom > ms.validFrom and su.validTo > ms.validTo and su.validFrom < ms.validTo) then su.validFrom
# MAGIC          when (su.validFrom < ms.validFrom and su.validTo >= ms.validTo) then ms.validFrom 
# MAGIC        end as validFrom,
# MAGIC        case 
# MAGIC          when (su.validFrom < ms.validFrom and su.validTo > ms.validFrom and su.validTo < ms.validTo) then su.validTo
# MAGIC          when (su.validFrom > ms.validFrom and su.validTo <= ms.validTo) then su.validTo
# MAGIC          when (su.validFrom > ms.validFrom and su.validTo > ms.validTo and su.validFrom < ms.validTo) then ms.validTo
# MAGIC          when (su.validFrom < ms.validFrom and su.validTo >= ms.validTo) then ms.validTo  
# MAGIC        end as validTo 
# MAGIC        from  thstrataunits su 
# MAGIC        inner join 
# MAGIC          thmastrataplan ms on su.strataPlanNumber = ms.strataPlanNumber 
# MAGIC          and  
# MAGIC          ((su.validFrom < ms.validFrom and su.validTo > ms.validFrom and su.validTo < ms.validTo) 
# MAGIC          or (su.validFrom > ms.validFrom and su.validTo <= ms.validTo) 
# MAGIC          or (su.validFrom > ms.validFrom and su.validTo > ms.validTo and su.validFrom < ms.validTo)
# MAGIC          or (su.validFrom < ms.validFrom and su.validTo >= ms.validTo))
# MAGIC          --where su.propertyNumber = 4951128 order by su.validFrom

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- other Props (not strataunits property)
# MAGIC 
# MAGIC create or replace temp view otherPropsNumber as
# MAGIC 
# MAGIC   select distinct propertyNumber from thproperty --2235181  2153746
# MAGIC   minus
# MAGIC   select distinct propertyNumber from strataProps

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --relatedprops
# MAGIC create or replace temp view otherProps as
# MAGIC 
# MAGIC   select rp.propertyNumber as propertyNumber, 
# MAGIC        rp.relatedPropertyNumber as parentPropertyNumber, 
# MAGIC        rp.relationshipType as relationshipType, 
# MAGIC        --rank() over (partition by rp.propertyNumber order by relationshipTypecode desc) as rnk,
# MAGIC        rp.validFrom,
# MAGIC        rp.validTo 
# MAGIC        from threlatedProps rp 
# MAGIC        inner join otherPropsNumber rem on rp.propertyNumber = rem.propertyNumber 
# MAGIC        where rp.relationshipTypeCode in ('P','U') --TODO: Check

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC create or replace temp view remainingPropsNumber as
# MAGIC 
# MAGIC   select distinct propertyNumber from thproperty
# MAGIC   minus
# MAGIC   select distinct propertyNumber from strataProps
# MAGIC   minus
# MAGIC   select distinct propertyNumber from otherProps

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC create or replace temp view remainingProps as
# MAGIC 
# MAGIC   select distinct pr.propertyNumber, 
# MAGIC          pr.propertyNumber as parentPropertyNumber, 
# MAGIC          'Self as Parent' as relationshipType,
# MAGIC          timestamp('1900-01-01') as validFrom,
# MAGIC          timestamp('9999-12-31') as validTo 
# MAGIC          from thproperty pr inner join remainingPropsNumber rem on pr.propertyNumber = rem.propertyNumber

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC create or replace temp view parents as
# MAGIC   select * from strataProps
# MAGIC   union all
# MAGIC   select * from otherProps
# MAGIC   union all
# MAGIC   select * from remainingProps

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC create or replace temp view propsAddress_1 as
# MAGIC 
# MAGIC select 
# MAGIC       pa.propertyNumber as locationId, 
# MAGIC       'ACCESS' as sourceSystemCode,
# MAGIC       trim(trim(coalesce(pa.buildingName1,'')||' '||coalesce(pa.buildingName2,''))||
# MAGIC                                       (case when pa.lotNumber is not null then ' LOT '||trim(pa.lotNumber) 
# MAGIC                                           when pa.roadsideMailBox is not null then ' RMB '||trim(pa.roadsideMailBox)
# MAGIC                                           else '' end)||
# MAGIC                                       (case when pa.floorLevelType is not null then ' ' || trim(coalesce(pa.floorLevelType,'')||' '||coalesce(pa.floorLevelNumber,''))
# MAGIC                                             else '' end)||
# MAGIC                                       (case when pa.flatUnitType is not null then ' ' || trim(coalesce(pa.flatUnitType,'')||' '||coalesce(pa.flatUnitNumber,''))
# MAGIC                                             else '' end)||
# MAGIC                                       (case when pa.houseNumber1 > 0 and pa.houseNumber2 > 0  then ' ' || trim(cast(pa.houseNumber1 as string)||coalesce(pa.houseNumber1Suffix,''))||'-'||cast(pa.houseNumber2 as string)||coalesce(pa.houseNumber2Suffix,'') 
# MAGIC                                             when pa.houseNumber1 > 0 and pa.houseNumber2 <= 0 then ' ' || trim(cast(pa.houseNumber1 as string)||coalesce(pa.houseNumber1Suffix,'')) 
# MAGIC                                             when pa.houseNumber1 <= 0 and pa.houseNumber2 > 0 then ' ' || trim(cast(pa.houseNumber2 as string)||coalesce(pa.houseNumber2Suffix,'')) 
# MAGIC                                             else '' end)||
# MAGIC                                       (' '|| trim(trim(sg.streetName||' '||coalesce(sg.streetTypeCode,''))||' '||coalesce(sg.streetSuffixCode,''))||', '||sg.suburb||' NSW '||sg.postcode)) 
# MAGIC                                    as formattedAddress,
# MAGIC      pa.houseNumber1 as addressNumber,
# MAGIC      pa.buildingName1 as buildingName1,
# MAGIC      pa.buildingName2 as buildingName2,
# MAGIC      pa.houseNumber2 as unitDetails,
# MAGIC      pa.floorLevelNumber as floorNumber,
# MAGIC      pa.houseNumber1 as houseNumber,
# MAGIC      pa.lotNumber as lotDetails, 
# MAGIC      sg.streetName as streetName,
# MAGIC      coalesce(sg.streetTypeCode,'') as streetLine1, 
# MAGIC      coalesce(sg.streetSuffixCode,'') as streetLine2, 
# MAGIC      sg.suburb as suburb, 
# MAGIC      pa.streetGuideCode as streetCode, 
# MAGIC      sg.suburb as cityCode, 
# MAGIC      sg.postcode as postCode, 
# MAGIC      'NSW' as stateCode, 
# MAGIC      pa.LGACode as LGACode,
# MAGIC      pa.LGA as LGA,
# MAGIC      case 
# MAGIC          when (pa.validFrom < sg.validFrom and pa.validTo > sg.validFrom and pa.validTo < sg.validTo) then sg.validFrom
# MAGIC          when (pa.validFrom > sg.validFrom and pa.validTo <= sg.validTo) then pa.validFrom
# MAGIC          when (pa.validFrom > sg.validFrom and pa.validTo > sg.validTo and pa.validFrom < sg.validTo) then pa.validFrom
# MAGIC          when (pa.validFrom < sg.validFrom and pa.validTo >= sg.validTo) then sg.validFrom 
# MAGIC          else pa.validFrom 
# MAGIC        end as validFrom,
# MAGIC        case 
# MAGIC          when (pa.validFrom < sg.validFrom and pa.validTo > sg.validFrom and pa.validTo < sg.validTo) then pa.validTo
# MAGIC          when (pa.validFrom > sg.validFrom and pa.validTo <= sg.validTo) then pa.validTo
# MAGIC          when (pa.validFrom > sg.validFrom and pa.validTo > sg.validTo and pa.validFrom < sg.validTo) then sg.validTo
# MAGIC          when (pa.validFrom < sg.validFrom and pa.validTo >= sg.validTo) then sg.validTo  
# MAGIC          else pa.validTo 
# MAGIC        end as validTo 
# MAGIC      --"latitude" as latitude, 
# MAGIC      --"longitude" as longitude                  
# MAGIC from thpropertyaddress pa left outer join streetGuideCode sg 
# MAGIC   on pa.streetGuideCode = sg.streetGuideCode and 
# MAGIC   ((pa.validFrom < sg.validFrom and pa.validTo > sg.validFrom and pa.validTo < sg.validTo) 
# MAGIC          or (pa.validFrom > sg.validFrom and pa.validTo <= sg.validTo) 
# MAGIC          or (pa.validFrom > sg.validFrom and pa.validTo > sg.validTo and pa.validFrom < sg.validTo)
# MAGIC          or (pa.validFrom < sg.validFrom and pa.validTo >= sg.validTo))

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC create or replace temp view propsAddress_2 as
# MAGIC 
# MAGIC   select 
# MAGIC     pa.locationId,
# MAGIC     pa.sourceSystemCode,
# MAGIC     pa.formattedAddress,
# MAGIC     pa.addressNumber,
# MAGIC     pa.buildingName1,
# MAGIC     pa.buildingName2,
# MAGIC     pa.unitDetails,
# MAGIC     pa.floorNumber,
# MAGIC     pa.houseNumber,
# MAGIC     pa.lotDetails,
# MAGIC     pa.streetName,
# MAGIC     pa.streetLine1,
# MAGIC     pa.streetLine2,
# MAGIC     pa.suburb,
# MAGIC     pa.streetCode,
# MAGIC     pa.cityCode,
# MAGIC     pa.postCode,
# MAGIC     pa.stateCode,
# MAGIC     pa.LGACode,
# MAGIC     hy.LGA,
# MAGIC     hy.latitude,
# MAGIC     hy.longitude,
# MAGIC     case 
# MAGIC            when (pa.validFrom < sg.validFrom and pa.validTo > sg.validFrom and pa.validTo < sg.validTo) then sg.validFrom
# MAGIC            when (pa.validFrom > sg.validFrom and pa.validTo <= sg.validTo) then pa.validFrom
# MAGIC            when (pa.validFrom > sg.validFrom and pa.validTo > sg.validTo and pa.validFrom < sg.validTo) then pa.validFrom
# MAGIC            when (pa.validFrom < sg.validFrom and pa.validTo >= sg.validTo) then sg.validFrom 
# MAGIC            else pa.validFrom 
# MAGIC          end as validFrom,
# MAGIC          case 
# MAGIC            when (pa.validFrom < sg.validFrom and pa.validTo > sg.validFrom and pa.validTo < sg.validTo) then pa.validTo
# MAGIC            when (pa.validFrom > sg.validFrom and pa.validTo <= sg.validTo) then pa.validTo
# MAGIC            when (pa.validFrom > sg.validFrom and pa.validTo > sg.validTo and pa.validFrom < sg.validTo) then sg.validTo
# MAGIC            when (pa.validFrom < sg.validFrom and pa.validTo >= sg.validTo) then sg.validTo  
# MAGIC            else pa.validTo 
# MAGIC          end as validTo  
# MAGIC     from propsAddress_1 pa left join parents sg
# MAGIC       on pa.locationId = sg.propertyNumber and 
# MAGIC       ((pa.validFrom < sg.validFrom and pa.validTo > sg.validFrom and pa.validTo < sg.validTo) 
# MAGIC              or (pa.validFrom > sg.validFrom and pa.validTo <= sg.validTo) 
# MAGIC              or (pa.validFrom > sg.validFrom and pa.validTo > sg.validTo and pa.validFrom < sg.validTo)
# MAGIC              or (pa.validFrom < sg.validFrom and pa.validTo >= sg.validTo))
# MAGIC          left join (
# MAGIC                      select propertyNumber, lga, latitude, longitude from 
# MAGIC                       (select propertyNumber, lga, latitude, longitude, 
# MAGIC                              row_number() over (partition by propertyNumber order by areaSize desc,latitude,longitude) recNum 
# MAGIC                       from cleansed.hydra_tlotparcel where  _RecordCurrent = 1 ) 
# MAGIC                       where recNum = 1) hy on hy.propertyNumber = sg.parentPropertyNumber

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC create or replace temp view propsAddress_3 as
# MAGIC 
# MAGIC with t1 as
# MAGIC (
# MAGIC   select 
# MAGIC   *,
# MAGIC   row_number() over(order by locationId, validFrom asc) as num_row
# MAGIC   from propsAddress_2 order by locationId, validFrom
# MAGIC ),
# MAGIC 
# MAGIC t2 as
# MAGIC (
# MAGIC   select a.*, 
# MAGIC   if( b.num_row is not null  
# MAGIC     and ((a.formattedAddress is null and b.formattedAddress is null) or (a.formattedAddress = b.formattedAddress))
# MAGIC     and ((a.addressNumber is null and b.addressNumber is null) or (a.addressNumber = b.addressNumber))
# MAGIC     and ((a.buildingName1 is null and b.buildingName1 is null) or (a.buildingName1 = b.buildingName1))
# MAGIC     and ((a.buildingName2 is null and b.buildingName2 is null) or (a.buildingName2 = b.buildingName2))
# MAGIC     and ((a.unitDetails is null and b.unitDetails is null) or (a.unitDetails = b.unitDetails))
# MAGIC     and ((a.floorNumber is null and b.floorNumber is null) or (a.floorNumber = b.floorNumber))
# MAGIC     and ((a.houseNumber is null and b.houseNumber is null) or (a.houseNumber = b.houseNumber))
# MAGIC     and ((a.lotDetails is null and b.lotDetails is null) or (a.lotDetails = b.lotDetails))
# MAGIC     and ((a.streetName is null and b.streetName is null) or (a.streetName = b.streetName))
# MAGIC     and ((a.streetLine1 is null and b.streetLine1 is null) or (a.streetLine1 = b.streetLine1))
# MAGIC     and ((a.streetLine2 is null and b.streetLine2 is null) or (a.streetLine2 = b.streetLine2))
# MAGIC     and ((a.suburb is null and b.suburb is null) or (a.suburb = b.suburb))
# MAGIC     and ((a.streetCode is null and b.streetCode is null) or (a.streetCode = b.streetCode))
# MAGIC     and ((a.cityCode is null and b.cityCode is null) or (a.cityCode = b.cityCode))
# MAGIC     and ((a.postCode is null and b.postCode is null) or (a.postCode = b.postCode))
# MAGIC     and ((a.stateCode is null and b.stateCode is null) or (a.stateCode = b.stateCode))
# MAGIC     and ((a.LGACode is null and b.LGACode is null) or (a.LGACode = b.LGACode))
# MAGIC     and ((a.LGA is null and b.LGA is null) or (a.LGA = b.LGA))
# MAGIC     and ((a.latitude is null and b.latitude is null) or (a.latitude = b.latitude))
# MAGIC     and ((a.longitude is null and b.longitude is null) or (a.longitude = b.longitude))
# MAGIC     , 0,1) as tIndicator   
# MAGIC   from t1 a 
# MAGIC   left join t1 b on a.locationId = b.locationId   
# MAGIC     and a.num_row - 1 = b.num_row
# MAGIC ),
# MAGIC 
# MAGIC t3 as
# MAGIC (
# MAGIC   select 
# MAGIC   *,
# MAGIC   sum(tIndicator) over(order by num_row) as tGroup 
# MAGIC   from t2
# MAGIC ),
# MAGIC 
# MAGIC t4 as
# MAGIC (
# MAGIC   select 
# MAGIC   *,
# MAGIC   min(validFrom) over(partition by tGroup) as new_validFrom,
# MAGIC   max(validTo) over(partition by tGroup) as new_validTo 
# MAGIC   from t3  
# MAGIC ),
# MAGIC 
# MAGIC t5 as
# MAGIC (
# MAGIC   select 
# MAGIC   locationId,
# MAGIC   sourceSystemCode,
# MAGIC   formattedAddress,
# MAGIC   addressNumber,
# MAGIC   buildingName1,
# MAGIC   buildingName2,
# MAGIC   unitDetails,
# MAGIC   floorNumber,
# MAGIC   houseNumber,
# MAGIC   lotDetails,
# MAGIC   streetName,
# MAGIC   streetLine1,
# MAGIC   streetLine2,
# MAGIC   suburb,
# MAGIC   streetCode,
# MAGIC   cityCode,
# MAGIC   postCode,
# MAGIC   stateCode,
# MAGIC   LGACode,
# MAGIC   LGA,
# MAGIC   latitude,
# MAGIC   longitude,
# MAGIC   new_validFrom as validFrom,
# MAGIC   new_validTo as validTo 
# MAGIC   from t4 where tIndicator = 1 
# MAGIC )
# MAGIC 
# MAGIC select * from t5 

# COMMAND ----------

schema = StructType([
                            StructField("locationId", StringType(), False),
                            StructField("sourceSystemCode", StringType(), True),
                            StructField("formattedAddress", StringType(), True),
                            StructField("addressNumber", StringType(), True),
                            StructField("buildingName1", StringType(), True),
                            StructField("buildingName2", StringType(), True),
                            StructField("unitDetails", StringType(), True),
                            StructField("floorNumber", StringType(), True),
                            StructField("houseNumber", StringType(), True),
                            StructField("lotDetails", StringType(), True),
                            StructField("streetName", StringType(), True),
                            StructField("streetLine1", StringType(), True),
                            StructField("streetLine2", StringType(), True),
                            StructField("suburb", StringType(), True),
                            StructField("streetCode", StringType(), True),
                            StructField("cityCode", StringType(), True),
                            StructField("postCode", StringType(), True),
                            StructField("stateCode", StringType(), True),
                            StructField("LGACode", StringType(), True),
                            StructField("LGA", StringType(), True),
                            StructField("latitude", DecimalType(9,6), True),
                            StructField("longitude", DecimalType(9,6), True)
                      ])

df = spark.sql("select * from propsAddress_3")
DeltaSaveDataframeDirect(df, 'accessts', 'access.propertyAddressTimeslice', ADS_DATABASE_CLEANSED, ADS_CONTAINER_CLEANSED, "overwrite", schema, "")
