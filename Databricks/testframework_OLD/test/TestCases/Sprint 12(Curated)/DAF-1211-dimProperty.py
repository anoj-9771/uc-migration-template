# Databricks notebook source
curateddf = spark.sql(f"select * from curated.dimProperty")
display(curateddf)

# COMMAND ----------

curateddf.printSchema()

# COMMAND ----------

# MAGIC %sql
# MAGIC select LGA from curated.dimProperty

# COMMAND ----------

# DBTITLE 1,Mapping rule(Access)
# MAGIC %sql
# MAGIC select 
# MAGIC propertyNumber 
# MAGIC ,'Access' as sourceSystemCode
# MAGIC ,propertyTypeEffectiveFrom as propertyStartDate
# MAGIC ,coalesce(lead(propertyTypeEffectiveFrom) over (partition by propertyNumber order by propertyTypeEffectiveFrom)-1, to_date('9999-12-31', 'yyyy-mm-dd'))  as propertyEndDate
# MAGIC ,propertyType
# MAGIC ,superiorPropertyType
# MAGIC ,CASE 
# MAGIC WHEN propertyAreaTypeCode = 'H' THEN  propertyArea * 10000 
# MAGIC ELSE propertyArea END AS areaSize
# MAGIC from
# MAGIC cleansed.access_z309_tproperty

# COMMAND ----------

# DBTITLE 1,Mapping rule(SAP)
# MAGIC %sql
# MAGIC select
# MAGIC a.propertyNumber
# MAGIC ,'SAP' as sourceSystemCode
# MAGIC --,pa.propertyTypeCode as parentPropertyTypeCode
# MAGIC --,pa.propertyType as 
# MAGIC ,pa.superiorPropertyTypeCode as parentPropertyType
# MAGIC ,pa.superiorPropertyType as parentSuperiorTypeCode
# MAGIC ,a.planTypeCode as parentSuperiorType
# MAGIC ,a.planType
# MAGIC ,a.lotTypeCode
# MAGIC ,a.planNumber
# MAGIC ,a.lotNumber
# MAGIC ,a.sectionNumber
# MAGIC ,a.architecturalObjectTypeCode
# MAGIC ,a.architecturalObjectType
# MAGIC from
# MAGIC cleansed.isu_0uc_connobj_attr_2 a
# MAGIC left join cleansed.isu_vibdnode vn
# MAGIC on a.architecturalObjectInternalId = vn.architecturalObjectInternalId
# MAGIC left join cleansed.isu_0uc_connobj_attr_2 pa 
# MAGIC on pa.architecturalObjectInternalId = vn.parentArchitecturalObjectInternalId

# COMMAND ----------

# DBTITLE 1,Query for cleansed.isu_zcd_tpropty_hist
# MAGIC %sql
# MAGIC select
# MAGIC coalesce(lead(validFromDate) over (partition by propertyNumber order by validFromDate)-1, to_date('9999-12-31', 'yyyy-mm-dd'))  as propertyEndDate
# MAGIC ,inferiorPropertyTypeCode as propertyTypeCode
# MAGIC ,inferiorPropertyType as propertyType
# MAGIC ,superiorPropertyTypeCode
# MAGIC ,superiorPropertyType 
# MAGIC from
# MAGIC cleansed.isu_zcd_tpropty_hist

# COMMAND ----------

# DBTITLE 1,Query for areaSize from sapisu_vibdao
# MAGIC %sql
# MAGIC select
# MAGIC case 
# MAGIC when d.hydraAreaUnit = 'M2' then d.hydraCalculatedArea
# MAGIC when d.hydraAreaUnit = 'HAR' then d.hydraCalculatedArea*10000
# MAGIC else d.hydraCalculatedArea end as areaSize
# MAGIC from
# MAGIC cleansed.isu_vibdao d
# MAGIC left join
# MAGIC cleansed.isu_0uc_connobj_attr_2 a
# MAGIC on d.architecturalObjectInternalId = a.architecturalObjectInternalId

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC --property_no, inf_prop_type, date_from
# MAGIC propertyNumber,inferiorPropertyTypeCode,validFromDate
# MAGIC from cleansed.isu_zcd_tpropty_hist cur

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC propertyNumber, propertyTypeEffectiveFrom
# MAGIC from cleansed.access_z309_tproperty

# COMMAND ----------

# MAGIC %sql
# MAGIC select propertyStartDate,* from curated.dimProperty where propertyNumber = '3100002'

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC validFromDate ,*
# MAGIC from cleansed.isu_zcd_tpropty_hist where propertyNumber = '3100002'

# COMMAND ----------

# DBTITLE 1,SAP mapping rule
# MAGIC %sql
# MAGIC select
# MAGIC a.propertyNumber as propertyNumber
# MAGIC ,'ISU' as sourceSystemCode
# MAGIC ,f.validFromDate as propertyStartDate
# MAGIC ,coalesce(lead(f.validFromDate) over (partition by f.propertyNumber order by f.validFromDate)-1, to_date('9999-12-31', 'yyyy-mm-dd'))  as propertyEndDate
# MAGIC ,f.inferiorPropertyTypeCode as propertyTypeCode
# MAGIC ,f.inferiorPropertyType as propertyType
# MAGIC ,f.superiorPropertyTypeCode
# MAGIC ,f.superiorPropertyType
# MAGIC ,case 
# MAGIC when d.hydraAreaUnit = 'M2' then d.hydraCalculatedArea
# MAGIC when d.hydraAreaUnit = 'HAR' then d.hydraCalculatedArea*10000
# MAGIC else d.hydraCalculatedArea end as areaSize
# MAGIC --,pa.propertyTypeCode as parentPropertyTypeCode def raise
# MAGIC --,pa.propertyType as def raise
# MAGIC ,c.parentArchitecturalObjectNumber as parentPropertyNumber
# MAGIC ,pa.superiorPropertyTypeCode as parentPropertyType
# MAGIC ,pa.superiorPropertyType as parentSuperiorTypeCode
# MAGIC ,a.planTypeCode as parentSuperiorType
# MAGIC ,a.planType as planType
# MAGIC ,a.lotTypeCode as lotTypeCode
# MAGIC ,b.domainValueText as lotType
# MAGIC ,a.planNumber as planNumber
# MAGIC ,a.lotNumber as lotNumber
# MAGIC ,a.sectionNumber as sectionNumber
# MAGIC ,a.architecturalObjectTypeCode as architecturalTypeCode
# MAGIC ,a.architecturalObjectType as architecturalType
# MAGIC from
# MAGIC cleansed.isu_0uc_connobj_attr_2 a
# MAGIC left join cleansed.isu_zcd_tpropty_hist f
# MAGIC on a.propertyNumber = f.propertyNumber
# MAGIC left join
# MAGIC cleansed.isu_vibdao d
# MAGIC on d.architecturalObjectInternalId = a.architecturalObjectInternalId
# MAGIC left join cleansed.isu_vibdnode c
# MAGIC on a.architecturalObjectInternalId = c.architecturalObjectInternalId
# MAGIC left join cleansed.isu_vibdnode vn
# MAGIC on a.architecturalObjectInternalId = vn.architecturalObjectInternalId
# MAGIC left join cleansed.isu_0uc_connobj_attr_2 pa 
# MAGIC on pa.architecturalObjectInternalId = vn.parentArchitecturalObjectInternalId
# MAGIC left join cleansed.isu_dd07t b
# MAGIC on a.lotTypeCode = domainValueSingleUpperLimit and domainName='ZCD_DO_ADDR_LOT_TYPE' where a.propertyNumber = '3100018'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from curated.dimProperty where propertyNumber = '3100018'

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC propertyNumber,inferiorPropertyTypeCode,validFromDate
# MAGIC from cleansed.isu_zcd_tpropty_hist cur

# COMMAND ----------

# MAGIC %sql
# MAGIC select propertyNumber, propertyTypeEffectiveFrom from cleansed.access_z309_tproperty

# COMMAND ----------

# DBTITLE 1,propertyStartDate query based on filter condition
# MAGIC %sql
# MAGIC select 
# MAGIC f.validFromDate as propertyStartDate
# MAGIC from
# MAGIC cleansed.isu_0uc_connobj_attr_2 a
# MAGIC left join cleansed.isu_zcd_tpropty_hist f
# MAGIC on a.propertyNumber = f.propertyNumber
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC --property_no, inf_prop_type, date_from  
# MAGIC select
# MAGIC cur.propertyNumber,curr.inferiorPropertyTypeCode,cur.validFromDate
# MAGIC from cleansed.isu_zcd_tpropty_hist cur
# MAGIC left join (select propertyNumber, propertyTypeEffectiveFrom from cleansed.access_z309_tproperty) lgc
# MAGIC on lgc.propertyNumber = cur.propertyNumber and lgc.propertyTypeEffectiveFrom = cur.validFromDate
# MAGIC where  lgc.propertyNumber is null

# COMMAND ----------

# DBTITLE 1,Query for parentPropertyNumber from sapisu_vibdnode
# MAGIC %sql
# MAGIC select
# MAGIC c.parentArchitecturalObjectNumber as parentPropertyNumber
# MAGIC from
# MAGIC cleansed.isu_0uc_connobj_attr_2 a
# MAGIC left join cleansed.isu_vibdnode c
# MAGIC on a.architecturalObjectInternalId = c.architecturalObjectInternalId

# COMMAND ----------

# DBTITLE 1,query for lotType from isu_dd07t
# MAGIC %sql
# MAGIC select 
# MAGIC b.domainValueText as lotType
# MAGIC ,* from
# MAGIC cleansed.isu_dd07t b
# MAGIC Left join cleansed.isu_0uc_connobj_attr_2 a
# MAGIC on a.lotTypeCode = domainValueSingleUpperLimit and domainName='ZCD_DO_ADDR_LOT_TYPE'

# COMMAND ----------

# DBTITLE 1,combined query for sap
# MAGIC %sql
# MAGIC select
# MAGIC a.propertyNumber as propertyNumber
# MAGIC ,'ISU' as sourceSystemCode
# MAGIC ,f.validFromDate as propertyStartDate 
# MAGIC 
# MAGIC ,f.coalesce(lead(validFromDate) over (partition by propertyNumber order by validFromDate)-1, to_date('9999-12-31', 'yyyy-mm-dd'))  as propertyEndDate
# MAGIC ,f.inferiorPropertyTypeCode as propertyTypeCode
# MAGIC ,f.inferiorPropertyType as propertyType
# MAGIC ,f.superiorPropertyTypeCode
# MAGIC ,f.superiorPropertyType
# MAGIC ,case 
# MAGIC when d.hydraAreaUnit = 'M2' then d.hydraCalculatedArea
# MAGIC when d.hydraAreaUnit = 'HAR' then d.hydraCalculatedArea*10000
# MAGIC else d.hydraCalculatedArea end as areaSize
# MAGIC --,pa.propertyTypeCode as parentPropertyTypeCode def raise
# MAGIC --,pa.propertyType as def raise
# MAGIC ,c.parentArchitecturalObjectNumber as parentPropertyNumber
# MAGIC ,pa.superiorPropertyTypeCode as parentPropertyType
# MAGIC ,pa.superiorPropertyType as parentSuperiorTypeCode
# MAGIC ,a.planTypeCode as parentSuperiorType
# MAGIC ,a.planType as planType
# MAGIC ,a.lotTypeCode as lotTypeCode
# MAGIC ,b.domainValueText as lotType
# MAGIC ,a.planNumber as planNumber
# MAGIC ,a.lotNumber as lotNumber
# MAGIC ,a.sectionNumber as sectionNumber
# MAGIC ,a.architecturalObjectTypeCode as architecturalTypeCode
# MAGIC ,a.architecturalObjectType as architecturalType
# MAGIC from
# MAGIC cleansed.isu_0uc_connobj_attr_2 a
# MAGIC left join
# MAGIC cleansed.isu_vibdao d
# MAGIC on d.architecturalObjectInternalId = a.architecturalObjectInternalId
# MAGIC left join cleansed.isu_zcd_tpropty_hist f
# MAGIC f.propertyNumber = a.propertyNumber
# MAGIC left join cleansed.isu_vibdnode c
# MAGIC on a.architecturalObjectInternalId = c.architecturalObjectInternalId
# MAGIC left join cleansed.isu_vibdnode vn
# MAGIC on a.architecturalObjectInternalId = vn.architecturalObjectInternalId
# MAGIC left join cleansed.isu_0uc_connobj_attr_2 pa 
# MAGIC on pa.architecturalObjectInternalId = vn.parentArchitecturalObjectInternalId
# MAGIC left join cleansed.isu_dd07t b
# MAGIC on a.lotTypeCode = domainValueSingleUpperLimit and domainName='ZCD_DO_ADDR_LOT_TYPE'
# MAGIC 
# MAGIC --join cleansed.access_z309_tproperty g
# MAGIC --on f.propertyNumber=g.propertyNumber

# COMMAND ----------

# DBTITLE 1,raise defect as columns are not present
# MAGIC %sql
# MAGIC select
# MAGIC propertyType
# MAGIC ,propertyTypeCode
# MAGIC from
# MAGIC cleansed.isu_0uc_connobj_attr_2

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC propertyAreaSize
# MAGIC ,areaSize
# MAGIC ,areaSizeUnit 
# MAGIC from cleansed.access_z309_tproperty 

# COMMAND ----------

# MAGIC %sql
# MAGIC select *,propertyArea from cleansed.access_z309_tproperty
# MAGIC  

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct(dimPropertySK) from curated.dimProperty where dimPropertySK is null or dimPropertySK in ('',' ')

# COMMAND ----------

# MAGIC %sql
# MAGIC select propertyArea,* from cleansed.access_z309_tproperty
# MAGIC where propertyAreaTypeCode = 'H'

# COMMAND ----------

# DBTITLE 0,Table
table1 = 'access_z309_tpropmeter'
table2 = 'isu_0UC_DEVICE_ATTR'
table3 = 'isu_0UC_DEVCAT_ATTR'
table4 = 'isu_0UC_DEVICEH_ATTR'
table5 = 'isu_0UC_REGIST_ATTR'

# COMMAND ----------

lakedf1 = spark.sql(f"select * from cleansed.{table1}")
display(lakedf1)

# COMMAND ----------

lakedf2 = spark.sql(f"select * from cleansed.{table2}")
display(lakedf2)

# COMMAND ----------

lakedf3 = spark.sql(f"select * from cleansed.{table3}")
display(lakedf3)

# COMMAND ----------

lakedf4 = spark.sql(f"select * from cleansed.{table4}")
display(lakedf4)

# COMMAND ----------

lakedf5 = spark.sql(f"select * from cleansed.{table5}")
display(lakedf5)

# COMMAND ----------

lakedf1.createOrReplaceTempView("Access")
lakedf2.createOrReplaceTempView("SAP")

# COMMAND ----------

lakedftarget = spark.sql("select * from curated.dimProperty")
display(lakedftarget)

# COMMAND ----------

# DBTITLE 1,[Target] Schema Check
lakedftarget.printSchema()

# COMMAND ----------

# DBTITLE 1,[Source] Schema Check
lakedf1.printSchema()

# COMMAND ----------

# DBTITLE 1,[Source] Applying Transformation
# MAGIC %sql
# MAGIC select * from (
# MAGIC select sourceSystemCode,
# MAGIC meterId,
# MAGIC meterSize,
# MAGIC waterMeterType
# MAGIC from (
# MAGIC 
# MAGIC select
# MAGIC sourceSystemCode,
# MAGIC meterId,
# MAGIC meterSize,
# MAGIC waterMeterType,
# MAGIC row_number () over (partition by meterMakerNumber order by meterFittedDate desc) as rn
# MAGIC from (
# MAGIC 
# MAGIC select 
# MAGIC 'Access' as sourceSystemCode
# MAGIC ,meterMakerNumber as meterId
# MAGIC ,meterSize
# MAGIC ,waterMeterType
# MAGIC ,meterMakerNumber
# MAGIC ,meterFittedDate
# MAGIC from Access 
# MAGIC where meterfitteddate <> meterremoveddate or meterRemovedDate is null
# MAGIC )a --where metermakernumber ='BDWE1717'
# MAGIC 
# MAGIC )a where rn = 1) 
# MAGIC 
# MAGIC union all
# MAGIC 
# MAGIC select 
# MAGIC 'SAPISU' as sourceSystemCode
# MAGIC ,a.equipmentNumber as meterNumber
# MAGIC ,a.deviceNumber as meterSerialNumber
# MAGIC ,a.logicalDeviceNumber as logicalDeviceNumber
# MAGIC ,a.materialNumber as materialNumber
# MAGIC ,case
# MAGIC when b.functionClassCode = 9000 then "Customer Standpipe"
# MAGIC else b.functionClassCode end as usageDeviceType
# MAGIC ,concat(a.deviceSize,'mm') as meterSize
# MAGIC --,b.deviceCategoryDescription as meterSize
# MAGIC ,case
# MAGIC when b.functionClassCode in(1000,2000) then functionClass 
# MAGIC else null end as waterType
# MAGIC ,b.constructionClassCode as meterCategoryCode
# MAGIC ,b.constructionClass as meterCategory
# MAGIC ,b.deviceCategoryName as meterReadingType
# MAGIC ,b.deviceCategoryDescription as meterDescription
# MAGIC ,a.assetManufacturerName as manufacturerName
# MAGIC ,a.manufacturerSerialNumber as manufacturerSerialNumber
# MAGIC --,a.manufacturerModelNumber as manufacturerModelNumber
# MAGIC ,d.unitOfMeasurementMeterReading as measurementUnit
# MAGIC ,c.activityReasonCode as latestActivityReasonCode
# MAGIC ,c.activityReason as latestActivityReason
# MAGIC ,case
# MAGIC when a.inspectionRelevanceIndicator = 'X' then 'Y'
# MAGIC else 'N' end as inspectionRelevanceFlag
# MAGIC ,d.registerNumber as registerNumber
# MAGIC ,c.registerGroupCode as registerGroupCode
# MAGIC ,c.registerGroup as registerGroup
# MAGIC ,d.registerTypeCode as registerTypeCode
# MAGIC ,d.registerType as registerType
# MAGIC ,d.registerCategoryCode as registerCategoryCode
# MAGIC ,d.registerCategory as registerCategory
# MAGIC ,d.registerIdCode as registerIdCode
# MAGIC ,d.registerId as registerId
# MAGIC ,d.divisionCategoryCode as divisionCategoryCode
# MAGIC ,d.divisionCategory as divisionCategory
# MAGIC from cleansed.isu_0UC_DEVICE_ATTR a
# MAGIC left join cleansed.isu_0UC_DEVCAT_ATTR b
# MAGIC on a.materialNumber = b.materialNumber
# MAGIC left join cleansed.isu_0UC_DEVICEH_ATTR c
# MAGIC on c.equipmentNumber = a.equipmentNumber
# MAGIC and current_date between c.validFromDate and c.validToDate
# MAGIC left join on cleansed.isu_0UC_REGIST_ATTR d
# MAGIC on d.equipmentNumber = a.equipmentNumber 
# MAGIC and current_date between d.validFromDate and d.validToDate
# MAGIC union all
# MAGIC 
# MAGIC select * from(
# MAGIC select 
# MAGIC 'ACCESS' as sourceSystemCode
# MAGIC ,'-1' as meterId
# MAGIC ,'Unknown' as meterSize
# MAGIC ,'Unknown' as waterMeterType
# MAGIC from Access limit 1)a
# MAGIC 
# MAGIC 
# MAGIC union all
# MAGIC 
# MAGIC select * from (
# MAGIC select 
# MAGIC 'SAPISU' as sourceSystemCode
# MAGIC ,'-1' as meterId
# MAGIC ,'Unknown' as meterSize
# MAGIC ,'Unknown' as waterMeterType
# MAGIC from Access limit 1)b

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from curated.dimmeter where dimmetersk =' '

# COMMAND ----------

# DBTITLE 1,[Verification] Auto Generate field check
# MAGIC %sql
# MAGIC select dimMeterSK from curated.dimmeter where dimmetersk in (null,'',' ')

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Check
# MAGIC %sql
# MAGIC SELECT dimmetersk, COUNT (*) as count
# MAGIC FROM curated.dimmeter
# MAGIC GROUP BY dimmetersk
# MAGIC HAVING COUNT (*) > 1

# COMMAND ----------

# DBTITLE 1,Duplicate check
# MAGIC %sql
# MAGIC SELECT meterId, COUNT (*) as count
# MAGIC FROM curated.dimmeter
# MAGIC GROUP BY meterId
# MAGIC HAVING COUNT (*) > 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from curated.dimMeter where meterId =-1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT metermakernumber, COUNT (*) as count
# MAGIC FROM cleansed.access_z309_tpropmeter
# MAGIC GROUP BY metermakernumber
# MAGIC HAVING COUNT (*) > 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select dimmetersk - 1, dimmetersk as missingNumber from curated.dimmeter a where  not exists (select dimmetersk from curated.dimmeter b where b.dimmetersk = a.dimmetersk -1)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from curated.dimmeter where dimmetersk = -1 --meterid = '' --order by meterid asc

# COMMAND ----------

# DBTITLE 1,[Verification] Count Check
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from curated.dimmeter
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from (
# MAGIC 
# MAGIC 
# MAGIC select * from (
# MAGIC select sourceSystemCode,
# MAGIC meterId,
# MAGIC meterSize,
# MAGIC waterMeterType
# MAGIC from (
# MAGIC 
# MAGIC select
# MAGIC sourceSystemCode,
# MAGIC meterId,
# MAGIC meterSize,
# MAGIC waterMeterType,
# MAGIC row_number () over (partition by meterMakerNumber order by meterFittedDate desc) as rn
# MAGIC from (
# MAGIC 
# MAGIC select 
# MAGIC 'Access' as sourceSystemCode
# MAGIC ,meterMakerNumber as meterId
# MAGIC ,meterSize
# MAGIC ,waterMeterType
# MAGIC ,meterMakerNumber
# MAGIC ,meterFittedDate
# MAGIC from Access 
# MAGIC where meterfitteddate <> meterremoveddate or meterRemovedDate is null
# MAGIC )a --where metermakernumber ='BDWE1717'
# MAGIC 
# MAGIC )a where rn = 1) 
# MAGIC 
# MAGIC union all
# MAGIC 
# MAGIC select 
# MAGIC 'SAPISU' as sourceSystemCode
# MAGIC ,a.equipmentNumber as meterId
# MAGIC ,b.deviceCategoryDescription as meterSize
# MAGIC ,b.functionClass as waterType
# MAGIC from cleansed.isu_0UC_DEVICE_ATTR a
# MAGIC left join cleansed.isu_0UC_DEVCAT_ATTR b
# MAGIC on a.materialNumber = b.materialNumber
# MAGIC 
# MAGIC union all
# MAGIC 
# MAGIC select * from(
# MAGIC select 
# MAGIC 'ACCESS' as sourceSystemCode
# MAGIC ,'-1' as meterId
# MAGIC ,'Unknown' as meterSize
# MAGIC ,'Unknown' as waterMeterType
# MAGIC from Access limit 1)a
# MAGIC 
# MAGIC 
# MAGIC union all
# MAGIC 
# MAGIC select * from (
# MAGIC select 
# MAGIC 'SAPISU' as sourceSystemCode
# MAGIC ,'-1' as meterId
# MAGIC ,'Unknown' as meterSize
# MAGIC ,'Unknown' as waterMeterType
# MAGIC from Access limit 1)b
# MAGIC 
# MAGIC 
# MAGIC )

# COMMAND ----------

# DBTITLE 1,[Verify] Source to Target Comparison
# MAGIC %sql
# MAGIC select * from (
# MAGIC select * from (
# MAGIC select sourceSystemCode,
# MAGIC meterId,
# MAGIC meterSize,
# MAGIC waterMeterType
# MAGIC from (
# MAGIC 
# MAGIC select
# MAGIC sourceSystemCode,
# MAGIC meterId,
# MAGIC meterSize,
# MAGIC waterMeterType,
# MAGIC row_number () over (partition by meterMakerNumber order by meterFittedDate desc) as rn
# MAGIC from (
# MAGIC 
# MAGIC select 
# MAGIC 'Access' as sourceSystemCode
# MAGIC ,case when meterMakerNumber is null then '' else meterMakerNumber end as meterId
# MAGIC ,meterSize
# MAGIC ,waterMeterType
# MAGIC ,meterMakerNumber
# MAGIC ,meterFittedDate
# MAGIC from Access 
# MAGIC where meterfitteddate <> meterremoveddate or meterRemovedDate is null
# MAGIC )a --where metermakernumber ='BDWE1717'
# MAGIC 
# MAGIC )a where rn = 1) 
# MAGIC 
# MAGIC union all
# MAGIC 
# MAGIC select 
# MAGIC 'ISU' as sourceSystemCode
# MAGIC ,a.equipmentNumber as meterId
# MAGIC ,b.deviceCategoryDescription as meterSize
# MAGIC ,b.functionClass as waterType
# MAGIC from cleansed.isu_0UC_DEVICE_ATTR a
# MAGIC left join cleansed.isu_0UC_DEVCAT_ATTR b
# MAGIC on a.materialNumber = b.materialNumber
# MAGIC 
# MAGIC union all
# MAGIC 
# MAGIC select * from(
# MAGIC select 
# MAGIC 'Access' as sourceSystemCode
# MAGIC ,'-1' as meterId
# MAGIC ,'Unknown' as meterSize
# MAGIC ,'Unknown' as waterMeterType
# MAGIC from Access limit 1)a
# MAGIC 
# MAGIC 
# MAGIC union all
# MAGIC 
# MAGIC select * from (
# MAGIC select 
# MAGIC 'ISU' as sourceSystemCode
# MAGIC ,'-1' as meterId
# MAGIC ,'Unknown' as meterSize
# MAGIC ,'Unknown' as waterMeterType
# MAGIC from Access limit 1)b
# MAGIC )zz
# MAGIC 
# MAGIC 
# MAGIC except
# MAGIC 
# MAGIC select sourceSystemCode,
# MAGIC meterId,
# MAGIC meterSize,
# MAGIC waterMeterType
# MAGIC from curated.dimmeter

# COMMAND ----------

# DBTITLE 1,[Verify] Target to Source Comparison
# MAGIC %sql
# MAGIC 
# MAGIC select sourceSystemCode,
# MAGIC meterId,
# MAGIC meterSize,
# MAGIC waterMeterType
# MAGIC from curated.dimmeter
# MAGIC 
# MAGIC except 
# MAGIC 
# MAGIC select * from (
# MAGIC select * from (
# MAGIC select sourceSystemCode,
# MAGIC meterId,
# MAGIC meterSize,
# MAGIC waterMeterType
# MAGIC from (
# MAGIC 
# MAGIC select
# MAGIC sourceSystemCode,
# MAGIC meterId,
# MAGIC meterSize,
# MAGIC waterMeterType,
# MAGIC row_number () over (partition by meterMakerNumber order by meterFittedDate desc) as rn
# MAGIC from (
# MAGIC 
# MAGIC select 
# MAGIC 'Access' as sourceSystemCode
# MAGIC ,case when meterMakerNumber is null then '' else meterMakerNumber end as meterId
# MAGIC ,meterSize
# MAGIC ,waterMeterType
# MAGIC ,meterMakerNumber
# MAGIC ,meterFittedDate
# MAGIC from Access 
# MAGIC where meterfitteddate <> meterremoveddate or meterRemovedDate is null
# MAGIC )a --where metermakernumber ='BDWE1717'
# MAGIC 
# MAGIC )a where rn = 1) 
# MAGIC 
# MAGIC union all
# MAGIC 
# MAGIC select 
# MAGIC 'SAPISU' as sourceSystemCode
# MAGIC ,a.equipmentNumber as meterId
# MAGIC ,b.deviceCategoryDescription as meterSize
# MAGIC ,b.functionClass as waterType
# MAGIC from cleansed.isu_0UC_DEVICE_ATTR a
# MAGIC left join cleansed.isu_0UC_DEVCAT_ATTR b
# MAGIC on a.materialNumber = b.materialNumber
# MAGIC 
# MAGIC union all
# MAGIC 
# MAGIC select * from(
# MAGIC select 
# MAGIC 'Access' as sourceSystemCode
# MAGIC ,'-1' as meterId
# MAGIC ,'Unknown' as meterSize
# MAGIC ,'Unknown' as waterMeterType
# MAGIC from Access limit 1)a
# MAGIC 
# MAGIC 
# MAGIC union all
# MAGIC 
# MAGIC select * from (
# MAGIC select 
# MAGIC 'SAPISU' as sourceSystemCode
# MAGIC ,'-1' as meterId
# MAGIC ,'Unknown' as meterSize
# MAGIC ,'Unknown' as waterMeterType
# MAGIC from Access limit 1)b
# MAGIC )zz

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from(
# MAGIC select 
# MAGIC 'Access' as sourceSystemCode
# MAGIC ,'-1' as meterId
# MAGIC ,'Unknown' as meterSize
# MAGIC ,'Unknown' as waterMeterType
# MAGIC from Access limit 1)a
# MAGIC 
# MAGIC 
# MAGIC union all
# MAGIC 
# MAGIC select * from (
# MAGIC select 
# MAGIC 'SAPISU' as sourceSystemCode
# MAGIC ,'-1' as meterId
# MAGIC ,'Unknown' as meterSize
# MAGIC ,'Unknown' as waterMeterType
# MAGIC from Access limit 1)b

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from curated.dimMeter
