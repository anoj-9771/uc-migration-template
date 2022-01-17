# Databricks notebook source
# DBTITLE 0,Table
table1 = 'isu_0bp_relations_attr'

# COMMAND ----------

lakedf1 = spark.sql(f"select * from cleansed.{table1}")
display(lakedf1)

# COMMAND ----------

lakedf1.createOrReplaceTempView("SAP")

# COMMAND ----------

lakedftarget = spark.sql("select * from curated.brgBusinessPartnerGroupRelation")
display(lakedftarget)

# COMMAND ----------

# DBTITLE 1,[Target] Schema Check
lakedftarget.printSchema()

# COMMAND ----------

# DBTITLE 1,[Source] Schema Check
lakedf1.printSchema()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from curated.brgBusinessPartnerGroupRelation

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from curated.dimBusinessPartner where businesspartnernumber in ('0010906132','0010906121')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from curated.dimBusinessPartnerGroup where businesspartnergroupnumber = '0003207540' --businesspartnergroupNumber = '0003104330'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sap where businessPartnerNumber1 = '0003207540'

# COMMAND ----------

# DBTITLE 1,[Source] Applying Transformation
# MAGIC %sql
# MAGIC select
# MAGIC b.dimBusinessPartnerGroupSK
# MAGIC ,c.dimBusinessPartnerSK
# MAGIC ,*
# MAGIC from SAP a
# MAGIC left join curated.dimBusinessPartnerGroup b on a.businessPartnerNumber1 = b.businesspartnergroupNumber
# MAGIC left join curated.dimBusinessPartner c on a.businessPartnerNumber2 = c.businesspartnernumber
# MAGIC where a.relationshipTypeCode = 'ZSW009' and a.relationshipDirection = '1' and a.deletedindicator is null
# MAGIC and b.businessPartnergroupnumber = '0003207540'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from curated.brgbusinesspartnergrouprelation where relationshipNumber = '000000106899'--businessPartnerSK in ('4701888','2494844')

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct sourcesystemcode from curated.brgbusinesspartnergrouprelation where businessPartnerSK = '-1'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from curated.dimcontract order by dimcontractsk desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from curated.dimdate

# COMMAND ----------

# DBTITLE 1,[Verification] Auto Generate field check
# MAGIC %sql
# MAGIC select * from curated.dimMeter where dimmetersk is null or dimmetersk = '' or dimmetersk = ' '

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Check
# MAGIC %sql
# MAGIC --duplicate check of Surrogate key
# MAGIC SELECT dimmetersk,COUNT (*) as count
# MAGIC FROM curated.dimmeter
# MAGIC GROUP BY dimmetersk
# MAGIC HAVING COUNT (*) > 1

# COMMAND ----------

# MAGIC %sql
# MAGIC --duplicate check of Unique key
# MAGIC SELECT sourceSystemCode,meterNumber,COUNT (*) as count
# MAGIC FROM curated.dimmeter
# MAGIC GROUP BY sourceSystemCode,meterNumber
# MAGIC HAVING COUNT (*) > 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from curated.dimMeter where meterNumber =-1

# COMMAND ----------

# DBTITLE 1,[Verification] Count Check
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from curated.dimmeter
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from (
# MAGIC 
# MAGIC select * from (
# MAGIC select sourceSystemCode,
# MAGIC meterNumber,
# MAGIC meterSerialNumber,
# MAGIC logicalDeviceNumber,
# MAGIC materialNumber,
# MAGIC usageDeviceType,
# MAGIC meterSize,
# MAGIC waterType,
# MAGIC meterCategoryCode,
# MAGIC meterCategory,
# MAGIC meterReadingType,
# MAGIC meterDescription,
# MAGIC manufacturerName,
# MAGIC manufacturerSerialNumber,
# MAGIC manufacturerModelNumber,
# MAGIC measurementUnit,
# MAGIC latestActivityReasonCode,
# MAGIC latestActivityReason,
# MAGIC inspectionRelevanceFlag,
# MAGIC registerNumber,
# MAGIC registerGroupCode,
# MAGIC registerGroup,
# MAGIC registerTypeCode,
# MAGIC registerType,
# MAGIC registerCategoryCode,
# MAGIC registerCategory,
# MAGIC registerIdCode,
# MAGIC registerId,
# MAGIC divisionCategoryCode,
# MAGIC divisionCategory
# MAGIC from (
# MAGIC select
# MAGIC sourceSystemCode,
# MAGIC meterNumber,
# MAGIC meterSerialNumber,
# MAGIC logicalDeviceNumber,
# MAGIC materialNumber,
# MAGIC usageDeviceType,
# MAGIC meterSize,
# MAGIC waterType,
# MAGIC meterCategoryCode,
# MAGIC meterCategory,
# MAGIC meterReadingType,
# MAGIC meterDescription,
# MAGIC manufacturerName,
# MAGIC manufacturerSerialNumber,
# MAGIC manufacturerModelNumber,
# MAGIC measurementUnit,
# MAGIC latestActivityReasonCode,
# MAGIC latestActivityReason,
# MAGIC inspectionRelevanceFlag,
# MAGIC registerNumber,
# MAGIC registerGroupCode,
# MAGIC registerGroup,
# MAGIC registerTypeCode,
# MAGIC registerType,
# MAGIC registerCategoryCode,
# MAGIC registerCategory,
# MAGIC registerIdCode,
# MAGIC registerId,
# MAGIC divisionCategoryCode,
# MAGIC divisionCategory,
# MAGIC row_number () over (partition by meterMakerNumber order by meterFittedDate desc) as rn
# MAGIC from (
# MAGIC select 
# MAGIC 'Access' as sourceSystemCode
# MAGIC ,'NULL' as meterNumber
# MAGIC ,meterMakerNumber as meterSerialNumber 
# MAGIC ,'NULL' as logicalDeviceNumber
# MAGIC ,'NULL' as materialNumber
# MAGIC ,'NULL' as usageDeviceType
# MAGIC ,meterSize
# MAGIC ,waterMeterType  as waterType
# MAGIC ,meterMakerNumber
# MAGIC ,meterFittedDate
# MAGIC ,'NULL' as meterCategoryCode
# MAGIC ,'NULL' as meterCategory
# MAGIC ,'NULL' as meterReadingType
# MAGIC ,'NULL' as meterDescription
# MAGIC ,'NULL' as manufacturerName
# MAGIC ,'NULL' as manufacturerSerialNumber
# MAGIC ,'NULL' as manufacturerModelNumber
# MAGIC ,'NULL' as measurementUnit
# MAGIC ,'NULL' as latestActivityReasonCode
# MAGIC ,'NULL' as latestActivityReason
# MAGIC ,'NULL' as inspectionRelevanceFlag
# MAGIC ,'NULL' as registerNumber
# MAGIC ,'NULL' as registerGroupCode
# MAGIC ,'NULL' as registerGroup
# MAGIC ,'NULL' as registerTypeCode
# MAGIC ,'NULL' as registerType
# MAGIC ,'NULL' as registerCategoryCode
# MAGIC ,'NULL' as registerCategory
# MAGIC ,'NULL' as registerIdCode
# MAGIC ,'NULL' as registerId
# MAGIC ,'NULL' as divisionCategoryCode
# MAGIC ,'NULL' as divisionCategory
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
# MAGIC ,a.equipmentNumber as meterNumber
# MAGIC ,a.deviceNumber as meterSerialNumber
# MAGIC ,c.logicalDeviceNumber as logicalDeviceNumber
# MAGIC ,a.materialNumber as materialNumber
# MAGIC ,case
# MAGIC when b.functionClassCode = 9000 then "Customer Standpipe"
# MAGIC else b.functionClassCode end as usageDeviceType
# MAGIC ,concat(a.deviceSize,'mm') as meterSize
# MAGIC ,case
# MAGIC when b.functionClassCode in(1000,2000) then functionClass 
# MAGIC else null end as waterType
# MAGIC ,b.constructionClassCode as meterCategoryCode
# MAGIC ,b.constructionClass as meterCategory
# MAGIC ,b.deviceCategoryName as meterReadingType
# MAGIC ,b.deviceCategoryDescription as meterDescription
# MAGIC ,a.assetManufacturerName as manufacturerName
# MAGIC ,a.manufacturerSerialNumber as manufacturerSerialNumber
# MAGIC ,a.manufacturerModelNumber as manufacturerModelNumber
# MAGIC ,d.unitOfMeasurementMeterReading as measurementUnit
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
# MAGIC left join cleansed.isu_0UC_REGIST_ATTR d
# MAGIC on d.equipmentNumber = a.equipmentNumber 
# MAGIC and current_date between d.validFromDate and d.validToDate
# MAGIC 
# MAGIC union all
# MAGIC 
# MAGIC select * from(
# MAGIC select 
# MAGIC 'ACCESS' as sourceSystemCode,
# MAGIC '-1' as meterNumber,
# MAGIC 'Unknown' as meterSerialNumber,
# MAGIC 'Unknown' as logicalDeviceNumber,
# MAGIC 'Unknown' as materialNumber,
# MAGIC 'Unknown' as usageDeviceType,
# MAGIC 'Unknown' as meterSize,
# MAGIC 'Unknown' as waterType,
# MAGIC 'Unknown' as meterCategoryCode,
# MAGIC 'Unknown' as meterCategory,
# MAGIC 'Unknown' as meterReadingType,
# MAGIC 'Unknown' as meterDescription,
# MAGIC 'Unknown' as manufacturerName,
# MAGIC 'Unknown' as manufacturerSerialNumber,
# MAGIC 'Unknown' as manufacturerModelNumber,
# MAGIC 'Unknown' as measurementUnit,
# MAGIC 'Unknown' as latestActivityReasonCode,
# MAGIC 'Unknown' as latestActivityReason,
# MAGIC 'Unknown' as inspectionRelevanceFlag,
# MAGIC 'Unknown' as registerNumber,
# MAGIC 'Unknown' as registerGroupCode,
# MAGIC 'Unknown' as registerGroup,
# MAGIC 'Unknown' as registerTypeCode,
# MAGIC 'Unknown' as registerType,
# MAGIC 'Unknown' as registerCategoryCode,
# MAGIC 'Unknown' as registerCategory,
# MAGIC 'Unknown' as registerIdCode,
# MAGIC 'Unknown' as registerId,
# MAGIC 'Unknown' as divisionCategoryCode,
# MAGIC 'Unknown' as divisionCategory
# MAGIC from Access limit 1)a
# MAGIC 
# MAGIC union all
# MAGIC 
# MAGIC select * from (
# MAGIC select 
# MAGIC 'ISU' as sourceSystemCode,
# MAGIC '-1' as meterNumber,
# MAGIC 'Unknown' as meterSerialNumber,
# MAGIC 'Unknown' as logicalDeviceNumber,
# MAGIC 'Unknown' as materialNumber,
# MAGIC 'Unknown' as usageDeviceType,
# MAGIC 'Unknown' as meterSize,
# MAGIC 'Unknown' as waterType,
# MAGIC 'Unknown' as meterCategoryCode,
# MAGIC 'Unknown' as meterCategory,
# MAGIC 'Unknown' as meterReadingType,
# MAGIC 'Unknown' as meterDescription,
# MAGIC 'Unknown' as manufacturerName,
# MAGIC 'Unknown' as manufacturerSerialNumber,
# MAGIC 'Unknown' as manufacturerModelNumber,
# MAGIC 'Unknown' as measurementUnit,
# MAGIC 'Unknown' as latestActivityReasonCode,
# MAGIC 'Unknown' as latestActivityReason,
# MAGIC 'Unknown' as inspectionRelevanceFlag,
# MAGIC 'Unknown' as registerNumber,
# MAGIC 'Unknown' as registerGroupCode,
# MAGIC 'Unknown' as registerGroup,
# MAGIC 'Unknown' as registerTypeCode,
# MAGIC 'Unknown' as registerType,
# MAGIC 'Unknown' as registerCategoryCode,
# MAGIC 'Unknown' as registerCategory,
# MAGIC 'Unknown' as registerIdCode,
# MAGIC 'Unknown' as registerId,
# MAGIC 'Unknown' as divisionCategoryCode,
# MAGIC 'Unknown' as divisionCategory
# MAGIC from Access limit 1)b)

# COMMAND ----------

# DBTITLE 1,[Verify] Source to Target Comparison
# MAGIC %sql
# MAGIC (select * from (
# MAGIC select sourceSystemCode,
# MAGIC meterNumber,
# MAGIC meterSerialNumber,
# MAGIC logicalDeviceNumber,
# MAGIC materialNumber,
# MAGIC usageDeviceType,
# MAGIC meterSize,
# MAGIC waterType,
# MAGIC meterCategoryCode,
# MAGIC meterCategory,
# MAGIC meterReadingType,
# MAGIC meterDescription,
# MAGIC manufacturerName,
# MAGIC manufacturerSerialNumber,
# MAGIC manufacturerModelNumber,
# MAGIC measurementUnit,
# MAGIC latestActivityReasonCode,
# MAGIC latestActivityReason,
# MAGIC inspectionRelevanceFlag,
# MAGIC registerNumber,
# MAGIC registerGroupCode,
# MAGIC registerGroup,
# MAGIC registerTypeCode,
# MAGIC registerType,
# MAGIC registerCategoryCode,
# MAGIC registerCategory,
# MAGIC registerIdCode,
# MAGIC registerId,
# MAGIC divisionCategoryCode,
# MAGIC divisionCategory
# MAGIC from (
# MAGIC select
# MAGIC sourceSystemCode,
# MAGIC meterNumber,
# MAGIC meterSerialNumber,
# MAGIC logicalDeviceNumber,
# MAGIC materialNumber,
# MAGIC usageDeviceType,
# MAGIC meterSize,
# MAGIC waterType,
# MAGIC meterCategoryCode,
# MAGIC meterCategory,
# MAGIC meterReadingType,
# MAGIC meterDescription,
# MAGIC manufacturerName,
# MAGIC manufacturerSerialNumber,
# MAGIC manufacturerModelNumber,
# MAGIC measurementUnit,
# MAGIC latestActivityReasonCode,
# MAGIC latestActivityReason,
# MAGIC inspectionRelevanceFlag,
# MAGIC registerNumber,
# MAGIC registerGroupCode,
# MAGIC registerGroup,
# MAGIC registerTypeCode,
# MAGIC registerType,
# MAGIC registerCategoryCode,
# MAGIC registerCategory,
# MAGIC registerIdCode,
# MAGIC registerId,
# MAGIC divisionCategoryCode,
# MAGIC divisionCategory,
# MAGIC row_number () over (partition by meterMakerNumber order by meterFittedDate desc) as rn
# MAGIC from (
# MAGIC select 
# MAGIC 'Access' as sourceSystemCode
# MAGIC ,'NULL' as meterNumber
# MAGIC ,meterMakerNumber as meterSerialNumber 
# MAGIC ,'NULL' as logicalDeviceNumber
# MAGIC ,'NULL' as materialNumber
# MAGIC ,'NULL' as usageDeviceType
# MAGIC ,meterSize
# MAGIC ,waterMeterType  as waterType
# MAGIC ,meterMakerNumber
# MAGIC ,meterFittedDate
# MAGIC ,'NULL' as meterCategoryCode
# MAGIC ,'NULL' as meterCategory
# MAGIC ,'NULL' as meterReadingType
# MAGIC ,'NULL' as meterDescription
# MAGIC ,'NULL' as manufacturerName
# MAGIC ,'NULL' as manufacturerSerialNumber
# MAGIC ,'NULL' as manufacturerModelNumber
# MAGIC ,'NULL' as measurementUnit
# MAGIC ,'NULL' as latestActivityReasonCode
# MAGIC ,'NULL' as latestActivityReason
# MAGIC ,'NULL' as inspectionRelevanceFlag
# MAGIC ,'NULL' as registerNumber
# MAGIC ,'NULL' as registerGroupCode
# MAGIC ,'NULL' as registerGroup
# MAGIC ,'NULL' as registerTypeCode
# MAGIC ,'NULL' as registerType
# MAGIC ,'NULL' as registerCategoryCode
# MAGIC ,'NULL' as registerCategory
# MAGIC ,'NULL' as registerIdCode
# MAGIC ,'NULL' as registerId
# MAGIC ,'NULL' as divisionCategoryCode
# MAGIC ,'NULL' as divisionCategory
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
# MAGIC ,a.equipmentNumber as meterNumber
# MAGIC ,a.deviceNumber as meterSerialNumber
# MAGIC ,c.logicalDeviceNumber as logicalDeviceNumber
# MAGIC ,a.materialNumber as materialNumber
# MAGIC ,case
# MAGIC when b.functionClassCode = 9000 then "Customer Standpipe"
# MAGIC else b.functionClassCode end as usageDeviceType
# MAGIC ,concat(a.deviceSize,'mm') as meterSize
# MAGIC ,case
# MAGIC when b.functionClassCode in(1000,2000) then functionClass 
# MAGIC else null end as waterType
# MAGIC ,b.constructionClassCode as meterCategoryCode
# MAGIC ,b.constructionClass as meterCategory
# MAGIC ,b.deviceCategoryName as meterReadingType
# MAGIC ,b.deviceCategoryDescription as meterDescription
# MAGIC ,a.assetManufacturerName as manufacturerName
# MAGIC ,a.manufacturerSerialNumber as manufacturerSerialNumber
# MAGIC ,a.manufacturerModelNumber as manufacturerModelNumber
# MAGIC ,d.unitOfMeasurementMeterReading as measurementUnit
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
# MAGIC left join cleansed.isu_0UC_REGIST_ATTR d
# MAGIC on d.equipmentNumber = a.equipmentNumber 
# MAGIC and current_date between d.validFromDate and d.validToDate
# MAGIC 
# MAGIC union all
# MAGIC 
# MAGIC select * from(
# MAGIC select 
# MAGIC 'ACCESS' as sourceSystemCode,
# MAGIC '-1' as meterNumber,
# MAGIC 'Unknown' as meterSerialNumber,
# MAGIC 'Unknown' as logicalDeviceNumber,
# MAGIC 'Unknown' as materialNumber,
# MAGIC 'Unknown' as usageDeviceType,
# MAGIC 'Unknown' as meterSize,
# MAGIC 'Unknown' as waterType,
# MAGIC 'Unknown' as meterCategoryCode,
# MAGIC 'Unknown' as meterCategory,
# MAGIC 'Unknown' as meterReadingType,
# MAGIC 'Unknown' as meterDescription,
# MAGIC 'Unknown' as manufacturerName,
# MAGIC 'Unknown' as manufacturerSerialNumber,
# MAGIC 'Unknown' as manufacturerModelNumber,
# MAGIC 'Unknown' as measurementUnit,
# MAGIC 'Unknown' as latestActivityReasonCode,
# MAGIC 'Unknown' as latestActivityReason,
# MAGIC 'Unknown' as inspectionRelevanceFlag,
# MAGIC 'Unknown' as registerNumber,
# MAGIC 'Unknown' as registerGroupCode,
# MAGIC 'Unknown' as registerGroup,
# MAGIC 'Unknown' as registerTypeCode,
# MAGIC 'Unknown' as registerType,
# MAGIC 'Unknown' as registerCategoryCode,
# MAGIC 'Unknown' as registerCategory,
# MAGIC 'Unknown' as registerIdCode,
# MAGIC 'Unknown' as registerId,
# MAGIC 'Unknown' as divisionCategoryCode,
# MAGIC 'Unknown' as divisionCategory
# MAGIC from Access limit 1)a
# MAGIC 
# MAGIC union all
# MAGIC 
# MAGIC select * from (
# MAGIC select 
# MAGIC 'ISU' as sourceSystemCode,
# MAGIC '-1' as meterNumber,
# MAGIC 'Unknown' as meterSerialNumber,
# MAGIC 'Unknown' as logicalDeviceNumber,
# MAGIC 'Unknown' as materialNumber,
# MAGIC 'Unknown' as usageDeviceType,
# MAGIC 'Unknown' as meterSize,
# MAGIC 'Unknown' as waterType,
# MAGIC 'Unknown' as meterCategoryCode,
# MAGIC 'Unknown' as meterCategory,
# MAGIC 'Unknown' as meterReadingType,
# MAGIC 'Unknown' as meterDescription,
# MAGIC 'Unknown' as manufacturerName,
# MAGIC 'Unknown' as manufacturerSerialNumber,
# MAGIC 'Unknown' as manufacturerModelNumber,
# MAGIC 'Unknown' as measurementUnit,
# MAGIC 'Unknown' as latestActivityReasonCode,
# MAGIC 'Unknown' as latestActivityReason,
# MAGIC 'Unknown' as inspectionRelevanceFlag,
# MAGIC 'Unknown' as registerNumber,
# MAGIC 'Unknown' as registerGroupCode,
# MAGIC 'Unknown' as registerGroup,
# MAGIC 'Unknown' as registerTypeCode,
# MAGIC 'Unknown' as registerType,
# MAGIC 'Unknown' as registerCategoryCode,
# MAGIC 'Unknown' as registerCategory,
# MAGIC 'Unknown' as registerIdCode,
# MAGIC 'Unknown' as registerId,
# MAGIC 'Unknown' as divisionCategoryCode,
# MAGIC 'Unknown' as divisionCategory
# MAGIC from Access limit 1)b)
# MAGIC 
# MAGIC except
# MAGIC 
# MAGIC select --dimmeterSK,
# MAGIC sourceSystemCode,
# MAGIC meterNumber,
# MAGIC meterSerialNumber,
# MAGIC logicalDeviceNumber,
# MAGIC materialNumber,
# MAGIC usageDeviceType,
# MAGIC meterSize,
# MAGIC waterType,
# MAGIC meterCategoryCode,
# MAGIC meterCategory,
# MAGIC meterReadingType,
# MAGIC meterDescription,
# MAGIC manufacturerName,
# MAGIC manufacturerSerialNumber,
# MAGIC manufacturerModelNumber,
# MAGIC measurementUnit,
# MAGIC latestActivityReasonCode,
# MAGIC latestActivityReason,
# MAGIC inspectionRelevanceFlag,
# MAGIC registerNumber,
# MAGIC registerGroupCode,
# MAGIC registerGroup,
# MAGIC registerTypeCode,
# MAGIC registerType,
# MAGIC registerCategoryCode,
# MAGIC registerCategory,
# MAGIC registerIdCode,
# MAGIC registerId,
# MAGIC divisionCategoryCode,
# MAGIC divisionCategory
# MAGIC from curated.dimmeter

# COMMAND ----------

# DBTITLE 1,[Verify] Target to Source Comparison
# MAGIC %sql
# MAGIC 
# MAGIC select --dimmeterSK,
# MAGIC sourceSystemCode,
# MAGIC meterNumber,
# MAGIC meterSerialNumber,
# MAGIC logicalDeviceNumber,
# MAGIC materialNumber,
# MAGIC usageDeviceType,
# MAGIC meterSize,
# MAGIC waterType,
# MAGIC meterCategoryCode,
# MAGIC meterCategory,
# MAGIC meterReadingType,
# MAGIC meterDescription,
# MAGIC manufacturerName,
# MAGIC manufacturerSerialNumber,
# MAGIC manufacturerModelNumber,
# MAGIC measurementUnit,
# MAGIC latestActivityReasonCode,
# MAGIC latestActivityReason,
# MAGIC inspectionRelevanceFlag,
# MAGIC registerNumber,
# MAGIC registerGroupCode,
# MAGIC registerGroup,
# MAGIC registerTypeCode,
# MAGIC registerType,
# MAGIC registerCategoryCode,
# MAGIC registerCategory,
# MAGIC registerIdCode,
# MAGIC registerId,
# MAGIC divisionCategoryCode,
# MAGIC divisionCategory
# MAGIC from curated.dimmeter
# MAGIC 
# MAGIC except 
# MAGIC 
# MAGIC (select * from (
# MAGIC select sourceSystemCode,
# MAGIC meterNumber,
# MAGIC meterSerialNumber,
# MAGIC logicalDeviceNumber,
# MAGIC materialNumber,
# MAGIC usageDeviceType,
# MAGIC meterSize,
# MAGIC waterType,
# MAGIC meterCategoryCode,
# MAGIC meterCategory,
# MAGIC meterReadingType,
# MAGIC meterDescription,
# MAGIC manufacturerName,
# MAGIC manufacturerSerialNumber,
# MAGIC manufacturerModelNumber,
# MAGIC measurementUnit,
# MAGIC latestActivityReasonCode,
# MAGIC latestActivityReason,
# MAGIC inspectionRelevanceFlag,
# MAGIC registerNumber,
# MAGIC registerGroupCode,
# MAGIC registerGroup,
# MAGIC registerTypeCode,
# MAGIC registerType,
# MAGIC registerCategoryCode,
# MAGIC registerCategory,
# MAGIC registerIdCode,
# MAGIC registerId,
# MAGIC divisionCategoryCode,
# MAGIC divisionCategory
# MAGIC from (
# MAGIC select
# MAGIC sourceSystemCode,
# MAGIC meterNumber,
# MAGIC meterSerialNumber,
# MAGIC logicalDeviceNumber,
# MAGIC materialNumber,
# MAGIC usageDeviceType,
# MAGIC meterSize,
# MAGIC waterType,
# MAGIC meterCategoryCode,
# MAGIC meterCategory,
# MAGIC meterReadingType,
# MAGIC meterDescription,
# MAGIC manufacturerName,
# MAGIC manufacturerSerialNumber,
# MAGIC manufacturerModelNumber,
# MAGIC measurementUnit,
# MAGIC latestActivityReasonCode,
# MAGIC latestActivityReason,
# MAGIC inspectionRelevanceFlag,
# MAGIC registerNumber,
# MAGIC registerGroupCode,
# MAGIC registerGroup,
# MAGIC registerTypeCode,
# MAGIC registerType,
# MAGIC registerCategoryCode,
# MAGIC registerCategory,
# MAGIC registerIdCode,
# MAGIC registerId,
# MAGIC divisionCategoryCode,
# MAGIC divisionCategory,
# MAGIC row_number () over (partition by meterMakerNumber order by meterFittedDate desc) as rn
# MAGIC from (
# MAGIC select 
# MAGIC 'Access' as sourceSystemCode
# MAGIC ,'NULL' as meterNumber
# MAGIC ,meterMakerNumber as meterSerialNumber 
# MAGIC ,'NULL' as logicalDeviceNumber
# MAGIC ,'NULL' as materialNumber
# MAGIC ,'NULL' as usageDeviceType
# MAGIC ,meterSize
# MAGIC ,waterMeterType  as waterType
# MAGIC ,meterMakerNumber
# MAGIC ,meterFittedDate
# MAGIC ,'NULL' as meterCategoryCode
# MAGIC ,'NULL' as meterCategory
# MAGIC ,'NULL' as meterReadingType
# MAGIC ,'NULL' as meterDescription
# MAGIC ,'NULL' as manufacturerName
# MAGIC ,'NULL' as manufacturerSerialNumber
# MAGIC ,'NULL' as manufacturerModelNumber
# MAGIC ,'NULL' as measurementUnit
# MAGIC ,'NULL' as latestActivityReasonCode
# MAGIC ,'NULL' as latestActivityReason
# MAGIC ,'NULL' as inspectionRelevanceFlag
# MAGIC ,'NULL' as registerNumber
# MAGIC ,'NULL' as registerGroupCode
# MAGIC ,'NULL' as registerGroup
# MAGIC ,'NULL' as registerTypeCode
# MAGIC ,'NULL' as registerType
# MAGIC ,'NULL' as registerCategoryCode
# MAGIC ,'NULL' as registerCategory
# MAGIC ,'NULL' as registerIdCode
# MAGIC ,'NULL' as registerId
# MAGIC ,'NULL' as divisionCategoryCode
# MAGIC ,'NULL' as divisionCategory
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
# MAGIC ,c.logicalDeviceNumber as logicalDeviceNumber
# MAGIC ,a.materialNumber as materialNumber
# MAGIC ,case
# MAGIC when b.functionClassCode = 9000 then "Customer Standpipe"
# MAGIC else b.functionClassCode end as usageDeviceType
# MAGIC ,concat(a.deviceSize,'mm') as meterSize
# MAGIC ,case
# MAGIC when b.functionClassCode in(1000,2000) then functionClass 
# MAGIC else null end as waterType
# MAGIC ,b.constructionClassCode as meterCategoryCode
# MAGIC ,b.constructionClass as meterCategory
# MAGIC ,b.deviceCategoryName as meterReadingType
# MAGIC ,b.deviceCategoryDescription as meterDescription
# MAGIC ,a.assetManufacturerName as manufacturerName
# MAGIC ,a.manufacturerSerialNumber as manufacturerSerialNumber
# MAGIC ,a.manufacturerModelNumber as manufacturerModelNumber
# MAGIC ,d.unitOfMeasurementMeterReading as measurementUnit
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
# MAGIC left join cleansed.isu_0UC_REGIST_ATTR d
# MAGIC on d.equipmentNumber = a.equipmentNumber 
# MAGIC and current_date between d.validFromDate and d.validToDate
# MAGIC 
# MAGIC union all
# MAGIC 
# MAGIC select * from(
# MAGIC select 
# MAGIC 'ACCESS' as sourceSystemCode,
# MAGIC '-1' as meterNumber,
# MAGIC 'Unknown' as meterSerialNumber,
# MAGIC 'Unknown' as logicalDeviceNumber,
# MAGIC 'Unknown' as materialNumber,
# MAGIC 'Unknown' as usageDeviceType,
# MAGIC 'Unknown' as meterSize,
# MAGIC 'Unknown' as waterType,
# MAGIC 'Unknown' as meterCategoryCode,
# MAGIC 'Unknown' as meterCategory,
# MAGIC 'Unknown' as meterReadingType,
# MAGIC 'Unknown' as meterDescription,
# MAGIC 'Unknown' as manufacturerName,
# MAGIC 'Unknown' as manufacturerSerialNumber,
# MAGIC 'Unknown' as manufacturerModelNumber,
# MAGIC 'Unknown' as measurementUnit,
# MAGIC 'Unknown' as latestActivityReasonCode,
# MAGIC 'Unknown' as latestActivityReason,
# MAGIC 'Unknown' as inspectionRelevanceFlag,
# MAGIC 'Unknown' as registerNumber,
# MAGIC 'Unknown' as registerGroupCode,
# MAGIC 'Unknown' as registerGroup,
# MAGIC 'Unknown' as registerTypeCode,
# MAGIC 'Unknown' as registerType,
# MAGIC 'Unknown' as registerCategoryCode,
# MAGIC 'Unknown' as registerCategory,
# MAGIC 'Unknown' as registerIdCode,
# MAGIC 'Unknown' as registerId,
# MAGIC 'Unknown' as divisionCategoryCode,
# MAGIC 'Unknown' as divisionCategory
# MAGIC from Access limit 1)a
# MAGIC 
# MAGIC union all
# MAGIC 
# MAGIC select * from (
# MAGIC select 
# MAGIC 'ISU' as sourceSystemCode,
# MAGIC '-1' as meterNumber,
# MAGIC 'Unknown' as meterSerialNumber,
# MAGIC 'Unknown' as logicalDeviceNumber,
# MAGIC 'Unknown' as materialNumber,
# MAGIC 'Unknown' as usageDeviceType,
# MAGIC 'Unknown' as meterSize,
# MAGIC 'Unknown' as waterType,
# MAGIC 'Unknown' as meterCategoryCode,
# MAGIC 'Unknown' as meterCategory,
# MAGIC 'Unknown' as meterReadingType,
# MAGIC 'Unknown' as meterDescription,
# MAGIC 'Unknown' as manufacturerName,
# MAGIC 'Unknown' as manufacturerSerialNumber,
# MAGIC 'Unknown' as manufacturerModelNumber,
# MAGIC 'Unknown' as measurementUnit,
# MAGIC 'Unknown' as latestActivityReasonCode,
# MAGIC 'Unknown' as latestActivityReason,
# MAGIC 'Unknown' as inspectionRelevanceFlag,
# MAGIC 'Unknown' as registerNumber,
# MAGIC 'Unknown' as registerGroupCode,
# MAGIC 'Unknown' as registerGroup,
# MAGIC 'Unknown' as registerTypeCode,
# MAGIC 'Unknown' as registerType,
# MAGIC 'Unknown' as registerCategoryCode,
# MAGIC 'Unknown' as registerCategory,
# MAGIC 'Unknown' as registerIdCode,
# MAGIC 'Unknown' as registerId,
# MAGIC 'Unknown' as divisionCategoryCode,
# MAGIC 'Unknown' as divisionCategory
# MAGIC from Access limit 1)b)
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
