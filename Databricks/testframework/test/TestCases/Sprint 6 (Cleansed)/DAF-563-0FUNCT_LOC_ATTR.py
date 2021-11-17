# Databricks notebook source
# DBTITLE 1,[Config] Connection Setup
storage_account_name = "saswcnonprod01landingtst"
storage_account_access_key = dbutils.secrets.get(scope="Test-Access",key="test-blob-key")
container_name = "archive"
file_location = "wasbs://archive@saswcnonprod01landingtst.blob.core.windows.net/sapisu/20210824/20210824_16:32:57/0FUNCT_LOC_ATTR_20210818114957.json"
file_location2 = "wasbs://archive@saswcnonprod01landingtst.blob.core.windows.net/sapisu/20210824/20210824_16:32:57/0FUNCT_LOC_ATTR_20210818141133.json"
file_location3 = "wasbs://archive@saswcnonprod01landingtst.blob.core.windows.net/sapisu/20210825/20210825_15:14:40/0FUNCT_LOC_ATTR_20210825105230.json"  
file_location4 = "wasbs://archive@saswcnonprod01landingtst.blob.core.windows.net/sapisu/20210901/20210901_13:17:34/0FUNCT_LOC_ATTR_20210831114358.json"
file_location5 = "wasbs://archive@saswcnonprod01landingtst.blob.core.windows.net/sapisu/20210901/20210901_13:17:34/0FUNCT_LOC_ATTR_20210831132742.json"
file_location6 = "wasbs://archive@saswcnonprod01landingtst.blob.core.windows.net/sapisu/20210916/20210916_16:55:08/0FUNCT_LOC_ATTR_20210904145700.json"
file_type = "json"
print(storage_account_name)

# COMMAND ----------

spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
  storage_account_access_key)


# COMMAND ----------

# DBTITLE 1,[Source] Loading data into Dataframe
df = spark.read.format(file_type).option("inferSchema", "true").load(file_location)
df2 = spark.read.format(file_type).option("inferSchema", "true").load(file_location2)
df3 = spark.read.format(file_type).option("inferSchema", "true").load(file_location3)
df4 = spark.read.format(file_type).option("inferSchema", "true").load(file_location4)
df5 = spark.read.format(file_type).option("inferSchema", "true").load(file_location5)
df6 = spark.read.format(file_type).option("inferSchema", "true").load(file_location6)

# COMMAND ----------

# DBTITLE 1,[Source] Schema Check - Refer to Raw2Cleansed Mapping
df.printSchema()
df2.printSchema()
df3.printSchema()
df4.printSchema()
df5.printSchema()
df6.printSchema()

# COMMAND ----------

# DBTITLE 0,[Result] Load Count Result into DataFrame
lakedf = spark.sql("select * from cleansed.t_sapisu_0funct_loc_attr")

# COMMAND ----------

display(lakedf)

# COMMAND ----------

# DBTITLE 1,[Target] Schema Check
lakedf.printSchema()

# COMMAND ----------

# DBTITLE 1,[Source] Creating Temporary Table
df.createOrReplaceTempView("Source1")
df2.createOrReplaceTempView("Source2")
df3.createOrReplaceTempView("Source3")
df4.createOrReplaceTempView("Source4")
df5.createOrReplaceTempView("Source5")
df6.createOrReplaceTempView("Source6")

# COMMAND ----------

df = spark.sql("select * from Source1")
df2 = spark.sql("select * from Source2")
df3 = spark.sql("select * from Source3")
df4 = spark.sql("select * from Source4")
df5 = spark.sql("select * from Source5")
df6 = spark.sql("select * from Source6")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from source4

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct workBreakdownStructureElement from cleansed.t_sapisu_0funct_loc_attr

# COMMAND ----------

# DBTITLE 1,[Source] with mapping
# MAGIC %sql
# MAGIC select functionalLocationNumber
# MAGIC ,functionalLocationCategory
# MAGIC ,maintenancePlanningPlant
# MAGIC ,maintenancePlant
# MAGIC ,addressNumber
# MAGIC ,controllingArea
# MAGIC ,companyCode
# MAGIC ,companyName
# MAGIC ,workBreakdownStructureElement
# MAGIC ,createdDate
# MAGIC ,lastChangedDate
# MAGIC ,architecturalObjectCount
# MAGIC ,zzaddressNumber
# MAGIC ,objectReferenceIndicator
# MAGIC ,premiseId
# MAGIC ,installationId
# MAGIC ,contractAccountNumber
# MAGIC ,alternativeAddressNumber
# MAGIC ,objectNumber
# MAGIC ,identificationNumber
# MAGIC ,businessPartnerNumber
# MAGIC ,connectionObjectId
# MAGIC ,locationDescription
# MAGIC ,buildingNumber
# MAGIC ,floorNumber
# MAGIC ,houseNumber2
# MAGIC ,houseNumber3
# MAGIC ,houseNumber1
# MAGIC ,streetName
# MAGIC ,streetLine1
# MAGIC ,streetLine2
# MAGIC ,cityName
# MAGIC ,stateCode
# MAGIC ,postCode
# MAGIC ,locationDescriptionSecondary
# MAGIC ,buildingNumberSecondary
# MAGIC ,floorNumberSecondary
# MAGIC ,houseNumber2Secondary
# MAGIC ,houseNumber3Secondary
# MAGIC ,houseNumber1Secondary
# MAGIC ,streetNameSecondary
# MAGIC ,streetLine1Secondary
# MAGIC ,streetLine2Secondary
# MAGIC ,cityNameSecondary
# MAGIC ,stateCodeSecondary
# MAGIC ,postCodeSecondary
# MAGIC ,buildingFeeDate from (
# MAGIC select
# MAGIC functionalLocationNumber
# MAGIC ,functionalLocationCategory
# MAGIC ,maintenancePlanningPlant
# MAGIC ,maintenancePlant
# MAGIC ,addressNumber
# MAGIC ,controllingArea
# MAGIC ,a.companyCode
# MAGIC ,b.companyName as companyName
# MAGIC ,workBreakdownStructureElement
# MAGIC ,createdDate
# MAGIC ,lastChangedDate
# MAGIC ,architecturalObjectCount
# MAGIC ,zzaddressNumber
# MAGIC ,objectReferenceIndicator
# MAGIC ,premiseId
# MAGIC ,installationId
# MAGIC ,contractAccountNumber
# MAGIC ,alternativeAddressNumber
# MAGIC ,objectNumber
# MAGIC ,identificationNumber
# MAGIC ,businessPartnerNumber
# MAGIC ,connectionObjectId
# MAGIC ,locationDescription
# MAGIC ,buildingNumber
# MAGIC ,floorNumber
# MAGIC ,houseNumber2
# MAGIC ,houseNumber3
# MAGIC ,houseNumber1
# MAGIC ,streetName
# MAGIC ,streetLine1
# MAGIC ,streetLine2
# MAGIC ,cityName
# MAGIC ,stateCode
# MAGIC ,postCode
# MAGIC ,locationDescriptionSecondary
# MAGIC ,buildingNumberSecondary
# MAGIC ,floorNumberSecondary
# MAGIC ,houseNumber2Secondary
# MAGIC ,houseNumber3Secondary
# MAGIC ,houseNumber1Secondary
# MAGIC ,streetNameSecondary
# MAGIC ,streetLine1Secondary
# MAGIC ,streetLine2Secondary
# MAGIC ,cityNameSecondary
# MAGIC ,stateCodeSecondary
# MAGIC ,postCodeSecondary
# MAGIC ,buildingFeeDate
# MAGIC ,row_number () over(partition by functionalLocationNumber order by fileload desc) as rn
# MAGIC from(
# MAGIC select 
# MAGIC TPLNR as functionalLocationNumber
# MAGIC ,FLTYP as functionalLocationCategory
# MAGIC ,IWERK as maintenancePlanningPlant
# MAGIC ,SWERK as maintenancePlant
# MAGIC ,ADRNR as addressNumber
# MAGIC ,KOKRS as controllingArea
# MAGIC ,BUKRS as companyCode
# MAGIC --,b.companyName as companyName
# MAGIC ,PROID as workBreakdownStructureElement
# MAGIC ,ERDAT as createdDate
# MAGIC ,AEDAT as lastChangedDate
# MAGIC --,LGWID as workCenterObjectId
# MAGIC --,PPSID as ppWorkCenterObjectId
# MAGIC --,ALKEY as labelingSystem
# MAGIC --,STRNO as functionalLocationLabel
# MAGIC --,LAM_START as startPoint
# MAGIC --,LAM_END as endPoint
# MAGIC --,LINEAR_LENGTH as linearLength
# MAGIC --,LINEAR_UNIT as unitOfMeasurement
# MAGIC ,ZZ_ZCD_AONR as architecturalObjectCount
# MAGIC ,ZZ_ADRNR as zzaddressNumber
# MAGIC ,ZZ_OWNER as objectReferenceIndicator
# MAGIC ,ZZ_VSTELLE as premiseId
# MAGIC ,ZZ_ANLAGE as installationId
# MAGIC ,ZZ_VKONTO as contractAccountNumber
# MAGIC ,ZZADRMA as alternativeAddressNumber
# MAGIC ,ZZ_OBJNR as objectNumber
# MAGIC ,ZZ_IDNUMBER as identificationNumber
# MAGIC ,ZZ_GPART as businessPartnerNumber
# MAGIC ,ZZ_HAUS as connectionObjectId
# MAGIC ,ZZ_LOCATION as locationDescription
# MAGIC ,ZZ_BUILDING as buildingNumber
# MAGIC ,ZZ_FLOOR as floorNumber
# MAGIC ,ZZ_HOUSE_NUM2 as houseNumber2
# MAGIC ,ZZ_HOUSE_NUM3 as houseNumber3
# MAGIC ,ZZ_HOUSE_NUM1 as houseNumber1
# MAGIC ,ZZ_STREET as streetName
# MAGIC ,ZZ_STR_SUPPL1 as streetLine1
# MAGIC ,ZZ_STR_SUPPL2 as streetLine2
# MAGIC ,ZZ_CITY1 as cityName
# MAGIC ,ZZ_REGION as stateCode
# MAGIC ,ZZ_POST_CODE1 as postCode
# MAGIC ,ZZZ_LOCATION as locationDescriptionSecondary
# MAGIC ,ZZZ_BUILDING as buildingNumberSecondary
# MAGIC ,ZZZ_FLOOR as floorNumberSecondary
# MAGIC ,ZZZ_HOUSE_NUM2 as houseNumber2Secondary
# MAGIC ,ZZZ_HOUSE_NUM3 as houseNumber3Secondary
# MAGIC ,ZZZ_HOUSE_NUM1 as houseNumber1Secondary
# MAGIC ,ZZZ_STREET as streetNameSecondary
# MAGIC ,ZZZ_STR_SUPPL1 as streetLine1Secondary
# MAGIC ,ZZZ_STR_SUPPL2 as streetLine2Secondary
# MAGIC ,ZZZ_CITY1 as cityNameSecondary
# MAGIC ,ZZZ_REGION as stateCodeSecondary
# MAGIC ,ZZZ_POST_CODE1 as postCodeSecondary
# MAGIC ,ZCD_BLD_FEE_DATE as buildingFeeDate
# MAGIC ,fileload
# MAGIC from (select *,'20210901' as fileload  from source4
# MAGIC union all
# MAGIC select *,'20210916' as fileload from source6))a
# MAGIC 
# MAGIC left join cleansed.t_sapisu_0COMP_CODE_TEXT b
# MAGIC on a.companyCode = b.companyCode)
# MAGIC where rn = 1
# MAGIC 
# MAGIC --where TPLNR = '6206880'

# COMMAND ----------

# MAGIC %sql
# MAGIC select createdDate, lastChangedDate from cleansed.t_sapisu_0funct_loc_attr

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select distinct ZZ_HOUSE_NUM2,ZZ_HOUSE_NUM1,ZZ_HOUSE_NUM3,zz_street,ZZZ_HOUSE_NUM1,ZZZ_HOUSE_NUM2,ZZZ_HOUSE_NUM3,ZZZ_STREET,ZZ_HAUS from (select *,'20210901' as fileload  from source4
# MAGIC union all
# MAGIC select *,'20210916' as fileload from source6)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from source4
# MAGIC union all
# MAGIC select * from source6

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cleansed.t_sapisu_0funct_loc_attr where functionallocationnumber = '6206880'

# COMMAND ----------

lakedf.createOrReplaceTempView("Target")

# COMMAND ----------

# DBTITLE 1,[Verification] Count Checks
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from cleansed.t_sapisu_0funct_loc_attr
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from (
# MAGIC select functionalLocationNumber
# MAGIC ,functionalLocationCategory
# MAGIC ,maintenancePlanningPlant
# MAGIC ,maintenancePlant
# MAGIC ,addressNumber
# MAGIC ,controllingArea
# MAGIC ,companyCode
# MAGIC ,companyName
# MAGIC ,workBreakdownStructureElement
# MAGIC ,createdDate
# MAGIC ,lastChangedDate
# MAGIC ,architecturalObjectCount
# MAGIC ,zzaddressNumber
# MAGIC ,objectReferenceIndicator
# MAGIC ,premiseId
# MAGIC ,installationId
# MAGIC ,contractAccountNumber
# MAGIC ,alternativeAddressNumber
# MAGIC ,objectNumber
# MAGIC ,identificationNumber
# MAGIC ,businessPartnerNumber
# MAGIC ,connectionObjectId
# MAGIC ,locationDescription
# MAGIC ,buildingNumber
# MAGIC ,floorNumber
# MAGIC ,houseNumber2
# MAGIC ,houseNumber3
# MAGIC ,houseNumber1
# MAGIC ,streetName
# MAGIC ,streetLine1
# MAGIC ,streetLine2
# MAGIC ,cityName
# MAGIC ,stateCode
# MAGIC ,postCode
# MAGIC ,locationDescriptionSecondary
# MAGIC ,buildingNumberSecondary
# MAGIC ,floorNumberSecondary
# MAGIC ,houseNumber2Secondary
# MAGIC ,houseNumber3Secondary
# MAGIC ,houseNumber1Secondary
# MAGIC ,streetNameSecondary
# MAGIC ,streetLine1Secondary
# MAGIC ,streetLine2Secondary
# MAGIC ,cityNameSecondary
# MAGIC ,stateCodeSecondary
# MAGIC ,postCodeSecondary
# MAGIC ,buildingFeeDate from (
# MAGIC select
# MAGIC functionalLocationNumber
# MAGIC ,functionalLocationCategory
# MAGIC ,maintenancePlanningPlant
# MAGIC ,maintenancePlant
# MAGIC ,addressNumber
# MAGIC ,controllingArea
# MAGIC ,a.companyCode
# MAGIC ,b.companyName as companyName
# MAGIC ,workBreakdownStructureElement
# MAGIC ,createdDate
# MAGIC ,lastChangedDate
# MAGIC ,architecturalObjectCount
# MAGIC ,zzaddressNumber
# MAGIC ,objectReferenceIndicator
# MAGIC ,premiseId
# MAGIC ,installationId
# MAGIC ,contractAccountNumber
# MAGIC ,alternativeAddressNumber
# MAGIC ,objectNumber
# MAGIC ,identificationNumber
# MAGIC ,businessPartnerNumber
# MAGIC ,connectionObjectId
# MAGIC ,locationDescription
# MAGIC ,buildingNumber
# MAGIC ,floorNumber
# MAGIC ,houseNumber2
# MAGIC ,houseNumber3
# MAGIC ,houseNumber1
# MAGIC ,streetName
# MAGIC ,streetLine1
# MAGIC ,streetLine2
# MAGIC ,cityName
# MAGIC ,stateCode
# MAGIC ,postCode
# MAGIC ,locationDescriptionSecondary
# MAGIC ,buildingNumberSecondary
# MAGIC ,floorNumberSecondary
# MAGIC ,houseNumber2Secondary
# MAGIC ,houseNumber3Secondary
# MAGIC ,houseNumber1Secondary
# MAGIC ,streetNameSecondary
# MAGIC ,streetLine1Secondary
# MAGIC ,streetLine2Secondary
# MAGIC ,cityNameSecondary
# MAGIC ,stateCodeSecondary
# MAGIC ,postCodeSecondary
# MAGIC ,buildingFeeDate
# MAGIC ,row_number () over(partition by functionalLocationNumber order by fileload desc) as rn
# MAGIC from(
# MAGIC select 
# MAGIC TPLNR as functionalLocationNumber
# MAGIC ,FLTYP as functionalLocationCategory
# MAGIC ,IWERK as maintenancePlanningPlant
# MAGIC ,SWERK as maintenancePlant
# MAGIC ,ADRNR as addressNumber
# MAGIC ,KOKRS as controllingArea
# MAGIC ,BUKRS as companyCode
# MAGIC --,b.companyName as companyName
# MAGIC ,PROID as workBreakdownStructureElement
# MAGIC ,ERDAT as createdDate
# MAGIC ,AEDAT as lastChangedDate
# MAGIC --,LGWID as workCenterObjectId
# MAGIC --,PPSID as ppWorkCenterObjectId
# MAGIC --,ALKEY as labelingSystem
# MAGIC --,STRNO as functionalLocationLabel
# MAGIC --,LAM_START as startPoint
# MAGIC --,LAM_END as endPoint
# MAGIC --,LINEAR_LENGTH as linearLength
# MAGIC --,LINEAR_UNIT as unitOfMeasurement
# MAGIC ,ZZ_ZCD_AONR as architecturalObjectCount
# MAGIC ,ZZ_ADRNR as zzaddressNumber
# MAGIC ,ZZ_OWNER as objectReferenceIndicator
# MAGIC ,ZZ_VSTELLE as premiseId
# MAGIC ,ZZ_ANLAGE as installationId
# MAGIC ,ZZ_VKONTO as contractAccountNumber
# MAGIC ,ZZADRMA as alternativeAddressNumber
# MAGIC ,ZZ_OBJNR as objectNumber
# MAGIC ,ZZ_IDNUMBER as identificationNumber
# MAGIC ,ZZ_GPART as businessPartnerNumber
# MAGIC ,ZZ_HAUS as connectionObjectId
# MAGIC ,ZZ_LOCATION as locationDescription
# MAGIC ,ZZ_BUILDING as buildingNumber
# MAGIC ,ZZ_FLOOR as floorNumber
# MAGIC ,ZZ_HOUSE_NUM2 as houseNumber2
# MAGIC ,ZZ_HOUSE_NUM3 as houseNumber3
# MAGIC ,ZZ_HOUSE_NUM1 as houseNumber1
# MAGIC ,ZZ_STREET as streetName
# MAGIC ,ZZ_STR_SUPPL1 as streetLine1
# MAGIC ,ZZ_STR_SUPPL2 as streetLine2
# MAGIC ,ZZ_CITY1 as cityName
# MAGIC ,ZZ_REGION as stateCode
# MAGIC ,ZZ_POST_CODE1 as postCode
# MAGIC ,ZZZ_LOCATION as locationDescriptionSecondary
# MAGIC ,ZZZ_BUILDING as buildingNumberSecondary
# MAGIC ,ZZZ_FLOOR as floorNumberSecondary
# MAGIC ,ZZZ_HOUSE_NUM2 as houseNumber2Secondary
# MAGIC ,ZZZ_HOUSE_NUM3 as houseNumber3Secondary
# MAGIC ,ZZZ_HOUSE_NUM1 as houseNumber1Secondary
# MAGIC ,ZZZ_STREET as streetNameSecondary
# MAGIC ,ZZZ_STR_SUPPL1 as streetLine1Secondary
# MAGIC ,ZZZ_STR_SUPPL2 as streetLine2Secondary
# MAGIC ,ZZZ_CITY1 as cityNameSecondary
# MAGIC ,ZZZ_REGION as stateCodeSecondary
# MAGIC ,ZZZ_POST_CODE1 as postCodeSecondary
# MAGIC ,ZCD_BLD_FEE_DATE as buildingFeeDate
# MAGIC ,fileload
# MAGIC from (select *,'20210901' as fileload  from source4
# MAGIC union all
# MAGIC select *,'20210916' as fileload from source6))a
# MAGIC 
# MAGIC left join cleansed.t_sapisu_0COMP_CODE_TEXT b
# MAGIC on a.companyCode = b.companyCode)
# MAGIC where rn = 1)

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT functionalLocationNumber, COUNT (*) as count
# MAGIC FROM cleansed.t_sapisu_0funct_loc_attr
# MAGIC GROUP BY functionalLocationNumber
# MAGIC HAVING COUNT (*) > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT * FROM (
# MAGIC SELECT
# MAGIC *,
# MAGIC row_number() OVER(PARTITION BY functionalLocationNumber order by functionalLocationNumber) as rn
# MAGIC FROM cleansed.t_sapisu_0funct_loc_attr
# MAGIC )a where a.rn > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Source and Target Data
# MAGIC %sql
# MAGIC 
# MAGIC 
# MAGIC select functionalLocationNumber
# MAGIC ,functionalLocationCategory
# MAGIC ,maintenancePlanningPlant
# MAGIC ,maintenancePlant
# MAGIC ,addressNumber
# MAGIC ,controllingArea
# MAGIC ,companyCode
# MAGIC ,companyName
# MAGIC ,workBreakdownStructureElement
# MAGIC ,createdDate
# MAGIC ,lastChangedDate
# MAGIC ,architecturalObjectCount
# MAGIC ,zzaddressNumber
# MAGIC ,objectReferenceIndicator
# MAGIC ,premiseId
# MAGIC ,installationId
# MAGIC ,contractAccountNumber
# MAGIC ,alternativeAddressNumber
# MAGIC ,objectNumber
# MAGIC ,identificationNumber
# MAGIC ,businessPartnerNumber
# MAGIC ,connectionObjectId
# MAGIC ,locationDescription
# MAGIC ,buildingNumber
# MAGIC ,floorNumber
# MAGIC ,houseNumber2
# MAGIC ,houseNumber3
# MAGIC ,houseNumber1
# MAGIC ,streetName
# MAGIC ,streetLine1
# MAGIC ,streetLine2
# MAGIC ,cityName
# MAGIC ,stateCode
# MAGIC ,postCode
# MAGIC ,locationDescriptionSecondary
# MAGIC ,buildingNumberSecondary
# MAGIC ,floorNumberSecondary
# MAGIC ,houseNumber2Secondary
# MAGIC ,houseNumber3Secondary
# MAGIC ,houseNumber1Secondary
# MAGIC ,streetNameSecondary
# MAGIC ,streetLine1Secondary
# MAGIC ,streetLine2Secondary
# MAGIC ,cityNameSecondary
# MAGIC ,stateCodeSecondary
# MAGIC ,postCodeSecondary
# MAGIC ,buildingFeeDate from (
# MAGIC select
# MAGIC functionalLocationNumber
# MAGIC ,functionalLocationCategory
# MAGIC ,maintenancePlanningPlant
# MAGIC ,maintenancePlant
# MAGIC ,addressNumber
# MAGIC ,controllingArea
# MAGIC ,a.companyCode
# MAGIC ,b.companyName as companyName
# MAGIC ,workBreakdownStructureElement
# MAGIC ,createdDate
# MAGIC ,lastChangedDate
# MAGIC ,architecturalObjectCount
# MAGIC ,zzaddressNumber
# MAGIC ,objectReferenceIndicator
# MAGIC ,premiseId
# MAGIC ,installationId
# MAGIC ,contractAccountNumber
# MAGIC ,alternativeAddressNumber
# MAGIC ,objectNumber
# MAGIC ,identificationNumber
# MAGIC ,businessPartnerNumber
# MAGIC ,connectionObjectId
# MAGIC ,locationDescription
# MAGIC ,buildingNumber
# MAGIC ,floorNumber
# MAGIC ,houseNumber2
# MAGIC ,houseNumber3
# MAGIC ,houseNumber1
# MAGIC ,streetName
# MAGIC ,streetLine1
# MAGIC ,streetLine2
# MAGIC ,cityName
# MAGIC ,stateCode
# MAGIC ,postCode
# MAGIC ,locationDescriptionSecondary
# MAGIC ,buildingNumberSecondary
# MAGIC ,floorNumberSecondary
# MAGIC ,houseNumber2Secondary
# MAGIC ,houseNumber3Secondary
# MAGIC ,houseNumber1Secondary
# MAGIC ,streetNameSecondary
# MAGIC ,streetLine1Secondary
# MAGIC ,streetLine2Secondary
# MAGIC ,cityNameSecondary
# MAGIC ,stateCodeSecondary
# MAGIC ,postCodeSecondary
# MAGIC ,buildingFeeDate
# MAGIC ,row_number () over(partition by functionalLocationNumber order by fileload desc) as rn
# MAGIC from(
# MAGIC select 
# MAGIC TPLNR as functionalLocationNumber
# MAGIC ,FLTYP as functionalLocationCategory
# MAGIC ,IWERK as maintenancePlanningPlant
# MAGIC ,SWERK as maintenancePlant
# MAGIC ,ADRNR as addressNumber
# MAGIC ,KOKRS as controllingArea
# MAGIC ,BUKRS as companyCode
# MAGIC --,b.companyName as companyName
# MAGIC ,PROID as workBreakdownStructureElement
# MAGIC ,ERDAT as createdDate
# MAGIC ,AEDAT as lastChangedDate
# MAGIC --,LGWID as workCenterObjectId
# MAGIC --,PPSID as ppWorkCenterObjectId
# MAGIC --,ALKEY as labelingSystem
# MAGIC --,STRNO as functionalLocationLabel
# MAGIC --,LAM_START as startPoint
# MAGIC --,LAM_END as endPoint
# MAGIC --,LINEAR_LENGTH as linearLength
# MAGIC --,LINEAR_UNIT as unitOfMeasurement
# MAGIC ,ZZ_ZCD_AONR as architecturalObjectCount
# MAGIC ,ZZ_ADRNR as zzaddressNumber
# MAGIC ,ZZ_OWNER as objectReferenceIndicator
# MAGIC ,ZZ_VSTELLE as premiseId
# MAGIC ,ZZ_ANLAGE as installationId
# MAGIC ,ZZ_VKONTO as contractAccountNumber
# MAGIC ,ZZADRMA as alternativeAddressNumber
# MAGIC ,ZZ_OBJNR as objectNumber
# MAGIC ,ZZ_IDNUMBER as identificationNumber
# MAGIC ,ZZ_GPART as businessPartnerNumber
# MAGIC ,ZZ_HAUS as connectionObjectId
# MAGIC ,ZZ_LOCATION as locationDescription
# MAGIC ,ZZ_BUILDING as buildingNumber
# MAGIC ,ZZ_FLOOR as floorNumber
# MAGIC ,ZZ_HOUSE_NUM2 as houseNumber2
# MAGIC ,ZZ_HOUSE_NUM3 as houseNumber3
# MAGIC ,ZZ_HOUSE_NUM1 as houseNumber1
# MAGIC ,ZZ_STREET as streetName
# MAGIC ,ZZ_STR_SUPPL1 as streetLine1
# MAGIC ,ZZ_STR_SUPPL2 as streetLine2
# MAGIC ,ZZ_CITY1 as cityName
# MAGIC ,ZZ_REGION as stateCode
# MAGIC ,ZZ_POST_CODE1 as postCode
# MAGIC ,ZZZ_LOCATION as locationDescriptionSecondary
# MAGIC ,ZZZ_BUILDING as buildingNumberSecondary
# MAGIC ,ZZZ_FLOOR as floorNumberSecondary
# MAGIC ,ZZZ_HOUSE_NUM2 as houseNumber2Secondary
# MAGIC ,ZZZ_HOUSE_NUM3 as houseNumber3Secondary
# MAGIC ,ZZZ_HOUSE_NUM1 as houseNumber1Secondary
# MAGIC ,ZZZ_STREET as streetNameSecondary
# MAGIC ,ZZZ_STR_SUPPL1 as streetLine1Secondary
# MAGIC ,ZZZ_STR_SUPPL2 as streetLine2Secondary
# MAGIC ,ZZZ_CITY1 as cityNameSecondary
# MAGIC ,ZZZ_REGION as stateCodeSecondary
# MAGIC ,ZZZ_POST_CODE1 as postCodeSecondary
# MAGIC ,ZCD_BLD_FEE_DATE as buildingFeeDate
# MAGIC ,fileload
# MAGIC from (select *,'20210901' as fileload  from source4
# MAGIC union all
# MAGIC select *,'20210916' as fileload from source6))a
# MAGIC 
# MAGIC left join cleansed.t_sapisu_0COMP_CODE_TEXT b
# MAGIC on a.companyCode = b.companyCode)
# MAGIC where rn = 1
# MAGIC 
# MAGIC EXCEPT
# MAGIC select
# MAGIC functionalLocationNumber
# MAGIC ,functionalLocationCategory
# MAGIC ,maintenancePlanningPlant
# MAGIC ,maintenancePlant
# MAGIC ,addressNumber
# MAGIC ,controllingArea
# MAGIC ,companyCode
# MAGIC ,companyName
# MAGIC ,workBreakdownStructureElement
# MAGIC ,createdDate
# MAGIC ,lastChangedDate
# MAGIC ,architecturalObjectCount
# MAGIC ,zzaddressNumber
# MAGIC ,objectReferenceIndicator
# MAGIC ,premiseId
# MAGIC ,installationId
# MAGIC ,contractAccountNumber
# MAGIC ,alternativeAddressNumber
# MAGIC ,objectNumber
# MAGIC ,identificationNumber
# MAGIC ,businessPartnerNumber
# MAGIC ,connectionObjectId
# MAGIC ,locationDescription
# MAGIC ,buildingNumber
# MAGIC ,floorNumber
# MAGIC ,houseNumber2
# MAGIC ,houseNumber3
# MAGIC ,houseNumber1
# MAGIC ,streetName
# MAGIC ,streetLine1
# MAGIC ,streetLine2
# MAGIC ,cityName
# MAGIC ,stateCode
# MAGIC ,postCode
# MAGIC ,locationDescriptionSecondary
# MAGIC ,buildingNumberSecondary
# MAGIC ,floorNumberSecondary
# MAGIC ,houseNumber2Secondary
# MAGIC ,houseNumber3Secondary
# MAGIC ,houseNumber1Secondary
# MAGIC ,streetNameSecondary
# MAGIC ,streetLine1Secondary
# MAGIC ,streetLine2Secondary
# MAGIC ,cityNameSecondary
# MAGIC ,stateCodeSecondary
# MAGIC ,postCodeSecondary
# MAGIC ,buildingFeeDate
# MAGIC 
# MAGIC 
# MAGIC FROM
# MAGIC cleansed.t_sapisu_0funct_loc_attr

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Target and Source Data
# MAGIC %sql
# MAGIC select
# MAGIC functionalLocationNumber
# MAGIC ,functionalLocationCategory
# MAGIC ,maintenancePlanningPlant
# MAGIC ,maintenancePlant
# MAGIC ,addressNumber
# MAGIC ,controllingArea
# MAGIC ,companyCode
# MAGIC ,companyName
# MAGIC ,workBreakdownStructureElement
# MAGIC ,createdDate
# MAGIC ,lastChangedDate
# MAGIC ,architecturalObjectCount
# MAGIC ,zzaddressNumber
# MAGIC ,objectReferenceIndicator
# MAGIC ,premiseId
# MAGIC ,installationId
# MAGIC ,contractAccountNumber
# MAGIC ,alternativeAddressNumber
# MAGIC ,objectNumber
# MAGIC ,identificationNumber
# MAGIC ,businessPartnerNumber
# MAGIC ,connectionObjectId
# MAGIC ,locationDescription
# MAGIC ,buildingNumber
# MAGIC ,floorNumber
# MAGIC ,houseNumber2
# MAGIC ,houseNumber3
# MAGIC ,houseNumber1
# MAGIC ,streetName
# MAGIC ,streetLine1
# MAGIC ,streetLine2
# MAGIC ,cityName
# MAGIC ,stateCode
# MAGIC ,postCode
# MAGIC ,locationDescriptionSecondary
# MAGIC ,buildingNumberSecondary
# MAGIC ,floorNumberSecondary
# MAGIC ,houseNumber2Secondary
# MAGIC ,houseNumber3Secondary
# MAGIC ,houseNumber1Secondary
# MAGIC ,streetNameSecondary
# MAGIC ,streetLine1Secondary
# MAGIC ,streetLine2Secondary
# MAGIC ,cityNameSecondary
# MAGIC ,stateCodeSecondary
# MAGIC ,postCodeSecondary
# MAGIC ,buildingFeeDate
# MAGIC 
# MAGIC 
# MAGIC FROM
# MAGIC cleansed.t_sapisu_0funct_loc_attr
# MAGIC except
# MAGIC 
# MAGIC select functionalLocationNumber
# MAGIC ,functionalLocationCategory
# MAGIC ,maintenancePlanningPlant
# MAGIC ,maintenancePlant
# MAGIC ,addressNumber
# MAGIC ,controllingArea
# MAGIC ,companyCode
# MAGIC ,companyName
# MAGIC ,workBreakdownStructureElement
# MAGIC ,createdDate
# MAGIC ,lastChangedDate
# MAGIC ,architecturalObjectCount
# MAGIC ,zzaddressNumber
# MAGIC ,objectReferenceIndicator
# MAGIC ,premiseId
# MAGIC ,installationId
# MAGIC ,contractAccountNumber
# MAGIC ,alternativeAddressNumber
# MAGIC ,objectNumber
# MAGIC ,identificationNumber
# MAGIC ,businessPartnerNumber
# MAGIC ,connectionObjectId
# MAGIC ,locationDescription
# MAGIC ,buildingNumber
# MAGIC ,floorNumber
# MAGIC ,houseNumber2
# MAGIC ,houseNumber3
# MAGIC ,houseNumber1
# MAGIC ,streetName
# MAGIC ,streetLine1
# MAGIC ,streetLine2
# MAGIC ,cityName
# MAGIC ,stateCode
# MAGIC ,postCode
# MAGIC ,locationDescriptionSecondary
# MAGIC ,buildingNumberSecondary
# MAGIC ,floorNumberSecondary
# MAGIC ,houseNumber2Secondary
# MAGIC ,houseNumber3Secondary
# MAGIC ,houseNumber1Secondary
# MAGIC ,streetNameSecondary
# MAGIC ,streetLine1Secondary
# MAGIC ,streetLine2Secondary
# MAGIC ,cityNameSecondary
# MAGIC ,stateCodeSecondary
# MAGIC ,postCodeSecondary
# MAGIC ,buildingFeeDate from (
# MAGIC select
# MAGIC functionalLocationNumber
# MAGIC ,functionalLocationCategory
# MAGIC ,maintenancePlanningPlant
# MAGIC ,maintenancePlant
# MAGIC ,addressNumber
# MAGIC ,controllingArea
# MAGIC ,a.companyCode
# MAGIC ,b.companyName as companyName
# MAGIC ,workBreakdownStructureElement
# MAGIC ,createdDate
# MAGIC ,lastChangedDate
# MAGIC ,architecturalObjectCount
# MAGIC ,zzaddressNumber
# MAGIC ,objectReferenceIndicator
# MAGIC ,premiseId
# MAGIC ,installationId
# MAGIC ,contractAccountNumber
# MAGIC ,alternativeAddressNumber
# MAGIC ,objectNumber
# MAGIC ,identificationNumber
# MAGIC ,businessPartnerNumber
# MAGIC ,connectionObjectId
# MAGIC ,locationDescription
# MAGIC ,buildingNumber
# MAGIC ,floorNumber
# MAGIC ,houseNumber2
# MAGIC ,houseNumber3
# MAGIC ,houseNumber1
# MAGIC ,streetName
# MAGIC ,streetLine1
# MAGIC ,streetLine2
# MAGIC ,cityName
# MAGIC ,stateCode
# MAGIC ,postCode
# MAGIC ,locationDescriptionSecondary
# MAGIC ,buildingNumberSecondary
# MAGIC ,floorNumberSecondary
# MAGIC ,houseNumber2Secondary
# MAGIC ,houseNumber3Secondary
# MAGIC ,houseNumber1Secondary
# MAGIC ,streetNameSecondary
# MAGIC ,streetLine1Secondary
# MAGIC ,streetLine2Secondary
# MAGIC ,cityNameSecondary
# MAGIC ,stateCodeSecondary
# MAGIC ,postCodeSecondary
# MAGIC ,buildingFeeDate
# MAGIC ,row_number () over(partition by functionalLocationNumber order by fileload desc) as rn
# MAGIC from(
# MAGIC select 
# MAGIC TPLNR as functionalLocationNumber
# MAGIC ,FLTYP as functionalLocationCategory
# MAGIC ,IWERK as maintenancePlanningPlant
# MAGIC ,SWERK as maintenancePlant
# MAGIC ,ADRNR as addressNumber
# MAGIC ,KOKRS as controllingArea
# MAGIC ,BUKRS as companyCode
# MAGIC --,b.companyName as companyName
# MAGIC ,PROID as workBreakdownStructureElement
# MAGIC ,ERDAT as createdDate
# MAGIC ,AEDAT as lastChangedDate
# MAGIC --,LGWID as workCenterObjectId
# MAGIC --,PPSID as ppWorkCenterObjectId
# MAGIC --,ALKEY as labelingSystem
# MAGIC --,STRNO as functionalLocationLabel
# MAGIC --,LAM_START as startPoint
# MAGIC --,LAM_END as endPoint
# MAGIC --,LINEAR_LENGTH as linearLength
# MAGIC --,LINEAR_UNIT as unitOfMeasurement
# MAGIC ,ZZ_ZCD_AONR as architecturalObjectCount
# MAGIC ,ZZ_ADRNR as zzaddressNumber
# MAGIC ,ZZ_OWNER as objectReferenceIndicator
# MAGIC ,ZZ_VSTELLE as premiseId
# MAGIC ,ZZ_ANLAGE as installationId
# MAGIC ,ZZ_VKONTO as contractAccountNumber
# MAGIC ,ZZADRMA as alternativeAddressNumber
# MAGIC ,ZZ_OBJNR as objectNumber
# MAGIC ,ZZ_IDNUMBER as identificationNumber
# MAGIC ,ZZ_GPART as businessPartnerNumber
# MAGIC ,ZZ_HAUS as connectionObjectId
# MAGIC ,ZZ_LOCATION as locationDescription
# MAGIC ,ZZ_BUILDING as buildingNumber
# MAGIC ,ZZ_FLOOR as floorNumber
# MAGIC ,ZZ_HOUSE_NUM2 as houseNumber2
# MAGIC ,ZZ_HOUSE_NUM3 as houseNumber3
# MAGIC ,ZZ_HOUSE_NUM1 as houseNumber1
# MAGIC ,ZZ_STREET as streetName
# MAGIC ,ZZ_STR_SUPPL1 as streetLine1
# MAGIC ,ZZ_STR_SUPPL2 as streetLine2
# MAGIC ,ZZ_CITY1 as cityName
# MAGIC ,ZZ_REGION as stateCode
# MAGIC ,ZZ_POST_CODE1 as postCode
# MAGIC ,ZZZ_LOCATION as locationDescriptionSecondary
# MAGIC ,ZZZ_BUILDING as buildingNumberSecondary
# MAGIC ,ZZZ_FLOOR as floorNumberSecondary
# MAGIC ,ZZZ_HOUSE_NUM2 as houseNumber2Secondary
# MAGIC ,ZZZ_HOUSE_NUM3 as houseNumber3Secondary
# MAGIC ,ZZZ_HOUSE_NUM1 as houseNumber1Secondary
# MAGIC ,ZZZ_STREET as streetNameSecondary
# MAGIC ,ZZZ_STR_SUPPL1 as streetLine1Secondary
# MAGIC ,ZZZ_STR_SUPPL2 as streetLine2Secondary
# MAGIC ,ZZZ_CITY1 as cityNameSecondary
# MAGIC ,ZZZ_REGION as stateCodeSecondary
# MAGIC ,ZZZ_POST_CODE1 as postCodeSecondary
# MAGIC ,ZCD_BLD_FEE_DATE as buildingFeeDate
# MAGIC ,fileload
# MAGIC from (select *,'20210901' as fileload  from source4
# MAGIC union all
# MAGIC select *,'20210916' as fileload from source6))a
# MAGIC 
# MAGIC left join cleansed.t_sapisu_0COMP_CODE_TEXT b
# MAGIC on a.companyCode = b.companyCode)
# MAGIC where rn = 1
