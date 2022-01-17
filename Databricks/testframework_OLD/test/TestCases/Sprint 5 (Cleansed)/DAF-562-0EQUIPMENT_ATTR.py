# Databricks notebook source
# DBTITLE 1,[Config] Connection Setup
storage_account_name = "saswcnonprod01landingtst"
storage_account_access_key = dbutils.secrets.get(scope="Test-Access",key="test-blob-key")
container_name = "archive"
file_location = "wasbs://archive@saswcnonprod01landingtst.blob.core.windows.net/sapisu/20210908/20210908_12:32:12/0EQUIPMENT_ATTR_20210908094414.json"
#file_location = "wasbs://archive@saswcnonprod01landingtst.blob.core.windows.net/sapisu/20210825/20210825_15:14:40/0EQUIPMENT_ATTR_20210825105230.json"
#file_location2 = "wasbs://archive@saswcnonprod01landingtst.blob.core.windows.net/sapisu/20210901/20210901_11:23:11/0EQUIPMENT_ATTR_20210831132742.json"
#file_location3 = "wasbs://archive@saswcnonprod01landingtst.blob.core.windows.net/sapisu/20210901/20210901_11:23:11/0EQUIPMENT_ATTR_20210831114358.json"  
#file_location4 = "wasbs://archive@saswcnonprod01landingtst.blob.core.windows.net/sapisu/20210907/20210907_11:20:15/0EQUIPMENT_ATTR_20210901145441.json"
#file_location5 = "wasbs://archive@saswcnonprod01landingtst.blob.core.windows.net/sapisu/20210907/20210907_14:13:03/0EQUIPMENT_ATTR_20210904145700.json"
file_type = "json"
print(storage_account_name)

# COMMAND ----------

spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
  storage_account_access_key)


# COMMAND ----------

# DBTITLE 1,[Source] Loading data into Dataframe
df = spark.read.format(file_type).option("inferSchema", "true").load(file_location)
#df2 = spark.read.format(file_type).option("inferSchema", "true").load(file_location2)
#df3 = spark.read.format(file_type).option("inferSchema", "true").load(file_location3)
#df4 = spark.read.format(file_type).option("inferSchema", "true").load(file_location4)
#df5 = spark.read.format(file_type).option("inferSchema", "true").load(file_location5)

# COMMAND ----------

# DBTITLE 1,[Source] Schema Check - Refer to Raw2Cleansed Mapping
df.printSchema()
#df2.printSchema()
#df3.printSchema()
#df4.printSchema()
#df5.printSchema()

# COMMAND ----------

# DBTITLE 0,[Result] Load Count Result into DataFrame
lakedf = spark.sql("select * from cleansed.t_sapisu_0equipment_attr")

# COMMAND ----------

display(lakedf)

# COMMAND ----------

# DBTITLE 1,[Target] Schema Check
lakedf.printSchema()

# COMMAND ----------

# DBTITLE 1,[Source] Creating Temporary Table
df.createOrReplaceTempView("Source1")
#df2.createOrReplaceTempView("Source2")
#df3.createOrReplaceTempView("Source3")
#df4.createOrReplaceTempView("Source4")
#df5.createOrReplaceTempView("Source5")

# COMMAND ----------

df = spark.sql("select * from Source1")
#df2 = spark.sql("select * from Source2")
#df3 = spark.sql("select * from Source3")
#df4 = spark.sql("select * from Source4")
#df5 = spark.sql("select * from Source5")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from source1 --where EQUNR = '000000000000000103'

# COMMAND ----------

# DBTITLE 1,[Source] Mutiple files query
# MAGIC %sql
# MAGIC select * from (
# MAGIC select EQUNR    
# MAGIC ,DATETO   
# MAGIC ,DATEFROM 
# MAGIC ,EQART    
# MAGIC ,INVNR    
# MAGIC ,IWERK    
# MAGIC ,KOKRS    
# MAGIC ,TPLNR    
# MAGIC ,SWERK    
# MAGIC ,ADRNR    
# MAGIC ,BUKRS      
# MAGIC ,MATNR    
# MAGIC ,ANSWT    
# MAGIC ,ANSDT    
# MAGIC ,ERDAT    
# MAGIC ,AEDAT    
# MAGIC ,INBDT    
# MAGIC ,PROID    
# MAGIC ,EQTYP
# MAGIC ,FILEDATE
# MAGIC ,row_number () over(partition by EQUNR, DATETO,DATEFROM order by FILEDATE desc) as rn
# MAGIC from (select * from(
# MAGIC select EQUNR    
# MAGIC ,DATETO   
# MAGIC ,DATEFROM 
# MAGIC ,EQART    
# MAGIC ,INVNR    
# MAGIC ,IWERK    
# MAGIC ,KOKRS    
# MAGIC ,TPLNR    
# MAGIC ,SWERK    
# MAGIC ,ADRNR    
# MAGIC ,BUKRS      
# MAGIC ,MATNR    
# MAGIC ,ANSWT    
# MAGIC ,ANSDT    
# MAGIC ,ERDAT    
# MAGIC ,AEDAT    
# MAGIC ,INBDT    
# MAGIC ,PROID    
# MAGIC ,EQTYP 
# MAGIC ,'20210831114358' as FILEDATE
# MAGIC from Source3
# MAGIC union all
# MAGIC select EQUNR    
# MAGIC ,DATETO   
# MAGIC ,DATEFROM 
# MAGIC ,EQART    
# MAGIC ,INVNR    
# MAGIC ,IWERK    
# MAGIC ,KOKRS    
# MAGIC ,TPLNR    
# MAGIC ,SWERK    
# MAGIC ,ADRNR    
# MAGIC ,BUKRS      
# MAGIC ,MATNR    
# MAGIC ,ANSWT    
# MAGIC ,ANSDT    
# MAGIC ,ERDAT    
# MAGIC ,AEDAT    
# MAGIC ,INBDT    
# MAGIC ,PROID    
# MAGIC ,EQTYP 
# MAGIC ,'20210901145441' as FILEDATE
# MAGIC from Source4
# MAGIC union all
# MAGIC select EQUNR    
# MAGIC ,DATETO   
# MAGIC ,DATEFROM 
# MAGIC ,EQART    
# MAGIC ,INVNR    
# MAGIC ,IWERK    
# MAGIC ,KOKRS    
# MAGIC ,TPLNR    
# MAGIC ,SWERK    
# MAGIC ,ADRNR    
# MAGIC ,BUKRS      
# MAGIC ,MATNR    
# MAGIC ,ANSWT    
# MAGIC ,ANSDT    
# MAGIC ,ERDAT    
# MAGIC ,AEDAT    
# MAGIC ,INBDT    
# MAGIC ,PROID    
# MAGIC ,EQTYP 
# MAGIC ,'20210904145700' as FILEDATE
# MAGIC from Source5
# MAGIC )a))-- where rn = 1 --and EQUNR = '000000000011520707'
# MAGIC --except
# MAGIC --SELECT *, '3/09' as filedate from Source;

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC EQUNR as equipmentNumber
# MAGIC ,DATETO as validToDate
# MAGIC ,DATEFROM as validFromDate
# MAGIC ,EQART as technicalObjectTypeCode
# MAGIC ,INVNR as inventoryNumber
# MAGIC ,IWERK as maintenancePlanningPlant
# MAGIC ,KOKRS as controllingArea
# MAGIC ,TPLNR as functionalLocationNumber
# MAGIC ,SWERK as maintenancePlant
# MAGIC ,ADRNR as addressNumber
# MAGIC ,BUKRS as companyCode
# MAGIC ,b.companyName as companyName
# MAGIC ,MATNR as materialNumber
# MAGIC ,ANSDT as acquisitionDate
# MAGIC ,ANSWT as acqusitionValue
# MAGIC ,ERDAT as createdDate
# MAGIC ,AEDAT as lastChangedDate
# MAGIC ,INBDT as startUpDate
# MAGIC ,PROID as workBreakdownStructureElement
# MAGIC ,EQTYP as equipmentCategoryCode
# MAGIC from Source1 a
# MAGIC left join cleansed.t_sapisu_0comp_code_Text b
# MAGIC on a.BUKRS = b.companyCode where EQUNR = '000000000000008395'

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct ANSDT from source1 limit 100

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct acquisitionValue from cleansed.t_sapisu_0equipment_attr limit 100

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cleansed.t_sapisu_0equipment_attr order by validfromdate desc-- where validfromdate = '1102-01-07'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from source1 where DATEFROM < '1900-01-01'

# COMMAND ----------

# DBTITLE 1,[Source] with mapping
# MAGIC %sql
# MAGIC select
# MAGIC EQUNR as equipmentNumber
# MAGIC ,DATETO as validToDate
# MAGIC ,DATEFROM as validFromDate
# MAGIC ,EQART as technicalObjectTypeCode
# MAGIC ,INVNR as inventoryNumber
# MAGIC ,IWERK as maintenancePlanningPlant
# MAGIC ,KOKRS as controllingArea
# MAGIC ,TPLNR as functionalLocationNumber
# MAGIC ,SWERK as maintenancePlant
# MAGIC ,ADRNR as addressNumber
# MAGIC ,BUKRS as companyCode
# MAGIC ,b.companyName as companyName
# MAGIC ,MATNR as materialNumber
# MAGIC ,ANSDT as acquisitionDate
# MAGIC ,ANSWT as acqusitionValue
# MAGIC ,ERDAT as createdDate
# MAGIC ,AEDAT as lastChangedDate
# MAGIC ,INBDT as startUpDate
# MAGIC ,PROID as workBreakdownStructureElement
# MAGIC ,EQTYP as equipmentCategoryCode
# MAGIC from Source1 a
# MAGIC left join cleansed.t_sapisu_0comp_code_Text b
# MAGIC on a.BUKRS = b.companyCode

# COMMAND ----------

lakedf.createOrReplaceTempView("Target")

# COMMAND ----------

# DBTITLE 1,[Verification] Count Checks
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from cleansed.t_sapisu_0equipment_attr
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from (select * from (
# MAGIC select
# MAGIC EQUNR as equipmentNumber
# MAGIC ,DATETO as validToDate
# MAGIC ,case when DATEFROM <'1900-01-01' then null else DATEFROM end as validFromDate
# MAGIC ,EQART as technicalObjectTypeCode
# MAGIC ,INVNR as inventoryNumber
# MAGIC ,IWERK as maintenancePlanningPlant
# MAGIC ,KOKRS as controllingArea
# MAGIC ,TPLNR as functionalLocationNumber
# MAGIC ,SWERK as maintenancePlant
# MAGIC ,ADRNR as addressNumber
# MAGIC ,BUKRS as companyCode
# MAGIC ,b.companyName as companyName
# MAGIC ,MATNR as materialNumber
# MAGIC ,ANSDT as acquisitionDate
# MAGIC ,ANSWT as acqusitionValue
# MAGIC ,ERDAT as createdDate
# MAGIC ,AEDAT as lastChangedDate
# MAGIC ,INBDT as startUpDate
# MAGIC ,PROID as workBreakdownStructureElement
# MAGIC ,EQTYP as equipmentCategoryCode
# MAGIC from Source1 a
# MAGIC left join cleansed.t_sapisu_0comp_code_Text b
# MAGIC on a.BUKRS = b.companyCode
# MAGIC )a)

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT equipmentNumber, validtodate, validfromdate, COUNT (*) as count
# MAGIC FROM cleansed.t_sapisu_0EQUIPMENT_ATTR
# MAGIC GROUP BY equipmentNumber, validtodate, validfromdate
# MAGIC HAVING COUNT (*) > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT * FROM (
# MAGIC SELECT
# MAGIC *,
# MAGIC row_number() OVER(PARTITION BY equipmentNumber, validtodate order by equipmentNumber) as rn
# MAGIC FROM cleansed.t_sapisu_0EQUIPMENT_ATTR
# MAGIC )a where a.rn > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Source and Target Data
# MAGIC %sql
# MAGIC select
# MAGIC EQUNR as equipmentNumber
# MAGIC ,DATETO as validToDate
# MAGIC ,case when DATEFROM <'1900-01-01' then null else DATEFROM end as validFromDate
# MAGIC ,EQART as technicalObjectTypeCode
# MAGIC ,INVNR as inventoryNumber
# MAGIC ,IWERK as maintenancePlanningPlant
# MAGIC ,KOKRS as controllingArea
# MAGIC ,TPLNR as functionalLocationNumber
# MAGIC ,SWERK as maintenancePlant
# MAGIC ,ADRNR as addressNumber
# MAGIC ,BUKRS as companyCode
# MAGIC ,b.companyName as companyName
# MAGIC ,MATNR as materialNumber
# MAGIC ,ANSDT as acquisitionDate
# MAGIC ,ANSWT as acqusitionValue
# MAGIC ,ERDAT as createdDate
# MAGIC ,AEDAT as lastChangedDate
# MAGIC ,INBDT as startUpDate
# MAGIC ,PROID as workBreakdownStructureElement
# MAGIC ,EQTYP as equipmentCategoryCode
# MAGIC from Source1 a
# MAGIC left join cleansed.t_sapisu_0comp_code_Text b
# MAGIC on a.BUKRS = b.companyCode
# MAGIC 
# MAGIC EXCEPT
# MAGIC select
# MAGIC equipmentNumber
# MAGIC ,validToDate
# MAGIC ,validFromDate
# MAGIC ,technicalObjectTypeCode
# MAGIC ,inventoryNumber
# MAGIC ,maintenancePlanningPlant
# MAGIC ,controllingArea
# MAGIC ,functionalLocationNumber
# MAGIC ,maintenancePlant
# MAGIC ,addressNumber
# MAGIC ,companyCode
# MAGIC ,companyName
# MAGIC ,materialNumber
# MAGIC ,acquisitionDate
# MAGIC ,acquisitionValue
# MAGIC ,createdDate
# MAGIC ,lastChangedDate
# MAGIC ,startUpDate
# MAGIC ,workBreakdownStructureElement
# MAGIC ,equipmentCategoryCode
# MAGIC 
# MAGIC FROM
# MAGIC cleansed.t_sapisu_0equipment_attr

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Target and Source Data
# MAGIC %sql
# MAGIC select
# MAGIC equipmentNumber
# MAGIC ,validToDate
# MAGIC ,validFromDate
# MAGIC ,technicalObjectTypeCode
# MAGIC ,inventoryNumber
# MAGIC ,maintenancePlanningPlant
# MAGIC ,controllingArea
# MAGIC ,functionalLocationNumber
# MAGIC ,maintenancePlant
# MAGIC ,addressNumber
# MAGIC ,companyCode
# MAGIC ,companyName
# MAGIC ,materialNumber
# MAGIC ,acquisitionDate
# MAGIC ,acquisitionValue
# MAGIC ,createdDate
# MAGIC ,lastChangedDate
# MAGIC ,startUpDate
# MAGIC ,workBreakdownStructureElement
# MAGIC ,equipmentCategoryCode
# MAGIC 
# MAGIC FROM
# MAGIC cleansed.t_sapisu_0equipment_attr
# MAGIC except
# MAGIC select
# MAGIC EQUNR as equipmentNumber
# MAGIC ,DATETO as validToDate
# MAGIC ,case when DATEFROM <'1900-01-01' then null else DATEFROM end as validFromDate
# MAGIC ,EQART as technicalObjectTypeCode
# MAGIC ,INVNR as inventoryNumber
# MAGIC ,IWERK as maintenancePlanningPlant
# MAGIC ,KOKRS as controllingArea
# MAGIC ,TPLNR as functionalLocationNumber
# MAGIC ,SWERK as maintenancePlant
# MAGIC ,ADRNR as addressNumber
# MAGIC ,BUKRS as companyCode
# MAGIC ,b.companyName as companyName
# MAGIC ,MATNR as materialNumber
# MAGIC ,ANSDT as acquisitionDate
# MAGIC ,ANSWT as acqusitionValue
# MAGIC ,ERDAT as createdDate
# MAGIC ,AEDAT as lastChangedDate
# MAGIC ,INBDT as startUpDate
# MAGIC ,PROID as workBreakdownStructureElement
# MAGIC ,EQTYP as equipmentCategoryCode
# MAGIC from Source1 a
# MAGIC left join cleansed.t_sapisu_0comp_code_Text b
# MAGIC on a.BUKRS = b.companyCode
