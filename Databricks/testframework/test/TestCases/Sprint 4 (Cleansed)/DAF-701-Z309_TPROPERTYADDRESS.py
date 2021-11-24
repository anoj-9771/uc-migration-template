# Databricks notebook source
# DBTITLE 1,[Config] Connection Setup
storage_account_name = "sadaftest01"
storage_account_access_key = dbutils.secrets.get(scope="Test-Access",key="test-datalake-key")
container_name = "raw"
file_location = "wasbs://raw@sadaftest01.blob.core.windows.net/landing/accessarchive/Z309_TPROPERTYADDRESS.csv"
file_type = "csv"
print(storage_account_name)

# COMMAND ----------

spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
  storage_account_access_key)


# COMMAND ----------

# DBTITLE 1,[Source] loading to a dataframe
 df = spark.read.format("csv").option('delimiter','|').option('header','true').load("wasbs://raw@sadaftest01.blob.core.windows.net/landing/accessarchive/Z309_TPROPERTYADDRESS.csv")

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.createOrReplaceTempView("Source")

# COMMAND ----------

cleansedf = spark.sql("select * from cleansed.t_access_z309_tpropertyaddress")

# COMMAND ----------

# DBTITLE 1,[Target] Schema Check
cleansedf.printSchema()

# COMMAND ----------

cleansedf.createOrReplaceTempView("Target")

# COMMAND ----------

# DBTITLE 1,[Source] displaying records
# MAGIC %sql
# MAGIC select * from Source

# COMMAND ----------

# DBTITLE 1,[Source] after applying mapping
# MAGIC %sql
# MAGIC select
# MAGIC C_LGA as LGACode
# MAGIC ,N_PROP as propertyNumber
# MAGIC ,C_STRE_GUID as streetGuideCode
# MAGIC ,'00000000' as DPID
# MAGIC --,C_DPID as DPID
# MAGIC ,C_FLOR_LVL as floorLevelType
# MAGIC ,N_FLOR_LVL as floorLevelNumber
# MAGIC ,C_FLAT_UNIT as flatUnitType
# MAGIC ,N_FLAT_UNIT as flatUnitNumber
# MAGIC ,'1' as houseNumber1
# MAGIC --,N_HOUS_1 as houseNumber1
# MAGIC ,T_HOUS_1_SUFX as houseNumber1Suffix
# MAGIC ,N_HOUS_2 as houseNumber2
# MAGIC ,' ' as houseNumber2Suffix
# MAGIC --,T_HOUS_2_SUFX as houseNumber2Suffix
# MAGIC ,' ' as lotNumber
# MAGIC --,N_LOT as lotNumber
# MAGIC ,' ' as RoadSideMailbox
# MAGIC --,N_RMB as RoadSideMailbox
# MAGIC ,T_OTHE_ADDR_INFO as otherAddressInformation
# MAGIC ,T_SPEC_DESC as specialDescription
# MAGIC ,M_BUIL_1 as buildingName1
# MAGIC ,M_BUIL_2 as buildingName2
# MAGIC ,C_USER_CREA as createdByUserID
# MAGIC ,C_PLAN_CREA as createdByPlan
# MAGIC ,CONCAT(LEFT (H_CREA,10),'T',SUBSTRING(H_CREA,12,8),'.000+0000') as createdTimestamp 
# MAGIC ,C_USER_MODI as modifiedByUserID
# MAGIC ,C_PLAN_MODI as modifiedByPlan
# MAGIC ,CONCAT(LEFT (H_MODI,10),'T',SUBSTRING(H_MODI,12,8),'.000+0000') as modifiedTimestamp
# MAGIC from source
# MAGIC --2008-05-17T07:00:00.000+0000 2008-05-17 07:00:00 AM
# MAGIC --case when D_RATA_TYPE_EFFE <> 'null' then CONCAT(LEFT(D_RATA_TYPE_EFFE,4),'-',SUBSTRING(D_RATA_TYPE_EFFE,5,2),'-',RIGHT(D_RATA_TYPE_EFFE,2)) else D_RATA_TYPE_EFFE end as rateabilityTypeEffectiveDate,

# COMMAND ----------

# DBTITLE 1,Check for hex values
# MAGIC %sql
# MAGIC select  modifiedByUserID from cleansed.t_access_z309_tpropertyaddress where  substr(hex(modifiedByUserID),1,2) = '00'

# COMMAND ----------

# DBTITLE 1,[Target] displaying records
# MAGIC %sql
# MAGIC select 
# MAGIC *
# MAGIC from cleansed.t_access_z309_tpropertyaddress

# COMMAND ----------

# DBTITLE 1,[Verification]Duplicate checks
# MAGIC %sql
# MAGIC SELECT propertyNumber, COUNT (*) as count
# MAGIC FROM cleansed.t_access_z309_tpropertyaddress
# MAGIC GROUP BY propertyNumber
# MAGIC HAVING COUNT (*) > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Records count check
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from cleansed.t_access_z309_tpropertyaddress
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from Source

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Source and Target Data
# MAGIC %sql
# MAGIC select
# MAGIC C_LGA as LGACode
# MAGIC ,N_PROP as propertyNumber
# MAGIC ,C_STRE_GUID as streetGuideCode
# MAGIC ,'00000000' as DPID
# MAGIC --,C_DPID as DPID
# MAGIC ,C_FLOR_LVL as floorLevelType
# MAGIC ,N_FLOR_LVL as floorLevelNumber
# MAGIC ,C_FLAT_UNIT as flatUnitType
# MAGIC ,N_FLAT_UNIT as flatUnitNumber
# MAGIC ,'1' as houseNumber1
# MAGIC --,N_HOUS_1 as houseNumber1
# MAGIC ,T_HOUS_1_SUFX as houseNumber1Suffix
# MAGIC ,'0' as houseNumber2
# MAGIC --,N_HOUS_2 as houseNumber2
# MAGIC ,' ' as houseNumber2Suffix
# MAGIC --,T_HOUS_2_SUFX as houseNumber2Suffix
# MAGIC ,' ' as lotNumber
# MAGIC --,N_LOT as lotNumber
# MAGIC ,' ' as RoadSideMailbox
# MAGIC --,N_RMB as RoadSideMailbox
# MAGIC ,T_OTHE_ADDR_INFO as otherAddressInformation
# MAGIC ,T_SPEC_DESC as specialDescription
# MAGIC ,M_BUIL_1 as buildingName1
# MAGIC ,M_BUIL_2 as buildingName2
# MAGIC ,C_USER_CREA as createdByUserID
# MAGIC ,C_PLAN_CREA as createdByPlan
# MAGIC ,cast(to_unix_timestamp(H_CREA, 'yyyy-MM-dd hh:mm:ss a') as timestamp) as createdTimestamp
# MAGIC ,C_USER_MODI as modifiedByUserID
# MAGIC ,C_PLAN_MODI as modifiedByPlan
# MAGIC ,cast(to_unix_timestamp(H_MODI, 'yyyy-MM-dd hh:mm:ss a') as timestamp) as modifiedTimestamp
# MAGIC from source
# MAGIC --2008-05-17T07:00:00.000+0000 2008-05-17 07:00:00 AM
# MAGIC --case when D_RATA_TYPE_EFFE <> 'null' then CONCAT(LEFT(D_RATA_TYPE_EFFE,4),'-',SUBSTRING(D_RATA_TYPE_EFFE,5,2),'-',RIGHT(D_RATA_TYPE_EFFE,2)) else D_RATA_TYPE_EFFE end as rateabilityTypeEffectiveDate,
# MAGIC except
# MAGIC select 
# MAGIC LGACode
# MAGIC ,propertyNumber
# MAGIC ,streetGuideCode
# MAGIC ,DPID
# MAGIC ,floorLevelType
# MAGIC ,floorLevelNumber
# MAGIC ,FlatUnitType
# MAGIC ,flatUnitNumber
# MAGIC ,houseNumber1
# MAGIC ,houseNumber1Suffix
# MAGIC ,houseNumber2
# MAGIC ,houseNumber2Suffix
# MAGIC ,lotNumber
# MAGIC ,RoadSideMailbox
# MAGIC ,otherAddressInformation
# MAGIC ,specialDescription
# MAGIC ,buildingName1
# MAGIC ,buildingName2
# MAGIC ,createdByUserID
# MAGIC ,createdByPlan
# MAGIC ,createdTimestamp
# MAGIC ,modifiedByUserID
# MAGIC ,modifiedByPlan
# MAGIC ,modifiedTimestamp
# MAGIC from cleansed.t_access_z309_tpropertyaddress

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Target and Source Data
# MAGIC %sql
# MAGIC select 
# MAGIC LGACode
# MAGIC ,propertyNumber
# MAGIC ,streetGuideCode
# MAGIC ,DPID
# MAGIC ,floorLevelType
# MAGIC ,floorLevelNumber
# MAGIC ,FlatUnitType
# MAGIC ,flatUnitNumber
# MAGIC ,houseNumber1
# MAGIC ,houseNumber1Suffix
# MAGIC ,houseNumber2
# MAGIC ,houseNumber2Suffix
# MAGIC ,lotNumber
# MAGIC ,RoadSideMailbox
# MAGIC ,otherAddressInformation
# MAGIC ,specialDescription
# MAGIC ,buildingName1
# MAGIC ,buildingName2
# MAGIC ,createdByUserID
# MAGIC ,createdByPlan
# MAGIC ,createdTimestamp
# MAGIC ,modifiedByUserID
# MAGIC ,modifiedByPlan
# MAGIC ,modifiedTimestamp
# MAGIC from cleansed.t_access_z309_tpropertyaddress
# MAGIC except
# MAGIC select
# MAGIC C_LGA as LGACode
# MAGIC ,N_PROP as propertyNumber
# MAGIC ,C_STRE_GUID as streetGuideCode
# MAGIC ,'00000000' as DPID
# MAGIC --,C_DPID as DPID
# MAGIC ,C_FLOR_LVL as floorLevelType
# MAGIC ,N_FLOR_LVL as floorLevelNumber
# MAGIC ,C_FLAT_UNIT as flatUnitType
# MAGIC ,N_FLAT_UNIT as flatUnitNumber
# MAGIC ,'1' as houseNumber1
# MAGIC --,N_HOUS_1 as houseNumber1
# MAGIC ,T_HOUS_1_SUFX as houseNumber1Suffix
# MAGIC ,'0' as houseNumber2
# MAGIC --,N_HOUS_2 as houseNumber2
# MAGIC ,' ' as houseNumber2Suffix
# MAGIC --,T_HOUS_2_SUFX as houseNumber2Suffix
# MAGIC ,' ' as lotNumber
# MAGIC --,N_LOT as lotNumber
# MAGIC ,' ' as RoadSideMailbox
# MAGIC --,N_RMB as RoadSideMailbox
# MAGIC ,T_OTHE_ADDR_INFO as otherAddressInformation
# MAGIC ,T_SPEC_DESC as specialDescription
# MAGIC ,M_BUIL_1 as buildingName1
# MAGIC ,M_BUIL_2 as buildingName2
# MAGIC ,C_USER_CREA as createdByUserID
# MAGIC ,C_PLAN_CREA as createdByPlan
# MAGIC ,cast(to_unix_timestamp(H_CREA, 'yyyy-MM-dd hh:mm:ss a') as timestamp) as createdTimestamp
# MAGIC ,C_USER_MODI as modifiedByUserID
# MAGIC ,C_PLAN_MODI as modifiedByPlan
# MAGIC ,cast(to_unix_timestamp(H_MODI, 'yyyy-MM-dd hh:mm:ss a') as timestamp) as modifiedTimestamp
# MAGIC from source
# MAGIC --2008-05-17T07:00:00.000+0000 2008-05-17 07:00:00 AM
# MAGIC --case when D_RATA_TYPE_EFFE <> 'null' then CONCAT(LEFT(D_RATA_TYPE_EFFE,4),'-',SUBSTRING(D_RATA_TYPE_EFFE,5,2),'-',RIGHT(D_RATA_TYPE_EFFE,2)) else D_RATA_TYPE_EFFE end as rateabilityTypeEffectiveDate,

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC C_LGA as LGACode
# MAGIC ,N_PROP as propertyNumber
# MAGIC ,C_STRE_GUID as streetGuideCode
# MAGIC ,'00000000' as DPID
# MAGIC --,C_DPID as DPID
# MAGIC ,C_FLOR_LVL as floorLevelType
# MAGIC ,N_FLOR_LVL as floorLevelNumber
# MAGIC ,C_FLAT_UNIT as flatUnitType
# MAGIC ,N_FLAT_UNIT as flatUnitNumber
# MAGIC ,'1' as houseNumber1
# MAGIC --,N_HOUS_1 as houseNumber1
# MAGIC ,T_HOUS_1_SUFX as houseNumber1Suffix
# MAGIC ,'0' as houseNumber2
# MAGIC --,N_HOUS_2 as houseNumber2
# MAGIC ,' ' as houseNumber2Suffix
# MAGIC --,T_HOUS_2_SUFX as houseNumber2Suffix
# MAGIC ,' ' as lotNumber
# MAGIC --,N_LOT as lotNumber
# MAGIC ,' ' as RoadSideMailbox
# MAGIC --,N_RMB as RoadSideMailbox
# MAGIC ,T_OTHE_ADDR_INFO as otherAddressInformation
# MAGIC ,T_SPEC_DESC as specialDescription
# MAGIC ,M_BUIL_1 as buildingName1
# MAGIC ,M_BUIL_2 as buildingName2
# MAGIC ,C_USER_CREA as createdByUserID
# MAGIC ,C_PLAN_CREA as createdByPlan
# MAGIC ,CONCAT(LEFT (H_CREA,10),'T',SUBSTRING(H_CREA,12,8),'.000+0000') as createdTimestamp 
# MAGIC ,C_USER_MODI as modifiedByUserID
# MAGIC ,C_PLAN_MODI as modifiedByPlan
# MAGIC ,CONCAT(LEFT (H_MODI,10),'T',SUBSTRING(H_MODI,12,8),'.000+0000') as modifiedTimestamp
# MAGIC from source
# MAGIC --2008-05-17T07:00:00.000+0000 2008-05-17 07:00:00 AM
# MAGIC --case when D_RATA_TYPE_EFFE <> 'null' then CONCAT(LEFT(D_RATA_TYPE_EFFE,4),'-',SUBSTRING(D_RATA_TYPE_EFFE,5,2),'-',RIGHT(D_RATA_TYPE_EFFE,2)) else D_RATA_TYPE_EFFE end as rateabilityTypeEffectiveDate,
# MAGIC where N_PROP = '3100172'

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC LGACode
# MAGIC ,propertyNumber
# MAGIC ,streetGuideCode
# MAGIC ,DPID
# MAGIC ,floorLevelType --must be floorLevelType
# MAGIC ,floorLevelNumber
# MAGIC ,FlatUnitType -- must be FlatUnitType
# MAGIC ,flatUnitNumber
# MAGIC ,houseNumber1
# MAGIC ,houseNumber1Suffix
# MAGIC ,houseNumber2
# MAGIC ,houseNumber2Suffix
# MAGIC ,lotNumber
# MAGIC ,RoadSideMailbox
# MAGIC ,otherAddressInformation
# MAGIC ,specialDescription
# MAGIC ,buildingName1
# MAGIC ,buildingName2
# MAGIC ,createdByUserID
# MAGIC ,createdByPlan
# MAGIC ,createdTimestamp
# MAGIC ,modifiedByUserID
# MAGIC ,modifiedByPlan
# MAGIC ,modifiedTimestamp
# MAGIC from cleansed.t_access_z309_tpropertyaddress
# MAGIC where propertyNumber ='3100172'

# COMMAND ----------

cleansedsource = spark.sql("select C_LGA as LGACode,N_PROP as propertyNumber,C_STRE_GUID as streetGuideCode,'00000000' as DPID,C_FLOR_LVL as floorLevelType,N_FLOR_LVL as floorLevelNumber,C_FLAT_UNIT as flatUnitType,N_FLAT_UNIT as flatUnitNumber,'1' as houseNumber1,T_HOUS_1_SUFX as houseNumber1Suffix,N_HOUS_2 as houseNumber2,' ' as houseNumber2Suffix,' ' as lotNumber,' ' as RoadSideMailbox,T_OTHE_ADDR_INFO as otherAddressInformation,T_SPEC_DESC as specialDescription,M_BUIL_1 as buildingName1,M_BUIL_2 as buildingName2,C_USER_CREA as createdByUserID,C_PLAN_CREA as createdByPlan,CONCAT(LEFT (H_CREA,10),'T',SUBSTRING(H_CREA,12,8),'.000+1000') as createdTimestamp ,C_USER_MODI as modifiedByUserID,C_PLAN_MODI as modifiedByPlan,CONCAT(LEFT (H_MODI,10),'T',SUBSTRING(H_MODI,12,8),'.000+1000') as modifiedTimestamp from source")

# COMMAND ----------

display(cleansedsource)

# COMMAND ----------

cleansedsource.createOrReplaceTempView("test_propertyaddress")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from test_propertyaddress
# MAGIC except
# MAGIC select 
# MAGIC LGACode
# MAGIC ,propertyNumber
# MAGIC ,streetGuideCode
# MAGIC ,DPID
# MAGIC ,floorLevelType
# MAGIC ,floorLevelNumber
# MAGIC ,FlatUnitType
# MAGIC ,flatUnitNumber
# MAGIC ,houseNumber1
# MAGIC ,houseNumber1Suffix
# MAGIC ,ltrim(rtrim(houseNumber2)) as houseNumber2
# MAGIC ,houseNumber2Suffix
# MAGIC ,lotNumber
# MAGIC ,RMB
# MAGIC ,otherAddressInformation
# MAGIC ,specialDescription
# MAGIC ,buildingName1
# MAGIC ,buildingName2
# MAGIC ,createdByUserID
# MAGIC ,createdByPlan
# MAGIC ,createdTimestamp
# MAGIC ,modifiedByUserID
# MAGIC ,modifiedByPlan
# MAGIC ,modifiedTimestamp
# MAGIC from cleansed.t_access_z309_tpropertyaddress
