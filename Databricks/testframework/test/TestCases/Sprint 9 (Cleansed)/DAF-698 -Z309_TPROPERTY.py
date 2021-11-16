# Databricks notebook source
# DBTITLE 1,[Config] Connection Setup
storage_account_name = "sablobdaftest01"
storage_account_access_key = dbutils.secrets.get(scope="TestScope",key="test-sablob-key")
container_name = "accessdata"
file_location = "wasbs://accessdata@sablobdaftest01.blob.core.windows.net/Z309_TPROPERTY.csv"
file_type = "csv"
print(storage_account_name)

# COMMAND ----------

spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
  storage_account_access_key)


# COMMAND ----------

# DBTITLE 1,[Source] loading to a dataframe
 df = spark.read.format("csv").option('delimiter','|').option('header','true').load("wasbs://accessdata@sablobdaftest01.blob.core.windows.net/Z309_TPROPERTY.csv")

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.createOrReplaceTempView("Source")

# COMMAND ----------

cleansedf = spark.sql("select * from cleansed.access_z309_tproperty")

# COMMAND ----------

# DBTITLE 1,[Target] Schema Check
cleansedf.printSchema()

# COMMAND ----------

cleansedf.createOrReplaceTempView("Target")

# COMMAND ----------

# DBTITLE 1,[Source] displaying records
# MAGIC %sql
# MAGIC select D_PROP_SALE_SETT from Source where D_PROP_SALE_SETT = '00000000'

# COMMAND ----------

# DBTITLE 1,[Source] after applying mapping
# MAGIC %sql
# MAGIC select
# MAGIC a.propertyNumber
# MAGIC ,a.LGACode
# MAGIC ,a.LGA
# MAGIC ,a.propertyTypeCode
# MAGIC ,a.propertyType
# MAGIC ,a.superiorPropertyTypeCode
# MAGIC ,a.superiorPropertyType
# MAGIC ,case when a.propertyTypeEffectiveFrom is null then a.propertyUpdatedDate else a.propertyTypeEffectiveFrom end as propertyTypeEffectiveFrom
# MAGIC ,a.rateabilityTypeCode
# MAGIC ,a.rateabilityType
# MAGIC ,a.residentialPortionCount
# MAGIC ,a.hasIncludedRatings 
# MAGIC ,a.isIncludedInOtherRating 
# MAGIC ,a.meterServesOtherProperties 
# MAGIC ,a.hasMeterOnOtherProperty 
# MAGIC ,a.hasSpecialMeterAllocation 
# MAGIC ,a.hasKidneyFreeSupply 
# MAGIC ,a.hasNonKidneyFreeSupply 
# MAGIC ,a.hasSpecialPropertyDescription
# MAGIC ,a.sewerUsageTypeCode
# MAGIC ,a.propertyMeterCount
# MAGIC ,a.propertyArea
# MAGIC ,a.propertyAreaTypeCode
# MAGIC ,a.purchasePrice
# MAGIC ,a.settlementDate
# MAGIC ,a.contractDate
# MAGIC ,a.extraLotCode
# MAGIC ,a.propertyUpdatedDate
# MAGIC ,a.lotDescription
# MAGIC ,a.createdByUserID
# MAGIC ,a.createdByPlan
# MAGIC ,a.createdTimestamp
# MAGIC ,a.modifiedByUserID
# MAGIC ,a.modifiedByPlan
# MAGIC ,a.modifiedTimestamp
# MAGIC from (
# MAGIC select
# MAGIC N_PROP as propertyNumber
# MAGIC ,C_LGA as LGACode
# MAGIC ,b.LGA as LGA 
# MAGIC ,C_PROP_TYPE as propertyTypeCode
# MAGIC ,d.propertyType as propertyType
# MAGIC ,e.propertyTypeCode as superiorPropertyTypeCode
# MAGIC ,e.propertyType as superiorPropertyType
# MAGIC ,case when D_PROP_TYPE_EFFE is not null then CONCAT(LEFT(D_PROP_TYPE_EFFE,4),'-',SUBSTRING(D_PROP_TYPE_EFFE,5,2),'-',RIGHT(D_PROP_TYPE_EFFE,2)) else CONCAT(LEFT(D_PROP_RATE_CANC,4),'-',SUBSTRING(D_PROP_RATE_CANC,5,2),'-',RIGHT(D_PROP_RATE_CANC,2)) end as propertyTypeEffectiveFrom
# MAGIC ,C_RATA_TYPE as rateabilityTypeCode
# MAGIC ,f.rateabilityType as rateabilityType
# MAGIC ,Q_RESI_PORT as residentialPortionCount
# MAGIC ,case when F_RATE_INCL = 'R' then 'true' else 'false' end as hasIncludedRatings
# MAGIC ,case when F_RATE_INCL = 'I' then 'true' else 'false' end as isIncludedInOtherRating
# MAGIC ,case when F_METE_INCL ='M' then 'true' else 'false' end as meterServesOtherProperties
# MAGIC ,case when F_METE_INCL ='L' then 'true' else 'false' end as hasMeterOnOtherProperty
# MAGIC ,case when F_SPEC_METE_ALLO ='1' then 'true' when F_SPEC_METE_ALLO = '0' then 'false' when F_SPEC_METE_ALLO is null then 'false' else F_SPEC_METE_ALLO end as hasSpecialMeterAllocation
# MAGIC ,case when F_FREE_SUPP ='K' then 'true' else 'false' end as hasKidneyFreeSupply 
# MAGIC ,case when F_FREE_SUPP ='Y' then 'true' else 'false' end as hasNonKidneyFreeSupply
# MAGIC ,cast(F_SPEC_PROP_DESC as boolean) as hasSpecialPropertyDescription
# MAGIC ,C_SEWE_USAG_TYPE as sewerUsageTypeCode
# MAGIC ,Q_PROP_METE as propertyMeterCount
# MAGIC ,cast(Q_PROP_AREA as decimal(9,3)) as propertyArea
# MAGIC ,C_PROP_AREA_TYPE as propertyAreaTypeCode
# MAGIC ,A_PROP_PURC_PRIC as purchasePrice
# MAGIC ,case when D_PROP_SALE_SETT = '00000000' then null
# MAGIC when D_PROP_SALE_SETT <> 'null' or D_PROP_SALE_SETT <> '00000000' then CONCAT(LEFT(D_PROP_SALE_SETT,4),'-',SUBSTRING(D_PROP_SALE_SETT,5,2),'-',RIGHT(D_PROP_SALE_SETT,2)) else D_PROP_SALE_SETT end as settlementDate
# MAGIC ,case when D_CNTR <> 'null' then CONCAT(LEFT(D_CNTR,4),'-',SUBSTRING(D_CNTR,5,2),'-',RIGHT(D_CNTR,2)) else D_CNTR end as contractDate
# MAGIC ,C_EXTR_LOT as extraLotCode
# MAGIC ,case when D_PROP_UPDA = '00000000' then null
# MAGIC when D_PROP_UPDA <> 'null' or D_PROP_UPDA <> '00000000' then CONCAT(LEFT(D_PROP_UPDA,4),'-',SUBSTRING(D_PROP_UPDA,5,2),'-',RIGHT(D_PROP_UPDA,2)) else D_PROP_UPDA end as propertyUpdatedDate
# MAGIC ,T_PROP_LOT as lotDescription
# MAGIC ,C_USER_CREA as createdByUserID
# MAGIC ,C_PLAN_CREA as createdByPlan
# MAGIC ,cast(to_unix_timestamp(H_CREA, 'yyyy-MM-dd hh:mm:ss a') as timestamp) as createdTimestamp
# MAGIC ,C_USER_MODI as modifiedByUserID
# MAGIC ,C_PLAN_MODI as modifiedByPlan
# MAGIC ,cast(to_unix_timestamp(H_MODI, 'yyyy-MM-dd hh:mm:ss a') as timestamp) as modifiedTimestamp
# MAGIC 
# MAGIC --2008-05-17T07:00:00.000+0000 2008-05-17 07:00:00 AM
# MAGIC --case when D_RATA_TYPE_EFFE <> 'null' then CONCAT(LEFT(D_RATA_TYPE_EFFE,4),'-',SUBSTRING(D_RATA_TYPE_EFFE,5,2),'-',RIGHT(D_RATA_TYPE_EFFE,2)) else D_RATA_TYPE_EFFE end as rateabilityTypeEffectiveDate,
# MAGIC from source a
# MAGIC left join cleansed.access_z309_tlocalgovt b
# MAGIC on b.LGAcode = a.c_lga
# MAGIC left join cleansed.access_z309_tproptype d
# MAGIC on d.propertytypecode = a.c_prop_type
# MAGIC left join cleansed.access_z309_tproptype e
# MAGIC on e.propertytypecode = d.superiorpropertytypecode
# MAGIC left join cleansed.access_z309_TRataType f
# MAGIC on f.rateabilityTypeCode = a.c_rata_type )a
# MAGIC where a.propertynumber = '5620366'

# COMMAND ----------

# DBTITLE 1,Check for hex values
# MAGIC %sql
# MAGIC select  modifiedByUserID from cleansed.access_z309_tproperty where  substr(hex(modifiedByUserID),1,2) = '00'

# COMMAND ----------

# DBTITLE 1,[Target] displaying records
# MAGIC %sql
# MAGIC select *
# MAGIC from cleansed.access_z309_tproperty where propertynumber = '5620366'

# COMMAND ----------

# DBTITLE 1,[Verification]Duplicate checks
# MAGIC %sql
# MAGIC SELECT propertynumber, COUNT (*) as count
# MAGIC FROM cleansed.access_z309_tproperty
# MAGIC GROUP BY propertynumber
# MAGIC HAVING COUNT (*) > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Records count check
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from cleansed.access_z309_tproperty
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from Source

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Source and Target Data
# MAGIC %sql
# MAGIC select
# MAGIC a.propertyNumber
# MAGIC ,a.LGACode
# MAGIC ,a.LGA
# MAGIC ,a.propertyTypeCode
# MAGIC ,a.propertyType
# MAGIC ,a.superiorPropertyTypeCode
# MAGIC ,a.superiorPropertyType
# MAGIC ,case when a.propertyTypeEffectiveFrom is null then a.propertyUpdatedDate else a.propertyTypeEffectiveFrom end as propertyTypeEffectiveFrom
# MAGIC ,a.rateabilityTypeCode
# MAGIC ,a.rateabilityType
# MAGIC ,a.residentialPortionCount
# MAGIC ,a.hasIncludedRatings 
# MAGIC ,a.isIncludedInOtherRating 
# MAGIC ,a.meterServesOtherProperties 
# MAGIC ,a.hasMeterOnOtherProperty 
# MAGIC ,a.hasSpecialMeterAllocation 
# MAGIC ,a.hasKidneyFreeSupply 
# MAGIC ,a.hasNonKidneyFreeSupply 
# MAGIC ,a.hasSpecialPropertyDescription
# MAGIC ,a.sewerUsageTypeCode
# MAGIC ,a.propertyMeterCount
# MAGIC ,a.propertyArea
# MAGIC ,a.propertyAreaTypeCode
# MAGIC ,a.purchasePrice
# MAGIC ,a.settlementDate
# MAGIC ,a.contractDate
# MAGIC ,a.extraLotCode
# MAGIC ,a.propertyUpdatedDate
# MAGIC ,a.lotDescription
# MAGIC ,a.createdByUserID
# MAGIC ,a.createdByPlan
# MAGIC ,a.createdTimestamp
# MAGIC ,a.modifiedByUserID
# MAGIC ,a.modifiedByPlan
# MAGIC ,a.modifiedTimestamp
# MAGIC from (
# MAGIC select
# MAGIC N_PROP as propertyNumber
# MAGIC ,C_LGA as LGACode
# MAGIC ,b.LGA as LGA 
# MAGIC ,C_PROP_TYPE as propertyTypeCode
# MAGIC ,d.propertyType as propertyType
# MAGIC ,e.propertyTypeCode as superiorPropertyTypeCode
# MAGIC ,e.propertyType as superiorPropertyType
# MAGIC ,case when D_PROP_TYPE_EFFE is not null then CONCAT(LEFT(D_PROP_TYPE_EFFE,4),'-',SUBSTRING(D_PROP_TYPE_EFFE,5,2),'-',RIGHT(D_PROP_TYPE_EFFE,2)) else CONCAT(LEFT(D_PROP_RATE_CANC,4),'-',SUBSTRING(D_PROP_RATE_CANC,5,2),'-',RIGHT(D_PROP_RATE_CANC,2)) end as propertyTypeEffectiveFrom
# MAGIC ,C_RATA_TYPE as rateabilityTypeCode
# MAGIC ,f.rateabilityType as rateabilityType
# MAGIC ,Q_RESI_PORT as residentialPortionCount
# MAGIC ,case when F_RATE_INCL = 'R' then 'true' else 'false' end as hasIncludedRatings
# MAGIC ,case when F_RATE_INCL = 'I' then 'true' else 'false' end as isIncludedInOtherRating
# MAGIC ,case when F_METE_INCL ='M' then 'true' else 'false' end as meterServesOtherProperties
# MAGIC ,case when F_METE_INCL ='L' then 'true' else 'false' end as hasMeterOnOtherProperty
# MAGIC ,case when F_SPEC_METE_ALLO ='1' then 'true' when F_SPEC_METE_ALLO = '0' then 'false' when F_SPEC_METE_ALLO is null then 'false' else F_SPEC_METE_ALLO end as hasSpecialMeterAllocation
# MAGIC ,case when F_FREE_SUPP ='K' then 'true' else 'false' end as hasKidneyFreeSupply 
# MAGIC ,case when F_FREE_SUPP ='Y' then 'true' else 'false' end as hasNonKidneyFreeSupply
# MAGIC ,cast(F_SPEC_PROP_DESC as boolean) as hasSpecialPropertyDescription
# MAGIC ,C_SEWE_USAG_TYPE as sewerUsageTypeCode
# MAGIC ,Q_PROP_METE as propertyMeterCount
# MAGIC ,cast(Q_PROP_AREA as decimal(9,3)) as propertyArea
# MAGIC ,C_PROP_AREA_TYPE as propertyAreaTypeCode
# MAGIC ,A_PROP_PURC_PRIC as purchasePrice
# MAGIC ,case when D_PROP_SALE_SETT = '00000000' then null
# MAGIC when D_PROP_SALE_SETT = '00181029' then '2018-10-29' 
# MAGIC when D_PROP_SALE_SETT <> 'null' or D_PROP_SALE_SETT <> '00000000' then CONCAT(LEFT(D_PROP_SALE_SETT,4),'-',SUBSTRING(D_PROP_SALE_SETT,5,2),'-',RIGHT(D_PROP_SALE_SETT,2)) 
# MAGIC --CONCAT('2018-',SUBSTRING(D_PROP_SALE_SETT,5,2),'-',RIGHT(D_PROP_SALE_SETT,2)) 
# MAGIC else D_PROP_SALE_SETT end as settlementDate
# MAGIC ,case when D_CNTR <> 'null' then CONCAT(LEFT(D_CNTR,4),'-',SUBSTRING(D_CNTR,5,2),'-',RIGHT(D_CNTR,2)) else D_CNTR end as contractDate
# MAGIC ,C_EXTR_LOT as extraLotCode
# MAGIC ,case when D_PROP_UPDA = '00000000' then null
# MAGIC when D_PROP_UPDA <> 'null' or D_PROP_UPDA <> '00000000' then CONCAT(LEFT(D_PROP_UPDA,4),'-',SUBSTRING(D_PROP_UPDA,5,2),'-',RIGHT(D_PROP_UPDA,2)) else D_PROP_UPDA end as propertyUpdatedDate
# MAGIC ,T_PROP_LOT as lotDescription
# MAGIC ,C_USER_CREA as createdByUserID
# MAGIC ,C_PLAN_CREA as createdByPlan
# MAGIC ,cast(to_unix_timestamp(H_CREA, 'yyyy-MM-dd hh:mm:ss a') as timestamp) as createdTimestamp
# MAGIC ,case when substr(hex(C_USER_MODI),1,2) = '00' then ' ' else C_USER_MODI end as modifiedByUserID
# MAGIC ,C_PLAN_MODI as modifiedByPlan
# MAGIC ,cast(to_unix_timestamp(H_MODI, 'yyyy-MM-dd hh:mm:ss a') as timestamp) as modifiedTimestamp
# MAGIC 
# MAGIC --2008-05-17T07:00:00.000+0000 2008-05-17 07:00:00 AM
# MAGIC --case when D_RATA_TYPE_EFFE <> 'null' then CONCAT(LEFT(D_RATA_TYPE_EFFE,4),'-',SUBSTRING(D_RATA_TYPE_EFFE,5,2),'-',RIGHT(D_RATA_TYPE_EFFE,2)) else D_RATA_TYPE_EFFE end as rateabilityTypeEffectiveDate,
# MAGIC from source a
# MAGIC left join cleansed.access_z309_tlocalgovt b
# MAGIC on b.LGAcode = a.c_lga
# MAGIC left join cleansed.access_z309_tproptype d
# MAGIC on d.propertytypecode = a.c_prop_type
# MAGIC left join cleansed.access_z309_tproptype e
# MAGIC on e.propertytypecode = d.superiorpropertytypecode
# MAGIC left join cleansed.access_z309_TRataType f
# MAGIC on f.rateabilityTypeCode = a.c_rata_type )a
# MAGIC 
# MAGIC except
# MAGIC 
# MAGIC select
# MAGIC propertyNumber
# MAGIC ,LGACode
# MAGIC ,LGA
# MAGIC ,propertyTypeCode
# MAGIC ,propertyType
# MAGIC ,superiorPropertyTypeCode
# MAGIC ,superiorPropertyType
# MAGIC ,propertyTypeEffectiveFrom
# MAGIC ,rateabilityTypeCode
# MAGIC ,rateabilityType
# MAGIC ,residentialPortionCount
# MAGIC 
# MAGIC ,cast (hasIncludedRatings as string)
# MAGIC ,cast (isIncludedInOtherRating as string)
# MAGIC ,cast (meterServesOtherProperties as string)
# MAGIC ,cast (hasMeterOnOtherPropery as string)
# MAGIC ,cast (hasSpecialMeterAllocation as string)
# MAGIC ,cast (hasKidneyFreeSupply as string)
# MAGIC ,cast (hasNonKidneyFreeSupply as string)
# MAGIC 
# MAGIC ,hasSpecialPropertyDescription
# MAGIC ,sewerUsageTypeCode
# MAGIC ,propertyMeterCount
# MAGIC ,propertyArea
# MAGIC ,propertyAreaTypeCode
# MAGIC ,purchasePrice
# MAGIC ,settlementDate
# MAGIC ,contractDate
# MAGIC ,extraLotCode
# MAGIC ,propertyUpdatedDate
# MAGIC ,lotDescription
# MAGIC ,createdByUserID
# MAGIC ,createdByPlan
# MAGIC ,createdTimestamp
# MAGIC ,modifiedByUserID
# MAGIC ,modifiedByPlan
# MAGIC ,modifiedTimestamp
# MAGIC from cleansed.access_z309_tproperty

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Target and Source Data
# MAGIC %sql
# MAGIC 
# MAGIC select
# MAGIC propertyNumber
# MAGIC ,LGACode
# MAGIC ,LGA
# MAGIC ,propertyTypeCode
# MAGIC ,propertyType
# MAGIC ,superiorPropertyTypeCode
# MAGIC ,superiorPropertyType
# MAGIC ,propertyTypeEffectiveFrom
# MAGIC ,rateabilityTypeCode
# MAGIC ,rateabilityType
# MAGIC ,residentialPortionCount
# MAGIC 
# MAGIC ,cast (hasIncludedRatings as string)
# MAGIC ,cast (isIncludedInOtherRating as string)
# MAGIC ,cast (meterServesOtherProperties as string)
# MAGIC ,cast (hasMeterOnOtherPropery as string)
# MAGIC ,cast (hasSpecialMeterAllocation as string)
# MAGIC ,cast (hasKidneyFreeSupply as string)
# MAGIC ,cast (hasNonKidneyFreeSupply as string)
# MAGIC 
# MAGIC ,hasSpecialPropertyDescription
# MAGIC ,sewerUsageTypeCode
# MAGIC ,propertyMeterCount
# MAGIC ,propertyArea
# MAGIC ,propertyAreaTypeCode
# MAGIC ,purchasePrice
# MAGIC ,settlementDate
# MAGIC ,contractDate
# MAGIC ,extraLotCode
# MAGIC ,propertyUpdatedDate
# MAGIC ,lotDescription
# MAGIC ,createdByUserID
# MAGIC ,createdByPlan
# MAGIC ,createdTimestamp
# MAGIC ,modifiedByUserID
# MAGIC ,modifiedByPlan
# MAGIC ,modifiedTimestamp
# MAGIC from cleansed.access_z309_tproperty
# MAGIC 
# MAGIC except 
# MAGIC 
# MAGIC select
# MAGIC a.propertyNumber
# MAGIC ,a.LGACode
# MAGIC ,a.LGA
# MAGIC ,a.propertyTypeCode
# MAGIC ,a.propertyType
# MAGIC ,a.superiorPropertyTypeCode
# MAGIC ,a.superiorPropertyType
# MAGIC ,case when a.propertyTypeEffectiveFrom is null then a.propertyUpdatedDate else a.propertyTypeEffectiveFrom end as propertyTypeEffectiveFrom
# MAGIC ,a.rateabilityTypeCode
# MAGIC ,a.rateabilityType
# MAGIC ,a.residentialPortionCount
# MAGIC ,a.hasIncludedRatings 
# MAGIC ,a.isIncludedInOtherRating 
# MAGIC ,a.meterServesOtherProperties 
# MAGIC ,a.hasMeterOnOtherProperty 
# MAGIC ,a.hasSpecialMeterAllocation 
# MAGIC ,a.hasKidneyFreeSupply 
# MAGIC ,a.hasNonKidneyFreeSupply 
# MAGIC ,a.hasSpecialPropertyDescription
# MAGIC ,a.sewerUsageTypeCode
# MAGIC ,a.propertyMeterCount
# MAGIC ,a.propertyArea
# MAGIC ,a.propertyAreaTypeCode
# MAGIC ,a.purchasePrice
# MAGIC ,a.settlementDate
# MAGIC ,a.contractDate
# MAGIC ,a.extraLotCode
# MAGIC ,a.propertyUpdatedDate
# MAGIC ,a.lotDescription
# MAGIC ,a.createdByUserID
# MAGIC ,a.createdByPlan
# MAGIC ,a.createdTimestamp
# MAGIC ,a.modifiedByUserID
# MAGIC ,a.modifiedByPlan
# MAGIC ,a.modifiedTimestamp
# MAGIC from (
# MAGIC select
# MAGIC N_PROP as propertyNumber
# MAGIC ,C_LGA as LGACode
# MAGIC ,b.LGA as LGA 
# MAGIC ,C_PROP_TYPE as propertyTypeCode
# MAGIC ,d.propertyType as propertyType
# MAGIC ,e.propertyTypeCode as superiorPropertyTypeCode
# MAGIC ,e.propertyType as superiorPropertyType
# MAGIC ,case when D_PROP_TYPE_EFFE is not null then CONCAT(LEFT(D_PROP_TYPE_EFFE,4),'-',SUBSTRING(D_PROP_TYPE_EFFE,5,2),'-',RIGHT(D_PROP_TYPE_EFFE,2)) else CONCAT(LEFT(D_PROP_RATE_CANC,4),'-',SUBSTRING(D_PROP_RATE_CANC,5,2),'-',RIGHT(D_PROP_RATE_CANC,2)) end as propertyTypeEffectiveFrom
# MAGIC ,C_RATA_TYPE as rateabilityTypeCode
# MAGIC ,f.rateabilityType as rateabilityType
# MAGIC ,Q_RESI_PORT as residentialPortionCount
# MAGIC ,case when F_RATE_INCL = 'R' then 'true' else 'false' end as hasIncludedRatings
# MAGIC ,case when F_RATE_INCL = 'I' then 'true' else 'false' end as isIncludedInOtherRating
# MAGIC ,case when F_METE_INCL ='M' then 'true' else 'false' end as meterServesOtherProperties
# MAGIC ,case when F_METE_INCL ='L' then 'true' else 'false' end as hasMeterOnOtherProperty
# MAGIC ,case when F_SPEC_METE_ALLO ='1' then 'true' when F_SPEC_METE_ALLO = '0' then 'false' when F_SPEC_METE_ALLO is null then 'false' else F_SPEC_METE_ALLO end as hasSpecialMeterAllocation
# MAGIC ,case when F_FREE_SUPP ='K' then 'true' else 'false' end as hasKidneyFreeSupply 
# MAGIC ,case when F_FREE_SUPP ='Y' then 'true' else 'false' end as hasNonKidneyFreeSupply
# MAGIC ,cast(F_SPEC_PROP_DESC as boolean) as hasSpecialPropertyDescription
# MAGIC ,C_SEWE_USAG_TYPE as sewerUsageTypeCode
# MAGIC ,Q_PROP_METE as propertyMeterCount
# MAGIC ,cast(Q_PROP_AREA as decimal(9,3)) as propertyArea
# MAGIC ,C_PROP_AREA_TYPE as propertyAreaTypeCode
# MAGIC ,A_PROP_PURC_PRIC as purchasePrice
# MAGIC ,case when D_PROP_SALE_SETT = '00000000' then null
# MAGIC when D_PROP_SALE_SETT = '00181029' then '2018-10-29' 
# MAGIC when D_PROP_SALE_SETT <> 'null' or D_PROP_SALE_SETT <> '00000000' then CONCAT(LEFT(D_PROP_SALE_SETT,4),'-',SUBSTRING(D_PROP_SALE_SETT,5,2),'-',RIGHT(D_PROP_SALE_SETT,2)) 
# MAGIC --CONCAT('2018-',SUBSTRING(D_PROP_SALE_SETT,5,2),'-',RIGHT(D_PROP_SALE_SETT,2)) 
# MAGIC else D_PROP_SALE_SETT end as settlementDate
# MAGIC ,case when D_CNTR <> 'null' then CONCAT(LEFT(D_CNTR,4),'-',SUBSTRING(D_CNTR,5,2),'-',RIGHT(D_CNTR,2)) else D_CNTR end as contractDate
# MAGIC ,C_EXTR_LOT as extraLotCode
# MAGIC ,case when D_PROP_UPDA = '00000000' then null
# MAGIC when D_PROP_UPDA <> 'null' or D_PROP_UPDA <> '00000000' then CONCAT(LEFT(D_PROP_UPDA,4),'-',SUBSTRING(D_PROP_UPDA,5,2),'-',RIGHT(D_PROP_UPDA,2)) else D_PROP_UPDA end as propertyUpdatedDate
# MAGIC ,T_PROP_LOT as lotDescription
# MAGIC ,C_USER_CREA as createdByUserID
# MAGIC ,C_PLAN_CREA as createdByPlan
# MAGIC ,cast(to_unix_timestamp(H_CREA, 'yyyy-MM-dd hh:mm:ss a') as timestamp) as createdTimestamp
# MAGIC ,case when substr(hex(C_USER_MODI),1,2) = '00' then ' ' else C_USER_MODI end as modifiedByUserID
# MAGIC ,C_PLAN_MODI as modifiedByPlan
# MAGIC ,cast(to_unix_timestamp(H_MODI, 'yyyy-MM-dd hh:mm:ss a') as timestamp) as modifiedTimestamp
# MAGIC 
# MAGIC --2008-05-17T07:00:00.000+0000 2008-05-17 07:00:00 AM
# MAGIC --case when D_RATA_TYPE_EFFE <> 'null' then CONCAT(LEFT(D_RATA_TYPE_EFFE,4),'-',SUBSTRING(D_RATA_TYPE_EFFE,5,2),'-',RIGHT(D_RATA_TYPE_EFFE,2)) else D_RATA_TYPE_EFFE end as rateabilityTypeEffectiveDate,
# MAGIC from source a
# MAGIC left join cleansed.access_z309_tlocalgovt b
# MAGIC on b.LGAcode = a.c_lga
# MAGIC left join cleansed.access_z309_tproptype d
# MAGIC on d.propertytypecode = a.c_prop_type
# MAGIC left join cleansed.access_z309_tproptype e
# MAGIC on e.propertytypecode = d.superiorpropertytypecode
# MAGIC left join cleansed.access_z309_TRataType f
# MAGIC on f.rateabilityTypeCode = a.c_rata_type )a
