# Databricks notebook source
# DBTITLE 1,[Config] Connection Setup
storage_account_name = "sadaftest01"
storage_account_access_key = dbutils.secrets.get(scope="Test-Access",key="test-datalake-key")
container_name = "raw"
file_location = "wasbs://raw@sadaftest01.blob.core.windows.net/landing/accessarchive/Z309_TPROPERTY.csv"
file_type = "csv"
print(storage_account_name)

# COMMAND ----------

spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
  storage_account_access_key)


# COMMAND ----------

# DBTITLE 1,[Source] loading to a dataframe
 df = spark.read.format("csv").option('delimiter','|').option('header','true').load("wasbs://raw@sadaftest01.blob.core.windows.net/landing/accessarchive/Z309_TPROPERTY.csv")

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.createOrReplaceTempView("Source")

# COMMAND ----------

cleansedf = spark.sql("select * from cleansed.t_access_z309_tproperty")

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
# MAGIC N_PROP as propertyNumber
# MAGIC ,C_LGA as LGACode
# MAGIC ,b.LGA as LGA 
# MAGIC ,C_PROP_TYPE as propertyTypeCode
# MAGIC ,d.propertyType as propertyType
# MAGIC --,d.superiorpropertytypecode as superiorpropertytypecode --get first
# MAGIC ,e.propertyTypeCode as superiorPropertyTypeCode
# MAGIC ,e.propertyType as superiorPropertyType
# MAGIC ,case when D_PROP_TYPE_EFFE is not null then D_PROP_TYPE_EFFE else D_PROP_RATE_CANC end as propertyTypeEffectiveDate
# MAGIC ,C_RATA_TYPE as rateabilityTypeCode
# MAGIC ,f.rateabilityType as rateabilityType
# MAGIC ,Q_RESI_PORT as residentialPortionCount
# MAGIC ,case when F_RATE_INCL = 'Y' then 'true' when F_RATE_INCL = 'N' then 'false' else F_RATE_INCL end as hasIncludedRatings
# MAGIC ,case when F_VALU_INCL = 'Y' then 'true' when F_VALU_INCL = 'N' then 'false' else F_VALU_INCL end as hasIncludedValuations
# MAGIC ,case when F_METE_INCL ='Y' then 'true' when F_METE_INCL = 'N' then 'false' else F_METE_INCL end as hasIncludedMeters
# MAGIC ,case when F_SPEC_METE_ALLO ='Y' then 'true' when F_SPEC_METE_ALLO = 'N' then 'false' else F_SPEC_METE_ALLO end as hasSpecialMeterAllocation
# MAGIC 
# MAGIC ,C_STRE_GUID as streetGuideCode
# MAGIC ,N_ADDR_UNIT as unitNumber
# MAGIC ,N_ADDR_STRE as streetNumber
# MAGIC ,C_OTHE_ADDR_TYPE as otherAddressTypeCode
# MAGIC ,T_OTHE_ADDR_INFO as otherAddressInformation
# MAGIC ,N_WATE_SERV_SKET as waterServiceSketchNumber
# MAGIC ,N_SEWE_DIAG as sewerDiagramNumber
# MAGIC 
# MAGIC ,D_PROP_TYPE_EFFE as propertyTypeEffectiveDate
# MAGIC 
# MAGIC ,D_PROP_RATE_CANC as propertyRatingCancelledDate
# MAGIC 
# MAGIC ,F_FREE_SUPP as hasFreeSupply
# MAGIC ,C_SEWE_USAG_TYPE as sewerUsageTypeCode
# MAGIC ,F_SPEC_PROP_DESC as hasSpecialPropertyDescription
# MAGIC ,Q_PROP_METE as propertyMeterCount
# MAGIC ,N_LAST_STMT_ISSU as lastStatementIssuedNumber
# MAGIC ,F_CERT_NO_METE as isCertificateNumberMeter
# MAGIC ,N_SEWE_REFE_SHEE as sewerReferenceSheetNumber
# MAGIC ,Q_PROP_AREA as propertyArea 
# MAGIC ,C_PROP_AREA_TYPE as propertyAreaTypeCode
# MAGIC ,A_PROP_PURC_PRIC as purchasePriceAmount
# MAGIC ,D_PROP_SALE_SETT as settlementDate
# MAGIC ,D_CNTR as contactDate
# MAGIC ,C_EXTR_LOT as extraLotCode
# MAGIC ,D_PROP_UPDA as propertyUpdatedDate
# MAGIC ,T_PROP_LOT as lotDescription
# MAGIC ,C_USER_CREA as createdByUserID
# MAGIC ,C_PLAN_CREA as createdByPlan
# MAGIC ,H_CREA as createdTimestamp
# MAGIC ,C_USER_MODI as modifiedByUserID
# MAGIC ,C_PLAN_MODI as modifiedByPlan
# MAGIC ,H_MODI as modifiedTimestamp
# MAGIC from source a
# MAGIC left join cleansed.t_access_z309_tlocalgovt b
# MAGIC on b.LGAcode = a.c_lga
# MAGIC left join cleansed.t_access_z309_tproptype d
# MAGIC on d.propertytypecode = a.c_prop_type
# MAGIC left join cleansed.t_access_z309_tproptype e
# MAGIC on e.propertytypecode = d.superiorpropertytypecode
# MAGIC left join cleansed.t_access_z309_TRataType f
# MAGIC on f.rateabilityTypeCode = a.c_rata_type

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cleansed.t_access_z309_tproptype

# COMMAND ----------

# DBTITLE 1,[Target] displaying records
# MAGIC %sql
# MAGIC select *
# MAGIC from cleansed.t_access_z309_tproperty where propertynumber = '3100030'

# COMMAND ----------

# DBTITLE 1,[Verification]Duplicate checks
# MAGIC %sql
# MAGIC SELECT debittypecode, COUNT (*) as count
# MAGIC FROM cleansed.t_access_z309_tdebittype
# MAGIC GROUP BY debittypecode
# MAGIC HAVING COUNT (*) > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Records count check
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from cleansed.t_access_z309_tdebittype
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from Source

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Source and Target Data
# MAGIC %sql
# MAGIC select C_DEBI_TYPE as debitTypeCode,
# MAGIC T_DEBI_TYPE_ABBR as debitTypeAbbreviation,
# MAGIC T_DEBI_TYPE_FULL as debitType,
# MAGIC case when D_DEBI_TYPE_EFFE <> 'null' then CONCAT(LEFT(D_DEBI_TYPE_EFFE,4),'-',SUBSTRING(D_DEBI_TYPE_EFFE,5,2),'-',RIGHT(D_DEBI_TYPE_EFFE,2)) else D_DEBI_TYPE_EFFE end as debitTypeEffectiveDate,
# MAGIC case when D_DEBI_TYPE_CANC <> 'null' then CONCAT(LEFT(D_DEBI_TYPE_CANC,4),'-',SUBSTRING(D_DEBI_TYPE_CANC,5,2),'-',RIGHT(D_DEBI_TYPE_CANC,2)) else D_DEBI_TYPE_CANC end as debitTypeCancelledDate
# MAGIC from source
# MAGIC 
# MAGIC except
# MAGIC 
# MAGIC select debitTypeCode,
# MAGIC debitTypeAbbreviation,
# MAGIC upper(debitType),
# MAGIC debitTypeEffectiveDate,
# MAGIC debitTypeCancelledDate
# MAGIC from cleansed.t_access_z309_tdebittype

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Target and Source Data
# MAGIC %sql
# MAGIC select debitTypeCode,
# MAGIC debitTypeAbbreviation,
# MAGIC upper(debitType),
# MAGIC debitTypeEffectiveDate,
# MAGIC debitTypeCancelledDate
# MAGIC from cleansed.t_access_z309_tdebittype
# MAGIC except
# MAGIC select C_DEBI_TYPE as debitTypeCode,
# MAGIC T_DEBI_TYPE_ABBR as debitTypeAbbreviation,
# MAGIC T_DEBI_TYPE_FULL as debitType,
# MAGIC case when D_DEBI_TYPE_EFFE <> 'null' then CONCAT(LEFT(D_DEBI_TYPE_EFFE,4),'-',SUBSTRING(D_DEBI_TYPE_EFFE,5,2),'-',RIGHT(D_DEBI_TYPE_EFFE,2)) else D_DEBI_TYPE_EFFE end as debitTypeEffectiveDate,
# MAGIC case when D_DEBI_TYPE_CANC <> 'null' then CONCAT(LEFT(D_DEBI_TYPE_CANC,4),'-',SUBSTRING(D_DEBI_TYPE_CANC,5,2),'-',RIGHT(D_DEBI_TYPE_CANC,2)) else D_DEBI_TYPE_CANC end as debitTypeCancelledDate
# MAGIC from source
