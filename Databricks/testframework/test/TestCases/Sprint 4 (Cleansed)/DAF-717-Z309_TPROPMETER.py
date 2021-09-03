# Databricks notebook source
# DBTITLE 1,[Config] Connection Setup
storage_account_name = "sadaftest01"
storage_account_access_key = dbutils.secrets.get(scope="Test-Access",key="test-datalake-key")
container_name = "raw"
file_location = "wasbs://raw@sadaftest01.blob.core.windows.net/landing/accessarchive/Z309_TPROPMETER.csv"
file_type = "csv"
print(storage_account_name)

# COMMAND ----------

spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
  storage_account_access_key)


# COMMAND ----------

# DBTITLE 1,[Source] loading to a dataframe
 df = spark.read.format("csv").option('delimiter','|').option('header','true').load("wasbs://raw@sadaftest01.blob.core.windows.net/landing/accessarchive/Z309_TPROPMETER.csv")

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.createOrReplaceTempView("Source")

# COMMAND ----------

cleansedf = spark.sql("select * from cleansed.t_access_z309_tpropmeter")

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

# MAGIC %sql
# MAGIC select ismastermeter, ischeckmeter, * from cleansed.t_access_z309_tpropmeter where propertynumber = '3104380'

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct F_METE_CONN from source

# COMMAND ----------

# DBTITLE 1,[Source] after applying mapping
# MAGIC %sql
# MAGIC select
# MAGIC N_PROP as propertyNumber,
# MAGIC N_PROP_METE as propertyMeterNumber,
# MAGIC N_METE_MAKE as meterMakerNumber,
# MAGIC C_METE_TYPE as meterTypeCode,
# MAGIC concat(b.metersize,b.metersizeunit) as metersize,
# MAGIC --C_METE_POSI_STAT as meterPositionStatusCode, defect raised DAF-771
# MAGIC case when F_METE_CONN is null then true
# MAGIC when F_METER_CONN = 'D' then false
# MAGIC else F_METER_CONN end as isMeterConnected,
# MAGIC C_METE_READ_FREQ as meterReadingFrequencyCode,
# MAGIC C_METE_CLAS as meterClassCode,
# MAGIC C_WATE_METE_TYPE as waterMeterType, -- fromTMeterClass
# MAGIC C_METE_CATE as meterCategoryCode,
# MAGIC T_METE_CATE as meterCategory, --fromTMeterCategory
# MAGIC C_METE_GROU as meterGroupCode,
# MAGIC 
# MAGIC N_METE_READ_ROUT as meterReadingRouteNumber,
# MAGIC C_METE_GRID_LOCA as meterGridLocationCode,
# MAGIC C_READ_INST_NUM1 as readingInstruction1Code,
# MAGIC C_READ_INST_NUM2 as readingInstruction2Code,
# MAGIC F_METE_ADDI_DESC as hasMeterAdditionalDescription,
# MAGIC F_METE_ROUT_PREP as hasMeterRoutePreparation,
# MAGIC F_METE_WARN_NOTE as hasMeterWarningNote,
# MAGIC D_METE_FIT as meterFittedDate,
# MAGIC N_METE_READ_SEQU as meterReadingSequenceNumber,
# MAGIC 
# MAGIC C_METE_CHAN_REAS as meterChangeReasonCode,
# MAGIC N_METE_CHAN_ADVI as meterChangeAdviceNumber,
# MAGIC D_METE_REMO as meterRemovedDate,
# MAGIC 
# MAGIC D_PROP_METE_UPDA as propertyMeterUpdatedDate,
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC C_METE_READ_LOCA as meterReadingLocationCode,
# MAGIC T_METE_SERV as meterServes
# MAGIC from source a
# MAGIC left join cleansed.t_access_z309_tmetertype b
# MAGIC on a.C_METE_TYPE = b.metertypecode
# MAGIC left join cleansed.t_access_z309_tmeterclass
# MAGIC on a.
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC --,CONCAT(LEFT (H_MODI,10),'T',SUBSTRING(H_MODI,12,8),'.000+0000') as modifiedTimestamp
# MAGIC --2008-05-17T07:00:00.000+0000 2008-05-17 07:00:00 AM
# MAGIC --case when D_RATA_TYPE_EFFE <> 'null' then CONCAT(LEFT(D_RATA_TYPE_EFFE,4),'-',SUBSTRING(D_RATA_TYPE_EFFE,5,2),'-',RIGHT(D_RATA_TYPE_EFFE,2)) else D_RATA_TYPE_EFFE end as rateabilityTypeEffectiveDate,

# COMMAND ----------

# DBTITLE 1,[Target] displaying records
# MAGIC %sql
# MAGIC select 
# MAGIC *
# MAGIC from cleansed.t_access_z309_tpropmeter

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
# MAGIC C_RATA_TYPE as rateabilityTypeCode,
# MAGIC T_RATA_TYPE as rateabilityType,
# MAGIC case when D_RATA_TYPE_EFFE <> 'null' then CONCAT(LEFT(D_RATA_TYPE_EFFE,4),'-',SUBSTRING(D_RATA_TYPE_EFFE,5,2),'-',RIGHT(D_RATA_TYPE_EFFE,2)) else D_RATA_TYPE_EFFE end as rateabilityTypeEffectiveDate,
# MAGIC case when D_RATA_TYPE_CANC <> 'null' then CONCAT(LEFT(D_RATA_TYPE_CANC,4),'-',SUBSTRING(D_RATA_TYPE_CANC,5,2),'-',RIGHT(D_RATA_TYPE_CANC,2)) else D_RATA_TYPE_CANC end as rateabilityTypeCancelledDate
# MAGIC from source
# MAGIC except
# MAGIC select 
# MAGIC rateabilityTypeCode,
# MAGIC upper(rateabilityType),
# MAGIC rateabilityTypeEffectiveDate,
# MAGIC rateabilityTypeCancelledDate
# MAGIC from cleansed.t_access_z309_tratatype

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Target and Source Data
# MAGIC %sql
# MAGIC select 
# MAGIC rateabilityTypeCode,
# MAGIC upper(rateabilityType),
# MAGIC rateabilityTypeEffectiveDate,
# MAGIC rateabilityTypeCancelledDate
# MAGIC from cleansed.t_access_z309_tratatype
# MAGIC except
# MAGIC select
# MAGIC C_RATA_TYPE as rateabilityTypeCode,
# MAGIC T_RATA_TYPE as rateabilityType,
# MAGIC case when D_RATA_TYPE_EFFE <> 'null' then CONCAT(LEFT(D_RATA_TYPE_EFFE,4),'-',SUBSTRING(D_RATA_TYPE_EFFE,5,2),'-',RIGHT(D_RATA_TYPE_EFFE,2)) else D_RATA_TYPE_EFFE end as rateabilityTypeEffectiveDate,
# MAGIC case when D_RATA_TYPE_CANC <> 'null' then CONCAT(LEFT(D_RATA_TYPE_CANC,4),'-',SUBSTRING(D_RATA_TYPE_CANC,5,2),'-',RIGHT(D_RATA_TYPE_CANC,2)) else D_RATA_TYPE_CANC end as rateabilityTypeCancelledDate
# MAGIC from source
