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

# DBTITLE 1,[Source] after applying mapping
# MAGIC %sql
# MAGIC select
# MAGIC N_PROP as propertyNumber,
# MAGIC N_PROP_METE as propertyMeterNumber,
# MAGIC N_METE_MAKE as meterMakerNumber,
# MAGIC C_METE_TYPE as meterSizeCode,
# MAGIC concat(b.metersize,b.metersizeunit) as metersize,
# MAGIC case when C_METE_POSI_STAT = 'M' then 'true' else 'false' end as isMasterMeter,
# MAGIC case when C_METE_POSI_STAT = 'C' then 'true' else 'false' end as isCheckMeter,
# MAGIC case when C_METE_POSI_STAT = 'A' then 'true' else 'false' end as allowAlso,
# MAGIC case when F_METE_CONN is null then 'true'
# MAGIC when F_METE_CONN = 'D' then 'false'
# MAGIC else F_METE_CONN end as isMeterConnected,
# MAGIC C_METE_READ_FREQ as meterReadingFrequencyCode,
# MAGIC C_METE_CLAS as meterClassCode,
# MAGIC c.meterclass as meterClass,
# MAGIC c.waterMeterType as waterMeterType,
# MAGIC C_METE_CATE as meterCategoryCode,
# MAGIC d.meterCategory as meterCategory, 
# MAGIC C_METE_GROU as meterGroupCode,
# MAGIC upper(e.meterGroup) as meterGroup,
# MAGIC C_METE_READ_LOCA as meterReadingLocationCode,
# MAGIC N_METE_READ_ROUT as meterReadingRouteNumber,
# MAGIC T_METE_SERV as meterServes,
# MAGIC C_METE_GRID_LOCA as meterGridLocationCode,
# MAGIC C_READ_INST_NUM1 as readingInstructionCode1,
# MAGIC C_READ_INST_NUM2 as readingInstructionCode2,
# MAGIC case when F_METE_ADDI_DESC = '1' then 'true' when F_METE_ADDI_DESC = '0' then 'false' else F_METE_ADDI_DESC end as hasAdditionalDescription,
# MAGIC case when F_METE_ROUT_PREP = '1' then 'true' when F_METE_ROUT_PREP = '0' then 'false' else F_METE_ROUT_PREP end as hasMeterRoutePreparation,
# MAGIC case when F_METE_WARN_NOTE= '1' then 'true' when F_METE_WARN_NOTE = '0' then 'false' else F_METE_WARN_NOTE end as hasMeterWarningNote,
# MAGIC case when D_METE_FIT <> 'null' then CONCAT(LEFT(D_METE_FIT,4),'-',SUBSTRING(D_METE_FIT,5,2),'-',RIGHT(D_METE_FIT,2)) else D_METE_FIT end as meterFittedDate,
# MAGIC N_METE_READ_SEQU as meterReadingSequenceNumber,
# MAGIC C_METE_CHAN_REAS as meterChangeReasonCode,
# MAGIC N_METE_CHAN_ADVI as meterChangeAdviceNumber,
# MAGIC case when D_METE_REMO <> 'null' then CONCAT(LEFT(D_METE_REMO,4),'-',SUBSTRING(D_METE_REMO,5,2),'-',RIGHT(D_METE_REMO,2)) else D_METE_REMO end as meterRemovedDate,
# MAGIC case when D_PROP_METE_UPDA <> 'null' then CONCAT(LEFT(D_PROP_METE_UPDA,4),'-',SUBSTRING(D_PROP_METE_UPDA,5,2),'-',RIGHT(D_PROP_METE_UPDA,2)) else D_PROP_METE_UPDA end as propertyMeterUpdatedDate
# MAGIC from source a
# MAGIC left join cleansed.t_access_z309_tmetertype b
# MAGIC on a.C_METE_TYPE = b.metertypecode
# MAGIC left join cleansed.t_access_z309_tmeterclass c
# MAGIC on a.c_mete_clas = c.meterClassCode 
# MAGIC left join cleansed.t_access_z309_tmetercategory d
# MAGIC on d.metercategorycode = a.c_mete_cate
# MAGIC left join cleansed.t_access_z309_tmeterGroup e
# MAGIC on e.metergroupcode = a.c_mete_grou
# MAGIC where N_PROP = '5789968'
# MAGIC 
# MAGIC --,CONCAT(LEFT (H_MODI,10),'T',SUBSTRING(H_MODI,12,8),'.000+0000') as modifiedTimestamp
# MAGIC --2008-05-17T07:00:00.000+0000 2008-05-17 07:00:00 AM
# MAGIC --case when D_RATA_TYPE_EFFE <> 'null' then CONCAT(LEFT(D_RATA_TYPE_EFFE,4),'-',SUBSTRING(D_RATA_TYPE_EFFE,5,2),'-',RIGHT(D_RATA_TYPE_EFFE,2)) else D_RATA_TYPE_EFFE end as rateabilityTypeEffectiveDate,

# COMMAND ----------

# DBTITLE 1,[Target] displaying records
# MAGIC %sql
# MAGIC select 
# MAGIC *
# MAGIC from cleansed.t_access_z309_tpropmeter where propertyNumber = '5789968'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from 
# MAGIC cleansed.t_access_z309_tmeterGroup

# COMMAND ----------

# DBTITLE 1,[Verification]Duplicate checks
# MAGIC %sql
# MAGIC SELECT propertyNumber,propertyMeterNumber, COUNT (*) as count
# MAGIC FROM cleansed.t_access_z309_tpropmeter
# MAGIC GROUP BY propertyNumber, propertyMeterNumber
# MAGIC HAVING COUNT (*) > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Records count check
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from cleansed.t_access_z309_tpropmeter
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from Source

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Source and Target Data
# MAGIC %sql
# MAGIC select
# MAGIC N_PROP as propertyNumber,
# MAGIC N_PROP_METE as propertyMeterNumber,
# MAGIC N_METE_MAKE as meterMakerNumber,
# MAGIC C_METE_TYPE as meterSizeCode,
# MAGIC concat(b.metersize,b.metersizeunit) as metersize,
# MAGIC case when C_METE_POSI_STAT = 'M' then 'true' else 'false' end as isMasterMeter,
# MAGIC case when C_METE_POSI_STAT = 'C' then 'true' else 'false' end as isCheckMeter,
# MAGIC case when C_METE_POSI_STAT = 'A' then 'true' else 'false' end as allowAlso,
# MAGIC case when F_METE_CONN is null then 'true'
# MAGIC when F_METE_CONN = 'D' then 'false'
# MAGIC else F_METE_CONN end as isMeterConnected,
# MAGIC C_METE_READ_FREQ as meterReadingFrequencyCode,
# MAGIC C_METE_CLAS as meterClassCode,
# MAGIC c.meterclass as meterClass,
# MAGIC c.waterMeterType as waterMeterType,
# MAGIC C_METE_CATE as meterCategoryCode,
# MAGIC upper(d.meterCategory) as meterCategory, 
# MAGIC C_METE_GROU as meterGroupCode,
# MAGIC upper(e.meterGroup) as meterGroup,
# MAGIC C_METE_READ_LOCA as meterReadingLocationCode,
# MAGIC N_METE_READ_ROUT as meterReadingRouteNumber,
# MAGIC upper(T_METE_SERV) as meterServes,
# MAGIC C_METE_GRID_LOCA as meterGridLocationCode,
# MAGIC C_READ_INST_NUM1 as readingInstructionCode1,
# MAGIC C_READ_INST_NUM2 as readingInstructionCode2,
# MAGIC case when F_METE_ADDI_DESC = '1' then 'true' when F_METE_ADDI_DESC = '0' then 'false' else F_METE_ADDI_DESC end as hasAdditionalDescription,
# MAGIC case when F_METE_ROUT_PREP = '1' then 'true' when F_METE_ROUT_PREP = '0' then 'false' else F_METE_ROUT_PREP end as hasMeterRoutePreparation,
# MAGIC case when F_METE_WARN_NOTE= '1' then 'true' when F_METE_WARN_NOTE = '0' then 'false' else F_METE_WARN_NOTE end as hasMeterWarningNote,
# MAGIC case when D_METE_FIT <> 'null' then CONCAT(LEFT(D_METE_FIT,4),'-',SUBSTRING(D_METE_FIT,5,2),'-',RIGHT(D_METE_FIT,2)) else D_METE_FIT end as meterFittedDate,
# MAGIC N_METE_READ_SEQU as meterReadingSequenceNumber,
# MAGIC C_METE_CHAN_REAS as meterChangeReasonCode,
# MAGIC N_METE_CHAN_ADVI as meterChangeAdviceNumber,
# MAGIC case when D_METE_REMO <> 'null' then CONCAT(LEFT(D_METE_REMO,4),'-',SUBSTRING(D_METE_REMO,5,2),'-',RIGHT(D_METE_REMO,2)) else D_METE_REMO end as meterRemovedDate,
# MAGIC case when D_PROP_METE_UPDA <> 'null' then CONCAT(LEFT(D_PROP_METE_UPDA,4),'-',SUBSTRING(D_PROP_METE_UPDA,5,2),'-',RIGHT(D_PROP_METE_UPDA,2)) else D_PROP_METE_UPDA end as propertyMeterUpdatedDate
# MAGIC from source a
# MAGIC left join cleansed.t_access_z309_tmetertype b
# MAGIC on a.C_METE_TYPE = b.metertypecode
# MAGIC left join cleansed.t_access_z309_tmeterclass c
# MAGIC on a.c_mete_clas = c.meterClassCode 
# MAGIC left join cleansed.t_access_z309_tmetercategory d
# MAGIC on d.metercategorycode = a.c_mete_cate
# MAGIC left join cleansed.t_access_z309_tmeterGroup e
# MAGIC on e.metergroupcode = a.c_mete_grou
# MAGIC 
# MAGIC except
# MAGIC select 
# MAGIC propertyNumber
# MAGIC ,propertyMeterNumber
# MAGIC ,meterMakerNumber
# MAGIC ,meterSizeCode
# MAGIC ,meterSize
# MAGIC ,cast(isMasterMeter as string)
# MAGIC ,cast(isCheckMeter as string)
# MAGIC ,cast(allowAlso as string)
# MAGIC ,cast(isMeterConnected as string)
# MAGIC ,meterReadingFrequencyCode
# MAGIC ,meterClassCode
# MAGIC ,meterClass
# MAGIC ,waterMeterType
# MAGIC ,meterCategoryCode
# MAGIC ,upper(meterCategory)
# MAGIC ,meterGroupCode
# MAGIC ,upper(meterGroup)
# MAGIC ,meterReadingLocationCode
# MAGIC ,meterReadingRouteNumber
# MAGIC ,upper(meterServes) as meterServes
# MAGIC ,meterGridLocationCode
# MAGIC ,readingInstructionCode1
# MAGIC ,readingInstructionCode2
# MAGIC ,cast(hasAdditionalDescription as string)
# MAGIC ,cast(hasMeterRoutePreparation as string)
# MAGIC ,cast(hasMeterWarningNote as string)
# MAGIC ,meterFittedDate
# MAGIC ,meterReadingSequenceNumber
# MAGIC ,meterChangeReasonCode
# MAGIC ,meterChangeAdviceNumber
# MAGIC ,meterRemovedDate
# MAGIC ,propertyMeterUpdatedDate
# MAGIC from cleansed.t_access_z309_tpropmeter

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Target and Source Data
# MAGIC %sql
# MAGIC select 
# MAGIC propertyNumber
# MAGIC ,propertyMeterNumber
# MAGIC ,meterMakerNumber
# MAGIC ,meterSizeCode
# MAGIC ,meterSize
# MAGIC ,cast(isMasterMeter as string)
# MAGIC ,cast(isCheckMeter as string)
# MAGIC ,cast(allowAlso as string)
# MAGIC ,cast(isMeterConnected as string)
# MAGIC ,meterReadingFrequencyCode
# MAGIC ,meterClassCode
# MAGIC ,meterClass
# MAGIC ,waterMeterType
# MAGIC ,meterCategoryCode
# MAGIC ,upper(meterCategory)
# MAGIC ,meterGroupCode
# MAGIC ,upper(meterGroup)
# MAGIC ,meterReadingLocationCode
# MAGIC ,meterReadingRouteNumber
# MAGIC ,upper(meterServes) as meterServes
# MAGIC ,meterGridLocationCode
# MAGIC ,readingInstructionCode1
# MAGIC ,readingInstructionCode2
# MAGIC ,cast(hasAdditionalDescription as string)
# MAGIC ,cast(hasMeterRoutePreparation as string)
# MAGIC ,cast(hasMeterWarningNote as string)
# MAGIC ,meterFittedDate
# MAGIC ,meterReadingSequenceNumber
# MAGIC ,meterChangeReasonCode
# MAGIC ,meterChangeAdviceNumber
# MAGIC ,meterRemovedDate
# MAGIC ,propertyMeterUpdatedDate
# MAGIC from cleansed.t_access_z309_tpropmeter
# MAGIC except
# MAGIC select
# MAGIC N_PROP as propertyNumber,
# MAGIC N_PROP_METE as propertyMeterNumber,
# MAGIC N_METE_MAKE as meterMakerNumber,
# MAGIC C_METE_TYPE as meterSizeCode,
# MAGIC concat(b.metersize,b.metersizeunit) as metersize,
# MAGIC case when C_METE_POSI_STAT = 'M' then 'true' else 'false' end as isMasterMeter,
# MAGIC case when C_METE_POSI_STAT = 'C' then 'true' else 'false' end as isCheckMeter,
# MAGIC case when C_METE_POSI_STAT = 'A' then 'true' else 'false' end as allowAlso,
# MAGIC case when F_METE_CONN is null then 'true'
# MAGIC when F_METE_CONN = 'D' then 'false'
# MAGIC else F_METE_CONN end as isMeterConnected,
# MAGIC C_METE_READ_FREQ as meterReadingFrequencyCode,
# MAGIC C_METE_CLAS as meterClassCode,
# MAGIC c.meterclass as meterClass,
# MAGIC c.waterMeterType as waterMeterType,
# MAGIC C_METE_CATE as meterCategoryCode,
# MAGIC upper(d.meterCategory) as meterCategory, 
# MAGIC C_METE_GROU as meterGroupCode,
# MAGIC upper(e.meterGroup) as meterGroup,
# MAGIC C_METE_READ_LOCA as meterReadingLocationCode,
# MAGIC N_METE_READ_ROUT as meterReadingRouteNumber,
# MAGIC upper(T_METE_SERV) as meterServes,
# MAGIC C_METE_GRID_LOCA as meterGridLocationCode,
# MAGIC C_READ_INST_NUM1 as readingInstructionCode1,
# MAGIC C_READ_INST_NUM2 as readingInstructionCode2,
# MAGIC case when F_METE_ADDI_DESC = '1' then 'true' when F_METE_ADDI_DESC = '0' then 'false' else F_METE_ADDI_DESC end as hasAdditionalDescription,
# MAGIC case when F_METE_ROUT_PREP = '1' then 'true' when F_METE_ROUT_PREP = '0' then 'false' else F_METE_ROUT_PREP end as hasMeterRoutePreparation,
# MAGIC case when F_METE_WARN_NOTE= '1' then 'true' when F_METE_WARN_NOTE = '0' then 'false' else F_METE_WARN_NOTE end as hasMeterWarningNote,
# MAGIC case when D_METE_FIT <> 'null' then CONCAT(LEFT(D_METE_FIT,4),'-',SUBSTRING(D_METE_FIT,5,2),'-',RIGHT(D_METE_FIT,2)) else D_METE_FIT end as meterFittedDate,
# MAGIC N_METE_READ_SEQU as meterReadingSequenceNumber,
# MAGIC C_METE_CHAN_REAS as meterChangeReasonCode,
# MAGIC N_METE_CHAN_ADVI as meterChangeAdviceNumber,
# MAGIC case when D_METE_REMO <> 'null' then CONCAT(LEFT(D_METE_REMO,4),'-',SUBSTRING(D_METE_REMO,5,2),'-',RIGHT(D_METE_REMO,2)) else D_METE_REMO end as meterRemovedDate,
# MAGIC case when D_PROP_METE_UPDA <> 'null' then CONCAT(LEFT(D_PROP_METE_UPDA,4),'-',SUBSTRING(D_PROP_METE_UPDA,5,2),'-',RIGHT(D_PROP_METE_UPDA,2)) else D_PROP_METE_UPDA end as propertyMeterUpdatedDate
# MAGIC from source a
# MAGIC left join cleansed.t_access_z309_tmetertype b
# MAGIC on a.C_METE_TYPE = b.metertypecode
# MAGIC left join cleansed.t_access_z309_tmeterclass c
# MAGIC on a.c_mete_clas = c.meterClassCode 
# MAGIC left join cleansed.t_access_z309_tmetercategory d
# MAGIC on d.metercategorycode = a.c_mete_cate
# MAGIC left join cleansed.t_access_z309_tmeterGroup e
# MAGIC on e.metergroupcode = a.c_mete_grou
