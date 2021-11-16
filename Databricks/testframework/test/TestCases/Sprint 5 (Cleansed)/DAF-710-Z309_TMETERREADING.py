# Databricks notebook source
# DBTITLE 1,[Config] Connection Setup
storage_account_name = "sadaftest01"
storage_account_access_key = dbutils.secrets.get(scope="Test-Access",key="test-datalake-key")
container_name = "raw"
file_location = "wasbs://raw@sadaftest01.blob.core.windows.net/landing/accessarchive/Z309_TMETERREADING.csv"
file_type = "csv"
print(storage_account_name)

# COMMAND ----------

spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
  storage_account_access_key)


# COMMAND ----------

# DBTITLE 1,[Source] loading to a dataframe
 df = spark.read.format("csv").option('delimiter','|').option('header','true').load("wasbs://raw@sadaftest01.blob.core.windows.net/landing/accessarchive/Z309_TMETERREADING.csv")

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.createOrReplaceTempView("Source")

# COMMAND ----------

# DBTITLE 1,[Source] displaying records
# MAGIC %sql
# MAGIC select * from Source

# COMMAND ----------

# DBTITLE 1,[Source] displaying records after mapping
# MAGIC %sql
# MAGIC select
# MAGIC cast(N_PROP as int) AS propertyNumber,
# MAGIC cast(N_PROP_METE as int) AS propertyMeterNumber,
# MAGIC cast(N_METE_READ as int) AS meterReadingNumber,
# MAGIC C_METE_READ_TOLE AS meterReadingToleranceCode,
# MAGIC C_METE_READ_TYPE AS meterReadingTypeCode,
# MAGIC initcap(e.meterReadingType) as meterReadingType,
# MAGIC C_METE_READ_CONS AS consumptionTypeCode,
# MAGIC C_METE_READ_STAT AS meterReadingStatusCode,
# MAGIC initcap(g.meterReadingStatus) as meterReadingStatus,
# MAGIC initcap(h.cannotReadReason) as cannotReadReason,
# MAGIC coalesce(C_METE_CANT_READ,'') AS cannotReadCode,
# MAGIC C_PDE_READ_METH AS PDEReadingMethodCode,
# MAGIC initcap(i.PDEReadingMethod) as PDEReadingMethod,
# MAGIC cast(Q_METE_READ as decimal(9,0)) AS meterReading,
# MAGIC cast(unix_timestamp(D_METE_READ||case when substr(trim(T_METE_READ_TIME),-1) = '-' then substr(T_METE_READ_TIME,1,5)||'0' 
# MAGIC                                               when substr(T_METE_READ_TIME,1,2) not between '00' and '23' or 
# MAGIC                                                    substr(T_METE_READ_TIME,3,2) not between '00' and '59' or 
# MAGIC                                                    substr(T_METE_READ_TIME,5,2) not between '00' and '59' then '120000' 
# MAGIC                                               when T_METE_READ_TIME is null then '120000' 
# MAGIC                                                                             else T_METE_READ_TIME end, 'yyyyMMddHHmmss') as Timestamp) as meterReadingTimestamp,
# MAGIC cast(Q_METE_READ_CONS as decimal(9,0)) AS meterReadingConsumption,
# MAGIC date_sub(to_date(D_METE_READ,'yyyyMMdd'),int(Q_METE_READ_DAYS) - 1) as readingFromDate,
# MAGIC to_date(D_METE_READ,'yyyyMMdd') as readingToDate,
# MAGIC cast(Q_METE_READ_DAYS as decimal(7,0)) AS meterReadingDays,
# MAGIC case when F_READ_COMM_CODE = '1' then true else false end AS hasReadingCommentCode, 
# MAGIC case when F_READ_COMM_FREE = '1' then true else false end AS hasReadingCommentFreeFormat,
# MAGIC cast(Q_PDE_HIGH_LOW as int) AS PDEHighLow,
# MAGIC cast(Q_PDE_REEN_COUN as int) AS PDEReenteredCount,
# MAGIC case when F_PDE_AUXI_READ = 'M' then true else false end AS isPDEAuxilaryReading,
# MAGIC to_date(D_METE_READ_UPDA, 'yyyyMMdd') AS meterReadingUpdatedDate
# MAGIC from
# MAGIC Source a
# MAGIC left join cleansed.t_access_z309_tmetereadtole d
# MAGIC on d.meterReadingToleranceCode = a.C_METE_READ_TOLE
# MAGIC left join cleansed.t_access_z309_tmetereadtype e
# MAGIC on e.meterReadingTypeCode = a.C_METE_READ_TYPE
# MAGIC left join cleansed.t_access_z309_tmetereadcontyp f
# MAGIC on f.consumptionTypeCode = a.C_METE_READ_CONS
# MAGIC left join cleansed.t_access_z309_tmrstatustype g
# MAGIC on g.meterReadingStatusCode = a.C_METE_READ_STAT
# MAGIC left join cleansed.t_access_z309_tmetercantread h 
# MAGIC on h.cannotReadCode = coalesce(a.C_METE_CANT_READ,'')
# MAGIC left join cleansed.t_access_z309_tpdereadmeth i
# MAGIC on i.PDEReadingMethodCode = a.C_PDE_READ_METH

# COMMAND ----------

# DBTITLE 1,[Target] displaying records
# MAGIC %sql
# MAGIC select * from cleansed.t_access_z309_tmeterreading

# COMMAND ----------

# DBTITLE 0,[Result] Load Count Result into DataFrame
cleansedf = spark.sql("select * from cleansed.t_access_z309_tmeterreading")

# COMMAND ----------

# DBTITLE 1,[Target] Schema Check
cleansedf.printSchema()

# COMMAND ----------

cleansedf.createOrReplaceTempView("Target")

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate checks
# MAGIC %sql
# MAGIC SELECT 
# MAGIC propertyNumber,propertyMeterNumber, meterReadingNumber,COUNT (*) as count
# MAGIC FROM cleansed.t_access_z309_tmeterreading
# MAGIC GROUP BY propertyNumber,propertyMeterNumber,meterReadingNumber
# MAGIC HAVING COUNT (*) > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Records count check
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from cleansed.t_access_z309_tmeterreading
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from Source

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Source and Target Data
# MAGIC %sql
# MAGIC select
# MAGIC cast(N_PROP as int) AS propertyNumber,
# MAGIC cast(N_PROP_METE as int) AS propertyMeterNumber,
# MAGIC cast(N_METE_READ as int) AS meterReadingNumber,
# MAGIC C_METE_READ_TOLE AS meterReadingToleranceCode,
# MAGIC C_METE_READ_TYPE AS meterReadingTypeCode,
# MAGIC initcap(e.meterReadingType) as meterReadingType,
# MAGIC C_METE_READ_CONS AS consumptionTypeCode,
# MAGIC C_METE_READ_STAT AS meterReadingStatusCode,
# MAGIC initcap(g.meterReadingStatus) as meterReadingStatus,
# MAGIC initcap(h.cannotReadReason) as cannotReadReason,
# MAGIC coalesce(C_METE_CANT_READ,'') AS cannotReadCode,
# MAGIC C_PDE_READ_METH AS PDEReadingMethodCode,
# MAGIC initcap(i.PDEReadingMethod) as PDEReadingMethod,
# MAGIC cast(Q_METE_READ as decimal(9,0)) AS meterReading,
# MAGIC cast(unix_timestamp(D_METE_READ||case when substr(trim(T_METE_READ_TIME),-1) = '-' then substr(T_METE_READ_TIME,1,5)||'0' 
# MAGIC                                               when substr(T_METE_READ_TIME,1,2) not between '00' and '23' or 
# MAGIC                                                    substr(T_METE_READ_TIME,3,2) not between '00' and '59' or 
# MAGIC                                                    substr(T_METE_READ_TIME,5,2) not between '00' and '59' then '120000' 
# MAGIC                                               when T_METE_READ_TIME is null then '120000' 
# MAGIC                                                                             else T_METE_READ_TIME end, 'yyyyMMddHHmmss') as Timestamp) as meterReadingTimestamp,
# MAGIC cast(Q_METE_READ_CONS as decimal(9,0)) AS meterReadingConsumption,
# MAGIC date_sub(to_date(D_METE_READ,'yyyyMMdd'),int(Q_METE_READ_DAYS) - 1) as readingFromDate,
# MAGIC to_date(D_METE_READ,'yyyyMMdd') as readingToDate,
# MAGIC cast(Q_METE_READ_DAYS as decimal(7,0)) AS meterReadingDays,
# MAGIC case when F_READ_COMM_CODE = '1' then true else false end AS hasReadingCommentCode, 
# MAGIC case when F_READ_COMM_FREE = '1' then true else false end AS hasReadingCommentFreeFormat,
# MAGIC cast(Q_PDE_HIGH_LOW as int) AS PDEHighLow,
# MAGIC cast(Q_PDE_REEN_COUN as int) AS PDEReenteredCount,
# MAGIC case when F_PDE_AUXI_READ = 'M' then true else false end AS isPDEAuxilaryReading,
# MAGIC to_date(D_METE_READ_UPDA, 'yyyyMMdd') AS meterReadingUpdatedDate
# MAGIC from
# MAGIC Source a
# MAGIC left join cleansed.t_access_z309_tmetereadtole d
# MAGIC on d.meterReadingToleranceCode = a.C_METE_READ_TOLE
# MAGIC left join cleansed.t_access_z309_tmetereadtype e
# MAGIC on e.meterReadingTypeCode = a.C_METE_READ_TYPE
# MAGIC left join cleansed.t_access_z309_tmetereadcontyp f
# MAGIC on f.consumptionTypeCode = a.C_METE_READ_CONS
# MAGIC left join cleansed.t_access_z309_tmrstatustype g
# MAGIC on g.meterReadingStatusCode = a.C_METE_READ_STAT
# MAGIC left join cleansed.t_access_z309_tmetercantread h 
# MAGIC on h.cannotReadCode = coalesce(a.C_METE_CANT_READ,'')
# MAGIC left join cleansed.t_access_z309_tpdereadmeth i
# MAGIC on i.PDEReadingMethodCode = a.C_PDE_READ_METH
# MAGIC 
# MAGIC except
# MAGIC select
# MAGIC propertyNumber,
# MAGIC propertyMeterNumber,
# MAGIC meterReadingNumber,
# MAGIC meterReadingToleranceCode,
# MAGIC meterReadingTypeCode,
# MAGIC meterReadingType,
# MAGIC consumptionTypeCode,
# MAGIC meterReadingStatusCode,
# MAGIC meterReadingStatus,
# MAGIC cannotReadReason,
# MAGIC cannotReadCode,
# MAGIC PDEReadingMethodCode,
# MAGIC PDEReadingMethod,
# MAGIC meterReading,
# MAGIC meterReadingTimestamp,
# MAGIC meterReadingConsumption,
# MAGIC readingFromDate,
# MAGIC readingToDate,
# MAGIC meterReadingDays,
# MAGIC hasReadingCommentCode,
# MAGIC hasReadingCommentFreeFormat,
# MAGIC PDEHighLow,
# MAGIC PDEReenteredCount,
# MAGIC isPDEAuxilaryReading,
# MAGIC meterReadingUpdatedDate
# MAGIC FROM cleansed.t_access_z309_tmeterreading

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Target and Source Data
# MAGIC %sql
# MAGIC select
# MAGIC propertyNumber,
# MAGIC propertyMeterNumber,
# MAGIC meterReadingNumber,
# MAGIC meterReadingToleranceCode,
# MAGIC meterReadingTypeCode,
# MAGIC meterReadingType,
# MAGIC consumptionTypeCode,
# MAGIC meterReadingStatusCode,
# MAGIC meterReadingStatus,
# MAGIC cannotReadReason,
# MAGIC cannotReadCode,
# MAGIC PDEReadingMethodCode,
# MAGIC PDEReadingMethod,
# MAGIC meterReading,
# MAGIC meterReadingTimestamp,
# MAGIC meterReadingConsumption,
# MAGIC readingFromDate,
# MAGIC readingToDate,
# MAGIC meterReadingDays,
# MAGIC hasReadingCommentCode,
# MAGIC hasReadingCommentFreeFormat,
# MAGIC PDEHighLow,
# MAGIC PDEReenteredCount,
# MAGIC isPDEAuxilaryReading,
# MAGIC meterReadingUpdatedDate
# MAGIC FROM cleansed.t_access_z309_tmeterreading
# MAGIC except
# MAGIC select
# MAGIC cast(N_PROP as int) AS propertyNumber,
# MAGIC cast(N_PROP_METE as int) AS propertyMeterNumber,
# MAGIC cast(N_METE_READ as int) AS meterReadingNumber,
# MAGIC C_METE_READ_TOLE AS meterReadingToleranceCode,
# MAGIC C_METE_READ_TYPE AS meterReadingTypeCode,
# MAGIC initcap(e.meterReadingType) as meterReadingType,
# MAGIC C_METE_READ_CONS AS consumptionTypeCode,
# MAGIC C_METE_READ_STAT AS meterReadingStatusCode,
# MAGIC initcap(g.meterReadingStatus) as meterReadingStatus,
# MAGIC initcap(h.cannotReadReason) as cannotReadReason,
# MAGIC coalesce(C_METE_CANT_READ,'') AS cannotReadCode,
# MAGIC C_PDE_READ_METH AS PDEReadingMethodCode,
# MAGIC initcap(i.PDEReadingMethod) as PDEReadingMethod,
# MAGIC cast(Q_METE_READ as decimal(9,0)) AS meterReading,
# MAGIC cast(unix_timestamp(D_METE_READ||case when substr(trim(T_METE_READ_TIME),-1) = '-' then substr(T_METE_READ_TIME,1,5)||'0' 
# MAGIC                                               when substr(T_METE_READ_TIME,1,2) not between '00' and '23' or 
# MAGIC                                                    substr(T_METE_READ_TIME,3,2) not between '00' and '59' or 
# MAGIC                                                    substr(T_METE_READ_TIME,5,2) not between '00' and '59' then '120000' 
# MAGIC                                               when T_METE_READ_TIME is null then '120000' 
# MAGIC                                                                             else T_METE_READ_TIME end, 'yyyyMMddHHmmss') as Timestamp) as meterReadingTimestamp,
# MAGIC cast(Q_METE_READ_CONS as decimal(9,0)) AS meterReadingConsumption,
# MAGIC date_sub(to_date(D_METE_READ,'yyyyMMdd'),int(Q_METE_READ_DAYS) - 1) as readingFromDate,
# MAGIC to_date(D_METE_READ,'yyyyMMdd') as readingToDate,
# MAGIC cast(Q_METE_READ_DAYS as decimal(7,0)) AS meterReadingDays,
# MAGIC case when F_READ_COMM_CODE = '1' then true else false end AS hasReadingCommentCode, 
# MAGIC case when F_READ_COMM_FREE = '1' then true else false end AS hasReadingCommentFreeFormat,
# MAGIC cast(Q_PDE_HIGH_LOW as int) AS PDEHighLow,
# MAGIC cast(Q_PDE_REEN_COUN as int) AS PDEReenteredCount,
# MAGIC case when F_PDE_AUXI_READ = 'M' then true else false end AS isPDEAuxilaryReading,
# MAGIC to_date(D_METE_READ_UPDA, 'yyyyMMdd') AS meterReadingUpdatedDate
# MAGIC from
# MAGIC Source a
# MAGIC left join cleansed.t_access_z309_tmetereadtole d
# MAGIC on d.meterReadingToleranceCode = a.C_METE_READ_TOLE
# MAGIC left join cleansed.t_access_z309_tmetereadtype e
# MAGIC on e.meterReadingTypeCode = a.C_METE_READ_TYPE
# MAGIC left join cleansed.t_access_z309_tmetereadcontyp f
# MAGIC on f.consumptionTypeCode = a.C_METE_READ_CONS
# MAGIC left join cleansed.t_access_z309_tmrstatustype g
# MAGIC on g.meterReadingStatusCode = a.C_METE_READ_STAT
# MAGIC left join cleansed.t_access_z309_tmetercantread h 
# MAGIC on h.cannotReadCode = coalesce(a.C_METE_CANT_READ,'')
# MAGIC left join cleansed.t_access_z309_tpdereadmeth i
# MAGIC on i.PDEReadingMethodCode = a.C_PDE_READ_METH
