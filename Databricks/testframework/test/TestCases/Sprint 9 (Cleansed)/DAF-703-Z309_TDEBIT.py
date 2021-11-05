# Databricks notebook source
# DBTITLE 1,[Config] Connection Setup
storage_account_name = "sablobdaftest01"
storage_account_access_key = dbutils.secrets.get(scope="TestScope",key="test-sablob-key")
container_name = "accessdata"
file_location = "wasbs://accessdata@sablobdaftest01.blob.core.windows.net/Z309_TDEBIT.csv"
file_type = "csv"
print(storage_account_name)

# COMMAND ----------

spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
  storage_account_access_key)


# COMMAND ----------

# DBTITLE 1,[Source] loading to a dataframe
 df = spark.read.format("csv").option('delimiter','|').option('header','true').load("wasbs://accessdata@sablobdaftest01.blob.core.windows.net/Z309_TDEBIT.csv")

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
# MAGIC C_LGA as LGACode,
# MAGIC b.LGA as LGA,
# MAGIC cast(N_PROP as int) as propertyNumber,
# MAGIC cast(N_DEBI_REFE as int) as debitReferenceNumber,
# MAGIC C_DEBI_TYPE as debitTypeCode,
# MAGIC c.debitType as debitType,
# MAGIC C_DEBI_REAS as debitReasonCode,
# MAGIC d.debitReason as debitReason,
# MAGIC cast(0 as decimal(15,2)) AS debitAmount,
# MAGIC cast(0 as decimal(15,2)) AS debitOutstandingAmount,
# MAGIC cast(0 as decimal(15,2)) AS waterAmount,
# MAGIC cast(0 as decimal(15,2)) AS sewerAmount,
# MAGIC cast(0 as decimal(15,2)) AS drainAmount,
# MAGIC to_date(D_DEFE_FROM, 'yyyyMMdd') as debitDeferredFrom,
# MAGIC to_date(D_DEFE_TO, 'yyyyMMdd') as debitDeferredTo,
# MAGIC to_date(D_DISP, 'yyyyMMdd') as dateDebitDisputed,
# MAGIC C_RECO_LEVE as recoveryLevelCode,
# MAGIC to_date(D_RECO_LEVE, 'yyyyMMdd') as dateRecoveryLevelSet,
# MAGIC case when F_OCCU = '1' then true else false end as isOwnerDebit,
# MAGIC --case when F_OCCU = '0' then false else true end as isOccupierDebit,
# MAGIC case when F_ARRE = 'Y' then true else false end as isInArrears,
# MAGIC case when N_FINA_YEAR is null then case when substr(d_debi_crea,5,2) < '07' then substr(d_debi_crea,1,4)
# MAGIC else cast(int(substr(D_DEBI_CREA,1,4)) + 1 as string) end 
# MAGIC else case when N_FINA_YEAR > '70' then '19'||N_FINA_YEAR else '20'||N_FINA_YEAR end
# MAGIC end as financialYear,
# MAGIC C_ISSU as issuedCode,
# MAGIC to_date(D_DEBI_CREA, 'yyyyMMdd') AS debitCreatedDate,
# MAGIC to_date(D_DEBI_UPDA, 'yyyyMMdd') AS debitUpdatedDate,
# MAGIC to_date(D_ORIG_ISSU, 'yyyyMMdd') AS originalIssueDate 
# MAGIC 
# MAGIC from Source a
# MAGIC left join cleansed.access_z309_tlocalgovt b
# MAGIC on b.LGAcode = a.c_lga
# MAGIC left join cleansed.access_z309_tdebittype c
# MAGIC on c.debitTypeCode = a.c_debi_type
# MAGIC left join cleansed.access_z309_tdebitreason d 
# MAGIC on  d.debitTypeCode = a.c_debi_type  and d.debitReasonCode = a.c_debi_reas 

# COMMAND ----------

# DBTITLE 1,[Target] displaying records
# MAGIC %sql
# MAGIC select * from cleansed.access_z309_tdebit

# COMMAND ----------

# DBTITLE 0,[Result] Load Count Result into DataFrame
cleansedf = spark.sql("select * from cleansed.access_z309_tdebit")

# COMMAND ----------

# DBTITLE 1,[Target] Schema Check
cleansedf.printSchema()

# COMMAND ----------

cleansedf.createOrReplaceTempView("Target")

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate checks
# MAGIC %sql
# MAGIC SELECT 
# MAGIC propertyNumber,debitReferenceNumber, COUNT (*) as count
# MAGIC FROM cleansed.access_z309_tdebit
# MAGIC GROUP BY propertyNumber,debitReferenceNumber
# MAGIC HAVING COUNT (*) > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Records count check
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from cleansed.access_z309_tdebit
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from Source

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Source and Target Data
# MAGIC %sql
# MAGIC select 
# MAGIC C_LGA as LGACode,
# MAGIC b.LGA as LGA,
# MAGIC cast(N_PROP as int) as propertyNumber,
# MAGIC cast(N_DEBI_REFE as int) as debitReferenceNumber,
# MAGIC C_DEBI_TYPE as debitTypeCode,
# MAGIC c.debitType as debitType,
# MAGIC C_DEBI_REAS as debitReasonCode,
# MAGIC d.debitReason as debitReason,
# MAGIC cast(0 as decimal(15,2)) AS debitAmount,
# MAGIC cast(0 as decimal(15,2)) AS debitOutstandingAmount,
# MAGIC cast(0 as decimal(15,2)) AS waterAmount,
# MAGIC cast(0 as decimal(15,2)) AS sewerAmount,
# MAGIC cast(0 as decimal(15,2)) AS drainAmount,
# MAGIC to_date(D_DEFE_FROM, 'yyyyMMdd') as debitDeferredFrom,
# MAGIC to_date(D_DEFE_TO, 'yyyyMMdd') as debitDeferredTo,
# MAGIC to_date(D_DISP, 'yyyyMMdd') as dateDebitDisputed,
# MAGIC C_RECO_LEVE as recoveryLevelCode,
# MAGIC to_date(D_RECO_LEVE, 'yyyyMMdd') as dateRecoveryLevelSet,
# MAGIC case when F_OCCU = '0' then true else false end as isOwnerDebit,
# MAGIC case when F_OCCU = '1' then true else false end as isOccupierDebit,
# MAGIC case when F_ARRE = 'Y' then true else false end as isInArrears,
# MAGIC case when N_FINA_YEAR is null then case when substr(d_debi_crea,5,2) < '07' then substr(d_debi_crea,1,4)
# MAGIC else cast(int(substr(D_DEBI_CREA,1,4)) + 1 as string) end 
# MAGIC else case when N_FINA_YEAR > '70' then '19'||N_FINA_YEAR else '20'||N_FINA_YEAR end
# MAGIC end as financialYear,
# MAGIC C_ISSU as issuedCode,
# MAGIC to_date(D_DEBI_CREA, 'yyyyMMdd') AS debitCreatedDate,
# MAGIC to_date(D_DEBI_UPDA, 'yyyyMMdd') AS debitUpdatedDate,
# MAGIC to_date(D_ORIG_ISSU, 'yyyyMMdd') AS originalIssueDate 
# MAGIC 
# MAGIC from Source a
# MAGIC left join cleansed.access_z309_tlocalgovt b
# MAGIC on b.LGAcode = a.c_lga
# MAGIC left join cleansed.access_z309_tdebittype c
# MAGIC on c.debitTypeCode = a.c_debi_type
# MAGIC left join cleansed.access_z309_tdebitreason d 
# MAGIC on  d.debitTypeCode = a.c_debi_type  and d.debitReasonCode = a.c_debi_reas 
# MAGIC except
# MAGIC 
# MAGIC select
# MAGIC LGACode,
# MAGIC LGA,
# MAGIC propertyNumber,
# MAGIC debitReferenceNumber,
# MAGIC debitTypeCode,
# MAGIC debitType,
# MAGIC debitReasonCode,
# MAGIC debitReason,
# MAGIC debitAmount,
# MAGIC debitOutstandingAmount,
# MAGIC waterAmount,
# MAGIC sewerAmount,
# MAGIC drainAmount,
# MAGIC debitDeferredFrom,
# MAGIC debitDeferredTo,
# MAGIC dateDebitDisputed,
# MAGIC recoveryLevelCode,
# MAGIC dateRecoveryLevelSet,
# MAGIC isOwnerDebit,
# MAGIC isOccupierDebit,
# MAGIC isInArrears,
# MAGIC financialYear,
# MAGIC issuedCode,
# MAGIC debitCreatedDate,
# MAGIC debitUpdatedDate,
# MAGIC originalIssueDate
# MAGIC FROM cleansed.access_z309_tdebit

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Target and Source Data
# MAGIC %sql
# MAGIC select
# MAGIC LGACode,
# MAGIC LGA,
# MAGIC propertyNumber,
# MAGIC debitReferenceNumber,
# MAGIC debitTypeCode,
# MAGIC debitType,
# MAGIC debitReasonCode,
# MAGIC debitReason,
# MAGIC debitAmount,
# MAGIC debitOutstandingAmount,
# MAGIC waterAmount,
# MAGIC sewerAmount,
# MAGIC drainAmount,
# MAGIC debitDeferredFrom,
# MAGIC debitDeferredTo,
# MAGIC dateDebitDisputed,
# MAGIC recoveryLevelCode,
# MAGIC dateRecoveryLevelSet,
# MAGIC isOwnerDebit,
# MAGIC isOccupierDebit,
# MAGIC isInArrears,
# MAGIC financialYear,
# MAGIC issuedCode,
# MAGIC debitCreatedDate,
# MAGIC debitUpdatedDate,
# MAGIC originalIssueDate
# MAGIC FROM cleansed.access_z309_tdebit
# MAGIC except
# MAGIC select 
# MAGIC C_LGA as LGACode,
# MAGIC b.LGA as LGA,
# MAGIC cast(N_PROP as int) as propertyNumber,
# MAGIC cast(N_DEBI_REFE as int) as debitReferenceNumber,
# MAGIC C_DEBI_TYPE as debitTypeCode,
# MAGIC c.debitType as debitType,
# MAGIC C_DEBI_REAS as debitReasonCode,
# MAGIC d.debitReason as debitReason,
# MAGIC cast(0 as decimal(15,2)) AS debitAmount,
# MAGIC cast(0 as decimal(15,2)) AS debitOutstandingAmount,
# MAGIC cast(0 as decimal(15,2)) AS waterAmount,
# MAGIC cast(0 as decimal(15,2)) AS sewerAmount,
# MAGIC cast(0 as decimal(15,2)) AS drainAmount,
# MAGIC to_date(D_DEFE_FROM, 'yyyyMMdd') as debitDeferredFrom,
# MAGIC to_date(D_DEFE_TO, 'yyyyMMdd') as debitDeferredTo,
# MAGIC to_date(D_DISP, 'yyyyMMdd') as dateDebitDisputed,
# MAGIC C_RECO_LEVE as recoveryLevelCode,
# MAGIC to_date(D_RECO_LEVE, 'yyyyMMdd') as dateRecoveryLevelSet,
# MAGIC case when F_OCCU = '0' then true else false end as isOwnerDebit,
# MAGIC case when F_OCCU = '1' then true else false end as isOccupierDebit,
# MAGIC case when F_ARRE = 'Y' then true else false end as isInArrears,
# MAGIC case when N_FINA_YEAR is null then case when substr(d_debi_crea,5,2) < '07' then substr(d_debi_crea,1,4)
# MAGIC else cast(int(substr(D_DEBI_CREA,1,4)) + 1 as string) end 
# MAGIC else case when N_FINA_YEAR > '70' then '19'||N_FINA_YEAR else '20'||N_FINA_YEAR end
# MAGIC end as financialYear,
# MAGIC C_ISSU as issuedCode,
# MAGIC to_date(D_DEBI_CREA, 'yyyyMMdd') AS debitCreatedDate,
# MAGIC to_date(D_DEBI_UPDA, 'yyyyMMdd') AS debitUpdatedDate,
# MAGIC to_date(D_ORIG_ISSU, 'yyyyMMdd') AS originalIssueDate 
# MAGIC 
# MAGIC from Source a
# MAGIC left join cleansed.access_z309_tlocalgovt b
# MAGIC on b.LGAcode = a.c_lga
# MAGIC left join cleansed.access_z309_tdebittype c
# MAGIC on c.debitTypeCode = a.c_debi_type
# MAGIC left join cleansed.access_z309_tdebitreason d 
# MAGIC on  d.debitTypeCode = a.c_debi_type  and d.debitReasonCode = a.c_debi_reas 
