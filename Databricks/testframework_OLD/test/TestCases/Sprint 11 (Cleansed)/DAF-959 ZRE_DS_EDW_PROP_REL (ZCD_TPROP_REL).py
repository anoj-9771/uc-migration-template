# Databricks notebook source
#config parameters
source = 'ISU' #either CRM or ISU
table = 'ZCD_TPROP_REL'

environment = 'test'
storage_account_name = "sablobdaftest01"
storage_account_access_key = dbutils.secrets.get(scope="TestScope",key="test-sablob-key")
containerName = "archive"


# COMMAND ----------

# MAGIC %run ../../includes/tableEvaluation

# COMMAND ----------

# DBTITLE 1,[Source] with mapping
# MAGIC %sql
# MAGIC select
# MAGIC property1Number,
# MAGIC property2Number,
# MAGIC relationshipTypeCode1,
# MAGIC relationshipType1,
# MAGIC validFromDate,
# MAGIC relationshipTypeCode2,
# MAGIC relationshipType2,
# MAGIC validToDate
# MAGIC from
# MAGIC (select
# MAGIC PROPERTY1 as property1Number
# MAGIC ,PROPERTY2 as property2Number
# MAGIC ,REL_TYPE1 as relationshipTypeCode1
# MAGIC ,b.relationshipType as relationshipType1
# MAGIC ,DATE_FROM as validFromDate
# MAGIC ,REL_TYPE2 as relationshipTypeCode2
# MAGIC ,c.relationshipType as relationshipType2
# MAGIC ,DATE_TO as validToDate
# MAGIC ,row_number() over (partition by PROPERTY1,PROPERTY2,REL_TYPE1,DATE_FROM order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table}
# MAGIC left join cleansed.ISU_ZCD_VIRELTYPTX b on b.relationshipTypeCode = REL_TYPE1
# MAGIC left join cleansed.ISU_ZCD_VIRELTYP2TX c on c.relationshipTypeCode = REL_TYPE2
# MAGIC )a where  a.rn = 1

# COMMAND ----------

# DBTITLE 1,[Verification] Count Check
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from cleansed.${vars.table}
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from (select
# MAGIC property1Number,
# MAGIC property2Number,
# MAGIC relationshipTypeCode1,
# MAGIC relationshipType1,
# MAGIC validFromDate,
# MAGIC relationshipTypeCode2,
# MAGIC relationshipType2,
# MAGIC validToDate
# MAGIC from
# MAGIC (select
# MAGIC PROPERTY1 as property1Number
# MAGIC ,PROPERTY2 as property2Number
# MAGIC ,REL_TYPE1 as relationshipTypeCode1
# MAGIC ,b.relationshipType as relationshipType1
# MAGIC ,DATE_FROM as validFromDate
# MAGIC ,REL_TYPE2 as relationshipTypeCode2
# MAGIC ,c.relationshipType as relationshipType2
# MAGIC ,DATE_TO as validToDate
# MAGIC ,row_number() over (partition by PROPERTY1,PROPERTY2,REL_TYPE1,DATE_FROM order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table}
# MAGIC left join cleansed.ISU_ZCD_VIRELTYPTX b on b.relationshipTypeCode = REL_TYPE1
# MAGIC left join cleansed.ISU_ZCD_VIRELTYP2TX c on c.relationshipTypeCode = REL_TYPE2
# MAGIC )a where  a.rn = 1)

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT property1Number,property2Number,relationshipTypeCode1,relationshipType1,
# MAGIC validFromDate,relationshipTypeCode2,relationshipType2,validToDate, COUNT (*) as count
# MAGIC FROM cleansed.${vars.table}
# MAGIC GROUP BY property1Number,property2Number,relationshipTypeCode1,relationshipType1,
# MAGIC validFromDate,relationshipTypeCode2,relationshipType2,validToDate
# MAGIC HAVING COUNT (*) > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT * FROM (
# MAGIC SELECT
# MAGIC *,
# MAGIC row_number() OVER(PARTITION BY property1Number,property2Number,relationshipTypeCode1,validFromDate 
# MAGIC order by property1Number,property2Number,relationshipTypeCode1,validFromDate) as rn
# MAGIC FROM  cleansed.${vars.table}
# MAGIC )a where a.rn > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Source and Target Data
# MAGIC %sql
# MAGIC select
# MAGIC property1Number,
# MAGIC property2Number,
# MAGIC relationshipTypeCode1,
# MAGIC relationshipType1,
# MAGIC validFromDate,
# MAGIC relationshipTypeCode2,
# MAGIC relationshipType2,
# MAGIC validToDate
# MAGIC from
# MAGIC (select
# MAGIC PROPERTY1 as property1Number
# MAGIC ,PROPERTY2 as property2Number
# MAGIC ,REL_TYPE1 as relationshipTypeCode1
# MAGIC ,b.relationshipType as relationshipType1
# MAGIC ,DATE_FROM as validFromDate
# MAGIC ,case
# MAGIC when REL_TYPE2 is null then ''
# MAGIC else REL_TYPE2 end as relationshipTypeCode2
# MAGIC ,c.relationshipType as relationshipType2
# MAGIC ,DATE_TO as validToDate
# MAGIC ,row_number() over (partition by PROPERTY1,PROPERTY2,REL_TYPE1,DATE_FROM order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table}
# MAGIC left join cleansed.ISU_ZCD_VIRELTYPTX b on b.relationshipTypeCode = REL_TYPE1
# MAGIC left join cleansed.ISU_ZCD_VIRELTYP2TX c on c.relationshipTypeCode = REL_TYPE2
# MAGIC )a where  a.rn = 1
# MAGIC except
# MAGIC select
# MAGIC property1Number,
# MAGIC property2Number,
# MAGIC relationshipTypeCode1,
# MAGIC relationshipType1,
# MAGIC validFromDate,
# MAGIC relationshipTypeCode2,
# MAGIC relationshipType2,
# MAGIC validToDate
# MAGIC from
# MAGIC cleansed.${vars.table}

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Target and Source Data
# MAGIC %sql
# MAGIC select
# MAGIC property1Number,
# MAGIC property2Number,
# MAGIC relationshipTypeCode1,
# MAGIC relationshipType1,
# MAGIC validFromDate,
# MAGIC relationshipTypeCode2,
# MAGIC relationshipType2,
# MAGIC validToDate
# MAGIC from
# MAGIC cleansed.${vars.table}
# MAGIC except
# MAGIC select
# MAGIC property1Number,
# MAGIC property2Number,
# MAGIC relationshipTypeCode1,
# MAGIC relationshipType1,
# MAGIC validFromDate,
# MAGIC relationshipTypeCode2,
# MAGIC relationshipType2,
# MAGIC validToDate
# MAGIC from
# MAGIC (select
# MAGIC PROPERTY1 as property1Number
# MAGIC ,PROPERTY2 as property2Number
# MAGIC ,REL_TYPE1 as relationshipTypeCode1
# MAGIC ,b.relationshipType as relationshipType1
# MAGIC ,DATE_FROM as validFromDate
# MAGIC ,case
# MAGIC when REL_TYPE2 is null then ''
# MAGIC else REL_TYPE2 end as relationshipTypeCode2
# MAGIC ,c.relationshipType as relationshipType2
# MAGIC ,DATE_TO as validToDate
# MAGIC ,row_number() over (partition by PROPERTY1,PROPERTY2,REL_TYPE1,DATE_FROM order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table}
# MAGIC left join cleansed.ISU_ZCD_VIRELTYPTX b on b.relationshipTypeCode = REL_TYPE1
# MAGIC left join cleansed.ISU_ZCD_VIRELTYP2TX c on c.relationshipTypeCode = REL_TYPE2
# MAGIC )a where  a.rn = 1
