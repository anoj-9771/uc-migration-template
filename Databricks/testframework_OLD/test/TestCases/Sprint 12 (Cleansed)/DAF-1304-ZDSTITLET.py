# Databricks notebook source
#config parameters
source = 'CRM' #either CRM or ISU
table = 'TSAD3T'

environment = 'test'
storage_account_name = "sablobdaftest01"
storage_account_access_key = dbutils.secrets.get(scope="TestScope",key="test-sablob-key")
containerName = "archive"


# COMMAND ----------

spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")

# COMMAND ----------

# MAGIC %run ../../includes/tableEvaluation

# COMMAND ----------

lakedf = spark.sql("select * from cleansed.${vars.table}")

# COMMAND ----------

lakedf.printSchema()

# COMMAND ----------

# DBTITLE 1,[Source] with mapping
# MAGIC %sql
# MAGIC select
# MAGIC titleCode
# MAGIC ,title
# MAGIC from
# MAGIC (select
# MAGIC TITLE as titleCode
# MAGIC ,TITLE_MEDI as title
# MAGIC ,row_number() over (partition by TITLE order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table}
# MAGIC )a where  a.rn = 1

# COMMAND ----------

# DBTITLE 1,[Verification] Count Check
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from cleansed.${vars.table}
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from (select
# MAGIC titleCode
# MAGIC ,title
# MAGIC from
# MAGIC (select
# MAGIC TITLE as titleCode
# MAGIC ,TITLE_MEDI as title
# MAGIC ,row_number() over (partition by TITLE order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table}
# MAGIC )a where  a.rn = 1)

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT titleCode
# MAGIC , COUNT (*) as count
# MAGIC FROM cleansed.${vars.table}
# MAGIC GROUP BY titleCode
# MAGIC HAVING COUNT (*) > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT * FROM (
# MAGIC SELECT
# MAGIC *,
# MAGIC row_number() OVER(PARTITION BY titleCode  order by titleCode) as rn
# MAGIC FROM  cleansed.${vars.table}
# MAGIC )a where a.rn > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Source and Target Data
# MAGIC %sql
# MAGIC select
# MAGIC titleCode
# MAGIC ,title
# MAGIC from
# MAGIC (select
# MAGIC TITLE as titleCode
# MAGIC ,TITLE_MEDI as title
# MAGIC ,row_number() over (partition by TITLE order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table}
# MAGIC )a where  a.rn = 1
# MAGIC except
# MAGIC select
# MAGIC titleCode
# MAGIC ,title
# MAGIC from
# MAGIC cleansed.${vars.table}

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Target and Source Data
# MAGIC %sql
# MAGIC select
# MAGIC titleCode
# MAGIC ,title
# MAGIC from
# MAGIC cleansed.${vars.table}
# MAGIC except
# MAGIC select
# MAGIC titleCode
# MAGIC ,title
# MAGIC from
# MAGIC (select
# MAGIC TITLE as titleCode
# MAGIC ,TITLE_MEDI as title
# MAGIC ,row_number() over (partition by TITLE order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table}
# MAGIC )a where  a.rn = 1
