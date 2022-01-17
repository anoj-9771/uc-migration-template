# Databricks notebook source
# DBTITLE 1,[Config] Connection Setup
storage_account_name = "sablobdaftest01"
storage_account_access_key = dbutils.secrets.get(scope="TestScope",key="test-sablob-key")
container_name = "archive"
file_location1="wasbs://archive@sablobdaftest01.blob.core.windows.net/isuref/20211029/20211029_18:07:26/T005T_20211028172321.json"
file_location2="wasbs://archive@sablobdaftest01.blob.core.windows.net/isuref/20211029/20211029_18:07:26/0BP_CAT_TEXT_20211028172321.json"
file_type = "json"
print(storage_account_name)

# COMMAND ----------

spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
  storage_account_access_key)


# COMMAND ----------

# DBTITLE 1,[Source] Loading data into Dataframe
df1 = spark.read.format(file_type).option("inferSchema", "true").load(file_location1)
df2 = spark.read.format(file_type).option("inferSchema", "true").load(file_location2)

# COMMAND ----------

# DBTITLE 1,[Source] Schema Check - Refer to Raw2Cleansed Mapping
df1.printSchema()
df2.printSchema()

# COMMAND ----------

# DBTITLE 0,[Result] Load Count Result into DataFrame
lakedf = spark.sql("select * from cleansed.isu_0bp_cat_text")

# COMMAND ----------

# DBTITLE 1,[Target] Schema Check
lakedf.printSchema()

# COMMAND ----------

# DBTITLE 1,[Source] Creating Temporary Table
df1.createOrReplaceTempView("Source1")
df2.createOrReplaceTempView("Source2")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from(
# MAGIC select *, row_number() over(partition by key1 order by extract_datetime desc) rn
# MAGIC from
# MAGIC (SELECT * from Source2
# MAGIC union all
# MAGIC select * From source1)) where rn = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * From cleansed.isu_0bp_cat_text-- where langu = 'E' MANDT

# COMMAND ----------

# DBTITLE 1,[Source] with mapping
# MAGIC %sql
# MAGIC select
# MAGIC KEY1 as businessPartnerCategoryCode
# MAGIC ,TXTLG as businessPartnerCategory
# MAGIC from(
# MAGIC select * from(
# MAGIC select *, row_number() over(partition by key1 order by extract_datetime desc) rn
# MAGIC from
# MAGIC (SELECT * from Source2
# MAGIC union all
# MAGIC select * From source1)) where rn = 1)

# COMMAND ----------

lakedf.createOrReplaceTempView("Target")

# COMMAND ----------

# DBTITLE 1,[Verification] Count Checks
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from cleansed.isu_0bp_cat_Text  
# MAGIC union all 
# MAGIC select count (*) as RecordCount, 'Source' as TableName from (select
# MAGIC KEY1 as businessPartnerCategoryCode
# MAGIC ,TXTLG as businessPartnerCategory
# MAGIC from(
# MAGIC select * from(
# MAGIC select *, row_number() over(partition by key1 order by extract_datetime desc) rn
# MAGIC from
# MAGIC (SELECT * from Source2
# MAGIC union all
# MAGIC select * From source1)) where rn = 1))

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT businessPartnerCategoryCode, COUNT (*) as count
# MAGIC FROM cleansed.isu_0bp_cat_text 
# MAGIC GROUP BY businessPartnerCategoryCode
# MAGIC HAVING COUNT (*) > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT * FROM (
# MAGIC SELECT
# MAGIC *,
# MAGIC row_number() OVER(PARTITION BY businessPartnerCategoryCode order by businessPartnerCategoryCode) as rn
# MAGIC FROM  cleansed.isu_0bp_cat_text 
# MAGIC )a where a.rn > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Compare  Source and Target  Data
# MAGIC %sql
# MAGIC select
# MAGIC KEY1 as businessPartnerCategoryCode
# MAGIC ,TXTLG as businessPartnerCategory
# MAGIC from(
# MAGIC select * from(
# MAGIC select *, row_number() over(partition by key1 order by extract_datetime desc) rn
# MAGIC from
# MAGIC (SELECT * from Source2
# MAGIC union all
# MAGIC select * From source1)) where rn = 1)
# MAGIC except
# MAGIC select
# MAGIC businessPartnerCategoryCode,
# MAGIC businessPartnerCategory
# MAGIC from
# MAGIC cleansed.isu_0bp_cat_text 

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Target and Source Data
# MAGIC %sql
# MAGIC select
# MAGIC businessPartnerCategoryCode,
# MAGIC businessPartnerCategory
# MAGIC from
# MAGIC cleansed.isu_0bp_cat_text 
# MAGIC except
# MAGIC select
# MAGIC KEY1 as businessPartnerCategoryCode
# MAGIC ,TXTLG as businessPartnerCategory
# MAGIC from(
# MAGIC select * from(
# MAGIC select *, row_number() over(partition by key1 order by extract_datetime desc) rn
# MAGIC from
# MAGIC (SELECT * from Source2
# MAGIC union all
# MAGIC select * From source1)) where rn = 1)
