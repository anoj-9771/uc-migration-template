# Databricks notebook source
# DBTITLE 1,[Config] Connection Setup
storage_account_name = "saswcnonprod01landingtst"
storage_account_access_key = dbutils.secrets.get(scope="Test-Access",key="test-blob-key")
container_name = "archive"
file_location = "wasbs://archive@saswcnonprod01landingtst.blob.core.windows.net/archive/sapisu/ercho/json/year=2021/month=08/day=05/ERCHO_20210804141617.json"
file_type = "json"
print(storage_account_name)

# COMMAND ----------

spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
  storage_account_access_key)


# COMMAND ----------

# DBTITLE 1,[Source] Loading data into Dataframe
df = spark.read.format(file_type).option("inferSchema", "true").load(file_location)

# COMMAND ----------

# DBTITLE 1,[Source] Schema Check - Refer to Raw2Cleansed Mapping
df.printSchema()

# COMMAND ----------

# DBTITLE 0,[Result] Load Count Result into DataFrame
lakedf = spark.sql("select * from cleansed.t_sapisu_ercho")

# COMMAND ----------

# DBTITLE 1,[Target] Schema Check
lakedf.printSchema()

# COMMAND ----------

# DBTITLE 1,[Source] Creating Temporary Table
df.createOrReplaceTempView("Source")

# COMMAND ----------

# DBTITLE 1,[Source] Displaying Records
# MAGIC %sql
# MAGIC SELECT
# MAGIC BELNR AS billingDocumentNumber,
# MAGIC OUTCNSO AS outsortingNumber,
# MAGIC VALIDATION AS billingValidationName,
# MAGIC MANOUTSORT AS manualOutsortingReasonCode,
# MAGIC FREI_AM AS documentReleasedDate,
# MAGIC FREI_VON AS documentReleasedUserName,
# MAGIC DEVIATION AS deviation,
# MAGIC SIMULATION AS billingSimulationIndicator,
# MAGIC OUTCOUNT AS manualOutsortingCount
# MAGIC FROM Source

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cleansed.t_sapisu_ercho

# COMMAND ----------

lakedf.createOrReplaceTempView("Target")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from cleansed.t_sapisu_ercho
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from Source

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT billingDocumentNumber,outsortingNumber, COUNT (*) as count
# MAGIC FROM cleansed.t_sapisu_ercho
# MAGIC GROUP BY billingDocumentNumber,outsortingNumber
# MAGIC HAVING COUNT (*) > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT * FROM (
# MAGIC SELECT
# MAGIC *,
# MAGIC row_number() OVER(PARTITION BY billingDocumentNumber,outsortingNumber order by billingDocumentNumber) as rn
# MAGIC FROM  cleansed.t_sapisu_ercho
# MAGIC )a where a.rn > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Source and Target Data
# MAGIC %sql
# MAGIC SELECT
# MAGIC BELNR AS billingDocumentNumber,
# MAGIC OUTCNSO AS outsortingNumber,
# MAGIC VALIDATION AS billingValidationName,
# MAGIC MANOUTSORT AS manualOutsortingReasonCode,
# MAGIC FREI_AM AS documentReleasedDate,
# MAGIC FREI_VON AS documentReleasedUserName,
# MAGIC DEVIATION AS deviation,
# MAGIC SIMULATION AS billingSimulationIndicator,
# MAGIC OUTCOUNT AS manualOutsortingCount
# MAGIC FROM Source
# MAGIC EXCEPT
# MAGIC select
# MAGIC billingDocumentNumber,
# MAGIC outsortingNumber,
# MAGIC billingValidationName,
# MAGIC manualOutsortingReasonCode,
# MAGIC documentReleasedDate,
# MAGIC documentReleasedUserName,
# MAGIC deviation,
# MAGIC billingSimulationIndicator,
# MAGIC manualOutsortingCount
# MAGIC FROM cleansed.t_sapisu_ercho

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Target and Source Data
# MAGIC %sql
# MAGIC select
# MAGIC billingDocumentNumber,
# MAGIC outsortingNumber,
# MAGIC billingValidationName,
# MAGIC manualOutsortingReasonCode,
# MAGIC documentReleasedDate,
# MAGIC documentReleasedUserName,
# MAGIC deviation,
# MAGIC billingSimulationIndicator,
# MAGIC manualOutsortingCount
# MAGIC FROM cleansed.t_sapisu_ercho
# MAGIC EXCEPT
# MAGIC SELECT
# MAGIC BELNR AS billingDocumentNumber,
# MAGIC OUTCNSO AS outsortingNumber,
# MAGIC VALIDATION AS billingValidationName,
# MAGIC MANOUTSORT AS manualOutsortingReasonCode,
# MAGIC FREI_AM AS documentReleasedDate,
# MAGIC FREI_VON AS documentReleasedUserName,
# MAGIC DEVIATION AS deviation,
# MAGIC SIMULATION AS billingSimulationIndicator,
# MAGIC OUTCOUNT AS manualOutsortingCount
# MAGIC FROM Source
