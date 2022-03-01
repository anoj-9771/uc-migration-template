# Databricks notebook source
# DBTITLE 1,[Config] Connection Setup
storage_account_name = "saswcnonprod01landingtst"
storage_account_access_key = dbutils.secrets.get(scope="Test-Access",key="test-blob-key")
container_name = "archive"
file_location = "wasbs://archive@saswcnonprod01landingtst.blob.core.windows.net/sapisu/20210825/20210825_15:14:40/0UC_MTR_DOC_20210825014125.json"
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
lakedf = spark.sql("select * from Cleansed.t_sapisu_vibdcharact")

# COMMAND ----------

# DBTITLE 1,[Target] Schema Check
lakedf.printSchema()

# COMMAND ----------

# DBTITLE 1,[Source] Creating Temporary Table
df.createOrReplaceTempView("Source")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from Source

# COMMAND ----------

# DBTITLE 1,[Source] Displaying Records
# MAGIC %sql
# MAGIC SELECT
# MAGIC INTRENO as architecturalObjectInternalId
# MAGIC ,FIXFITCHARACT as fixtureAndFittingCharacteristicCode
# MAGIC ,b.fixtureAndFittingCharacteristic as fixtureAndFittingCharacteristic -- Reference table passed as part of checks in Reference table: cleansed.t_sapisu_0DF_REFIXFI_TEXT
# MAGIC ,VALIDTO as validToDate
# MAGIC ,VALIDFROM as validFromDate
# MAGIC ,AMOUNTPERAREA as amountPerAreaUnit
# MAGIC ,FFCTACCURATE as applicableIndicator
# MAGIC ,CHARACTAMTAREA as characteristicAmountArea
# MAGIC ,CHARACTPERCENT as characteristicPercentage
# MAGIC ,CHARACTAMTABS as characteristicPriceAmount
# MAGIC ,CHARACTCOUNT as characteristicCount
# MAGIC ,SUPPLEMENTINFO as supplementInfo
# MAGIC FROM Source a
# MAGIC LEFT JOIN cleansed.t_sapisu_0DF_REFIXFI_TEXT b
# MAGIC ON a.FIXFITCHARACT = b.fixtureAndFittingCharacteristicCode
# MAGIC --WHERE a.SPRAS ='E'   condition not required as this has been checked in Reference table: cleansed.t_sapisu_0DF_REFIXFI_TEXT

# COMMAND ----------

# DBTITLE 1,[Target] Displaying Records
# MAGIC %sql
# MAGIC select * from Cleansed.t_sapisu_vibdcharact

# COMMAND ----------

lakedf.createOrReplaceTempView("Target")

# COMMAND ----------

# DBTITLE 1,[Verification] Count Checks
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from Cleansed.t_sapisu_vibdcharact
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from Source

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT architecturalObjectInternalId, fixtureAndFittingCharacteristicCode, validToDate , COUNT (*) as count
# MAGIC FROM cleansed.t_sapisu_VIBDCHARACT
# MAGIC GROUP BY architecturalObjectInternalId, fixtureAndFittingCharacteristicCode, validToDate 
# MAGIC HAVING COUNT (*) > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT * FROM (
# MAGIC SELECT
# MAGIC *,
# MAGIC row_number() OVER(PARTITION BY architecturalObjectInternalId, fixtureAndFittingCharacteristicCode, validToDate order by validToDate) as rn
# MAGIC FROM  cleansed.t_sapisu_VIBDCHARACT
# MAGIC )a where a.rn > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Source and Target Data
# MAGIC %sql
# MAGIC SELECT
# MAGIC INTRENO as architecturalObjectInternalId
# MAGIC ,FIXFITCHARACT as fixtureAndFittingCharacteristicCode
# MAGIC ,b.fixtureAndFittingCharacteristic as fixtureAndFittingCharacteristic -- Reference table passed as part of checks in Reference table: cleansed.t_sapisu_0DF_REFIXFI_TEXT
# MAGIC ,VALIDTO as validToDate
# MAGIC ,VALIDFROM as validFromDate
# MAGIC ,AMOUNTPERAREA as amountPerAreaUnit
# MAGIC ,FFCTACCURATE as applicableIndicator
# MAGIC ,CHARACTAMTAREA as characteristicAmountArea
# MAGIC ,CHARACTPERCENT as characteristicPercentage
# MAGIC ,CHARACTAMTABS as characteristicPriceAmount
# MAGIC ,CHARACTCOUNT as characteristicCount
# MAGIC ,SUPPLEMENTINFO as supplementInfo
# MAGIC FROM Source a
# MAGIC LEFT JOIN cleansed.t_sapisu_0DF_REFIXFI_TEXT b
# MAGIC ON a.FIXFITCHARACT = b.fixtureAndFittingCharacteristicCode
# MAGIC --WHERE a.SPRAS ='E'   condition not required as this has been checked in Reference table: cleansed.t_sapisu_0DF_REFIXFI_TEXT
# MAGIC 
# MAGIC EXCEPT
# MAGIC 
# MAGIC select
# MAGIC architecturalObjectInternalId,
# MAGIC fixtureAndFittingCharacteristicCode,
# MAGIC fixtureAndFittingCharacteristic,
# MAGIC validToDate,
# MAGIC validFromDate,
# MAGIC amountPerAreaUnit,
# MAGIC applicableIndicator,
# MAGIC characteristicAmountArea,
# MAGIC characteristicPercentage,
# MAGIC characteristicPriceAmount,
# MAGIC characteristicCount,
# MAGIC supplementInfo
# MAGIC FROM
# MAGIC cleansed.t_sapisu_VIBDCHARACT

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Target and Source Data
# MAGIC %sql
# MAGIC select
# MAGIC architecturalObjectInternalId,
# MAGIC fixtureAndFittingCharacteristicCode,
# MAGIC fixtureAndFittingCharacteristic,
# MAGIC validToDate,
# MAGIC validFromDate,
# MAGIC amountPerAreaUnit,
# MAGIC applicableIndicator,
# MAGIC characteristicAmountArea,
# MAGIC characteristicPercentage,
# MAGIC characteristicPriceAmount,
# MAGIC characteristicCount,
# MAGIC supplementInfo
# MAGIC FROM
# MAGIC cleansed.t_sapisu_VIBDCHARACT
# MAGIC 
# MAGIC EXCEPT
# MAGIC 
# MAGIC SELECT
# MAGIC INTRENO as architecturalObjectInternalId
# MAGIC ,FIXFITCHARACT as fixtureAndFittingCharacteristicCode
# MAGIC ,b.fixtureAndFittingCharacteristic as fixtureAndFittingCharacteristic -- Reference table passed as part of checks in Reference table: cleansed.t_sapisu_0DF_REFIXFI_TEXT
# MAGIC ,VALIDTO as validToDate
# MAGIC ,VALIDFROM as validFromDate
# MAGIC ,AMOUNTPERAREA as amountPerAreaUnit
# MAGIC ,FFCTACCURATE as applicableIndicator
# MAGIC ,CHARACTAMTAREA as characteristicAmountArea
# MAGIC ,CHARACTPERCENT as characteristicPercentage
# MAGIC ,CHARACTAMTABS as characteristicPriceAmount
# MAGIC ,CHARACTCOUNT as characteristicCount
# MAGIC ,SUPPLEMENTINFO as supplementInfo
# MAGIC FROM Source a
# MAGIC LEFT JOIN cleansed.t_sapisu_0DF_REFIXFI_TEXT b
# MAGIC ON a.FIXFITCHARACT = b.fixtureAndFittingCharacteristicCode
# MAGIC --WHERE a.SPRAS ='E'   condition not required as this has been checked in Reference table: cleansed.t_sapisu_0DF_REFIXFI_TEXT

# COMMAND ----------

# DBTITLE 1,Extra Verification
# MAGIC %sql
# MAGIC --SELECT architecturalObjectInternalId,COUNT (*) as count
# MAGIC --FROM cleansed.t_sapisu_VIBDCHARACT
# MAGIC --GROUP BY architecturalObjectInternalId
# MAGIC --HAVING COUNT (*) > 1
# MAGIC 
# MAGIC select * from cleansed.t_sapisu_VIBDCHARACT
# MAGIC 
# MAGIC where architecturalObjectInternalId='I000100482039'
