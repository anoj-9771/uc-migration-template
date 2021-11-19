# Databricks notebook source
#config parameters
source = 'ISU' #either CRM or ISU
table = '0DF_REFIXFI_ATTR'

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
# MAGIC architecturalObjectInternalId
# MAGIC ,fixtureAndFittingCharacteristicCode
# MAGIC ,validToDate
# MAGIC ,validFromDate
# MAGIC ,weightingValue
# MAGIC ,resultValue
# MAGIC ,characteristicAdditionalValue
# MAGIC ,amountPerAreaUnit 
# MAGIC ,applicableIndicator 
# MAGIC ,characteristicAmountArea 
# MAGIC ,characteristicPercentage
# MAGIC ,characteristicPriceAmount 
# MAGIC from
# MAGIC (select
# MAGIC INTRENO as architecturalObjectInternalId
# MAGIC ,FIXFITCHARACT as fixtureAndFittingCharacteristicCode
# MAGIC ,VALIDTO as validToDate
# MAGIC ,VALIDFROM as validFromDate
# MAGIC ,WEIGHT as weightingValue
# MAGIC ,RESULTVAL as resultValue
# MAGIC ,ADDITIONALINFO as characteristicAdditionalValue
# MAGIC ,AMOUNTPERAREA as amountPerAreaUnit
# MAGIC ,FFCTACCURATE as applicableIndicator
# MAGIC ,CHARACTAMTAREA as characteristicAmountArea
# MAGIC ,CHARACTPERCENT as characteristicPercentage
# MAGIC ,CHARACTAMTABS as characteristicPriceAmount
# MAGIC ,row_number() over (partition by INTRENO,FIXFITCHARACT,VALIDTO order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table}) where rn = 1

# COMMAND ----------

# DBTITLE 1,[Verification] Count Check
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from cleansed.${vars.table}
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from (
# MAGIC select
# MAGIC architecturalObjectInternalId
# MAGIC ,fixtureAndFittingCharacteristicCode
# MAGIC ,validToDate
# MAGIC ,validFromDate
# MAGIC ,weightingValue
# MAGIC ,resultValue
# MAGIC ,characteristicAdditionalValue
# MAGIC ,amountPerAreaUnit 
# MAGIC ,applicableIndicator 
# MAGIC ,characteristicAmountArea 
# MAGIC ,characteristicPercentage
# MAGIC ,characteristicPriceAmount 
# MAGIC from
# MAGIC (select
# MAGIC INTRENO as architecturalObjectInternalId
# MAGIC ,FIXFITCHARACT as fixtureAndFittingCharacteristicCode
# MAGIC ,VALIDTO as validToDate
# MAGIC ,VALIDFROM as validFromDate
# MAGIC ,WEIGHT as weightingValue
# MAGIC ,RESULTVAL as resultValue
# MAGIC ,ADDITIONALINFO as characteristicAdditionalValue
# MAGIC ,AMOUNTPERAREA as amountPerAreaUnit
# MAGIC ,FFCTACCURATE as applicableIndicator
# MAGIC ,CHARACTAMTAREA as characteristicAmountArea
# MAGIC ,CHARACTPERCENT as characteristicPercentage
# MAGIC ,CHARACTAMTABS as characteristicPriceAmount
# MAGIC ,row_number() over (partition by INTRENO,FIXFITCHARACT,VALIDTO order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table}) where rn = 1)

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT architecturalObjectInternalId,fixtureAndFittingCharacteristicCode, validToDate, COUNT (*) as count
# MAGIC FROM cleansed.${vars.table}
# MAGIC GROUP BY architecturalObjectInternalId,fixtureAndFittingCharacteristicCode, validToDate
# MAGIC HAVING COUNT (*) > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT * FROM (
# MAGIC SELECT
# MAGIC *,
# MAGIC row_number() OVER(PARTITION BY architecturalObjectInternalId,fixtureAndFittingCharacteristicCode, validToDate order by architecturalObjectInternalId,fixtureAndFittingCharacteristicCode, validToDate) as rn
# MAGIC FROM  cleansed.${vars.table}
# MAGIC )a where a.rn > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Source and Target Data
# MAGIC %sql
# MAGIC select
# MAGIC architecturalObjectInternalId
# MAGIC ,fixtureAndFittingCharacteristicCode
# MAGIC ,validToDate
# MAGIC ,validFromDate
# MAGIC ,weightingValue
# MAGIC ,resultValue
# MAGIC ,characteristicAdditionalValue
# MAGIC ,amountPerAreaUnit 
# MAGIC ,applicableIndicator 
# MAGIC ,characteristicAmountArea 
# MAGIC ,characteristicPercentage
# MAGIC ,characteristicPriceAmount 
# MAGIC from
# MAGIC (select
# MAGIC INTRENO as architecturalObjectInternalId
# MAGIC ,FIXFITCHARACT as fixtureAndFittingCharacteristicCode
# MAGIC ,VALIDTO as validToDate
# MAGIC ,VALIDFROM as validFromDate
# MAGIC ,WEIGHT as weightingValue
# MAGIC ,RESULTVAL as resultValue
# MAGIC ,ADDITIONALINFO as characteristicAdditionalValue
# MAGIC ,AMOUNTPERAREA as amountPerAreaUnit
# MAGIC ,FFCTACCURATE as applicableIndicator
# MAGIC ,CHARACTAMTAREA as characteristicAmountArea
# MAGIC ,CHARACTPERCENT as characteristicPercentage
# MAGIC ,CHARACTAMTABS as characteristicPriceAmount
# MAGIC ,row_number() over (partition by INTRENO,FIXFITCHARACT,VALIDTO order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table}) where rn = 1
# MAGIC except
# MAGIC select
# MAGIC architecturalObjectInternalId
# MAGIC ,fixtureAndFittingCharacteristicCode
# MAGIC ,validToDate
# MAGIC ,validFromDate
# MAGIC ,weightingValue
# MAGIC ,resultValue
# MAGIC ,characteristicAdditionalValue
# MAGIC ,amountPerAreaUnit 
# MAGIC ,applicableIndicator 
# MAGIC ,characteristicAmountArea 
# MAGIC ,characteristicPercentage
# MAGIC ,characteristicPriceAmount
# MAGIC from
# MAGIC cleansed.${vars.table}

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Target and Source Data
# MAGIC %sql
# MAGIC select
# MAGIC architecturalObjectInternalId
# MAGIC ,fixtureAndFittingCharacteristicCode
# MAGIC ,validToDate
# MAGIC ,validFromDate
# MAGIC ,weightingValue
# MAGIC ,resultValue
# MAGIC ,characteristicAdditionalValue
# MAGIC ,amountPerAreaUnit 
# MAGIC ,applicableIndicator 
# MAGIC ,characteristicAmountArea 
# MAGIC ,characteristicPercentage
# MAGIC ,characteristicPriceAmount
# MAGIC from
# MAGIC cleansed.${vars.table}
# MAGIC except
# MAGIC select
# MAGIC architecturalObjectInternalId
# MAGIC ,fixtureAndFittingCharacteristicCode
# MAGIC ,validToDate
# MAGIC ,validFromDate
# MAGIC ,weightingValue
# MAGIC ,resultValue
# MAGIC ,characteristicAdditionalValue
# MAGIC ,amountPerAreaUnit 
# MAGIC ,applicableIndicator 
# MAGIC ,characteristicAmountArea 
# MAGIC ,characteristicPercentage
# MAGIC ,characteristicPriceAmount 
# MAGIC from
# MAGIC (select
# MAGIC INTRENO as architecturalObjectInternalId
# MAGIC ,FIXFITCHARACT as fixtureAndFittingCharacteristicCode
# MAGIC ,VALIDTO as validToDate
# MAGIC ,VALIDFROM as validFromDate
# MAGIC ,WEIGHT as weightingValue
# MAGIC ,RESULTVAL as resultValue
# MAGIC ,ADDITIONALINFO as characteristicAdditionalValue
# MAGIC ,AMOUNTPERAREA as amountPerAreaUnit
# MAGIC ,FFCTACCURATE as applicableIndicator
# MAGIC ,CHARACTAMTAREA as characteristicAmountArea
# MAGIC ,CHARACTPERCENT as characteristicPercentage
# MAGIC ,CHARACTAMTABS as characteristicPriceAmount
# MAGIC ,row_number() over (partition by INTRENO,FIXFITCHARACT,VALIDTO order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table}) where rn = 1
