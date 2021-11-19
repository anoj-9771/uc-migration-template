# Databricks notebook source
#config parameters
source = 'ISU' #either CRM or ISU
table = '0CACONT_ACC_ATTR_2'

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
# MAGIC contractAccountNumber,
# MAGIC createdDate,
# MAGIC createdBy,
# MAGIC lastChangedDate,
# MAGIC lastChangedBy,
# MAGIC deletedIndicator,
# MAGIC applicationArea,
# MAGIC contractAccountCategoryCode,
# MAGIC contractAccountCategory,
# MAGIC legacyContractAccountNumber
# MAGIC from
# MAGIC (SELECT
# MAGIC VKONT as contractAccountNumber,
# MAGIC ERDAT as createdDate,
# MAGIC ERNAM as createdBy,
# MAGIC AEDAT as lastChangedDate,
# MAGIC AENAM as lastChangedBy,
# MAGIC LOEVM as deletedIndicator,
# MAGIC APPLK as applicationArea,
# MAGIC VKTYP as contractAccountCategoryCode,
# MAGIC b.contractAccountCategory as  contractAccountCategory,
# MAGIC VKONA as legacyContractAccountNumber
# MAGIC ,row_number() over (partition by VKONT order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table}
# MAGIC JOIN cleansed.ISU_0UC_VKTYP_TEXT b on VKTYP = b.contractAccountCategoryCode 
# MAGIC and APPLK = b.applicationArea
# MAGIC )a where  a.rn = 1

# COMMAND ----------

# DBTITLE 1,[Verification] Count Check
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from cleansed.${vars.table}
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from (select
# MAGIC contractAccountNumber,
# MAGIC createdDate,
# MAGIC createdBy,
# MAGIC lastChangedDate,
# MAGIC lastChangedBy,
# MAGIC deletedIndicator,
# MAGIC applicationArea,
# MAGIC contractAccountCategoryCode,
# MAGIC contractAccountCategory,
# MAGIC legacyContractAccountNumber
# MAGIC from
# MAGIC (SELECT
# MAGIC VKONT as contractAccountNumber,
# MAGIC ERDAT as createdDate,
# MAGIC ERNAM as createdBy,
# MAGIC AEDAT as lastChangedDate,
# MAGIC AENAM as lastChangedBy,
# MAGIC LOEVM as deletedIndicator,
# MAGIC APPLK as applicationArea,
# MAGIC VKTYP as contractAccountCategoryCode,
# MAGIC b.contractAccountCategory as  contractAccountCategory,
# MAGIC VKONA as legacyContractAccountNumber
# MAGIC ,row_number() over (partition by VKONT order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table}
# MAGIC JOIN cleansed.ISU_0UC_VKTYP_TEXT b on VKTYP = b.contractAccountCategoryCode 
# MAGIC and APPLK = b.applicationArea
# MAGIC )a where  a.rn = 1)

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT contractAccountNumber,createdDate,createdBy,lastChangedDate,lastChangedBy,deletedIndicator,applicationArea,
# MAGIC contractAccountCategoryCode,contractAccountCategory,legacyContractAccountNumber, COUNT (*) as count
# MAGIC FROM cleansed.${vars.table}
# MAGIC GROUP BY contractAccountNumber,createdDate,createdBy,lastChangedDate,lastChangedBy,deletedIndicator,applicationArea,
# MAGIC contractAccountCategoryCode,contractAccountCategory,legacyContractAccountNumber
# MAGIC HAVING COUNT (*) > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT * FROM (
# MAGIC SELECT
# MAGIC *,
# MAGIC row_number() OVER(PARTITION BY contractAccountNumber order by contractAccountNumber) as rn
# MAGIC FROM  cleansed.${vars.table}
# MAGIC )a where a.rn > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Source and Target Data
# MAGIC %sql
# MAGIC select
# MAGIC contractAccountNumber,
# MAGIC createdDate,
# MAGIC createdBy,
# MAGIC lastChangedDate,
# MAGIC lastChangedBy,
# MAGIC deletedIndicator,
# MAGIC applicationArea,
# MAGIC contractAccountCategoryCode,
# MAGIC contractAccountCategory,
# MAGIC legacyContractAccountNumber
# MAGIC from
# MAGIC (SELECT
# MAGIC VKONT as contractAccountNumber,
# MAGIC ERDAT as createdDate,
# MAGIC ERNAM as createdBy,
# MAGIC AEDAT as lastChangedDate,
# MAGIC AENAM as lastChangedBy,
# MAGIC LOEVM as deletedIndicator,
# MAGIC APPLK as applicationArea,
# MAGIC VKTYP as contractAccountCategoryCode,
# MAGIC b.contractAccountCategory as  contractAccountCategory,
# MAGIC VKONA as legacyContractAccountNumber
# MAGIC ,row_number() over (partition by VKONT order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table}
# MAGIC JOIN cleansed.ISU_0UC_VKTYP_TEXT b on VKTYP = b.contractAccountCategoryCode 
# MAGIC and APPLK = b.applicationArea
# MAGIC )a where  a.rn = 1
# MAGIC except
# MAGIC select
# MAGIC contractAccountNumber,
# MAGIC createdDate,
# MAGIC createdBy,
# MAGIC lastChangedDate,
# MAGIC lastChangedBy,
# MAGIC deletedIndicator,
# MAGIC applicationArea,
# MAGIC contractAccountCategoryCode,
# MAGIC contractAccountCategory,
# MAGIC legacyContractAccountNumber
# MAGIC from
# MAGIC cleansed.${vars.table}

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Target and Source Data
# MAGIC %sql
# MAGIC select
# MAGIC contractAccountNumber,
# MAGIC createdDate,
# MAGIC createdBy,
# MAGIC lastChangedDate,
# MAGIC lastChangedBy,
# MAGIC deletedIndicator,
# MAGIC applicationArea,
# MAGIC contractAccountCategoryCode,
# MAGIC contractAccountCategory,
# MAGIC legacyContractAccountNumber
# MAGIC from
# MAGIC cleansed.${vars.table}
# MAGIC except
# MAGIC select
# MAGIC contractAccountNumber,
# MAGIC createdDate,
# MAGIC createdBy,
# MAGIC lastChangedDate,
# MAGIC lastChangedBy,
# MAGIC deletedIndicator,
# MAGIC applicationArea,
# MAGIC contractAccountCategoryCode,
# MAGIC contractAccountCategory,
# MAGIC legacyContractAccountNumber
# MAGIC from
# MAGIC (SELECT
# MAGIC VKONT as contractAccountNumber,
# MAGIC ERDAT as createdDate,
# MAGIC ERNAM as createdBy,
# MAGIC AEDAT as lastChangedDate,
# MAGIC AENAM as lastChangedBy,
# MAGIC LOEVM as deletedIndicator,
# MAGIC APPLK as applicationArea,
# MAGIC VKTYP as contractAccountCategoryCode,
# MAGIC b.contractAccountCategory as  contractAccountCategory,
# MAGIC VKONA as legacyContractAccountNumber
# MAGIC ,row_number() over (partition by VKONT order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table}
# MAGIC JOIN cleansed.ISU_0UC_VKTYP_TEXT b on VKTYP = b.contractAccountCategoryCode 
# MAGIC and APPLK = b.applicationArea
# MAGIC )a where  a.rn = 1
