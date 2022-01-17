# Databricks notebook source
#config parameters
source = 'ISU' #either CRM or ISU
table = 'ETTIFN'

environment = 'test'
storage_account_name = "sablobdaftest01"
storage_account_access_key = dbutils.secrets.get(scope="TestScope",key="test-sablob-key")
containerName = "archive"


# COMMAND ----------

# MAGIC %run ../../includes/tableEvaluation

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from cleansed.isu_0UC_STATTART_TEXT

# COMMAND ----------

# DBTITLE 1,[Source] with mapping
# MAGIC %sql
# MAGIC select
# MAGIC installationId
# MAGIC ,operandCode
# MAGIC ,validFromDate
# MAGIC ,consecutiveDaysFromDate
# MAGIC ,validToDate
# MAGIC ,billingDocumentNumber
# MAGIC ,mBillingDocumentNumber
# MAGIC ,moveOutIndicator
# MAGIC ,expiryDate
# MAGIC ,inactiveIndicator
# MAGIC ,manualChangeIndicator
# MAGIC ,rateTypeCode
# MAGIC ,rateType
# MAGIC ,rateFactGroupCode
# MAGIC ,entryValue
# MAGIC ,valueToBeBilled
# MAGIC ,operandValue1
# MAGIC ,operandValue3
# MAGIC ,amount
# MAGIC ,currencyKey
# MAGIC from
# MAGIC (select
# MAGIC ANLAGE as installationId
# MAGIC ,OPERAND as operandCode
# MAGIC ,AB as validFromDate
# MAGIC ,ABLFDNR as consecutiveDaysFromDate
# MAGIC ,BIS as validToDate
# MAGIC ,BELNR as billingDocumentNumber
# MAGIC ,MBELNR as mBillingDocumentNumber
# MAGIC ,MAUSZUG as moveOutIndicator
# MAGIC ,ALTBIS as expiryDate
# MAGIC ,INAKTIV as inactiveIndicator
# MAGIC ,MANAEND as manualChangeIndicator
# MAGIC ,TARIFART as rateTypeCode
# MAGIC ,b.rateType as rateType
# MAGIC ,KONDIGR as rateFactGroupCode
# MAGIC ,WERT1 as entryValue
# MAGIC ,WERT2 as valueToBeBilled
# MAGIC ,STRING1 as operandValue1
# MAGIC ,STRING3 as operandValue3
# MAGIC ,BETRAG as amount
# MAGIC ,WAERS as currencyKey
# MAGIC ,row_number() over (partition by ANLAGE,OPERAND,AB,ABLFDNR order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table}
# MAGIC left join cleansed.isu_0UC_STATTART_TEXT b
# MAGIC on TARIFART = b.rateTypeCode
# MAGIC )a where  a.rn = 1

# COMMAND ----------

# DBTITLE 1,[Verification] Count Check
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from cleansed.${vars.table}
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from (select
# MAGIC installationId
# MAGIC ,operandCode
# MAGIC ,validFromDate
# MAGIC ,consecutiveDaysFromDate
# MAGIC ,validToDate
# MAGIC ,billingDocumentNumber
# MAGIC ,mBillingDocumentNumber
# MAGIC ,moveOutIndicator
# MAGIC ,expiryDate
# MAGIC ,inactiveIndicator
# MAGIC ,manualChangeIndicator
# MAGIC ,rateTypeCode
# MAGIC ,rateType
# MAGIC ,rateFactGroupCode
# MAGIC ,entryValue
# MAGIC ,valueToBeBilled
# MAGIC ,operandValue1
# MAGIC ,operandValue3
# MAGIC ,amount
# MAGIC ,currencyKey
# MAGIC from
# MAGIC (select
# MAGIC ANLAGE as installationId
# MAGIC ,OPERAND as operandCode
# MAGIC ,AB as validFromDate
# MAGIC ,ABLFDNR as consecutiveDaysFromDate
# MAGIC ,BIS as validToDate
# MAGIC ,BELNR as billingDocumentNumber
# MAGIC ,MBELNR as mBillingDocumentNumber
# MAGIC ,MAUSZUG as moveOutIndicator
# MAGIC ,ALTBIS as expiryDate
# MAGIC ,INAKTIV as inactiveIndicator
# MAGIC ,MANAEND as manualChangeIndicator
# MAGIC ,TARIFART as rateTypeCode
# MAGIC ,b.rateType as rateType
# MAGIC ,KONDIGR as rateFactGroupCode
# MAGIC ,WERT1 as entryValue
# MAGIC ,WERT2 as valueToBeBilled
# MAGIC ,STRING1 as operandValue1
# MAGIC ,STRING3 as operandValue3
# MAGIC ,BETRAG as amount
# MAGIC ,WAERS as currencyKey
# MAGIC ,row_number() over (partition by ANLAGE,OPERAND,AB,ABLFDNR order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table}
# MAGIC left join cleansed.isu_0UC_STATTART_TEXT b
# MAGIC on TARIFART = b.rateTypeCode
# MAGIC )a where  a.rn = 1)

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT installationId,operandCode,validFromDate,consecutiveDaysFromDate
# MAGIC , COUNT (*) as count
# MAGIC FROM cleansed.${vars.table}
# MAGIC GROUP BY installationId,operandCode,validFromDate,consecutiveDaysFromDate
# MAGIC HAVING COUNT (*) > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT * FROM (
# MAGIC SELECT
# MAGIC *,
# MAGIC row_number() OVER(PARTITION BY installationId,operandCode,validFromDate,consecutiveDaysFromDate  order by installationId,operandCode,validFromDate,consecutiveDaysFromDate) as rn
# MAGIC FROM  cleansed.${vars.table}
# MAGIC )a where a.rn > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Source and Target Data
# MAGIC %sql
# MAGIC select
# MAGIC installationId
# MAGIC ,operandCode
# MAGIC ,validFromDate
# MAGIC ,consecutiveDaysFromDate
# MAGIC ,validToDate
# MAGIC ,billingDocumentNumber
# MAGIC ,mBillingDocumentNumber
# MAGIC ,moveOutIndicator
# MAGIC ,expiryDate
# MAGIC ,inactiveIndicator
# MAGIC ,manualChangeIndicator
# MAGIC ,rateTypeCode
# MAGIC ,rateType
# MAGIC ,rateFactGroupCode
# MAGIC ,entryValue
# MAGIC ,valueToBeBilled
# MAGIC ,operandValue1
# MAGIC ,operandValue3
# MAGIC ,amount
# MAGIC ,currencyKey
# MAGIC from
# MAGIC (select
# MAGIC ANLAGE as installationId
# MAGIC ,OPERAND as operandCode
# MAGIC ,AB as validFromDate
# MAGIC ,ABLFDNR as consecutiveDaysFromDate
# MAGIC ,BIS as validToDate
# MAGIC ,BELNR as billingDocumentNumber
# MAGIC ,MBELNR as mBillingDocumentNumber
# MAGIC ,MAUSZUG as moveOutIndicator
# MAGIC ,ALTBIS as expiryDate
# MAGIC ,INAKTIV as inactiveIndicator
# MAGIC ,MANAEND as manualChangeIndicator
# MAGIC ,TARIFART as rateTypeCode
# MAGIC ,b.rateType as rateType
# MAGIC ,KONDIGR as rateFactGroupCode
# MAGIC ,WERT1 as entryValue
# MAGIC ,WERT2 as valueToBeBilled
# MAGIC ,STRING1 as operandValue1
# MAGIC ,STRING3 as operandValue3
# MAGIC ,BETRAG as amount
# MAGIC ,WAERS as currencyKey
# MAGIC ,row_number() over (partition by ANLAGE,OPERAND,AB,ABLFDNR order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table}
# MAGIC left join cleansed.isu_0UC_STATTART_TEXT b
# MAGIC on TARIFART = b.rateTypeCode
# MAGIC )a where  a.rn = 1
# MAGIC except
# MAGIC select
# MAGIC installationId
# MAGIC ,operandCode
# MAGIC ,validFromDate
# MAGIC ,consecutiveDaysFromDate
# MAGIC ,validToDate
# MAGIC ,billingDocumentNumber
# MAGIC ,mBillingDocumentNumber
# MAGIC ,moveOutIndicator
# MAGIC ,expiryDate
# MAGIC ,inactiveIndicator
# MAGIC ,manualChangeIndicator
# MAGIC ,rateTypeCode
# MAGIC ,rateType
# MAGIC ,rateFactGroupCode
# MAGIC ,entryValue
# MAGIC ,valueToBeBilled
# MAGIC ,operandValue1
# MAGIC ,operandValue3
# MAGIC ,amount
# MAGIC ,currencyKey
# MAGIC from
# MAGIC cleansed.${vars.table}

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Target and Source Data
# MAGIC %sql
# MAGIC select
# MAGIC installationId
# MAGIC ,operandCode
# MAGIC ,validFromDate
# MAGIC ,consecutiveDaysFromDate
# MAGIC ,validToDate
# MAGIC ,billingDocumentNumber
# MAGIC ,mBillingDocumentNumber
# MAGIC ,moveOutIndicator
# MAGIC ,expiryDate
# MAGIC ,inactiveIndicator
# MAGIC ,manualChangeIndicator
# MAGIC ,rateTypeCode
# MAGIC ,rateType
# MAGIC ,rateFactGroupCode
# MAGIC ,entryValue
# MAGIC ,valueToBeBilled
# MAGIC ,operandValue1
# MAGIC ,operandValue3
# MAGIC ,amount
# MAGIC ,currencyKey
# MAGIC from
# MAGIC cleansed.${vars.table}
# MAGIC except
# MAGIC select
# MAGIC installationId
# MAGIC ,operandCode
# MAGIC ,validFromDate
# MAGIC ,consecutiveDaysFromDate
# MAGIC ,validToDate
# MAGIC ,billingDocumentNumber
# MAGIC ,mBillingDocumentNumber
# MAGIC ,moveOutIndicator
# MAGIC ,expiryDate
# MAGIC ,inactiveIndicator
# MAGIC ,manualChangeIndicator
# MAGIC ,rateTypeCode
# MAGIC ,rateType
# MAGIC ,rateFactGroupCode
# MAGIC ,entryValue
# MAGIC ,valueToBeBilled
# MAGIC ,operandValue1
# MAGIC ,operandValue3
# MAGIC ,amount
# MAGIC ,currencyKey
# MAGIC from
# MAGIC (select
# MAGIC ANLAGE as installationId
# MAGIC ,OPERAND as operandCode
# MAGIC ,AB as validFromDate
# MAGIC ,ABLFDNR as consecutiveDaysFromDate
# MAGIC ,BIS as validToDate
# MAGIC ,BELNR as billingDocumentNumber
# MAGIC ,MBELNR as mBillingDocumentNumber
# MAGIC ,MAUSZUG as moveOutIndicator
# MAGIC ,ALTBIS as expiryDate
# MAGIC ,INAKTIV as inactiveIndicator
# MAGIC ,MANAEND as manualChangeIndicator
# MAGIC ,TARIFART as rateTypeCode
# MAGIC ,b.rateType as rateType
# MAGIC ,KONDIGR as rateFactGroupCode
# MAGIC ,WERT1 as entryValue
# MAGIC ,WERT2 as valueToBeBilled
# MAGIC ,STRING1 as operandValue1
# MAGIC ,STRING3 as operandValue3
# MAGIC ,BETRAG as amount
# MAGIC ,WAERS as currencyKey
# MAGIC ,row_number() over (partition by ANLAGE,OPERAND,AB,ABLFDNR order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table}
# MAGIC left join cleansed.isu_0UC_STATTART_TEXT b
# MAGIC on TARIFART = b.rateTypeCode
# MAGIC )a where  a.rn = 1
