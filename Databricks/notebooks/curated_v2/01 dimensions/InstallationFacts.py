# Databricks notebook source
# MAGIC %md 
# MAGIC # dimInstallationFact

# COMMAND ----------

# MAGIC %run ../common/common-curated-includeMain

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. InstallationFact Dataframe

# COMMAND ----------


# ----------------
# Main Installation History Table
# ----------------
df_installation_fact = spark.sql(f"""
    SELECT
        'ISU'                          AS sourceSystemCode,
        installationId                 AS installationNumber,
        operandCode                    AS operandCode,
        validFromDate                  AS validFromDate,
        consecutiveDaysFromDate        AS consecutiveDaysFromDate,
        validToDate                    AS validToDate,
        billingDocumentNumber          AS billingDocumentNumber,
        mBillingDocumentNumber         AS mBillingDocumentNumber,
        moveOutIndicator               AS moveOutIndicator,
        expiryDate                     AS expiryDate,
        inactiveIndicator              AS inactiveIndicator,
        manualChangeIndicator          AS manualChangeIndicator,
        rateTypeCode                   AS rateTypeCode,
        rateType                       AS rateType,
        rateFactGroupCode              AS rateFactGroupCode,
        rateFactGroup                  AS rateFactGroup,
        entryValue                     AS entryValue,
        valueToBeBilled                AS valueToBeBilled,
        operandValue1                  AS operandValue1,
        operandValue3                  AS operandValue3,
        amount                         AS amount,
        currencyKey                    AS currencyKey
    FROM {ADS_DATABASE_CLEANSED}.isu_ettifn
"""    
).drop_duplicates()

# ----------------
# Dummy Dimension
# ----------------
dummyDimRecDf = spark.createDataFrame(
    [("-1", "1900-01-01", "unknown")],
    ["installationNumber", "description"]
)

#UNION TABLES
df_installation_fact = (
    df_installation_fact
    .unionByName(dummyDimRecDf, allowMissingColumns = True)
    .drop_duplicates()
)    


# COMMAND ----------

schema = StructType([
    StructField('installationFactsSK',StringType(),False),
    StructField('sourceSystemCode',StringType(),True),
    StructField('installationNumber',StringType(),False),
    StructField('operandCode',StringType(),False),
    StructField('validFromDate',DateType(),False),
    StructField('consecutiveDaysFromDate',StringType(),True),
    StructField('validToDate',DateType(),True),
    StructField('billingDocumentNumber',StringType(),True),
    StructField('mBillingDocumentNumber',StringType(),True),
    StructField('moveOutIndicator',StringType(),True),
    StructField('expiryDate',DateType(),True),
    StructField('inactiveIndicator',StringType(),True),
    StructField('manualChangeIndicator',StringType(),True),
    StructField('rateTypeCode',StringType(),True),
    StructField('rateType',StringType(),True),
    StructField('rateFactGroupCode',StringType(),True),
    StructField('rateFactGroup',StringType(),True),
    StructField('entryValue',DecimalType(),True),
    StructField('valueToBeBilled',DecimalType(),True),
    StructField('operandValue1',StringType(),True),
    StructField('operandValue3',StringType(),True),
    StructField('amount',DecimalType(),True),
    StructField('currencyKey',StringType(),True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Load

# COMMAND ----------

TemplateTimeSliceEtlSCD(
    df_installation_fact, 
    entity="dimInstallationFact", 
    businessKey="installationNumber", 
    schema=schema
)
