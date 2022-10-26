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
# Main Installation Facts Table
# ----------------
df_installation_fact = spark.sql(f"""
    SELECT
        'ISU'                             AS sourceSystemCode,
        installationNumber                AS installationNumber,
        operandCode                       AS operandCode,
        validFromDate                     AS validFromDate, 
        consecutiveDaysFromDate           AS consecutiveDaysFromDate,
        validToDate                       AS validToDate, 
        billingDocumentNumber             AS billingDocumentNumber,
        mBillingDocumentNumber            AS mBillingDocumentNumber,
        moveOutFlag                       AS moveOutFlag,
        expiryDate                        AS expiryDate, 
        inactiveFlag                      AS inactiveFlag,
        manualChangeFlag                  AS manualChangeFlag,
        rateTypeCode                      AS rateTypeCode,
        rateType                          AS rateType,
        rateFactGroupCode                 AS rateFactGroupCode,
        rateFactGroup                     AS rateFactGroup,
        entryValue                        AS entryValue,
        valueToBeBilled                   AS valueToBeBilled,
        operandValue1                     AS operandValue1,
        operandValue3Flag                 AS operandValue3Flag,
        amount                            AS amount,
        currencyKey                       AS currencyKey
    FROM {ADS_DATABASE_CLEANSED}.isu_ettifn
    WHERE 
        _RecordCurrent = 1 
        AND _RecordDeleted = 0 
"""    
).drop_duplicates()

# ----------------
# Dummy Dimension
# ----------------
dummyDimRecDf = (
    spark.createDataFrame(
        [("-1","-1", "-1", "1900-01-01", "9999-12-31")], 
        ["installationNumber", "operandCode", "consecutiveDaysFromDate", "validFromDate", "validToDate"]
    )
)

# Master Table
df_installation_fact = (
    df_installation_fact
    # --- Add Dummy Dimension --- #
    .unionByName(dummyDimRecDf, allowMissingColumns = True)
    .drop_duplicates()
    # --- Cast Data Types --- # 
    .withColumn("validFromDate",col("validFromDate").cast("date"))
    .withColumn("validToDate",col("validToDate").cast("date"))
    .withColumn("expiryDatec",col("expiryDate").cast("date"))
)    

# print('df count',df_installation_fact.count())
# display(df_installation_fact)

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
    StructField('moveOutFlag',StringType(),True),
    StructField('expiryDate',DateType(),True),
    StructField('inactiveFlag',StringType(),True),
    StructField('manualChangeFlag',StringType(),True),
    StructField('rateTypeCode',StringType(),True),
    StructField('rateType',StringType(),True),
    StructField('rateFactGroupCode',StringType(),True),
    StructField('rateFactGroup',StringType(),True),
    StructField('entryValue',DecimalType(),True),
    StructField('valueToBeBilled',DecimalType(),True),
    StructField('operandValue1',StringType(),True),
    StructField('operandValue3Flag',StringType(),True),
    StructField('amount',DecimalType(),True),
    StructField('currencyKey',StringType(),True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Load

# COMMAND ----------

TemplateTimeSliceEtlSCD(
    df_installation_fact, 
    entity="dimInstallationFacts", 
    businessKey="installationNumber,operandCode,validFromDate,consecutiveDaysFromDate", 
    schema=schema
)

# COMMAND ----------

dbutils.notebook.exit("1")
