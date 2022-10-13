# Databricks notebook source
# MAGIC %md 
# MAGIC # dimInstallationHistory

# COMMAND ----------

# MAGIC %run ../common/common-curated-includeMain

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. InstallationHistory Dataframe

# COMMAND ----------


# ----------------
# Main Installation History Table
# ----------------
df_installation_history = spark.sql(f"""
    SELECT
        'ISU'                          AS sourceSystemCode,
        i.installationNumber           AS installationNumber,
        i.validFromDate                AS validFromDate,
        i.validToDate                  AS validToDate,
        i.rateCategoryCode             AS rateCategoryCode,
        i.rateCategory                 AS rateCategory,
        m.portionNumber                AS portionNumber,
        m.portionText                  AS portionText,
        i.industryCode                 AS industryCode,
        i.industry                     AS industry,
        i.billingClassCode             AS billingClassCode,
        i.billingClass                 AS billingClass,
        i.meterReadingUnit             AS meterReadingUnit,
        i.industrySystemCode           AS industrySystemCode,
        i.IndustrySystem               AS IndustrySystem
    FROM {ADS_DATABASE_CLEANSED}.isu_0ucinstallah_attr_2 i
    LEFT OUTER JOIN {ADS_DATABASE_CLEANSED}.isu_0ucmtrdunit_attr m ON 
        i.meterReadingUnit = m.portionNumber
    WHERE 
        i._RecordCurrent = 1 
        AND i._RecordDeleted = 0 
        AND m._RecordCurrent = 1
        AND m._RecordDeleted = 0
"""    
).drop_duplicates()

# ----------------
# Dummy Dimension
# ----------------
dummyDimRecDf = spark.createDataFrame(
    [("-1", "1900-01-01", "9999-12-31")],
    ["installationNumber", "validFromDate", "validToDate"]
)

#UNION TABLES
df_installation_history = (
    df_installation_history
    .unionByName(dummyDimRecDf, allowMissingColumns = True)
    .drop_duplicates()
)    


# COMMAND ----------

schema = StructType([
    StructField('installationHistorySK',StringType(),False),
    StructField('sourceSystemCode',StringType(),True),
    StructField('installationNumber',StringType(),False),
    StructField('validFromDate',DateType(),True),
    StructField('validToDate',DateType(),False),
    StructField('rateCategoryCode',StringType(),True),
    StructField('rateCategory',StringType(),True),
    StructField('portionNumber',StringType(),True),
    StructField('portionText',StringType(),True),
    StructField('industryCode',StringType(),True),
    StructField('industry',StringType(),True),
    StructField('billingClassCode',StringType(),True),
    StructField('billingClass',StringType(),True),
    StructField('meterReadingUnit',StringType(),True),
    StructField('industrySystemCode',StringType(),True),
    StructField('IndustrySystem',StringType(),True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Load

# COMMAND ----------

TemplateTimeSliceEtlSCD(
    df_installation_history, 
    entity="dimInstallationHistory", 
    businessKey="installationNumber,validToDate", 
    schema=schema
)

# COMMAND ----------

dbutils.notebook.exit("1")