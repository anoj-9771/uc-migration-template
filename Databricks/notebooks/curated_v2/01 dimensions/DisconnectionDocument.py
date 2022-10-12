# Databricks notebook source
# MAGIC %md
# MAGIC # DimDisconnectionDocument

# COMMAND ----------

# MAGIC %md 
# MAGIC ## 1. Load From Cleansed

# COMMAND ----------

# MAGIC %run ../common/common-curated-includeMain

# COMMAND ----------


df_isu_0uc_isu_32 = spark.sql(f"""
    SELECT
        'ISU'                                     AS sourceSystemCode,
        disconnectionDocumentNumber               AS disconnectionDocumentNumber,
        disconnectionObjectNumber                 AS disconnectionObjectNumber,
        disconnectionActivityPeriod               AS disconnectionActivityPeriod,
        disconnectionDate                         AS disconnectionDate,
        validFromDate                             AS validFromDate,
        validToDate                               AS validToDate,
        disconnectionActivityTypeCode             AS disconnectionActivityTypeCode,
        disconnectionActivityType                 AS disconnectionActivityType,
        disconnectionObjectTypeCode               AS disconnectionObjectTypeCode,
        referenceObjectTypeCode                   AS referenceObjectTypeCode,
        disconnectionReasonCode                   AS disconnectionReasonCode,
        disconnectionReason                       AS disconnectionReason,
        processingVariantCode                     AS processingVariantCode,
        processingVariant                         AS processingVariant,
        disconnectionReconnectionStatusCode       AS disconnectionReconnectionStatusCode,
        disconnectionReconnectionStatus           AS disconnectionReconnectionStatus,
        disconnectionDocumentStatusCode           AS disconnectionDocumentStatusCode,
        disconnectionDocumentStatus               AS disconnectionDocumentStatus,
        installationNumber                        AS installationNumber,
        equipmentNumber                           AS equipmentNumber,
        propertyNumber                            AS propertyNumber
    FROM {ADS_DATABASE_CLEANSED}.isu_0uc_isu_32
    WHERE 
        _RecordCurrent = 1 
        AND _RecordDeleted = 0
    """
)

dummy_dim = (
    spark.createDataFrame(
    [("-1", "-1", "Unknown")], 
    ["disconnectionDocumentNumber", "disconnectionObjectNumber", "disconnectionActivityPeriod"]
    )
)

df_disconnection_document = (
    df_isu_0uc_isu_32
    .unionByName(dummy_dim, allowMissingColumns = True)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Load

# COMMAND ----------

schema = StructType([
    StructField('disconnectionDocumentSK',StringType(),False),
    StructField('sourceSystemCode',StringType(),True),
    StructField('disconnectionDocumentNumber',StringType(),False),
    StructField('disconnectionObjectNumber',StringType(),False),
    StructField('disconnectionActivityPeriod',StringType(),False),
    StructField('disconnectionDate',DateType(),True),
    StructField('validFromDate',DateType(),True),
    StructField('validToDate',DateType(),True),
    StructField('disconnectionActivityTypeCode',StringType(),True),
    StructField('disconnectionActivityType',StringType(),True),
    StructField('disconnectionObjectTypeCode',StringType(),True),
    StructField('referenceObjectTypeCode', StringType(), True),
    StructField('disconnectionReasonCode',StringType(),True),
    StructField('disconnectionReason',StringType(),True),
    StructField('processingVariantCode',StringType(),True),
    StructField('processingVariant',StringType(),True),
    StructField('disconnectionReconnectionStatusCode',StringType(),True),
    StructField('disconnectionReconnectionStatus',StringType(),True),
    StructField('disconnectionDocumentStatusCode',StringType(),True),
    StructField('disconnectionDocumentStatus',StringType(),True),
    StructField('installationNumber',StringType(),True),
    StructField('equipmentNumber',StringType(),True),
    StructField('propertyNumber',StringType(),True)
])

# COMMAND ----------

TemplateEtlSCD(
    df_disconnection_document, 
    entity="dimDisconnectionDocument", 
    businessKey="sourceSystemCode,disconnectionDocumentNumber,disconnectionObjectNumber,disconnectionActivityPeriod",
    schema=schema
)

# COMMAND ----------

dbutils.notebook.exit("1")
