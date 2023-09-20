# Databricks notebook source
# MAGIC %md 
# MAGIC # dimBusinessPartnerIdentification

# COMMAND ----------

# MAGIC %run ../common/common-curated-includeMain

# COMMAND ----------

# MAGIC %md
# MAGIC ***
# MAGIC __Acronyms__
# MAGIC ***
# MAGIC *BP = Business Partner*
# MAGIC <br> *BPID = Business Partner Identification*
# MAGIC ***
# MAGIC __Methodology__
# MAGIC ***
# MAGIC 1. Build BPG Tables
# MAGIC     - Query ISU & CRM BP tables
# MAGIC     
# MAGIC 2. Combine ISU and CRM tables
# MAGIC     <br> **ISU / CRM Combination Logic** 
# MAGIC     - **IF** ISU & CRM BPID exists:
# MAGIC         - **THEN** return ISU columns
# MAGIC     -  **ELSE** return CRM Columns
# MAGIC
# MAGIC 3. Load Source Table into `{ADS_DATABASE_CURATED}.dim.BusinessPartnerIdentification`
# MAGIC Using the SCD loading function `TemplateEtlSCD` to merge results from source to target
# MAGIC ***

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. BP Identification Tables
# MAGIC - Build Business Partner Identification dataframes from ISU & CRM.
# MAGIC - Merge both CRM & ISU dataframes to create a "Master Business Partner Identification" dataframe

# COMMAND ----------

df_isu_0bp_id_attr = (
 spark.sql(f""" 
        SELECT 
            'ISU'                       AS sourceSystemCode, 
            businessPartnerNumber,
            businessPartnerIdNumber,
            identificationTypeCode,
            identificationType,
            validFromDate,
            validToDate,
            entryDate,
            institute,
            _RecordDeleted 
        FROM {ADS_DATABASE_CLEANSED}.isu.0bp_id_attr
        WHERE 
            _RecordCurrent = 1 
    """
    )
    .cache()
)

df_crm_0bp_id_attr = (
    spark.sql(f""" 
        SELECT 
            'CRM'                       AS sourceSystemCode, 
            businessPartnerNumber,
            businessPartnerIdNumber,
            identificationTypeCode,
            identificationType,
            validFromDate,
            validToDate,
            entryDate,
            institute,
            _RecordDeleted 
        FROM {ADS_DATABASE_CLEANSED}.crm.0bp_id_attr
        WHERE 
            _RecordCurrent = 1 
"""
    )
    .cache()
)

# print('isu count', df_isu_0bp_id_attr.count())
# display(df_isu_0bp_id_attr)

# print('crm count', df_crm_0bp_id_attr.count())
# display(df_crm_0bp_id_attr)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Combine Dataframes

# COMMAND ----------

from pyspark.sql.functions import col, coalesce

# ------------------------------- #
# Unique CRM
# ------------------------------- #
df_bpid_crm_unique = (
    df_crm_0bp_id_attr.alias("crm")
    .join(
        df_isu_0bp_id_attr.alias("isu"),
        [
            col("crm.businessPartnerNumber") == col("isu.businessPartnerNumber"),
            col("crm.businessPartnerIdNumber") == col("isu.businessPartnerIdNumber"),
            col("crm.identificationTypeCode") == col("isu.identificationTypeCode")
        ],
        how = 'leftanti'
    )
    .select("crm.*")
    .drop_duplicates()
)

# ------------------------------- #
# Dummy Dimension
# ------------------------------- #
dummyDimRecDf = (
    spark.createDataFrame(
        [("-1", "-1", "Unknown")], 
        ["businessPartnerNumber", "businessPartnerIdNumber", "identificationTypeCode"]
    )
)

# ------------------------------- #
# Master Business Partner
# ------------------------------- #

df_bpid_master = (
    df_isu_0bp_id_attr
    .unionByName(df_bpid_crm_unique)
    .unionByName(dummyDimRecDf, allowMissingColumns = True)
    .drop_duplicates()
    .cache()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Load

# COMMAND ----------

# ---- Apply Schema Definition ---- #
from pyspark.sql.types import *
schema = StructType([
    StructField('businessPartnerIdentificationSK', StringType(), False),
    StructField('sourceSystemCode', StringType(), True),
    StructField('businessPartnerNumber', StringType(), False),
    StructField('businessPartnerIdNumber', StringType(), False),
    StructField('identificationTypeCode', StringType(), True),
    StructField('identificationType', StringType(), True),
    StructField('validFromDate', StringType(), True),
    StructField('validToDate', StringType(), True),
    StructField('entryDate', StringType(), True),
    StructField('institute', StringType(), True)
    ])

# ---- Load Data with SCD --- #
TemplateEtlSCD(
    df_bpid_master, 
    entity="dim.businessPartnerIdentification", 
    businessKey="businessPartnerNumber,businessPartnerIdNumber,identificationTypeCode",
    schema=schema
)

# COMMAND ----------

dbutils.notebook.exit("1")
