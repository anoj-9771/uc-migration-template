# Databricks notebook source
# MAGIC %md 
# MAGIC # dimBusinessPartnerGroup

# COMMAND ----------

# MAGIC %run ../common/common-curated-includeMain

# COMMAND ----------

# MAGIC %md
# MAGIC ***
# MAGIC __Acronyms__
# MAGIC ***
# MAGIC *BP = Business Partner*
# MAGIC <br> *BPG = Business Partner Group*
# MAGIC ***
# MAGIC __Methodology__
# MAGIC ***
# MAGIC 1. Build BPG Tables
# MAGIC     - Query ISU & CRM BP tables
# MAGIC     
# MAGIC 2. Combine ISU and CRM tables
# MAGIC     <br> **ISU / CRM Combination Logic** 
# MAGIC     - **IF** ISU & CRM BP exists:
# MAGIC         - **THEN** return ISU columns + CRM unique columns
# MAGIC     -  **IF** ISU BP exists, and CRM BP does not:
# MAGIC         - **THEN** return ISU Columns
# MAGIC     - **IF** ISU BP does not exist, and CRM BP does:
# MAGIC         - **THEN** return CRM columns
# MAGIC 
# MAGIC 3. Load Source Table into `curated.dimBusinessPartnerGroup`
# MAGIC Using the SCD loading function `TemplateEtlSCD` to merge results from source to target
# MAGIC ***

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Business Partner Tables
# MAGIC - Build Business Partner dataframes from ISU & CRM.
# MAGIC - Merge both CRM & ISU dataframes to create a "Master Business Partner" dataframe

# COMMAND ----------

# MAGIC %md
# MAGIC ### ISU Business Partner Group

# COMMAND ----------

# ---- isu_0bpartner_attr ---- #
df_isu_0bpartner_attr = (
    spark.sql(f""" 
        SELECT 
            'ISU'                                     AS sourceSystemCode, 
            businessPartnerNumber                     AS businessPartnerGroupNumber, -- RENAMED
            businessPartnerGroupCode                  AS businessPartnerGroupCode,
            businessPartnerGroup                      AS businessPartnerGroup,
            businessPartnerCategoryCode               AS businessPartnerCategoryCode,
            businessPartnerCategory                   AS businessPartnerCategory,
            businessPartnerTypeCode                   AS businessPartnerTypeCode,
            businessPartnerType                       AS businessPartnerType,
            externalBusinessPartnerNumber             AS externalNumber, -- RENAMED
            businessPartnerGUID                       AS businessPartnerGUID,
            nameGroup1                                AS businessPartnerGroupName1, -- RENAMED
            nameGroup2                                AS businessPartnerGroupName2, -- RENAMED
            createdBy                                 AS createdBy,
            createdDateTime                           AS createdDateTime,
            lastUpdatedBy                             AS lastUpdatedBy, 
            lastUpdatedDateTime                       AS lastUpdatedDateTime, 
            validFromDate                             AS validFromDate,
            validToDate                               AS validToDate
        FROM {ADS_DATABASE_CLEANSED}.isu_0bpartner_attr isu
        WHERE
            businessPartnerCategoryCode = '3' -- FILTERS TO GROUP
            AND _RecordCurrent = 1 
            AND _RecordDeleted = 0 
    """
    )
    .cache()
)
# print('isu_0bpartner_attr count:', df_isu_0bpartner_attr.count())
# display(df_isu_0bpartner_attr)

# COMMAND ----------

# MAGIC %md
# MAGIC ### CRM Business Partner Group

# COMMAND ----------

# ---- crm_0bpartner_attr ---- #
df_crm_0bpartner_attr = (
    spark.sql(f"""
        SELECT 
            'CRM'                                     AS sourceSystemCode, 
            businessPartnerNumber                     AS businessPartnerGroupNumber, -- RENAMED
            businessPartnerGroupCode                  AS businessPartnerGroupCode,
            businessPartnerGroup                      AS businessPartnerGroup,
            businessPartnerCategoryCode               AS businessPartnerCategoryCode,
            businessPartnerCategory                   AS businessPartnerCategory,
            businessPartnerTypeCode                   AS businessPartnerTypeCode,
            businessPartnerType                       AS businessPartnerType,
            externalBusinessPartnerNumber             AS externalNumber, -- RENAMED
            businessPartnerGUID                       AS businessPartnerGUID,
            nameGroup1                                AS businessPartnerGroupName1, -- RENAMED
            nameGroup2                                AS businessPartnerGroupName2, -- RENAMED
            createdBy                                 AS createdBy,
            createdDateTime                           AS createdDateTime,
            lastUpdatedBy                             AS lastUpdatedBy, 
            lastUpdatedDateTime                       AS lastUpdatedDateTime, 
            validFromDate                             AS validFromDate,
            validToDate                               AS validToDate,
            /* CRM columns */
            paymentAssistSchemeFlag                   AS paymentAssistSchemeFlag,
            billAssistFlag                            AS billAssistFlag,
            consent1Indicator                         AS consent1Indicator,
            warWidowFlag                              AS warWidowFlag,
            userId                                    AS indicatorCreatedUserId, -- RENAMED
            createdDate                               AS indicatorCreatedDate, -- RENAMED
            kidneyDialysisFlag                        AS kidneyDialysisFlag,
            patientUnit                               AS patientUnit,
            patientTitleCode                          AS patientTitleCode,
            patientTitle                              AS patientTitle,
            patientFirstName                          AS patientFirstName,
            patientSurname                            AS patientSurname,
            patientAreaCode                           AS patientAreaCode,
            patientPhoneNumber                        AS patientPhoneNumber,
            hospitalCode                              AS hospitalCode,
            hospitalName                              AS hospitalName,
            patientMachineTypeCode                    AS patientMachineTypeCode,
            patientMachineType                        AS patientMachineType,
            machineTypeValidFromDate                  AS machineTypeValidFromDate,
            machineTypeValidToDate                    AS machineTypeValidToDate,
            machineOffReasonCode                      AS machineOffReasonCode,
            machineOffReason                          AS machineOffReason
        FROM {ADS_DATABASE_CLEANSED}.crm_0bpartner_attr crm
        WHERE 
            businessPartnerCategoryCode = '3' -- FILTERS TO GROUP
            AND _RecordCurrent = 1 
            AND _RecordDeleted = 0
    """
    )
    .cache()
)
# print('crm_0bpartner_attr count:', df_crm_0bpartner_attr.count())
# display(df_crm_0bpartner_attr)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Master Business Partner Group

# COMMAND ----------

from pyspark.sql.functions import col, coalesce
# ------------------------------- #
# ISU & CRM Inner
# ------------------------------- #
df_bpartner_isu = (
    df_isu_0bpartner_attr.alias("isu")
    .join(
        df_crm_0bpartner_attr.alias("crm"),
        col("isu.businessPartnerGroupNumber") == col("crm.businessPartnerGroupNumber"),
        how = 'left'
    )
    # --- Select Columns --- #
    .select(
        "isu.*",
        # --- CRM attributes ---#
        "crm.paymentAssistSchemeFlag",
        "crm.billAssistFlag",
        "crm.consent1Indicator",
        "crm.warWidowFlag",
        "crm.indicatorCreatedUserId",
        "crm.indicatorCreatedDate",
        "crm.kidneyDialysisFlag",
        "crm.patientUnit",
        "crm.patientTitleCode",
        "crm.patientTitle",
        "crm.patientFirstName",
        "crm.patientSurname",
        "crm.patientAreaCode",
        "crm.patientPhoneNumber",
        "crm.hospitalCode",
        "crm.hospitalName",
        "crm.patientMachineTypeCode",
        "crm.patientMachineType",
        "crm.machineTypeValidFromDate",
        "crm.machineTypeValidToDate",
        "crm.machineOffReasonCode",
        "crm.machineOffReason"
    )
    .drop_duplicates()
)

# ------------------------------- #
# CRM Unique
# ------------------------------- #
df_bpartner_crm_unique = (
    df_crm_0bpartner_attr.alias("crm")
    .join(
        df_isu_0bpartner_attr.alias("isu"),
        col("crm.businessPartnerGroupNumber") == col("isu.businessPartnerGroupNumber"),
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
        [("-1", "Unknown")], 
        ["businessPartnerGroupNumber","businessPartnerGroupName1"]
    )
)

# ------------------------------- #
# Master Business Partner Group
# ------------------------------- #
df_bpartnergroup_master = (
    df_bpartner_isu
    # --- Append Dataframes --- #
    .unionByName(df_bpartner_crm_unique)
    .unionByName(
        dummyDimRecDf,
        allowMissingColumns = True
    )
    # --- order columms --- #
    .select(
        "sourceSystemCode",
        "businessPartnerGroupNumber",
        "businessPartnerGroupCode",
        "businessPartnerGroup",
        "businessPartnerCategoryCode",
        "businessPartnerCategory",
        "businessPartnerTypeCode",
        "businessPartnerType",
        "externalNumber",
        "businessPartnerGUID",
        "businessPartnerGroupName1",
        "businessPartnerGroupName2",
        "paymentAssistSchemeFlag",
        "billAssistFlag",
        "consent1Indicator",
        "warWidowFlag",
        "indicatorCreatedUserId",
        "indicatorCreatedDate",
        "kidneyDialysisFlag",
        "patientUnit",
        "patientTitleCode",
        "patientTitle",
        "patientFirstName",
        "patientSurname",
        "patientAreaCode",
        "patientPhoneNumber",
        "hospitalCode",
        "hospitalName",
        "patientMachineTypeCode",
        "patientMachineType",
        "machineTypeValidFromDate",
        "machineTypeValidToDate",
        "machineOffReasonCode",
        "machineOffReason",
        "createdBy",
        "createdDateTime",
        "lastUpdatedBy",
        "lastUpdatedDateTime",
        "validFromDate",
        "validToDate"
    )
    .drop_duplicates()
    .cache()
)

# print('df_bpartner_inner count:', df_bpartner_inner.count())
# print('df_bpartner_isu_unique count:', df_bpartner_isu_unique.count())
# print('df_bpartner_crm_unique count', df_bpartner_crm_unique.count())
# print('dummyDimRecDf count:', dummyDimRecDf.count())
# print('df_bpartnergroup_master count:', df_bpartnergroup_master.count())
# display(df_bpartnergroup_master)

# COMMAND ----------

# ---- Apply Schema Definition ---- #
from pyspark.sql.types import *
schema = StructType([
    StructField('businessPartnerGroupSK', StringType(), False),
    StructField('sourceSystemCode', StringType(), True),
    StructField('businessPartnerGroupNumber', StringType(), False),
    StructField('businessPartnerGroupCode', StringType(), True),
    StructField('businessPartnerGroup', StringType(), True),
    StructField('businessPartnerCategoryCode', StringType(), True),
    StructField('businessPartnerCategory', StringType(), True),
    StructField('businessPartnerTypeCode', StringType(), True),
    StructField('businessPartnerType', StringType(), True),
    StructField('externalNumber', StringType(), True),
    StructField('businessPartnerGUID', StringType(), True),
    StructField('businessPartnerGroupName1', StringType(), True),
    StructField('businessPartnerGroupName2', StringType(), True),
    StructField('paymentAssistSchemeFlag', StringType(), True),
    StructField('billAssistFlag', StringType(), True),
    StructField('consent1Indicator', StringType(), True),
    StructField('warWidowFlag', StringType(), True),
    StructField('indicatorCreatedUserId', StringType(), True),
    StructField('indicatorCreatedDate', DateType(), True),
    StructField('kidneyDialysisFlag', StringType(), True),
    StructField('patientUnit', StringType(), True),
    StructField('patientTitleCode', StringType(), True),
    StructField('patientTitle', StringType(), True),
    StructField('patientFirstName', StringType(), True),
    StructField('patientSurname', StringType(), True),
    StructField('patientAreaCode', StringType(), True),
    StructField('patientPhoneNumber', StringType(), True),
    StructField('hospitalCode', StringType(), True),
    StructField('hospitalName', StringType(), True),
    StructField('patientMachineTypeCode', StringType(), True),
    StructField('patientMachineType', StringType(), True),
    StructField('machineTypeValidFromDate', StringType(), True),
    StructField('machineTypeValidToDate', StringType(), True),
    StructField('machineOffReasonCode', StringType(), True),
    StructField('machineOffReason', StringType(), True),
    StructField('createdBy', StringType(), True),
    StructField('createdDateTime', TimestampType(), True),
    StructField('lastUpdatedBy', StringType(), True),
    StructField('lastUpdatedDateTime', TimestampType(), True),
    StructField('validFromDate', DateType(), True),
    StructField('validToDate', DateType(), True)
]) 

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Load

# COMMAND ----------

TemplateEtlSCD(
    df_bpartnergroup_master, 
    entity="dimBusinessPartnerGroup", 
    businessKey="businessPartnerGroupNumber",
    schema=schema
)

# COMMAND ----------

dbutils.notebook.exit("1")
