# Databricks notebook source
# MAGIC %md 
# MAGIC # dimBusinessPartner

# COMMAND ----------

# MAGIC %md
# MAGIC ***
# MAGIC __Acronyms__
# MAGIC ***
# MAGIC *BP = Business Partner*
# MAGIC ***
# MAGIC __Methodology__
# MAGIC ***
# MAGIC 1. Build BP Tables
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
# MAGIC 3. Load Source Table into `curated.dimBusinessPartner`
# MAGIC Using the SCD loading function `TemplateEtlSCD` to merge results from source to target
# MAGIC ***

# COMMAND ----------

# MAGIC %run ../common/common-curated-includeMain

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Business Partner Tables
# MAGIC Build ISU & CRM. Business Partner dataframes

# COMMAND ----------

# MAGIC %md
# MAGIC ### ISU Business Partner

# COMMAND ----------

# ---- isu_0bpartner_attr ---- #
df_isu_0bpartner_attr = (
    spark.sql(f""" 
        SELECT 
            'ISU'                                                   AS sourceSystemCode, -- RENAMED
            businessPartnerNumber                                   AS businessPartnerNumber,
            businessPartnerCategoryCode                             AS businessPartnerCategoryCode, 
            businessPartnerCategory                                 AS businessPartnerCategory, 
            businessPartnerTypeCode                                 AS businessPartnerTypeCode, 
            businessPartnerType                                     AS businessPartnerType, 
            businessPartnerGroupCode                                AS businessPartnerGroupCode, 
            businessPartnerGroup                                    AS businessPartnerGroup, 
            externalBusinessPartnerNumber                           AS externalNumber, --RENAMED
            businessPartnerGUID                                     AS businessPartnerGUID, 
            firstName                                               AS firstName, 
            lastName                                                AS lastName, 
            middleName                                              AS middleName, 
            nickName                                                AS nickName, 
            titleCode                                               AS titleCode, 
            title                                                   AS title, 
            dateOfBirth                                             AS dateOfBirth, 
            dateOfDeath                                             AS dateOfDeath, 
            validFromDate                                           AS validFromDate, 
            validToDate                                             AS validToDate, 
            personNumber                                            AS personNumber, 
            personnelNumber                                         AS personnelNumber, 
            organizationName                                        AS organizationName, 
            organizationFoundedDate                                 AS organizationFoundedDate, -- TRANSFORMATION
            externalBusinessPartnerNumber                           AS externalBusinessPartnerNumber, 
            createdDateTime                                         AS createdDateTime, 
            createdBy                                               AS createdBy, 
            lastUpdatedDateTime, 
            lastUpdatedBy,
            naturalPersonFlag                                       AS naturalPersonFlag
        FROM {ADS_DATABASE_CLEANSED}.isu_0bpartner_attr isu
        WHERE
            businessPartnerCategoryCode in ('1','2') 
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
# MAGIC ### CRM Business Partner

# COMMAND ----------

# ---- crm_0bpartner_attr ---- #
df_crm_0bpartner_attr = (
    spark.sql(f"""
        SELECT
            'CRM'                                                  AS sourceSystemCode, -- RENAMED
            businessPartnerNumber                                  AS businessPartnerNumber, 
            businessPartnerCategoryCode                            AS businessPartnerCategoryCode, 
            businessPartnerCategory                                AS businessPartnerCategory, 
            businessPartnerTypeCode                                AS businessPartnerTypeCode, 
            businessPartnerType                                    AS businessPartnerType, 
            businessPartnerGroupCode                               AS businessPartnerGroupCode, 
            businessPartnerGroup                                   AS businessPartnerGroup, 
            externalBusinessPartnerNumber                          AS externalNumber, -- RENAMED
            businessPartnerGUID                                    AS businessPartnerGUID, 
            firstName                                              AS firstName, 
            lastName                                               AS lastName, 
            middleName                                             AS middleName, 
            nickName                                               AS nickName, 
            titleCode                                              AS titleCode, 
            title                                                  AS title, 
            dateOfBirth                                            AS dateOfBirth, 
            dateOfDeath                                            AS dateOfDeath, 
            validFromDate                                          AS validFromDate, 
            validToDate                                            AS validToDate, 
            personNumber                                           AS personNumber, 
            personnelNumber                                        AS personnelNumber, 
            organizationName                                       AS organizationName,
            organizationFoundedDate                                AS organizationFoundedDate,
            externalBusinessPartnerNumber                          AS externalBusinessPartnerNumber, 
            createdDateTime                                        AS createdDateTime, 
            createdBy                                              AS createdBy, 
            lastUpdatedDateTime, 
            lastUpdatedBy,
            /* CRM columns */
            warWidowFlag                                           AS warWidowFlag,
            deceasedFlag                                           AS deceasedFlag,
            disabilityFlag                                         AS disabilityFlag,
            goldCardHolderFlag                                     AS goldCardHolderFlag,
            consent1Indicator                                      AS consent1Indicator,
            consent2Indicator                                      AS consent2Indicator,
            eligibilityFlag                                        AS eligibilityFlag,
            paymentAssistSchemeFlag                                AS paymentAssistSchemeFlag,
            plannedChangeDocument                                  AS plannedChangeDocument,
            paymentStartDate                                       AS paymentStartDate,
            dateOfCheck                                            AS dateOfCheck,
            pensionConcessionCardFlag                              AS pensionConcessionCardFlag,
            pensionType                                            AS pensionType,
            naturalPersonFlag                                      AS naturalPersonFlag
        FROM {ADS_DATABASE_CLEANSED}.crm_0bpartner_attr 
        WHERE 
            businessPartnerCategoryCode in ('1','2') 
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
# MAGIC ## 2. dimBusinessPartner
# MAGIC Combine ISU and CRM to build a master BP dataframe
# MAGIC 
# MAGIC <br> **ISU / CRM Combination Logic** 
# MAGIC - **IF** ISU & CRM BP exists:
# MAGIC     - **THEN** return ISU columns + CRM unique columns
# MAGIC -  **IF** ISU BP exists, and CRM BP does not:
# MAGIC     - **THEN** return ISU Columns
# MAGIC - **IF** ISU BP does not exist, and CRM BP does:
# MAGIC     - **THEN** return CRM columns

# COMMAND ----------

from pyspark.sql.functions import col, coalesce
# ------------------------------- #
# Steps:
#   1) Combine ISU & CRM
#       -> ISU & CRM Matches
#       -> CRM Unique
#   2) Dummy Dimension DF
#   3) Union Dataframes
# ------------------------------- #

# ------------------------------- #
# 1.1) ISU & CRM Combined
# ------------------------------- #
df_bpartner_isu = (
    df_isu_0bpartner_attr.alias("isu")
    .join(
        df_crm_0bpartner_attr.alias("crm"),
        col("isu.businessPartnerNumber") == col("crm.businessPartnerNumber"),
        how = 'left'
    )
    .drop("crm.sourceSystemCode")
    # --- Transformations --- #
    # naturalPersonFlag
    .withColumn(
        '_naturalPersonFlag',
        coalesce(
            col("isu.naturalPersonFlag"), 
            col("crm.naturalPersonFlag")
        )
    )
    # --- drop duplicate columns --- #
    .drop("naturalPersonFlag") 
    # --- Select Columns --- #
    .select(
        "isu.*",
        "crm.warWidowFlag",
        "crm.deceasedFlag",
        "crm.disabilityFlag",
        "crm.goldCardHolderFlag",
        "crm.consent1Indicator",
        "crm.consent2Indicator",
        "crm.eligibilityFlag",
        "crm.paymentAssistSchemeFlag",
        "crm.plannedChangeDocument",
        "crm.paymentStartDate",
        "crm.dateOfCheck",
        "crm.pensionConcessionCardFlag",
        "crm.pensionType",
        "_naturalPersonFlag"
    )
    # --- rename duplicate columns --- #
    .withColumnRenamed(
        "_naturalPersonFlag",
        "naturalPersonFlag"
    )
    .drop_duplicates()
)

# ------------------------------- #
# 1.2) CRM Unique
# ------------------------------- #
df_bpartner_crm_unique = (
    df_crm_0bpartner_attr.alias("crm")
    .join(
        df_isu_0bpartner_attr.alias("isu"),
        col("crm.businessPartnerNumber") == col("isu.businessPartnerNumber"),
        how = 'leftanti'
    )
    .select("crm.*")
    .drop_duplicates()
)

# ------------------------------- #
# 2) Dummy Dimension
# ------------------------------- #
# dummy dimension
dummyDimRecDf = (
    spark.createDataFrame(
        [("-1")], 
        ["businessPartnerNumber"]
    )
)

# ------------------------------- #
# 3) Master Dataframe
# ------------------------------- #
df_bpartner_master = (
    df_bpartner_isu
    # --- Union Dataframes --- #
    .unionByName(df_bpartner_crm_unique)
    .unionByName(dummyDimRecDf, allowMissingColumns = True)
    
    # --- Order Columns --- #
    .select(
        "sourceSystemCode",
        "businessPartnerNumber",
        "businessPartnerCategoryCode",
        "businessPartnerCategory",
        "businessPartnerTypeCode",
        "businessPartnerType",
        "businessPartnerGroupCode",
        "businessPartnerGroup",
        "externalNumber",
        "businessPartnerGUID",
        "firstName",
        "lastName",
        "middleName",
        "nickName",
        "titleCode",
        "title",
        "dateOfBirth",
        "dateOfDeath",
        "validFromDate",
        "validToDate",
        "warWidowFlag",
        "deceasedFlag",
        "disabilityFlag",
        "goldCardHolderFlag",
        "naturalPersonFlag",
        "consent1Indicator",
        "consent2Indicator",
        "eligibilityFlag",
        "paymentAssistSchemeFlag",
        "plannedChangeDocument",
        "paymentStartDate",
        "dateOfCheck",
        "pensionConcessionCardFlag",
        "pensionType",
        "personNumber",
        "personnelNumber",
        "organizationName",
        "organizationFoundedDate",
        "createdDateTime",
        "createdBy",
        "lastUpdatedBy",
        "lastUpdatedDateTime"
    )
    .cache()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Load

# COMMAND ----------

# ---- Apply Schema Definition ---- #
from pyspark.sql.types import *
schema = StructType([
    StructField('businessPartnerSK', StringType(), False),
    StructField('sourceSystemCode', StringType(), True),
    StructField('businessPartnerNumber', StringType(), False),
    StructField('businessPartnerCategoryCode', StringType(), True),
    StructField('businessPartnerCategory', StringType(), True),
    StructField('businessPartnerTypeCode', StringType(), True),
    StructField('businessPartnerType', StringType(), True),
    StructField('businessPartnerGroupCode', StringType(), True),
    StructField('businessPartnerGroup', StringType(), True),
    StructField('externalNumber', StringType(), True),
    StructField('businessPartnerGUID', StringType(), True),
    StructField('firstName', StringType(), True),
    StructField('lastName', StringType(), True),
    StructField('middleName', StringType(), True),
    StructField('nickName', StringType(), True),
    StructField('titleCode', StringType(), True),
    StructField('title', StringType(), True),
    StructField('dateOfBirth', DateType(), True),
    StructField('dateOfDeath', DateType(), True),
    StructField('validFromDate', DateType(), True),
    StructField('validToDate', DateType(), True),
    StructField('warWidowFlag', StringType(), True),
    StructField('deceasedFlag', StringType(), True),
    StructField('disabilityFlag', StringType(), True),
    StructField('goldCardHolderFlag', StringType(), True),
    StructField('naturalPersonFlag', StringType(), True),
    StructField('consent1Indicator', StringType(), True),
    StructField('consent2Indicator', StringType(), True),
    StructField('eligibilityFlag', StringType(), True),
    StructField('paymentAssistSchemeFlag', StringType(), True),
    StructField('plannedChangeDocument', StringType(), True),
    StructField('paymentStartDate', DateType(), True),
    StructField('dateOfCheck', DateType(), True),
    StructField('pensionConcessionCardFlag', StringType(), True),
    StructField('pensionType', StringType(), True),
    StructField('personNumber', StringType(), True),
    StructField('personnelNumber', StringType(), True),
    StructField('organizationName', StringType(), True),
    StructField('organizationFoundedDate', DateType(), True),
    StructField('createdDateTime', TimestampType(), True),
    StructField('createdBy', StringType(), True),
    StructField('lastUpdatedBy', StringType(), True),
    StructField('lastUpdatedDateTime', StringType(), True)
]) 

# ---- Load Data with SCD --- #
TemplateEtlSCD(
    df_bpartner_master, 
    entity="dimBusinessPartner", 
    businessKey="businessPartnerNumber",
    schema=schema
)

# COMMAND ----------

dbutils.notebook.exit("1")
