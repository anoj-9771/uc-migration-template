# Databricks notebook source
# MAGIC %md 
# MAGIC # dimBusinessPartnerAddress

# COMMAND ----------

# MAGIC %run ../common/common-curated-includeMain

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. BP Address Tables

# COMMAND ----------

# MAGIC %md
# MAGIC - Build Business Partner Address dataframes from ISU & CRM.
# MAGIC     - CRM unique records will be appended. All other records will persist with ISU information

# COMMAND ----------

df_isu_addr_attr = (
    spark.sql(f""" 
        SELECT 
            'ISU'                                                               AS sourceSystemCode, -- NEW COLUMN
            businessPartnerNumber                                               AS businessPartnerNumber,
            addressNumber                                                       AS businessPartnerAddressNumber, 
            validFromDate                                                       AS addressValidFromDate, -- RENAMED
            validToDate                                                         AS addressValidToDate, -- RENAMED
            phoneNumber                                                         AS phoneNumber, 
            phoneExtension                                                      AS phoneExtension, 
            faxNumber                                                           AS faxNumber, 
            faxExtension                                                        AS faxExtension, 
            emailAddress                                                        AS emailAddress, 
            personalAddressFlag                                                 AS personalAddressFlag, 
            coName                                                              AS coName, 
            shortFormattedAddress2                                              AS shortFormattedAddress2, 
            streetLine5                                                         AS streetLine5, 
            building                                                            AS building, 
            floorNumber                                                         AS floorNumber, 
            apartmentNumber                                                     AS apartmentNumber, 
            housePrimaryNumber                                                  AS housePrimaryNumber, 
            houseSupplementNumber                                               AS houseSupplementNumber, 
            streetName                                                          AS streetPrimaryName, -- RENAMED
            streetSupplementName1                                               AS streetSupplementName1, 
            streetSupplementName2                                               AS streetSupplementName2, 
            otherLocationName                                                   AS otherLocationName, 
            concat( 
                coalesce(concat(streetLine5, ', '), ''), 
                coalesce(concat(building, ', '), ''), 
                coalesce(concat(floorNumber, ', '), ''), 
                coalesce(concat(apartmentNumber, ', '), ''), 
                coalesce(concat(houseSupplementNumber, ', '), ''), 
                coalesce(concat(housePrimaryNumber, ', '), '') 
            )                                                                   AS houseNumber, -- TRANSFORMATION
            concat( 
                coalesce(concat(streetName, ', '), ''), 
                coalesce(concat(streetSupplementName1, ', '), ''),
                coalesce(concat(streetSupplementName2, ', '), ''), 
                coalesce(concat(otherLocationName, ', '), '')  
            )                                                                   AS streetName, -- TRANSFORMATION
            streetCode                                                          AS streetCode,   
            cityName                                                            AS cityName, 
            cityCode                                                            AS cityCode, 
            CASE 
                WHEN countryCode = 'AU' AND cityCode IS NULL 
                THEN '' 
                ELSE stateCode 
            END                                                                 AS stateCode, -- TRANSFORMATION
            CASE 
                WHEN countryCode = 'AU' AND cityCode IS NULL 
                THEN '' 
                ELSE stateName 
            END                                                                 AS stateName, -- TRANSFORMATION
            postalCode                                                          AS postalCode, 
            CASE 
                WHEN countryCode = 'AU' AND cityCode IS NULL 
                THEN '' 
                ELSE countryCode 
            END                                                                 AS countryCode, -- TRANSFORMATION
            CASE 
                WHEN countryCode = 'AU' AND cityCode IS NULL 
                THEN '' 
                ELSE countryName
            END                                                                 AS countryName, -- TRANSFORMATION
            poBoxCode                                                           AS poBoxCode, 
            poBoxCity                                                           AS poBoxCity, 
            postalCodeExtension                                                 AS postalCodeExtension, 
            poBoxExtension                                                      AS poBoxExtension, 
            deliveryServiceTypeCode                                             AS deliveryServiceTypeCode, 
            deliveryServiceType                                                 AS deliveryServiceType, -- RENAMED
            deliveryServiceNumber                                               AS deliveryServiceNumber, 
            addressTimeZone                                                     AS addressTimeZone, 
            communicationAddressNumber                                          AS communicationAddressNumber,
            CASE 
                WHEN countryCode = 'AU' AND cityCode IS NULL 
                THEN ''
                ELSE CONCAT(
                    coalesce(concat(streetLine5, ', '), ''), 
                    coalesce(concat(building, ', '), ''), 
                    coalesce(concat(floorNumber, ', '), ''), 
                    coalesce(concat(apartmentNumber, ', '), ''), 
                    coalesce(concat(houseSupplementNumber, ', '), ''), 
                    coalesce(concat(housePrimaryNumber, ', '), ''),
                    coalesce(concat(streetName, ', '), ''), 
                    coalesce(concat(streetSupplementName1, ', '), ''),
                    coalesce(concat(streetSupplementName2, ', '), ''), 
                    coalesce(concat(otherLocationName, ', '), ''),
                    coalesce(concat(deliveryServiceTypeCode, ', '), ''),
                    coalesce(concat(deliveryServiceNumber, ', '), ''),
                    coalesce(concat(cityName, ', '), ''),
                    coalesce(concat(stateCode, ', '), ''),
                    coalesce(postalCode, '')
                )
            END                                                                 AS addressFullText -- TRANSFORMATION
        FROM {ADS_DATABASE_CLEANSED}.isu_0bp_def_address_attr 
        WHERE 
            _RecordCurrent = 1 
            AND _RecordDeleted = 0 
            AND addressNumber IS NOT NULL
            AND addressNumber <> ''
    """
    )
    .cache()
)

df_crm_addr_attr = (
    spark.sql(f""" 
        SELECT 
            'CRM'                                                               AS sourceSystemCode, -- NEW COLUMN
            businessPartnerNumber                                               AS businessPartnerNumber,
            addressNumber                                                       AS businessPartnerAddressNumber,
            validFromDate                                                       AS addressValidFromDate, -- RENAMED
            validToDate                                                         AS addressValidToDate, -- RENAMED
            phoneNumber                                                         AS phoneNumber,
            phoneExtension                                                      AS phoneExtension,
            faxNumber                                                           AS faxNumber,
            faxExtension                                                        AS faxExtension,
            emailAddress                                                        AS emailAddress,
            personalAddressFlag                                                 AS personalAddressFlag,
            coName                                                              AS coName,
            shortFormattedAddress2                                              AS shortFormattedAddress2,
            streetLine5                                                         AS streetLine5,
            building                                                            AS building,
            floorNumber                                                         AS floorNumber,
            apartmentNumber                                                     AS apartmentNumber,
            housePrimaryNumber                                                  AS housePrimaryNumber, 
            houseSupplementNumber                                               AS houseSupplementNumber, 
            streetName                                                          AS streetPrimaryName, -- RENAMED
            streetSupplementName1                                               AS streetSupplementName1,
            streetSupplementName2                                               AS streetSupplementName2,
            otherLocationName                                                   AS otherLocationName,
            concat( 
                coalesce(concat(streetLine5, ', '), ''), 
                coalesce(concat(building, ', '), ''), 
                coalesce(concat(floorNumber, ', '), ''), 
                coalesce(concat(apartmentNumber, ', '), ''), 
                coalesce(concat(houseSupplementNumber, ', '), ''), 
                coalesce(concat(housePrimaryNumber, ', '), '') 
            )                                                                   AS houseNumber, -- TRANSFORMATION
            concat( 
                coalesce(concat(streetName, ', '), ''), 
                coalesce(concat(streetSupplementName1, ', '), ''),
                coalesce(concat(streetSupplementName2, ', '), ''), 
                coalesce(concat(otherLocationName, ', '), '')  
            )                                                                   AS streetName, -- TRANSFORMATION 
            streetCode                                                          AS streetCode,
            cityName                                                            AS cityName,
            cityCode                                                            AS cityCode,
            CASE 
                WHEN countryCode = 'AU' AND cityCode IS NULL 
                THEN '' 
                ELSE stateCode 
            END                                                                 AS stateCode, -- TRANSFORMATION
            CASE 
                WHEN countryCode = 'AU' AND cityCode IS NULL 
                THEN '' 
                ELSE stateName 
            END                                                                 AS stateName, -- TRANSFORMATION
            postalCode                                                          AS postalCode,
            CASE 
                WHEN countryCode = 'AU' AND cityCode IS NULL 
                THEN '' 
                ELSE countryCode 
            END                                                                 AS countryCode, -- TRANSFORMATION 
            countryName                                                         AS countryName,
            poBoxCode                                                           AS poBoxCode,
            poBoxCity                                                           AS poBoxCity,
            NULL                                                                AS postalCodeExtension, -- COLUMN N/A
            NULL                                                                AS poBoxExtension,  -- COLUMN N/A
            NULL                                                                AS deliveryServiceTypeCode,  -- COLUMN N/A
            NULL                                                                AS deliveryServiceType, -- COLUMN N/A
            NULL                                                                AS deliveryServiceNumber,  -- COLUMN N/A
            addressTimeZone                                                     AS addressTimeZone,
            communicationAddressNumber                                          AS communicationAddressNumber,
            CASE 
                WHEN countryCode = 'AU' AND cityCode IS NULL 
                THEN ''
                ELSE CONCAT(
                    coalesce(concat(streetLine5, ', '), ''), 
                    coalesce(concat(building, ', '), ''), 
                    coalesce(concat(floorNumber, ', '), ''), 
                    coalesce(concat(apartmentNumber, ', '), ''), 
                    coalesce(concat(houseSupplementNumber, ', '), ''), 
                    coalesce(concat(housePrimaryNumber, ', '), ''),
                    coalesce(concat(streetName, ', '), ''), 
                    coalesce(concat(streetSupplementName1, ', '), ''),
                    coalesce(concat(streetSupplementName2, ', '), ''), 
                    coalesce(concat(otherLocationName, ', '), ''),
                    -- coalesce(concat(deliveryServiceTypeCode, ', '), ''), -- N/A for CRM
                    -- coalesce(concat(deliveryServiceNumber, ', '), ''), -- N/A for CRM
                    coalesce(concat(cityName, ', '), ''),
                    coalesce(concat(stateCode, ', '), ''),
                    coalesce(postalCode, '')
                )
            END                                                                 AS addressFullText -- TRANSFORMATION
        FROM {ADS_DATABASE_CLEANSED}.crm_0bp_def_address_attr 
        WHERE 
            _RecordCurrent = 1 
            AND _RecordDeleted = 0 
            AND addressNumber IS NOT NULL
            AND addressNumber <> ''
"""
    )
    .cache()
)

# print('')
# print('isu count', df_isu_addr_attr.count())
# display(df_isu_addr_attr)

# print('crm count', df_crm_addr_attr.count())
# display(df_crm_addr_attr)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Combine Dataframes

# COMMAND ----------

from pyspark.sql.functions import col, coalesce, trim

# ------------------------------- #
# Unique CRM
# ------------------------------- #
df_bp_addr_crm_unique = (
    df_crm_addr_attr.alias("crm")
    .join(
        df_isu_addr_attr.alias("isu"),
        [
            col("crm.businessPartnerNumber") == col("isu.businessPartnerNumber"),
            col("crm.businessPartnerAddressNumber") == col("isu.businessPartnerAddressNumber")
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
        [("-1", "-1")], 
        ["businessPartnerAddressNumber", "businessPartnerNumber"]
    )
)

# ------------------------------- #
# Master Business Partner Addr
# ------------------------------- #
df_bp_addr_master = (
    # --- Union Tables ---#
    df_isu_addr_attr
    .unionByName(df_bp_addr_crm_unique)
    .unionByName(dummyDimRecDf, allowMissingColumns = True)
    .drop_duplicates()
    #--- Order Columns --- #
    .selectExpr(
        "sourceSystemCode",
        "businessPartnerAddressNumber",
        "businessPartnerNumber",
        "addressValidFromDate",
        "addressValidToDate",
        "phoneNumber",
        "phoneExtension",
        "faxNumber",
        "faxExtension",
        "emailAddress",
        "personalAddressFlag",
        "coName",
        "shortFormattedAddress2",
        "streetLine5",
        "building",
        "floorNumber",
        "apartmentNumber",
        "housePrimaryNumber",
        "houseSupplementNumber",
        "streetPrimaryName",
        "streetSupplementName1",
        "streetSupplementName2",
        "otherLocationName",
        "TRIM(TRAILING ',' FROM TRIM(houseNumber))       AS houseNumber",
        "TRIM(TRAILING ',' FROM TRIM(streetName))        AS streetName",
        "streetCode",
        "cityName",
        "cityCode",
        "stateCode",
        "stateName",
        "postalCode",
        "countryCode",
        "countryName",
        "TRIM(TRAILING ',' FROM TRIM(addressFullText))   AS addressFullText",
        "poBoxCode",
        "poBoxCity",
        "postalCodeExtension",
        "poBoxExtension",
        "deliveryServiceTypeCode",
        "deliveryServiceType",
        "deliveryServiceNumber",
        "addressTimeZone",
        "communicationAddressNumber"
    )
    .cache()
)

# print('df_isu_addr_attr count:', df_isu_addr_attr.count())
# print('df_bp_addr_crm_unique count:', df_bp_addr_crm_unique.count())
# print('df_bp_addr_master', df_bp_addr_master.count())
# display(df_bp_addr_master)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## 3. Load

# COMMAND ----------

# ---- Apply Schema Definition ---- #
from pyspark.sql.types import *
schema = StructType([
    StructField('businessPartnerAddressSK', StringType(), False),
    StructField('sourceSystemCode', StringType(), True),
    StructField('businessPartnerAddressNumber', StringType(), False),
    StructField('businessPartnerNumber', StringType(), False),
    StructField('addressValidFromDate', DateType(), True),
    StructField('addressValidToDate', DateType(), True),
    StructField('phoneNumber', StringType(), True),
    StructField('phoneExtension', StringType(), True),
    StructField('faxNumber', StringType(), True),
    StructField('faxExtension', StringType(), True),
    StructField('emailAddress', StringType(), True),
    StructField('personalAddressFlag', StringType(), True),
    StructField('coName', StringType(), True),
    StructField('shortFormattedAddress2', StringType(), True),
    StructField('streetLine5', StringType(), True),
    StructField('building', StringType(), True),
    StructField('floorNumber', StringType(), True),
    StructField('apartmentNumber', StringType(), True),
    StructField('housePrimaryNumber', StringType(), True),
    StructField('houseSupplementNumber', StringType(), True),
    StructField('streetPrimaryName', StringType(), True),
    StructField('streetSupplementName1', StringType(), True),
    StructField('streetSupplementName2', StringType(), True),
    StructField('otherLocationName', StringType(), True),
    StructField('houseNumber', StringType(), True),
    StructField('streetName', StringType(), True),
    StructField('streetCode', StringType(), True),
    StructField('cityName', StringType(), True),
    StructField('cityCode', StringType(), True),
    StructField('stateCode', StringType(), True),
    StructField('stateName', StringType(), True),
    StructField('postalCode', StringType(), True),
    StructField('countryCode', StringType(), True),
    StructField('countryName', StringType(), True),
    StructField('addressFullText', StringType(), True),
    StructField('poBoxCode', StringType(), True),
    StructField('poBoxCity', StringType(), True),
    StructField('postalCodeExtension', StringType(), True),
    StructField('poBoxExtension', StringType(), True),
    StructField('deliveryServiceTypeCode', StringType(), True),
    StructField('deliveryServiceType', StringType(), True),
    StructField('deliveryServiceNumber', StringType(), True),
    StructField('addressTimeZone', StringType(), True),
    StructField('communicationAddressNumber', StringType(), True)
])

# ---- Load Data with SCD --- #
TemplateEtlSCD(
    df_bp_addr_master, 
    entity="dimBusinessPartnerAddress", 
    businessKey="businessPartnerAddressNumber,businessPartnerNumber",
    schema=schema
)

# COMMAND ----------

dbutils.notebook.exit("1")
