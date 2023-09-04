# Databricks notebook source
###########################################################################################################################
# Loads BUSINESSPARTNERGROUP relationship table
#############################################################################################################################
# Method
# 1.Load Cleansed layer table data into dataframe and transform
# 2.JOIN TABLES
# 3.UNION TABLES
# 4.SELECT / TRANSFORM
# 5.SCHEMA DEFINITION
#############################################################################################################################

# COMMAND ----------

# MAGIC %run ../common/common-curated-includeMain

# COMMAND ----------

# MAGIC %run ./BusinessPartner

# COMMAND ----------

# MAGIC %run ./BusinessPartnerGroup

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Business Partner Group Relationship Tables
# MAGIC Build ISU & CRM. Business Partner Group Relationship dataframes

# COMMAND ----------

# MAGIC %md
# MAGIC ### ISU Business Partner Group Relationship

# COMMAND ----------

# ---- isu_0bp_relations_attr ---- #
isu0bpRelationsAttrDf  = (
        spark.sql(f"""
            select 
                'ISU'                               as sourceSystemCode, 
                businessPartnerNumber1              as businessPartnerGroupNumber, 
                businessPartnerNumber2              as businessPartnerNumber, 
                validFromDate                       as validFromDate, 
                validToDate                         as validToDate, 
                businessPartnerRelationshipNumber   as relationshipNumber, 
                relationshipTypeCode                as relationshipTypeCode, 
                relationshipType                    as relationshipType,
                _RecordDeleted,
                _DLCleansedZoneTimeStamp 
            FROM {ADS_DATABASE_CLEANSED}.isu.0bp_relations_attr 
            where 
                relationshipDirection = '1' 
                and _RecordCurrent = 1 
            """
        )
    ).cache()
# print('isu_0bp_relations_attr count:', isu0bpRelationsAttrDf.count())
# display(isu0bpRelationsAttrDf)

# COMMAND ----------

# MAGIC %md
# MAGIC ### CRM Business Partner Group Relationship

# COMMAND ----------

# ---- crm_0bp_relations_attr ---- #
crm0bpRelationsAttrDf  = (
        spark.sql(f"""
            select 
                'CRM'                               as sourceSystemCode, 
                businessPartnerNumber1              as businessPartnerGroupNumber, 
                businessPartnerNumber2              as businessPartnerNumber, 
                validFromDate                       as validFromDate, 
                validToDate                         as validToDate, 
                businessPartnerRelationshipNumber   as relationshipNumber, 
                relationshipTypeCode                as relationshipTypeCode, 
                relationshipType                    as relationshipType,
                _RecordDeleted,
                _DLCleansedZoneTimeStamp 
            FROM {ADS_DATABASE_CLEANSED}.crm.0bp_relations_attr 
            where 
                relationshipDirection = '1' 
                and _RecordCurrent = 1 --and businessPartnerNumber1 in ('0004005333','0005021635')
            """
        )
    ).cache()
# print('crm_0bp_relations_attr count:', crm0bpRelationsAttrDf.count())
# display(crm0bpRelationsAttrDf)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. dimBusinessPartnerGroupRelationship
# MAGIC Combine ISU and CRM to build a master BP Relationship dataframe
# MAGIC <br> **ISU / CRM Combination Logic** 
# MAGIC - **IF** ISU & CRM BP Relationship exists:
# MAGIC     - **THEN** return CRM columns
# MAGIC -  **IF** ISU BP Relationship exists, and CRM BP Relationship does not:
# MAGIC     - **THEN** return ISU Columns
# MAGIC - **IF** ISU BP Relationship does not exist, and CRM BP Relationship does:
# MAGIC     - **THEN** return CRM columns

# COMMAND ----------

def getBusinessPartnerGroupRelationship():
# ------------------------------- #
# 1.1) ISU & CRM Combined
# ------------------------------- #
    df_bpRelation_crm = (
    crm0bpRelationsAttrDf.alias("crm")
    .join(
        isu0bpRelationsAttrDf.alias("isu"),
        (col("crm.businessPartnerGroupNumber") == col("isu.businessPartnerGroupNumber")) & (col("crm.businessPartnerNumber") == col("isu.businessPartnerNumber")),
        how = 'left'
        )    
    # --- Select Columns --- #
    .select("crm.*") 
    .drop_duplicates()
    )
#     print('df_bpRelation_crm count:', df_bpRelation_crm.count())
#     display(df_bpRelation_crm)

# ------------------------------- #
# 1.2) ISU Unique
# ------------------------------- #
    df_bpRelation_isu_unique = (
    isu0bpRelationsAttrDf.alias("isu")
    .join(
        crm0bpRelationsAttrDf.alias("crm"),
        (col("crm.businessPartnerGroupNumber") == col("isu.businessPartnerGroupNumber")) & (col("crm.businessPartnerNumber") == col("isu.businessPartnerNumber")),
        how = 'leftanti'
        )
    .select("isu.*")
    .drop_duplicates()
    )
#     print('df_bpRelation_isu_unique count:', df_bpRelation_isu_unique.count())
#     display(df_bpRelation_isu_unique)

# ------------------------------- #
# 2) Union CRM & ISU Dataframe
# ------------------------------- #
    df_bprelation_master = (
    df_bpRelation_crm.unionByName(df_bpRelation_isu_unique)    
    # --- Order Columns --- #
    .select(
            "sourceSystemCode",
            "businessPartnerGroupNumber",
            "businessPartnerNumber",
            "validFromDate", 
            "validToDate", 
            "relationshipNumber", 
            "relationshipTypeCode", 
            "relationshipType",
            "_RecordDeleted",
            "_DLCleansedZoneTimeStamp" 
            )
    )
#     print('df_bprelation_master count:', df_bprelation_master.count())
#     display(df_bprelation_master)

# ------------------------------- #
# 3) Dimension Dataframes
# ------------------------------- #
    df_dim0bpartner = (
        spark.sql(f"""
            select 
                sourceSystemCode, 
                businessPartnerSK, 
                businessPartnerNumber, 
                validFromDate,
                validToDate 
            from {ADS_DATABASE_CURATED}.dimBusinessPartner 
            where 
                _RecordCurrent = 1 
            """
        )
    )

    df_dim0bpGroup = (
        spark.sql(f"""
            select 
                sourceSystemCode, 
                businessPartnerGroupSK, 
                businessPartnerGroupNumber,
                validFromDate,
                validToDate 
            from {ADS_DATABASE_CURATED}.dimBusinessPartnerGroup
            where 
                _RecordCurrent = 1 
            """
         )
    )

# ------------------------------- #
# 4) Joins to derive SKs
# ------------------------------- #
    df_bprelation_master = (
        df_bprelation_master
        .join(
            df_dim0bpartner, 
            (df_bprelation_master.businessPartnerNumber == df_dim0bpartner.businessPartnerNumber), 
            how="left"
        )
        .select(
            df_bprelation_master['*'], 
            df_dim0bpartner['businessPartnerSK']
        )
    )

    df_bprelation_master = (
        df_bprelation_master
        .join(
            df_dim0bpGroup, 
            (df_bprelation_master.businessPartnerGroupNumber == df_dim0bpGroup.businessPartnerGroupNumber), 
            how="left"
        )
        .select(
            df_bprelation_master['*'], 
            df_dim0bpGroup['businessPartnerGroupSK']
        )
    )
    
    dummyDimRecDf = (
        spark.sql(f"""
            select 
                businessPartnerSK         as dummyDimSK, 
                'dimBusinessPartner'      as dimension 
            from {ADS_DATABASE_CURATED}.dimBusinessPartner 
            where businessPartnerNumber = '-1' 
            union 
            select 
                businessPartnerGroupSK    as dummyDimSK, 
                'dimBusinessPartnerGroup' as dimension 
            from {ADS_DATABASE_CURATED}.dimBusinessPartnerGroup 
            where businessPartnerGroupNumber = '-1'
        """
        )
    )    

# ---------------------------------------------------------------------------------------------------------------------------- #
# 4) Joins to derive SKs of dummy dimension(-1) records, to be used when the lookup fails for dimensionSk
# ---------------------------------------------------------------------------------------------------------------------------- #
    
    df_bprelation_master = (
        df_bprelation_master
        .join(
            dummyDimRecDf, 
            (dummyDimRecDf.dimension == 'dimBusinessPartner'), 
            how="left"
        ) 
        .select(
            df_bprelation_master['*'], 
            dummyDimRecDf['dummyDimSK'].alias('dummyBusinessPartnerSK')
        )
    )

    df_bprelation_master = (
        df_bprelation_master
        .join(
            dummyDimRecDf, 
            (dummyDimRecDf.dimension == 'dimBusinessPartnerGroup'),
             how="left"
        )
        .select(
            df_bprelation_master['*'], 
            dummyDimRecDf['dummyDimSK'].alias('dummyBusinessPartnerGroupSK')
        )
    )

# ---------------------------- #
# 5) SELECT / TRANSFORM
# ---------------------------- #
    
    df_bprelation_master = (
        df_bprelation_master
        .selectExpr ( 
            "sourceSystemCode", 
            "coalesce(businessPartnerGroupSK, dummyBusinessPartnerGroupSK) as businessPartnerGroupSK", 
            "coalesce(businessPartnerSK, dummyBusinessPartnerSK) as businessPartnerSK", 
            "businessPartnerNumber", 
            "businessPartnerGroupNumber", 
            "validFromDate", 
            "validToDate", 
            "relationshipNumber", 
            "relationshipTypeCode", 
            "relationshipType",
            "_RecordDeleted",
            "_DLCleansedZoneTimeStamp" 
        )
    )

# ---------------------------- #
# 6) SELECT / TRANSFORM
# ---------------------------- #


    schema = StructType([
    StructField('businessPartnerRelationSK', StringType(), False),
    StructField('sourceSystemCode', StringType(), True),
    StructField('businessPartnerGroupSK', StringType(), True),
    StructField('businessPartnerSK', StringType(), True),
    StructField('businessPartnerNumber', StringType(), False),
    StructField('businessPartnerGroupNumber', StringType(), False),
    StructField('validFromDate', DateType(), True),
    StructField('validToDate', DateType(), False),
    StructField('relationshipNumber', StringType(), False),
    StructField('relationshipTypeCode', StringType(), True),
    StructField('relationshipType', StringType(), True)
    ]) 
    
    return df_bprelation_master, schema  

# COMMAND ----------

df, schema = getBusinessPartnerGroupRelationship()
TemplateTimeSliceEtlSCD(
    df, 
    entity="dim.businessPartnerRelation", 
    businessKey="businessPartnerGroupNumber,businessPartnerNumber,relationshipNumber,validToDate", 
    schema=schema
)

# COMMAND ----------

dbutils.notebook.exit("1")
