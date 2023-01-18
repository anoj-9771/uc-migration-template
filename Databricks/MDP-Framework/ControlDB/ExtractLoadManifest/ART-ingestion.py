# Databricks notebook source
# MAGIC %run /MDP-Framework/ControlDB/common-controldb

# COMMAND ----------

SYSTEM_CODE = "ART"
j = json.loads(spark.conf.get("spark.databricks.clusterUsageTags.clusterAllTags"))
environment = [x['value'] for x in j if x['key'] == 'Environment'][0]

# COMMAND ----------

df = spark.sql("""
    WITH _Base AS 
    (
      SELECT 'BPM_ASSETRENEWALS' SourceSchema, 'daf-oracle-ART-connectionstring' SourceKeyVaultSecret, 'oracle-load' SourceHandler, '' RawFileExtension, 'raw-load-delta' RawHandler, '' ExtendedProperties, 'cleansed-load-delta' CleansedHandler, '' WatermarkColumn
    )
    SELECT 'ART_ASSESSMENT_COMMENT' SourceTableName, NULL SourceQuery, * FROM _Base
    UNION 
    SELECT 'ART_ASSESSMENT_MXES_DATA' SourceTableName, NULL SourceQuery, * FROM _Base
    UNION 
    SELECT 'ART_ASSESSMENT_WS' SourceTableName, 'select WORKSHOP_ID, REVIEW_DATE, MXES_PROCESS, CREATED_DATETIME, MXES_FACILITY, CREATED_BY, COMPLETED_DATETIME, COMPLETED_BY, STATUS, WS_NAME, UPDATED_DATETIME, UPDATE_BY, AREA, PRODUCT, FACILITY_TYPE  from BPM_ASSETRENEWALS.ART_ASSESSMENT_WS' SourceQuery, * FROM _Base
    UNION 
    SELECT 'ART_ATTACHMENT' SourceTableName, NULL SourceQuery, * FROM _Base
    UNION 
    SELECT 'ART_CONCEPT' SourceTableName, NULL SourceQuery, * FROM _Base
    UNION 
    SELECT 'ART_CONCEPT_ASSET' SourceTableName, NULL SourceQuery, * FROM _Base
    UNION 
    SELECT 'ART_CONCEPT_COMMENT' SourceTableName, NULL SourceQuery, * FROM _Base
    UNION 
    SELECT 'ART_PROJECT_CASHFLOW' SourceTableName, NULL SourceQuery, * FROM _Base
    UNION 
    SELECT 'ART_PROJECT_DETAILS' SourceTableName, NULL SourceQuery, * FROM _Base
    UNION 
    SELECT 'ATTACHMENT' SourceTableName, NULL SourceQuery, * FROM _Base
    UNION 
    SELECT 'RENEWALS_ASSESSMENT_MXES_DATA' SourceTableName, NULL SourceQuery, * FROM _Base
    UNION 
    SELECT 'RENEWALS_ASSESSMENT_WORKSHOP' SourceTableName, 'select WORKSHOP_ID, REVIEW_DATE, MXES_PROCESS, CREATED_DATETIME, MXES_FACILTY, CREATED_BY, COMPLETED_DATETIME, COMPLETED_BY, STATUS, WS_NAME, PRODUCT, PRODUCT_SUBGROUP, AREA, FACILITY_CODE, UPDATE_BY, UPDATED_DATETIME from BPM_ASSETRENEWALS.RENEWALS_ASSESSMENT_WORKSHOP' SourceQuery, * FROM _Base
    UNION 
    SELECT 'RENEWALS_ASSESSMENT_WS_COMMENT' SourceTableName, NULL SourceQuery, * FROM _Base
    UNION 
    SELECT 'RENEWALS_CONCEPT' SourceTableName, NULL SourceQuery, * FROM _Base
    UNION 
    SELECT 'RENEWALS_CONCEPT_ASSET' SourceTableName, NULL SourceQuery, * FROM _Base
    """)

# COMMAND ----------

def ConfigureManifest(df):
    # ------------- CONSTRUCT QUERY ----------------- #
    
    # ------------- DISPLAY ----------------- #
    ShowQuery(df)

    # ------------- SAVE ----------------- #
    AddIngestion(df)
    
    ExecuteStatement("""
    update dbo.extractLoadManifest
    set sourceQuery = NULL
    where sourceQuery = 'none'
    """)
    
    ExecuteStatement("""
    update dbo.extractLoadManifest
    set destinationSchema = 'art'
    where destinationSchema = 'BPM_ASSETRENEWALS'
    """)
    
    ExecuteStatement("""
    update dbo.extractLoadManifest set 
    businessKeyColumn = case sourceTableName 
    when 'ART_ASSESSMENT_COMMENT' then 'workshopId,locationId,assetId,role'
    when 'ART_ASSESSMENT_MXES_DATA' then 'workshopId,locationId,assetId'
    when 'ART_ASSESSMENT_WS' then 'workshopId'
    when 'ART_ATTACHMENT' then 'id,type,documentId'
    when 'ART_CONCEPT' then 'conceptId'
    when 'ART_CONCEPT_ASSET' then 'conceptId,locationId'
    when 'ART_PROJECT_CASHFLOW' then 'projectNumber,fyStart'
    when 'ART_PROJECT_DETAILS' then 'projectNumber'
    when 'ATTACHMENT' then 'id,type,documentId'
    when 'RENEWALS_ASSESSMENT_WORKSHOP' then 'workshopId'
    when 'RENEWALS_ASSESSMENT_WS_COMMENT' then 'workshopId,locationId'
    when 'RENEWALS_CONCEPT' then 'conceptId'
    when 'RENEWALS_CONCEPT_ASSET' then 'conceptId,locationId'
    end
    where systemCode = 'art'
    """)
    
    #the below table does not exist in dev and test environment. Only enable it in preprod and prod.
    ExecuteStatement(f"""
    update dbo.extractLoadManifest set
    enabled = iif('{environment}' in ('preprod','prod'),1,0)
    where systemCode = 'art'
    and sourceTableName = 'ART_CONCEPT_COMMENT'
    """)
    
    # ------------- ShowConfig ----------------- #
    ShowConfig()
    

ConfigureManifest(df)       
