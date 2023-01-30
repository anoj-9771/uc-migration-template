# Databricks notebook source
# MAGIC %run ../common-controldb

# COMMAND ----------

SYSTEM_CODE = 'SCADA'

# COMMAND ----------

df = spark.sql("""
    WITH _Base AS 
    (
      SELECT 'SCADASTG' SourceSchema, 'daf-oracle-IICATS-stg-connectionstring' SourceKeyVaultSecret, 'oracle-load' SourceHandler, '' RawFileExtension, 'raw-load-delta' RawHandler, '' ExtendedProperties, 'cleansed-load-delta' CleansedHandler
    )
    SELECT 'tsv' SourceTableName, 'HT_CRT_DT' WatermarkColumn, '' SourceQuery, * FROM _Base
    UNION
    SELECT 'event' SourceTableName, 'HT_CRT_DT' WatermarkColumn, '' SourceQuery, * FROM _Base
    UNION 
    SELECT 'hierarchy_cnfgn' SourceTableName, 'EFF_FROM_DT' WatermarkColumn, '' SourceQuery, * FROM _Base
    UNION 
    SELECT 'point_cnfgn' SourceTableName, 'EFF_FROM_DT' WatermarkColumn, '' SourceQuery, * FROM _Base
    UNION 
    SELECT 'point_limit' SourceTableName, 'EFF_FROM_DT' WatermarkColumn, '' SourceQuery, * FROM _Base
    UNION 
    SELECT 'rtu' SourceTableName, 'EFF_FROM_DT' WatermarkColumn, '' SourceQuery, * FROM _Base    
    UNION 
    SELECT 'tsv_point_cnfgn' SourceTableName, 'EFF_FROM_DT' WatermarkColumn, '' SourceQuery, * FROM _Base
    UNION 
    SELECT 'qlty_config' SourceTableName, 'EFF_FROM_DT' WatermarkColumn, '' SourceQuery, * FROM _Base    
    UNION 
    SELECT 'scxfield' SourceTableName, '' WatermarkColumn, '' SourceQuery, * FROM _Base
     UNION 
    SELECT 'scxuser' SourceTableName, '' WatermarkColumn, '' SourceQuery, * FROM _Base
    ORDER BY SourceSchema, SourceTableName
    """)

# COMMAND ----------

def ConfigureManifest(df):
    # ------------- CONSTRUCT QUERY ----------------- #
    
    # ------------- DISPLAY ----------------- #
    ShowQuery(df)

    # ------------- SAVE ----------------- #
    AddIngestion(df)
    
    # ------------- ShowConfig ----------------- #
    ShowConfig()
    
ConfigureManifest(df)       

# COMMAND ----------

# ------------- POST LOAD UPDATE ----------------- #  
#ADD BUSINESS KEY
ExecuteStatement(f"""
update dbo.extractLoadManifest set
businessKeyColumn = case sourceTableName
when 'tsv' then 'objectInternalId,measurementResultAESTDateTime,statisticTypeCd,timeBaseCd,sourceRecordCreationDateTime'
when 'event' then 'objectInternalId,eventAESTDateTime,eventSequenceNumber,eventTimeMilliseconds,sourceRecordCreationDateTime'
when 'hierarchy_cnfgn' then 'objectInternalId,effectiveFromDateTime,sourceRecordCreationDateTime'
when 'point_cnfgn' then 'pointInternalId'
when 'point_limit' then 'pointInternalId,pointLimitTypeCd'
when 'rtu' then 'RTUInternalId'
when 'tsv_point_cnfgn' then 'objectInternalId,pointStatisticTypeCd,timeBaseCd,sourceRecordCreationDateTime'
when 'qlty_config' then 'objectInternalId,effectiveFromDateTime,sourceRecordCreationDateTime'
when 'scxfield' then 'fieldName,IICATSStagingTableName,IICATSStagingFieldName,sourceRecordSystemId'
when 'scxuser' then 'userInternalId'
else businessKeyColumn
end
,destinationSchema = systemCode
where systemCode = '{SYSTEM_CODE}'
""")


# COMMAND ----------

#ADD RECORD INTO CONFIG TABLE TO CREATE VIEWS IN ADF
ExecuteStatement(f"""
merge into dbo.config as target using(
    select
        keyGroup = 'runviewCreation'
        ,[key] = '{SYSTEM_CODE}'
) as source on
    target.keyGroup = source.keyGroup
    and target.[key] = source.[key]
when not matched then insert(
    keyGroup
    ,[key]
    ,value
    ,createdDTS
)
values(
    source.keyGroup
    ,source.[key]
    ,1
    ,getutcdate()
);
""")
