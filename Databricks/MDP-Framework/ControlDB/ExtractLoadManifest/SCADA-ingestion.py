# Databricks notebook source
# MAGIC %run ../common-controldb

# COMMAND ----------

SYSTEM_CODE = 'scada'

# COMMAND ----------

df = spark.sql("""
    WITH _Base AS 
    (
      SELECT 'SCADASTG' SourceSchema, 'daf-oracle-IICATS-stg-connectionstring' SourceKeyVaultSecret, 'oracle-load' SourceHandler, '' RawFileExtension, 'raw-load-delta' RawHandler, '' ExtendedProperties, 'cleansed-load-delta' CleansedHandler
    )
    SELECT 'tsv' SourceTableName, "TO_CHAR(HT_CRT_DT, ''YYYY/MM/DD HH24:MI:SS'')" WatermarkColumn, '' SourceQuery, * FROM _Base
    UNION
    SELECT 'event' SourceTableName, "TO_CHAR(HT_CRT_DT, ''YYYY/MM/DD HH24:MI:SS'')" WatermarkColumn, '' SourceQuery, * FROM _Base
    UNION 
    SELECT 'hierarchy_cnfgn' SourceTableName, "TO_CHAR(EFF_FROM_DT, ''YYYY/MM/DD HH24:MI:SS'')" WatermarkColumn, '' SourceQuery, * FROM _Base
    UNION 
    SELECT 'point_cnfgn' SourceTableName, "TO_CHAR(EFF_FROM_DT, ''YYYY/MM/DD HH24:MI:SS'')" WatermarkColumn, '' SourceQuery, * FROM _Base
    UNION 
    SELECT 'point_limit' SourceTableName, "TO_CHAR(EFF_FROM_DT, ''YYYY/MM/DD HH24:MI:SS'')" WatermarkColumn, '' SourceQuery, * FROM _Base
    UNION 
    SELECT 'rtu' SourceTableName, "TO_CHAR(EFF_FROM_DT, ''YYYY/MM/DD HH24:MI:SS'')" WatermarkColumn, '' SourceQuery, * FROM _Base    
    UNION 
    SELECT 'tsv_point_cnfgn' SourceTableName, "TO_CHAR(EFF_FROM_DT, ''YYYY/MM/DD HH24:MI:SS'')" WatermarkColumn, '' SourceQuery, * FROM _Base
    UNION 
    SELECT 'qlty_config' SourceTableName, "TO_CHAR(EFF_FROM_DT, ''YYYY/MM/DD HH24:MI:SS'')" WatermarkColumn, '' SourceQuery, * FROM _Base    
    UNION 
    SELECT 'scxfield' SourceTableName, '' WatermarkColumn, '' SourceQuery, * FROM _Base
     UNION 
    SELECT 'scxuser' SourceTableName, "TO_CHAR(EFF_FROM_DT, ''YYYY/MM/DD HH24:MI:SS'')" WatermarkColumn, '' SourceQuery, * FROM _Base
    ORDER BY SourceSchema, SourceTableName
    """)

dfDaily = df.where("sourceTableName not in ('event','tsv')")
df15Min = df.where("sourceTableName in ('event','tsv')")

# COMMAND ----------

def ConfigureManifest(df):
    # ------------- CONSTRUCT QUERY ----------------- #
    
    # ------------- DISPLAY ----------------- #
    ShowQuery(df)

    # ------------- SAVE ----------------- #
    AddIngestion(df)
    
    # ------------- ShowConfig ----------------- #
    ShowConfig()
    
ConfigureManifest(dfDaily)

SYSTEM_CODE = "scada|15min"
ConfigureManifest(df15Min)

# COMMAND ----------

# ------------- POST LOAD UPDATE ----------------- #  
#ADD BUSINESS KEY AND SET DESTINATION SCHEMA TO SCADA
ExecuteStatement(f"""
update dbo.extractLoadManifest set
businessKeyColumn = case sourceTableName
when 'tsv' then 'objectInternalId,measurementResultAESTDateTime,statisticTypeCd,timeBaseCd,sourceRecordCreationDateTime,sourceRecordUpsertLogic'
when 'event' then 'objectInternalId,eventAESTDateTime,eventSequenceNumber,eventTimeMilliseconds,sourceRecordCreationDateTime'
when 'hierarchy_cnfgn' then 'objectInternalId,effectiveFromDateTime,sourceRecordCreationDateTime'
when 'point_cnfgn' then 'pointInternalId,effectiveFromDateTime,sourceRecordCreationDateTime'
when 'point_limit' then 'pointInternalId,pointLimitTypeCd,effectiveFromDateTime,sourceRecordCreationDateTime'
when 'rtu' then 'RTUInternalId,effectiveFromDateTime,sourceRecordCreationDateTime'
when 'tsv_point_cnfgn' then 'objectInternalId,effectiveFromDateTime,pointStatisticTypeCd,timeBaseCd,sourceRecordCreationDateTime'
when 'qlty_config' then 'objectInternalId,effectiveFromDateTime,sourceRecordCreationDateTime'
when 'scxfield' then 'fieldName,IICATSStagingTableName,IICATSStagingFieldName,sourceRecordSystemId'
when 'scxuser' then 'userInternalId,effectiveFromDateTime,sourceRecordCreationDateTime'
else businessKeyColumn
end
,destinationSchema = 'scada'
where systemCode like 'scada%'
""")

# COMMAND ----------

#ADD RECORD INTO CONFIG TABLE TO CREATE VIEWS IN ADF
ExecuteStatement(f"""
merge into dbo.config as target using(
    select
        keyGroup = 'runviewCreation'
        ,[key] = 'scada'
    union 
    select
        keyGroup = 'runviewCreation'
        ,[key] = 'scada|15min'
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
