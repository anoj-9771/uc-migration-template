# Databricks notebook source
# MAGIC %run ../common-controldb

# COMMAND ----------

from pyspark.sql.functions import lit, when, lower, expr
df = spark.sql("""
    WITH _Base AS 
    (
      SELECT 'iicatsref' SystemCode, 'iicats' SourceSchema, 'daf-oracle-IICATS-connectionstring' SourceKeyVaultSecret, 'oracle-load' SourceHandler, '' RawFileExtension, 'raw-load-delta' RawHandler, 'cleansed-load-delta' CleansedHandler, '' ExtendedProperties
    )
    SELECT 'std_asset_type' SourceTableName, "TO_CHAR(M_DATE, ''YYYY/MM/DD HH24:MI:SS'')" WatermarkColumn, '' SourceQuery, * FROM _Base
    UNION 
    SELECT 'std_facility_type' SourceTableName, "TO_CHAR(M_DATE, ''YYYY/MM/DD HH24:MI:SS'')" WatermarkColumn, '' SourceQuery, * FROM _Base
    UNION     
    SELECT 'bi_reference_codes' SourceTableName, "TO_CHAR(M_DATE, ''YYYY/MM/DD HH24:MI:SS'')" WatermarkColumn, '' SourceQuery, * FROM _Base
    UNION 
    SELECT 'std_unit' SourceTableName, "TO_CHAR(M_DATE, ''YYYY/MM/DD HH24:MI:SS'')" WatermarkColumn, '' SourceQuery, * FROM _Base
    UNION
    (
    WITH _Base AS 
    (
      SELECT 'iicatsref' SystemCode, 'scxstg' SourceSchema, 'daf-oracle-IICATS-stg-connectionstring' SourceKeyVaultSecret, 'oracle-load' SourceHandler, '' RawFileExtension, 'raw-load-delta' RawHandler, 'cleansed-load-delta' CleansedHandler, '' ExtendedProperties
    )
    SELECT 'hierarchy_cnfgn' SourceTableName, "TO_CHAR(EFF_FROM_DT, ''YYYY/MM/DD HH24:MI:SS'')" WatermarkColumn, "select * from scxstg.hierarchy_cnfgn where lower(substr(site_cd,1,2)) <> ''tw''" SourceQuery, * FROM _Base    
    )        
    UNION
    (
    WITH _Base AS 
    (
      SELECT 'iicatsdata' SystemCode, 'iicats' SourceSchema, 'daf-oracle-IICATS-connectionstring' SourceKeyVaultSecret, 'oracle-load' SourceHandler, '' RawFileExtension, 'raw-load-delta' RawHandler, 'cleansed-load-delta' CleansedHandler
    )
    SELECT 'scx_facility' SourceTableName, "TO_CHAR(M_DATE, ''YYYY/MM/DD HH24:MI:SS'')" WatermarkColumn, "select * from iicats.scx_facility where lower(edw_export_config) = ''y''" SourceQuery, *, '{"deleteRecordsQuery" : "select * from {vwCleanseDataFrame} where facilityInternalId in (select objectInternalId from cleansed.iicats_hierarchy_cnfgn where sourceRecordUpsertLogic = \\'\\'D\\'\\' and objestLevelNumber = 9 and _DLCleansedZoneTimeStamp > \\'\\'{lastLoadTimeStamp}\\'\\')"}' ExtendedProperties FROM _Base
    UNION 
    SELECT 'groups' SourceTableName, "TO_CHAR(M_DATE, ''YYYY/MM/DD HH24:MI:SS'')" WatermarkColumn, '' SourceQuery, *, '' ExtendedProperties FROM _Base
    UNION 
    SELECT 'scx_point' SourceTableName, "TO_CHAR(M_DATE, ''YYYY/MM/DD HH24:MI:SS'')" WatermarkColumn, "select * from iicats.scx_point where lower(edw_export_config) = ''y''" SourceQuery, *, '{"deleteRecordsQuery" : "select * from {vwCleanseDataFrame} where pointInternalId in (select objectInternalId from cleansed.iicats_hierarchy_cnfgn where sourceRecordUpsertLogic = \\'\\'D\\'\\' and objestLevelNumber = 11 and _DLCleansedZoneTimeStamp > \\'\\'{lastLoadTimeStamp}\\'\\')"}' ExtendedProperties FROM _Base
    UNION 
    SELECT 'wfp_daily_demand_archive' SourceTableName, "TO_CHAR(M_DATE, ''YYYY/MM/DD HH24:MI:SS'')" WatermarkColumn, '' SourceQuery, *, '' ExtendedProperties FROM _Base
    UNION 
    SELECT 'site_hierarchy' SourceTableName, "TO_CHAR(M_DATE, ''YYYY/MM/DD HH24:MI:SS'')" WatermarkColumn, "select * from iicats.site_hierarchy where lower(substr(scx_site_code,1,2)) <> ''tw'' and lower(edw_export_config) = ''y''" SourceQuery, *, '{"deleteRecordsQuery" : "select * from {vwCleanseDataFrame} where siteInternalId in (select objectInternalId from cleansed.iicats_hierarchy_cnfgn where sourceRecordUpsertLogic = \\'\\'D\\'\\' and objestLevelNumber = 8 and _DLCleansedZoneTimeStamp > \\'\\'{lastLoadTimeStamp}\\'\\')"}' ExtendedProperties FROM _Base    
    )    
    
    UNION
    (
    WITH _Base AS 
    (
      SELECT 'iicatsdata' SystemCode, 'scxstg' SourceSchema, 'daf-oracle-IICATS-stg-connectionstring' SourceKeyVaultSecret, 'oracle-load' SourceHandler, '' RawFileExtension, 'raw-load-delta' RawHandler, 'cleansed-load-delta' CleansedHandler, '' ExtendedProperties
    )
    SELECT 'event' SourceTableName, "TO_CHAR(HT_CRT_DT, ''YYYY/MM/DD HH24:MI:SS'')" WatermarkColumn, '' SourceQuery, * FROM _Base
    UNION 
    SELECT 'iicats_work_orders' SourceTableName, "TO_CHAR(HT_CRT_DT, ''YYYY/MM/DD HH24:MI:SS'')" WatermarkColumn, '' SourceQuery, * FROM _Base
    UNION 
    SELECT 'point_limit' SourceTableName, "TO_CHAR(EFF_FROM_DT, ''YYYY/MM/DD HH24:MI:SS'')" WatermarkColumn, '' SourceQuery, * FROM _Base
    UNION 
    SELECT 'tsv' SourceTableName, "TO_CHAR(HT_CRT_DT, ''YYYY/MM/DD HH24:MI:SS'')" WatermarkColumn, '' SourceQuery, * FROM _Base
    UNION 
    SELECT 'tsv_point_cnfgn' SourceTableName, "TO_CHAR(EFF_FROM_DT, ''YYYY/MM/DD HH24:MI:SS'')" WatermarkColumn, '' SourceQuery, * FROM _Base
    UNION 
    SELECT 'wkly_prof_cnfgn' SourceTableName, "TO_CHAR(EFF_FROM_DT, ''YYYY/MM/DD HH24:MI:SS'')" WatermarkColumn, '' SourceQuery, * FROM _Base
    UNION 
    SELECT 'dly_prof_cnfgn' SourceTableName, "TO_CHAR(EFF_FROM_DT, ''YYYY/MM/DD HH24:MI:SS'')" WatermarkColumn, '' SourceQuery, * FROM _Base
    UNION 
    SELECT 'point_cnfgn' SourceTableName, "TO_CHAR(EFF_FROM_DT, ''YYYY/MM/DD HH24:MI:SS'')" WatermarkColumn, '' SourceQuery, * FROM _Base
    UNION 
    SELECT 'rtu' SourceTableName, "TO_CHAR(EFF_FROM_DT, ''YYYY/MM/DD HH24:MI:SS'')" WatermarkColumn, '' SourceQuery, * FROM _Base    
    UNION 
    SELECT 'qlty_config' SourceTableName, "TO_CHAR(EFF_FROM_DT, ''YYYY/MM/DD HH24:MI:SS'')" WatermarkColumn, '' SourceQuery, * FROM _Base        
    UNION 
    SELECT 'scxuser' SourceTableName, "TO_CHAR(EFF_FROM_DT, ''YYYY/MM/DD HH24:MI:SS'')" WatermarkColumn, '' SourceQuery, * FROM _Base
    UNION 
    SELECT 'scxfield' SourceTableName, '' WatermarkColumn, '' SourceQuery, * FROM _Base
    )    
    ORDER BY SourceSchema, SourceTableName
    """)

#Tables that needs to be refreshed every 15 mins
tables15Mins = ['event','iicats_work_orders','tsv','qlty_config','wfp_daily_demand_archive']
df = (
    df.withColumn('SystemCode', when(lower(df.SourceTableName).isin(tables15Mins),lit('iicats|15min')) 
                                        .otherwise(expr('SystemCode')))
      .withColumn('ExtendedProperties', expr('trim("{}" FROM ExtendedProperties)'))
      .withColumn('ExtendedProperties', when(lower(df.SourceTableName).isin(tables15Mins)
                                        ,expr('ltrim(",",ExtendedProperties ||", "||\'\"OverrideClusterName\" : \"interactive\"\')')) 
                                        .otherwise(expr('ExtendedProperties')))
      .withColumn('ExtendedProperties', expr('if(ExtendedProperties<>"","{"||ExtendedProperties ||"}","")'))                                         
)
appendTables = ['tsv','event']
df = (
    df.withColumn('ExtendedProperties', expr('trim("{}" FROM ExtendedProperties)'))
      .withColumn('ExtendedProperties', when(lower(df.SourceTableName).isin(appendTables)
                                        ,expr('ltrim(",",ExtendedProperties ||", "||\'\"LoadType\" : \"Append\"\')')) 
                                        .otherwise(expr('ExtendedProperties')))
      .withColumn('ExtendedProperties', expr('if(ExtendedProperties<>"","{"||ExtendedProperties ||"}","")')) 
)
display(df)

# COMMAND ----------

def ConfigureManifest(df):
    # ------------- CONSTRUCT QUERY ----------------- #
    
    # ------------- DISPLAY ----------------- #
    ShowQuery(df)

    # ------------- SAVE ----------------- #
    AddIngestion(df)
    
    # ------------- ShowConfig ----------------- #
    ShowConfig()
    
for system_code in ['iicatsref','iicatsdata','iicats|15min']:
    SYSTEM_CODE = system_code
    print(SYSTEM_CODE)
    ConfigureManifest(df.where(f"SystemCode = '{SYSTEM_CODE}'"))       

# COMMAND ----------

# ------------- POST LOAD UPDATE ----------------- #  
#FIX DESTINATION SCHEMA
ExecuteStatement("""
update dbo.extractLoadManifest set
destinationSchema = 'iicats'
where systemCode in ('iicatsref','iicatsdata','iicats|15min')-- and sourceSchema = 'scxstg' and destinationSchema <> 'iicats'
""")

#FIX DESTINATION Table
ExecuteStatement("""
update dbo.extractLoadManifest set
DestinationTableName = 'work_orders'
where systemCode in ('iicatsref','iicatsdata','iicats|15min') and DestinationTableName = 'iicats_work_orders'
""")

#ADD BUSINESS KEY
ExecuteStatement("""
update dbo.extractLoadManifest set
businessKeyColumn = case sourceTableName
when 'std_asset_type' then 'assetType'
when 'std_facility_type' then 'facilityType'
when 'std_unit' then 'unitId,sourceRecordModifiedDateTime'
when 'scx_facility' then 'facilityInternalId,sourceRecordModifiedDateTime'
when 'groups' then 'legacyPointGroupName'
when 'scx_point' then 'pointInternalId,sourceRecordModifiedDateTime'
when 'wfp_daily_demand_archive' then 'measurementResultDateTime,sourceRecordModifiedDateTime,legacyPointInternalId'
when 'site_hierarchy' then 'siteInternalId,sourceRecordModifiedDateTime'
when 'event' then 'objectInternalId,eventAESTDateTime,eventSequenceNumber,eventTimeMilliseconds,sourceRecordCreationDateTime'
when 'iicats_work_orders' then 'IICATSWorkOrderRequestNumber,sourceRecordCreationDateTime'
when 'point_limit' then 'pointInternalId,pointLimitTypeCd,effectiveFromDateTime,sourceRecordCreationDateTime'
when 'qlty_config' then 'objectInternalId,effectiveFromDateTime,sourceRecordCreationDateTime'
when 'tsv_point_cnfgn' then 'objectInternalId,effectiveFromDateTime,pointStatisticTypeCd,timeBaseCd,sourceRecordCreationDateTime'
when 'wkly_prof_cnfgn' then 'pointInternalId,effectiveFromDateTime,sourceRecordCreationDateTime'
when 'dly_prof_cnfgn' then 'dailyProfileId,effectiveFromDateTime,sourceRecordCreationDateTime'
when 'point_cnfgn' then 'pointInternalId,effectiveFromDateTime,sourceRecordCreationDateTime'
when 'rtu' then 'RTUInternalId,effectiveFromDateTime,sourceRecordCreationDateTime'
when 'scxuser' then 'userInternalId,effectiveFromDateTime,sourceRecordCreationDateTime'
when 'scxfield' then 'fieldName,IICATSStagingTableName,IICATSStagingFieldName,sourceRecordSystemId'
when 'hierarchy_cnfgn' then 'objectInternalId,effectiveFromDateTime,sourceRecordCreationDateTime'
when 'tsv' then 'objectInternalId,measurementResultAESTDateTime,statisticTypeCd,timeBaseCd,sourceRecordCreationDateTime,sourceRecordUpsertLogic'
when 'bi_reference_codes' then 'referenceField,referenceCd'
else businessKeyColumn
end
where systemCode in ('iicatsref','iicatsdata','iicats|15min')
""")

# COMMAND ----------

#ADD RECORD INTO CONFIG TABLE TO CREATE VIEWS IN ADF
ExecuteStatement("""
merge into dbo.config as target using(
    select
        keyGroup = 'runviewCreation'
        ,[key] = 'iicatsref'
    union all
    select
        keyGroup = 'runviewCreation'
        ,[key] = 'iicatsdata'
    union all
    select
        keyGroup = 'runviewCreation'
        ,[key] = 'iicats|15min'
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

# COMMAND ----------

#ADD RECORD INTO CONFIG TABLE FOR 15 MIN TRIGGER IN ADF
# ExecuteStatement("""
# merge into dbo.config as target using(
#     select
#         keyGroup = 'TriggerInterval'
#         ,[key] = '15min'
# ) as source on
#     target.keyGroup = source.keyGroup
#     and target.[key] = source.[key]
# when not matched then insert(
#     keyGroup
#     ,[key]
#     ,value
#     ,createdDTS
# )
# values(
#     source.keyGroup
#     ,source.[key]
#     ,'iicats|15min'
#     ,getutcdate()
# )
# when matched then update set
#     target.[keyGroup] = source.[keyGroup]
#     ,target.[key] = source.[key]
#     ,target.[value] = iif(charindex('iicats|15min', target.[value]) > 0, target.[value], target.[value] + ',' + 'iicats|15min')
# ;
# """)
