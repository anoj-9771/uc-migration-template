# Databricks notebook source
# MAGIC %run ../common-controldb

# COMMAND ----------

df = spark.sql("""
    WITH _Base AS 
    (
      SELECT 'iicatsref' SystemCode, 'iicats' SourceSchema, 'daf-oracle-IICATS-connectionstring' SourceKeyVaultSecret, 'oracle-load' SourceHandler, '' RawFileExtension, 'raw-load-delta' RawHandler, '' ExtendedProperties, 'cleansed-load-delta' CleansedHandler
    )
    SELECT 'std_asset_type' SourceTableName, 'M_DATE' WatermarkColumn, '' SourceQuery, * FROM _Base
    UNION 
    SELECT 'std_facility_type' SourceTableName, 'M_DATE' WatermarkColumn, '' SourceQuery, * FROM _Base
    UNION 
    
    SELECT 'bi_reference_codes' SourceTableName, 'M_DATE' WatermarkColumn, '' SourceQuery, * FROM _Base
    UNION 
    SELECT 'std_unit' SourceTableName, 'M_DATE' WatermarkColumn, '' SourceQuery, * FROM _Base
    UNION
    (
    WITH _Base AS 
    (
      SELECT 'iicatsdata' SystemCode, 'iicats' SourceSchema, 'daf-oracle-IICATS-connectionstring' SourceKeyVaultSecret, 'oracle-load' SourceHandler, '' RawFileExtension, 'raw-load-delta' RawHandler, '' ExtendedProperties, 'cleansed-load-delta' CleansedHandler
    )
    SELECT 'scx_facility' SourceTableName, 'M_DATE' WatermarkColumn, 'select * from iicats.scx_facility where lower(edw_export_config) = ''y''' SourceQuery, * FROM _Base
    UNION 
    SELECT 'groups' SourceTableName, 'M_DATE' WatermarkColumn, '' SourceQuery, * FROM _Base
    UNION 
    SELECT 'scx_point' SourceTableName, 'M_DATE' WatermarkColumn, 'select * from iicats.scx_point where lower(edw_export_config) = ''y''' SourceQuery, * FROM _Base
    UNION 
    SELECT 'wfp_daily_demand_archive' SourceTableName, 'M_DATE' WatermarkColumn, '' SourceQuery, * FROM _Base
    UNION 
    SELECT 'site_hierarchy' SourceTableName, 'M_DATE' WatermarkColumn, "select * from iicats.site_hierarchy where lower(substr(scx_site_code,1,2)) <> ''tw'' and lower(edw_export_config) = ''y''" SourceQuery, * FROM _Base    
    )    
    UNION
    (
    WITH _Base AS 
    (
      SELECT 'iicatsdata' SystemCode, 'scxstg' SourceSchema, 'daf-oracle-IICATS-stg-connectionstring' SourceKeyVaultSecret, 'oracle-load' SourceHandler, '' RawFileExtension, 'raw-load-delta' RawHandler, '' ExtendedProperties, 'cleansed-load-delta' CleansedHandler
    )
    SELECT 'event' SourceTableName, 'HT_CRT_DT' WatermarkColumn, '' SourceQuery, * FROM _Base
    UNION 
    SELECT 'iicats_work_orders' SourceTableName, 'HT_CRT_DT' WatermarkColumn, '' SourceQuery, * FROM _Base
    UNION 
    SELECT 'point_limit' SourceTableName, 'EFF_FROM_DT' WatermarkColumn, '' SourceQuery, * FROM _Base
    UNION 
    SELECT 'tsv' SourceTableName, 'HT_CRT_DT' WatermarkColumn, '' SourceQuery, * FROM _Base
    UNION 
    SELECT 'tsv_point_cnfgn' SourceTableName, 'EFF_FROM_DT' WatermarkColumn, '' SourceQuery, * FROM _Base
    UNION 
    SELECT 'wkly_prof_cnfgn' SourceTableName, 'EFF_FROM_DT' WatermarkColumn, '' SourceQuery, * FROM _Base
    UNION 
    SELECT 'dly_prof_cnfgn' SourceTableName, 'EFF_FROM_DT' WatermarkColumn, '' SourceQuery, * FROM _Base
    UNION 
    SELECT 'hierarchy_cnfgn' SourceTableName, 'EFF_FROM_DT' WatermarkColumn, "select * from scxstg.hierarchy_cnfgn where lower(substr(site_cd,1,2)) <> ''tw''" SourceQuery, * FROM _Base
    UNION 
    SELECT 'point_cnfgn' SourceTableName, 'EFF_FROM_DT' WatermarkColumn, '' SourceQuery, * FROM _Base
    UNION 
    SELECT 'rtu' SourceTableName, 'EFF_FROM_DT' WatermarkColumn, '' SourceQuery, * FROM _Base    
    UNION 
    SELECT 'qlty_config' SourceTableName, 'EFF_FROM_DT' WatermarkColumn, '' SourceQuery, * FROM _Base        
    UNION 
    SELECT 'scxuser' SourceTableName, 'EFF_FROM_DT' WatermarkColumn, '' SourceQuery, * FROM _Base
    UNION 
    SELECT 'scxfield' SourceTableName, 'HT_CRT_DT' WatermarkColumn, '' SourceQuery, * FROM _Base
    )    
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
    
for system_code in ['iicatsref','iicatsdata']:
    SYSTEM_CODE = system_code
    print(SYSTEM_CODE)
    ExecuteStatement(f"delete from dbo.extractLoadManifest where systemCode = '{SYSTEM_CODE}'")
    ConfigureManifest(df.where(f"SystemCode = '{SYSTEM_CODE}'"))       

# COMMAND ----------

# ------------- POST LOAD UPDATE ----------------- #  
#FIX DESTINATION SCHEMA
ExecuteStatement("""
update dbo.extractLoadManifest set
destinationSchema = 'iicats'
where systemCode in ('iicatsref','iicatsdata')-- and sourceSchema = 'scxstg' and destinationSchema <> 'iicats'
""")

#FIX DESTINATION Table
ExecuteStatement("""
update dbo.extractLoadManifest set
DestinationTableName = 'work_orders'
where systemCode in ('iicatsref','iicatsdata') and DestinationTableName = 'iicats_work_orders'
""")

#ADD BUSINESS KEY
ExecuteStatement("""
update dbo.extractLoadManifest set
businessKeyColumn = case sourceTableName
when 'std_asset_type' then 'assetType'
when 'std_facility_type' then 'facilityType'
when 'std_unit' then 'unitId'
when 'scx_facility' then 'facilityInternalId'
when 'groups' then 'legacyPointGroupName'
when 'scx_point' then 'pointInternalId'
when 'wfp_daily_demand_archive' then 'legacyRTUInternalId,measurementResultDateTime,measurementResultValue,sourceRecordModifiedDateTime,legacyPointInternalId'
when 'site_hierarchy' then 'siteInternalId'
when 'event' then 'objectInternalId,eventAESTDateTime,eventSequenceNumber,eventTimeMilliseconds,sourceRecordCreationDateTime'
when 'iicats_work_orders' then 'IICATSWorkOrderRequestNumber,workOrderModifiedDate'
when 'point_limit' then 'pointInternalId,pointLimitTypeCd'
when 'qlty_config' then 'objectInternalId,effectiveFromDateTime,sourceRecordCreationDateTime'
when 'tsv_point_cnfgn' then 'objectInternalId,pointStatisticTypeCd,timeBaseCd,sourceRecordCreationDateTime'
when 'wkly_prof_cnfgn' then 'pointInternalId'
when 'dly_prof_cnfgn' then 'dailyProfileId'
when 'point_cnfgn' then 'pointInternalId'
when 'rtu' then 'RTUInternalId'
when 'scxuser' then 'userInternalId'
when 'scxfield' then 'fieldName,IICATSStagingTableName,IICATSStagingFieldName,sourceRecordSystemId'
when 'hierarchy_cnfgn' then 'objectInternalId,effectiveFromDateTime,sourceRecordCreationDateTime'
when 'tsv' then 'objectInternalId,measurementResultAESTDateTime,statisticTypeCd,timeBaseCd,sourceRecordCreationDateTime'
when 'bi_reference_codes' then 'referenceField,referenceCd'
else businessKeyColumn
end
where systemCode in ('iicatsref','iicatsdata')
""")

# COMMAND ----------

#ADD RECORD INTO CONFIG TABLE TO CREATE VIEWS IN ADF
ExecuteStatement("""
merge into dbo.config as target using(
    select
        keyGroup = 'runviewCreation'
        ,[key] = 'iicats'
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
