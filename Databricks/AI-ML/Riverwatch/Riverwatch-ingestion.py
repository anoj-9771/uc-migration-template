# Databricks notebook source
# MAGIC %run /MDP-Framework/Common/common-jdbc

# COMMAND ----------

# MAGIC %run /build/includes/global-variables-python

# COMMAND ----------

spark.conf.set("r.catalog_name", ADS_DATABASE_RAW)

# COMMAND ----------

df = spark.sql("""
WITH _Base AS 
(
  SELECT 'BeachWatch' SystemCode, '' SourceKeyVaultSecret, 'http-binary-load' SourceHandler, 'xml' RawFileExtension, 'raw-load ' RawHandler, 'cleansed-load ' CleansedHandler
)
SELECT SystemCode, 'BeachWatch' SourceSchema, SourceKeyVaultSecret, SourceHandler, RawFileExtension, RawHandler, CleansedHandler, 'Chiswick_Baths_Pollution_WeatherForecast' SourceTableName, 'https://api.beachwatch.nsw.gov.au/public/sites/rss?site_name=%3Cbeach%20/site%20name%3E&site_name=Chiswick%20Baths' SourceQuery, '{"rowTag" : "channel", "CleansedQuery" : "Select 5 locationId, * from ${r.catalog_name}.beachwatch.chiswick_baths_pollution_weatherforecast", "OverrideClusterName":"interactive"}' ExtendedProperties, NULL WatermarkColumn
from _Base

UNION
SELECT SystemCode, 'BeachWatch' SourceSchema, SourceKeyVaultSecret, SourceHandler, RawFileExtension, RawHandler, CleansedHandler, 'Cabarita_Beach_Pollution_WeatherForecast' SourceTableName, 'https://api.beachwatch.nsw.gov.au/public/sites/rss?site_name=%3Cbeach%20/site%20name%3E&site_name=Cabarita%20Beach' SourceQuery, '{"rowTag" : "channel", "CleansedQuery" : "Select 4 locationId, * from ${r.catalog_name}.beachwatch.cabarita_beach_pollution_weatherforecast", "OverrideClusterName":"interactive"}' ExtendedProperties, NULL WatermarkColumn
from _Base

UNION
SELECT SystemCode, 'BeachWatch' SourceSchema, SourceKeyVaultSecret, SourceHandler, RawFileExtension, RawHandler, CleansedHandler, 'Dawn_Fraser_Pool_Pollution_WeatherForecast' SourceTableName, 'https://api.beachwatch.nsw.gov.au/public/sites/rss?site_name=%3Cbeach%20/site%20name%3E&site_name=Dawn%20Fraser%20Pool' SourceQuery, '{"rowTag" : "channel", "CleansedQuery" : "Select 3 locationId, * from ${r.catalog_name}.beachwatch.dawn_fraser_pool_pollution_weatherforecast", "OverrideClusterName":"interactive"}' ExtendedProperties, NULL WatermarkColumn
from _Base

UNION 
SELECT 'BoM' SystemCode, 'BoM' SourceSchema, '' SourceKeyVaultSecret, 'http-binary-load' SourceHandler, 'xml' RawFileExtension, 'raw-load' RawHandler, 'cleansed-load' CleansedHandler, 'FortDenision_Tide' SourceTableName, 'http://www.bom.gov.au/ntc/IDO59001/IDO59001_2023_NSW_TP007.xml' SourceQuery
,     '{"rowTag" : "forecast-period", "TransformMethod": "ExpandTable", "OverrideClusterName":"interactive"}' ExtendedProperties, NULL WatermarkColumn


UNION
SELECT 'BoM' SystemCode, 'BoM' SourceSchema, '' SourceKeyVaultSecret, 'ftp-binary-load' SourceHandler, 'xml' RawFileExtension, 'raw-load' RawHandler, 'cleansed-load' CleansedHandler, 'WeatherForecast' SourceTableName, 'ftp://ftp.bom.gov.au/anon/gen/fwo/IDN10064.xml' SourceQuery, '{"rowTag" : "area", "OverrideClusterName":"interactive"}' ExtendedProperties, NULL WatermarkColumn

UNION
SELECT 'BoM' SystemCode, 'BoM' SourceSchema, '' SourceKeyVaultSecret, 'http-binary-load' SourceHandler, 'csv' RawFileExtension, 'raw-bom-csv' RawHandler, 'cleansed-load' CleansedHandler, 'DailyWeatherObservation_SydneyAirport' SourceTableName, 'http://www.bom.gov.au/climate/dwo/$yyyy$$MM$/text/IDCJDW2125.$yyyy$$MM$.csv' SourceQuery, '{"DayOffset" : "-1"}' ExtendedProperties, NULL WatermarkColumn

UNION
SELECT 'BoM' SystemCode, 'BoM' SourceSchema, '' SourceKeyVaultSecret, 'http-binary-load' SourceHandler, 'json' RawFijsonxtension, 'raw-load' RawHandler, 'cleansed-load' CleansedHandler, 'WeatherObservation_SydneyAirport' SourceTableName, 'http://www.bom.gov.au/fwo/IDN60801/IDN60801.94767.json' SourceQuery, '{ "CleansedQuery" : "SELECT r.* FROM ( SELECT explode(observations.data) r FROM {tableFqn}) Q"  , "OverrideClusterName":"interactive"}' ExtendedProperties, NULL WatermarkColumn

UNION
SELECT 'BoM' SystemCode, 'BoM' SourceSchema, '' SourceKeyVaultSecret, 'http-binary-load' SourceHandler, 'json' RawFijsonxtension, 'raw-load' RawHandler, 'cleansed-load' CleansedHandler, 'WeatherObservation_Parramatta' SourceTableName, 'http://www.bom.gov.au/fwo/IDN60901/IDN60901.94764.json' SourceQuery, '{ "CleansedQuery" : "SELECT r.* FROM ( SELECT explode(observations.data) r FROM {tableFqn}) Q"  , "OverrideClusterName":"interactive"}' ExtendedProperties, NULL WatermarkColumn

UNION 
SELECT 'iicats_rw' SystemCode, 'scxstg' SourceSchema, 'daf-oracle-IICATS-stg-connectionstring' SourceKeyVaultSecret, 'oracle-load' SourceHandler, NULL RawFileExtension, 'raw-load ' RawHandler, 'cleansed-load ' CleansedHandler, 'hierarchy_cnfgn' SourceTableName, null SourceQuery, NULL ExtendedProperties, NULL WatermarkColumn
UNION 
SELECT 'iicats_rw' SystemCode, 'scxstg' SourceSchema, 'daf-oracle-IICATS-stg-connectionstring' SourceKeyVaultSecret, 'oracle-load' SourceHandler, NULL RawFileExtension, 'raw-load ' RawHandler, 'cleansed-load ' CleansedHandler, 'tsv_point_cnfgn' SourceTableName, null SourceQuery, NULL ExtendedProperties, NULL WatermarkColumn
UNION 
SELECT 'iicats_rw' SystemCode, 'scxstg' SourceSchema, 'daf-oracle-IICATS-stg-connectionstring' SourceKeyVaultSecret, 'oracle-load' SourceHandler, NULL RawFileExtension, 'raw-load ' RawHandler, 'cleansed-load ' CleansedHandler, 'tsv' SourceTableName, "select tsv.* from scxstg.tsv inner join scxstg.tsv_point_cnfgn t on tsv.cdb_obj_id = t.pnt_cdb_obj_id inner join scxstg.hierarchy_cnfgn h on t.cdb_obj_id = h.cdb_obj_id where h. site_cd in(''GG0022'',''GG0064'',''GG0020'',''GG0008'',''GG0019'',''GG0047'',''GG0016'') and h.obj_nm = ''Rainfall 15M Total'' and tsv.ht_crt_dt > (select sysdate - 7 from dual)" SourceQuery, NULL ExtendedProperties, NULL WatermarkColumn

/* -- CHANGED TO ONLY TAKE NECESSARY TABLES TO REDUCE LOAD
SELECT 'iicats_rw' SystemCode, 'scxstg' SourceSchema, 'daf-oracle-IICATS-stg-connectionstring' SourceKeyVaultSecret, 'oracle-load' SourceHandler, NULL RawFileExtension, 'raw-load ' RawHandler, 'cleansed-load ' CleansedHandler, 'dly_prof_cnfgn' SourceTableName, null SourceQuery, NULL ExtendedProperties
UNION 
SELECT 'iicats_rw' SystemCode, 'scxstg' SourceSchema, 'daf-oracle-IICATS-stg-connectionstring' SourceKeyVaultSecret, 'oracle-load' SourceHandler, NULL RawFileExtension, 'raw-load ' RawHandler, 'cleansed-load ' CleansedHandler, 'event' SourceTableName, null SourceQuery, NULL ExtendedProperties
UNION 

UNION 
SELECT 'iicats_rw' SystemCode, 'scxstg' SourceSchema, 'daf-oracle-IICATS-stg-connectionstring' SourceKeyVaultSecret, 'oracle-load' SourceHandler, NULL RawFileExtension, 'raw-load ' RawHandler, 'cleansed-load ' CleansedHandler, 'iicats_work_orders' SourceTableName, null SourceQuery, NULL ExtendedProperties

UNION 
SELECT 'iicats_rw' SystemCode, 'scxstg' SourceSchema, 'daf-oracle-IICATS-stg-connectionstring' SourceKeyVaultSecret, 'oracle-load' SourceHandler, NULL RawFileExtension, 'raw-load ' RawHandler, 'cleansed-load ' CleansedHandler, 'point_limit' SourceTableName, null SourceQuery, NULL ExtendedProperties
UNION 
SELECT 'iicats_rw' SystemCode, 'scxstg' SourceSchema, 'daf-oracle-IICATS-stg-connectionstring' SourceKeyVaultSecret, 'oracle-load' SourceHandler, NULL RawFileExtension, 'raw-load ' RawHandler, 'cleansed-load ' CleansedHandler, 'rtu' SourceTableName, null SourceQuery, NULL ExtendedProperties

UNION 
SELECT 'iicats_rw' SystemCode, 'scxstg' SourceSchema, 'daf-oracle-IICATS-stg-connectionstring' SourceKeyVaultSecret, 'oracle-load' SourceHandler, NULL RawFileExtension, 'raw-load ' RawHandler, 'cleansed-load ' CleansedHandler, 'tsv_point_cnfgn' SourceTableName, null SourceQuery, NULL ExtendedProperties
UNION 
SELECT 'iicats_rw' SystemCode, 'scxstg' SourceSchema, 'daf-oracle-IICATS-stg-connectionstring' SourceKeyVaultSecret, 'oracle-load' SourceHandler, NULL RawFileExtension, 'raw-load ' RawHandler, 'cleansed-load ' CleansedHandler, 'wkly_prof_cnfgn' SourceTableName, null SourceQuery, NULL ExtendedProperties
*/
""")
display(df)

# COMMAND ----------

def ExecuteStatement(sql):
    jdbc = JdbcConnectionFromSqlConnectionString(dbutils.secrets.get(scope = "ADS", key = "daf-sql-controldb-connectionstring"))
    connection = spark._sc._gateway.jvm.java.sql.DriverManager.getConnection(jdbc)
    connection.prepareCall(sql).execute()
    connection.close()

# COMMAND ----------

##DO NON IICATS
dfNonIicats = df.where("systemCode <> 'iicats_rw'")

for i in dfNonIicats.collect():
    ExecuteStatement(f"""
    DECLARE @RC int
    DECLARE @SystemCode varchar(max) = '{i.SystemCode}'
    DECLARE @Schema varchar(max) = '{i.SourceSchema}'
    DECLARE @Table varchar(max) = '{i.SourceTableName}'
    DECLARE @Query varchar(max) = '{i.SourceQuery}'
    DECLARE @WatermarkColumn varchar(max) = NULL
    DECLARE @SourceHandler varchar(max) = '{i.SourceHandler}'
    DECLARE @RawFileExtension varchar(max) = '{i.RawFileExtension}'
    DECLARE @KeyVaultSecret varchar(max) = '{i.SourceKeyVaultSecret}'
    DECLARE @ExtendedProperties varchar(max) = '{i.ExtendedProperties}'
    DECLARE @RawHandler varchar(max) = '{i.RawHandler}'
    DECLARE @CleansedHandler varchar(max) = '{i.CleansedHandler}'
    
    EXECUTE @RC = [dbo].[AddIngestion] 
       @SystemCode
      ,@Schema
      ,@Table
      ,@Query
      ,@WatermarkColumn
      ,@SourceHandler
      ,@RawFileExtension
      ,@KeyVaultSecret
      ,@ExtendedProperties
      ,@RawHandler
      ,@CleansedHandler
    """)

    
##DO IICATS
dfIicats = df.where("systemCode = 'iicats_rw'")

for i in dfIicats.collect():
    ExecuteStatement(f"""
    DECLARE @RC int
    DECLARE @SystemCode varchar(max) = '{i.SystemCode}'
    DECLARE @Schema varchar(max) = '{i.SourceSchema}'
    DECLARE @Table varchar(max) = '{i.SourceTableName}'
    DECLARE @Query varchar(max) = '{i.SourceQuery}'
    DECLARE @WatermarkColumn varchar(max) = '{i.WatermarkColumn}'
    DECLARE @SourceHandler varchar(max) = '{i.SourceHandler}'
    DECLARE @RawFileExtension varchar(max) = NULL
    DECLARE @KeyVaultSecret varchar(max) = '{i.SourceKeyVaultSecret}'
    DECLARE @ExtendedProperties varchar(max) = NULL
    DECLARE @RawHandler varchar(max) = '{i.RawHandler}'
    DECLARE @CleansedHandler varchar(max) = '{i.CleansedHandler}'
    
    EXECUTE @RC = [dbo].[AddIngestion] 
       @SystemCode
      ,@Schema
      ,@Table
      ,@Query
      ,@WatermarkColumn
      ,@SourceHandler
      ,@RawFileExtension
      ,@KeyVaultSecret
      ,@ExtendedProperties
      ,@RawHandler
      ,@CleansedHandler
    """)

#FIX DESTINATION SCHEMA
ExecuteStatement("""
update dbo.extractLoadManifest set
destinationSchema = 'iicats_rw'
where systemCode = 'iicats_rw' and sourceSchema = 'scxstg' and destinationSchema <> 'iicats'
""")

#FIX SOURCE QUERY SETTING TO NULL
ExecuteStatement("""
update dbo.extractLoadManifest set
sourceQuery = NULL
where systemCode = 'iicats_rw' and sourceSchema = 'scxstg' and sourceQuery = 'None'
""")

#FIX WATERMARK COLUMN
ExecuteStatement("""
update dbo.extractLoadManifest set
waterMarkColumn = NULL
where systemCode = 'iicats_rw' and sourceSchema = 'scxstg' and waterMarkColumn = 'None'
""")
