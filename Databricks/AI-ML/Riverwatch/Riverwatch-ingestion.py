# Databricks notebook source
# MAGIC %run /MDP-Framework/Common/common-jdbc

# COMMAND ----------

df = spark.sql("""
WITH _Base AS 
(
  SELECT 'BeachWatch' SystemCode, '' SourceKeyVaultSecret, 'http-binary-load' SourceHandler, 'xml' RawFileExtension, 'raw-load-delta' RawHandler, 'cleansed-load-delta' CleansedHandler
)
SELECT SystemCode, SourceKeyVaultSecret, SourceHandler, RawFileExtension, RawHandler, CleansedHandler, 'Chiswick_Baths_Pollution_WeatherForecast' SourceTableName, 'http://www.environment.nsw.gov.au/Beachapp/SingleRss.aspx?surl=%2Fbeachapp%2FSydneyBulletin.xml&title=Chiswick%20Baths' SourceQuery, '{"rowTag" : "channel", "CleansedQuery" : "Select 5 locationId, * from raw.BeachWatch_Chiswick_Baths_Pollution_WeatherForecast"}' ExtendedProperties
from _Base

UNION
SELECT SystemCode, SourceKeyVaultSecret, SourceHandler, RawFileExtension, RawHandler, CleansedHandler, 'Cabarita_Beach_Pollution_WeatherForecast' SourceTableName, 'http://www.environment.nsw.gov.au/Beachapp/SingleRss.aspx?surl=%2Fbeachapp%2FSydneyBulletin.xml&title=Cabarita%20Beach' SourceQuery, '{"rowTag" : "channel", "CleansedQuery" : "Select 4 locationId, * from raw.BeachWatch_Cabarita_Beach_Pollution_WeatherForecast"}' ExtendedProperties
from _Base

UNION
SELECT SystemCode, SourceKeyVaultSecret, SourceHandler, RawFileExtension, RawHandler, CleansedHandler, 'Dawn_Fraser_Pool_Pollution_WeatherForecast' SourceTableName, 'http://www.environment.nsw.gov.au/Beachapp/SingleRss.aspx?surl=%2Fbeachapp%2FSydneyBulletin.xml&title=Dawn%20Fraser%20Pool' SourceQuery, '{"rowTag" : "channel", "CleansedQuery" : "Select 3 locationId, * from raw.BeachWatch_Dawn_Fraser_Pool_Pollution_WeatherForecast"}' ExtendedProperties
from _Base

UNION 
SELECT 'BoM' SystemCode, '' SourceKeyVaultSecret, 'http-binary-load' SourceHandler, 'xml' RawFileExtension, 'raw-load' RawHandler, 'cleansed-load' CleansedHandler, 'FortDenision_Tide' SourceTableName, 'http://www.bom.gov.au/ntc/IDO59001/IDO59001_2023_NSW_TP007.xml' SourceQuery
,     '{"rowTag" : "forecast-period", "TransformMethod": "ExpandTable"}' ExtendedProperties

""")
display(df)

# COMMAND ----------

def ExecuteStatement(sql):
    jdbc = JdbcConnectionFromSqlConnectionString(dbutils.secrets.get(scope = "ADS", key = "daf-sql-controldb-connectionstring"))
    connection = spark._sc._gateway.jvm.java.sql.DriverManager.getConnection(jdbc)
    connection.prepareCall(sql).execute()
    connection.close()

# COMMAND ----------

ExecuteStatement("""
DELETE FROM dbo.extractloadmanifest
WHERE SystemCode in ('BeachWatch','BoM')
""")

for i in df.rdd.collect():
    ExecuteStatement(f"""
    DECLARE @RC int
    DECLARE @SystemCode varchar(max) = '{i.SystemCode}'
    DECLARE @Schema varchar(max) = '{i.SystemCode}'
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
    #break   

# COMMAND ----------


