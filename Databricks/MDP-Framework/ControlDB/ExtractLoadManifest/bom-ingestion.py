# Databricks notebook source
# MAGIC %run ../common-controldb

# COMMAND ----------

SYSTEM_CODE = "BoM2"

# COMMAND ----------

def ConfigureManifest():
    # ------------- CONSTRUCT QUERY ----------------- #
    df = spark.sql("""
        WITH _Base AS (
            SELECT '' SourceKeyVaultSecret, 'http-binary-load' SourceHandler, 'csv' RawFileExtension, 'raw-bom-csv' RawHandler, '' ExtendedProperties
        )
        SELECT 'DailyWeatherObservation_SydneyAirport' SourceTableName, 'http://www.bom.gov.au/climate/dwo/$yyyy$$MM$/text/IDCJDW2125.$yyyy$$MM$.csv' SourceQuery, * FROM _Base
        UNION SELECT 'DailyWeatherObservation_ParramattaNorth' SourceTableName, 'http://www.bom.gov.au/climate/dwo/$yyyy$$MM$/text/IDCJDW2107.$yyyy$$MM$.csv' SourceQuery, * FROM _Base
        UNION SELECT 'FortDenision_Tide' SourceTableName, 'http://www.bom.gov.au/ntc/IDO59001/IDO59001_2023_NSW_TP007.xml' SourceQuery, '' SourceKeyVaultSecret, 'http-binary-load' SourceHandler, 'xml' RawFileExtension, 'raw-load' RawHandler, '{"rowTag" : "element"}' ExtendedProperties
        UNION SELECT 'WeatherForecast' SourceTableName, 'ftp://ftp.bom.gov.au/anon/gen/fwo/IDN10064.xml' SourceQuery, '' SourceKeyVaultSecret, 'ftp-binary-load' SourceHandler, 'xml' RawFileExtension, 'raw-load' RawHandler, '{"rowTag" : "area"}' ExtendedProperties
        UNION SELECT 'Chiswick_Baths_Pollution_WeatherForecast' SourceTableName, 'http://www.environment.nsw.gov.au/Beachapp/SingleRss.aspx?surl=%2Fbeachapp%2FSydneyBulletin.xml&title=Chiswick%20Baths' SourceQuery, '' SourceKeyVaultSecret, 'http-binary-load' SourceHandler, 'xml' RawFileExtension, 'raw-load' RawHandler, '{"rowTag" : "channel"}' ExtendedProperties
        UNION SELECT 'Cabarita_Beach_Pollution_WeatherForecast' SourceTableName, 'http://www.environment.nsw.gov.au/Beachapp/SingleRss.aspx?surl=%2Fbeachapp%2FSydneyBulletin.xml&title=Cabarita%20Beach' SourceQuery, '' SourceKeyVaultSecret, 'http-binary-load' SourceHandler, 'xml' RawFileExtension, 'raw-load' RawHandler, '{"rowTag" : "channel"}' ExtendedProperties
        UNION SELECT 'Dawn_Fraser_Pool_Pollution_WeatherForecast' SourceTableName, 'http://www.environment.nsw.gov.au/Beachapp/SingleRss.aspx?surl=%2Fbeachapp%2FSydneyBulletin.xml&title=Dawn%20Fraser%20Pool' SourceQuery, '' SourceKeyVaultSecret, 'http-binary-load' SourceHandler, 'xml' RawFileExtension, 'raw-load' RawHandler, '{"rowTag" : "channel"}' ExtendedProperties
        --ORDER BY RawFileExtension
    """)
    
    df = df.limit(1)
    # ------------- DISPLAY ----------------- #
    ShowQuery(df)

    # ------------- SAVE ----------------- #
    #AddIngestion(df)
    
    # ------------- ShowConfig ----------------- #
    ShowConfig()
    
ConfigureManifest()

# COMMAND ----------


