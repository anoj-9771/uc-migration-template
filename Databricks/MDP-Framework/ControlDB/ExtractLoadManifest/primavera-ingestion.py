# Databricks notebook source
# MAGIC %run /MDP-Framework/ControlDB/common-controldb

# COMMAND ----------

SYSTEM_CODE = "primavera"

# COMMAND ----------

df = spark.sql("""
    WITH _Base AS 
    (
      SELECT 'primavera' SystemCode, 'primavera' SourceSchema, '' SourceKeyVaultSecret, 'nas-binary-load' SourceHandler, 'csv' RawFileExtension, 'raw-load-delta' RawHandler, '' ExtendedProperties, 'cleansed-load-delta' CleansedHandler, '' WatermarkColumn
    )
SELECT 'activeprojects'  SourceTableName ,'PRI_Activeprojects' as SourceQuery  ,* FROM _Base  
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

#ADD BUSINESS KEY
ExecuteStatement("""
update dbo.extractLoadManifest set
businessKeyColumn = case sourceTableName
when 'activeprojects' then 'projectId,milestoneName,projectFinishdate,loadPeriod'
else businessKeyColumn
end
where systemCode in ('primavera')
""")

# COMMAND ----------

ExecuteStatement("""
    UPDATE controldb.dbo.extractloadmanifest
    SET ExtendedProperties = '{"RawTableNameMatchSource":"True","CleansedQuery":"SELECT * FROM (SELECT *,ROW_NUMBER() OVER (PARTITION BY Projectid,IPAMLT,Finish ORDER BY  _DLRawZoneTimeStamp DESC) rn FROM {tableFqn} WHERE _DLRawZoneTimeStamp > ''{lastLoadTimeStamp}'') WHERE rn=1"}'
    WHERE (SystemCode in ('primavera') )""")   

# COMMAND ----------

ExecuteStatement(f"update dbo.extractLoadManifest set enabled = 1 where systemCode = '{SYSTEM_CODE}'")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from controldb.dbo_extractloadmanifest where systemCode = 'primavera'
