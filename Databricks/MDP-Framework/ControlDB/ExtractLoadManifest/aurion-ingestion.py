# Databricks notebook source
# MAGIC %run /MDP-Framework/ControlDB/common-controldb

# COMMAND ----------

SYSTEM_CODE = "aurion"

# COMMAND ----------

df = spark.sql("""
    WITH _Base AS 
    (
      SELECT 'aurion' SystemCode, 'aurion' SourceSchema, '' SourceKeyVaultSecret, 'nas-binary-load' SourceHandler, 'csv' RawFileExtension, 'raw-load-delta' RawHandler, '' ExtendedProperties, 'cleansed-load-delta' CleansedHandler, '' WatermarkColumn
    )
    SELECT 'organisation' SourceTableName, '70094_org' as SourceQuery, * FROM _Base
    UNION 
    SELECT 'position' SourceTableName, '70095_pos' as SourceQuery, * FROM _Base
    UNION 
    SELECT 'active_employees' SourceTableName, '70096_active' as SourceQuery, * FROM _Base
    UNION 
    SELECT 'terminated_employees' SourceTableName, '70097_term' as SourceQuery, * FROM _Base
    UNION
    SELECT 'employee_history' SourceTableName, '70098_emp_pos_hist' as SourceQuery, * FROM _Base
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
when 'position' then 'positionNumber'
when 'organisation' then 'organisationUnitNumber'
when 'active_employees' then 'employeeNumber,personNumber,dateEffective,positionNumber'
when 'terminated_employees' then 'employeeNumber,personNumber,dateEffective,positionNumber'
when 'employee_history' then 'employeeNumber,personNumber,dateEffective,positionNumber,dateTo'
else businessKeyColumn
end
where systemCode in ('aurion')
""")

# COMMAND ----------

ExecuteStatement(f"update dbo.extractLoadManifest set enabled = 1 where systemCode = '{SYSTEM_CODE}' and sourceTableName <> 'employee_history' ")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from controldb.dbo_extractloadmanifest where systemCode = 'aurion'
