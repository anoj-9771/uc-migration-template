# Databricks notebook source
# MAGIC %run /MDP-Framework/ControlDB/common-controldb

# COMMAND ----------

SYSTEM_CODE = "aurion"

# COMMAND ----------

df = spark.sql("""
    WITH _Base AS 
    (
      SELECT 'aurion' SystemCode, 'aurion' SourceSchema, '' SourceKeyVaultSecret, 'file-binary-load' SourceHandler, 'csv' RawFileExtension, 'raw-load-delta' RawHandler, '' ExtendedProperties, 'cleansed-load-delta' CleansedHandler, '' WatermarkColumn
    )
    SELECT 'organisation' SourceTableName, 'aurion/70094_org_dev_230119.csv' as SourceQuery, * FROM _Base
    UNION 
    SELECT 'position' SourceTableName, 'aurion/70095_pos_dev_230119.csv' as SourceQuery, * FROM _Base
    UNION 
    SELECT 'active_employees' SourceTableName, 'aurion/70096_active_dev_230119.csv' as SourceQuery, * FROM _Base
    UNION 
    SELECT 'terminated_employees' SourceTableName, 'aurion/70097_term_dev_230119.csv' as SourceQuery, * FROM _Base
    UNION
    SELECT 'employee_history' SourceTableName, 'aurion/70098_emp_pos_hist_post_190101.csv' as SourceQuery, * FROM _Base
    """)

# COMMAND ----------

ExecuteStatement(f"delete from dbo.extractLoadManifest where systemCode = '{SYSTEM_CODE}'")

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
when 'employee_history' then 'employeeNumber,personNumber,dateEffective,positionNumber'
else businessKeyColumn
end
where systemCode in ('aurion')
""")

# COMMAND ----------

ExecuteStatement(f"update dbo.extractLoadManifest set enabled = 1 where systemCode = '{SYSTEM_CODE}'")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from controldb.dbo_extractloadmanifest where systemCode = 'aurion'

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select
# MAGIC * from raw.aurion_employee_history
