# Databricks notebook source
# MAGIC %run ../../Common/common-helpers

# COMMAND ----------

# MAGIC %run ../../Common/common-transform

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {get_table_namespace(f'{DEFAULT_TARGET}', 'viewAncestorLocationHierarchyPivot')} AS
(
    SELECT
    assetLocationIdentifier,
    assetLocationFK,
    assetLocationAncestorHierarchySystemName,
    assetLocationAncestorLevel10Name,
    assetLocationAncestorLevel10Description,
    assetLocationAncestorLevel20Name,
    assetLocationAncestorLevel20Description,
    assetLocationAncestorLevel30Name,
    assetLocationAncestorLevel30Description,
    assetLocationAncestorLevel40Name,
    assetLocationAncestorLevel40Description,
    assetLocationAncestorLevel50Name,
    assetLocationAncestorLevel50Description,
    assetLocationAncestorLevel60Name,
    assetLocationAncestorLevel60Description,
    assetLocationAncestorLevel70Name,
    assetLocationAncestorLevel70Description,
    assetLocationAncestorLevel80Name,
    assetLocationAncestorLevel80Description,
    assetLocationAncestorLevel83Name,
    assetLocationAncestorLevel83Description,
    assetLocationAncestorLevel85Name,
    assetLocationAncestorLevel85Description,
    assetLocationAncestorLevel87Name,
    assetLocationAncestorLevel87Description,
    assetLocationAncestorLevel90Name,
    assetLocationAncestorLevel90Description,
    assetLocationAncestorLevel93Name,
    assetLocationAncestorLevel93Description
    FROM 
    (
    select 
    assetLocationIdentifier,
    assetLocationFK,
    assetLocationAncestorHierarchySystemName,
    max(case
    when assetLocationAncestorLevelCode = 10 then assetLocationAncestorName end
    ) as assetLocationAncestorLevel10Name,
    max(case
    when assetLocationAncestorLevelCode = 10 then assetLocationAncestorDescription end
    ) as assetLocationAncestorLevel10Description,
    max(case
    when assetLocationAncestorLevelCode = 20 then assetLocationAncestorName end
    ) as assetLocationAncestorLevel20Name,
    max(case
    when assetLocationAncestorLevelCode = 20 then assetLocationAncestorDescription end
    ) as assetLocationAncestorLevel20Description,
    max(case
    when assetLocationAncestorLevelCode = 30 then assetLocationAncestorName end
    ) as assetLocationAncestorLevel30Name,
    max(case
    when assetLocationAncestorLevelCode = 30 then assetLocationAncestorDescription end
    ) as assetLocationAncestorLevel30Description,
    max(case
    when assetLocationAncestorLevelCode = 40 then assetLocationAncestorName end
    ) as assetLocationAncestorLevel40Name,  
    max(case
    when assetLocationAncestorLevelCode = 40 then assetLocationAncestorDescription end
    ) as assetLocationAncestorLevel40Description,
    max(case
    when assetLocationAncestorLevelCode = 50 then assetLocationAncestorName end
    ) as assetLocationAncestorLevel50Name,    
    max(case
    when assetLocationAncestorLevelCode = 50 then assetLocationAncestorDescription end
    ) as assetLocationAncestorLevel50Description,
    max(case
    when assetLocationAncestorLevelCode = 60 then assetLocationAncestorName end
    ) as assetLocationAncestorLevel60Name,    
    max(case
    when assetLocationAncestorLevelCode = 60 then assetLocationAncestorDescription end
    ) as assetLocationAncestorLevel60Description,
    max(case
    when assetLocationAncestorLevelCode = 70 then assetLocationAncestorName end
    ) as assetLocationAncestorLevel70Name,      
    max(case
    when assetLocationAncestorLevelCode = 70 then assetLocationAncestorDescription end
    ) as assetLocationAncestorLevel70Description,  
    max(case
    when assetLocationAncestorLevelCode = 80 then assetLocationAncestorName end
    ) as assetLocationAncestorLevel80Name,        
    max(case
    when assetLocationAncestorLevelCode = 80 then assetLocationAncestorDescription end
    ) as assetLocationAncestorLevel80Description,
    max(case
    when assetLocationAncestorLevelCode = 83 then assetLocationAncestorName end
    ) as assetLocationAncestorLevel83Name,     
    max(case
    when assetLocationAncestorLevelCode = 83 then assetLocationAncestorDescription end
    ) as assetLocationAncestorLevel83Description,
    max(case
    when assetLocationAncestorLevelCode = 85 then assetLocationAncestorName end
    ) as assetLocationAncestorLevel85Name,      
    max(case
    when assetLocationAncestorLevelCode = 85 then assetLocationAncestorDescription end
    ) as assetLocationAncestorLevel85Description,
    max(case
    when assetLocationAncestorLevelCode = 87 then assetLocationAncestorName end
    ) as assetLocationAncestorLevel87Name,  
    max(case
    when assetLocationAncestorLevelCode = 87 then assetLocationAncestorDescription end
    ) as assetLocationAncestorLevel87Description,
    max(case
    when assetLocationAncestorLevelCode = 90 then assetLocationAncestorName end
    ) as assetLocationAncestorLevel90Name,  
    max(case
    when assetLocationAncestorLevelCode = 90 then assetLocationAncestorDescription end
    ) as assetLocationAncestorLevel90Description,
    max(case
    when assetLocationAncestorLevelCode = 93 then assetLocationAncestorName end
    ) as assetLocationAncestorLevel93Name,    
    max(case
    when assetLocationAncestorLevelCode = 93 then assetLocationAncestorDescription end
    ) as assetLocationAncestorLevel93Description
    from {get_table_namespace(f'{DEFAULT_TARGET}', 'dimAssetLocationAncestor')}
    where sourceRecordCurrent = 1
    group by assetLocationIdentifier, assetLocationFK, assetLocationAncestorHierarchySystemName
    ) DT
    WHERE DT.assetLocationAncestorLevel10Name = 'SWC')
""")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from  {get_table_namespace('curated', 'viewAncestorLocationHierarchyPivot')} limit 10

# COMMAND ----------


