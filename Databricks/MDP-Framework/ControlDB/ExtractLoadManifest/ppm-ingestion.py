# Databricks notebook source
# MAGIC %run ../common-controldb

# COMMAND ----------

from pyspark.sql.functions import lit, when, lower, expr
df = spark.sql("""
WITH _Base AS 
(
  SELECT 'ppmref' SystemCode, 'ppm' SourceSchema, 'daf-sa-blob-sastoken' SourceKeyVaultSecret, 'ppm' DestinationSchema, 'bods-load' SourceHandler, 'json' RawFileExtension, 'raw-load-bods' RawHandler, --'{ "DeleteSourceFiles" : "True" }' ExtendedProperties,
    'cleansed-load-bods-slt' CleansedHandler, '' SystemPath
)
SELECT '0INM_INITIATIVE_GUID_TEXT' SourceTableName, 'ppmref/0INM_INITIATIVE_GUID_TEXT' SourceQuery, 'DELTA' WatermarkColumn, * FROM _Base
UNION 
SELECT '0RPM_PORT_GUID_ID_TEXT' SourceTableName, 'ppmref/0RPM_PORT_GUID_ID_TEXT' SourceQuery, 'DELTA' WatermarkColumn, * FROM _Base
UNION 
SELECT '0RPM_ITEM_TYPE_TEXT' SourceTableName, 'ppmref/0RPM_ITEM_TYPE_TEXT' SourceQuery, '' WatermarkColumn, * FROM _Base
UNION 
SELECT '0RPM_DECISION_ID_TEXT' SourceTableName, 'ppmref/0RPM_DECISION_ID_TEXT' SourceQuery, '' WatermarkColumn, * FROM _Base
UNION 
SELECT '0PROJECT_TEXT' SourceTableName, 'ppmref/0PROJECT_TEXT' SourceQuery, 'DELTA' WatermarkColumn, * FROM _Base
UNION 
SELECT '0WBS_ELEMT_TEXT' SourceTableName, 'ppmref/0WBS_ELEMT_TEXT' SourceQuery, 'DELTA' WatermarkColumn, * FROM _Base
UNION 
SELECT '0RPM_DECISION_GUID_ID_TEXT' SourceTableName, 'ppmref/0RPM_DECISION_GUID_ID_TEXT' SourceQuery, 'DELTA' WatermarkColumn, * FROM _Base
UNION 
SELECT '0RPM_BUCKET_GUID_ID_TEXT' SourceTableName, 'ppmref/0RPM_BUCKET_GUID_ID_TEXT' SourceQuery, 'DELTA' WatermarkColumn, * FROM _Base
UNION
--Start of tables that may get converted to SLT later
SELECT 'RPM_STATUS_T' SourceTableName, 'ppmref/RPM_STATUS_T' SourceQuery, '' WatermarkColumn, * FROM _Base  
UNION
SELECT 'TCJ4T' SourceTableName, 'ppmref/TCJ4T' SourceQuery, '' WatermarkColumn, * FROM _Base  
UNION
SELECT 'ZEPT_SUB_DELPART' SourceTableName, 'ppmref/ZEPT_SUB_DELPART' SourceQuery, '' WatermarkColumn, * FROM _Base  
UNION
SELECT 'CGPL_TEXT' SourceTableName, 'ppmref/CGPL_TEXT' SourceQuery, '' WatermarkColumn, * FROM _Base
UNION
SELECT 'ZEPT_MPALIST' SourceTableName, 'ppmref/ZEPT_MPALIST' SourceQuery, '' WatermarkColumn, * FROM _Base
UNION
SELECT 'ZEPT_DEL_PARTNER' SourceTableName, 'ppmref/ZEPT_DEL_PARTNER' SourceQuery, '' WatermarkColumn, * FROM _Base
UNION
SELECT 'ZEPT_CIP_TYP' SourceTableName, 'ppmref/ZEPT_CIP_TYP' SourceQuery, '' WatermarkColumn, * FROM _Base

--End of tables that may get converted to SLT later
UNION
(
WITH _Base AS 
(
  SELECT 'ppmdata' SystemCode, 'ppm' SourceSchema, 'daf-sa-blob-sastoken' SourceKeyVaultSecret, 'ppm' DestinationSchema, 'bods-load' SourceHandler, 'json' RawFileExtension, 'raw-load-bods' RawHandler, --'{ "DeleteSourceFiles" : "True" }' ExtendedProperties,
  'cleansed-load-bods-slt' CleansedHandler, '' SystemPath
)
SELECT '0RPM_DECISION_GUID_ATTR' SourceTableName, 'ppmdata/0RPM_DECISION_GUID_ATTR' SourceQuery, 'DELTA' WatermarkColumn, * FROM _Base
UNION
SELECT '0PROJECT_ATTR' SourceTableName, 'ppmdata/0PROJECT_ATTR' SourceQuery, 'DELTA' WatermarkColumn, * FROM _Base   
UNION
SELECT '0RPM_PORT_GUID_ATTR' SourceTableName, 'ppmdata/0RPM_PORT_GUID_ATTR' SourceQuery, 'DELTA' WatermarkColumn, * FROM _Base  
UNION
--Start of tables that may get converted to SLT later
SELECT 'RPM_ITEM_D' SourceTableName, 'ppmdata/RPM_ITEM_D' SourceQuery, '' WatermarkColumn, * FROM _Base  
UNION
SELECT 'IHPA' SourceTableName, 'ppmdata/IHPA' SourceQuery, '' WatermarkColumn, * FROM _Base  
UNION
SELECT 'RPSCO' SourceTableName, 'ppmdata/RPSCO' SourceQuery, '' WatermarkColumn, * FROM _Base  
UNION
SELECT 'RPM_RELATION_D' SourceTableName, 'ppmdata/RPM_RELATION_D' SourceQuery, '' WatermarkColumn, * FROM _Base  
UNION 
SELECT '0WBS_ELEMT_ATTR' SourceTableName, 'ppmdata/0WBS_ELEMT_ATTR' SourceQuery, 'DELTA' WatermarkColumn, * FROM _Base
UNION 
SELECT '0INM_INITIATIVE_GUID_ATTR' SourceTableName, 'ppmdata/0INM_INITIATIVE_GUID_ATTR' SourceQuery, 'DELTA' WatermarkColumn, * FROM _Base
UNION 
SELECT '0RPM_BUCKET_GUID_ATTR' SourceTableName, 'ppmdata/0RPM_BUCKET_GUID_ATTR' SourceQuery, 'DELTA' WatermarkColumn, * FROM _Base
UNION 
SELECT 'ZPS_PSF_COMMLOG' SourceTableName, 'ppmdata/ZPS_PSF_COMMLOG' SourceQuery, '' WatermarkColumn, * FROM _Base

--End of tables that may get converted to SLT later
)    
ORDER BY SourceSchema, SourceTableName
""")

#Delta Tables with delete image. These tables would have di_operation_type column. 
tables = ['0inm_initiative_guid_attr','0inm_initiative_guid_text','0project_attr','0project_text','0rpm_bucket_guid_id_text','0rpm_decision_guid_attr','0rpm_port_guid_attr','0rpm_port_guid_id_text','0wbs_elemt_attr','0wbs_elemt_text','0rpm_bucket_guid_attr']
tablesWithNullableKeys = ['rpsco']
tablesUsingKingCluster = ['rpsco']
#Non-Prod "DeleteSourceFiles" : "False" while testing
# deleteSourceFiles = "False"
#Production
deleteSourceFiles = "True"
df = (
    df.withColumn('ExtendedProperties', lit(f'"DeleteSourceFiles" : "{deleteSourceFiles}"'))
      .withColumn('ExtendedProperties', when(lower(df.SourceTableName).isin(tables),expr('ExtendedProperties ||", "||\'\"SourceRecordDeletion\" : \"True\"\'')) 
                                        .otherwise(expr('ExtendedProperties')))
      .withColumn('ExtendedProperties', when(lower(df.SourceTableName).isin(tablesWithNullableKeys),expr('ExtendedProperties ||", "||\'\"CreateTableConstraints\" : \"False\"\''))
                                        .otherwise(expr('ExtendedProperties')))
      .withColumn('ExtendedProperties', when(lower(df.SourceTableName).isin(tablesUsingKingCluster),expr('ExtendedProperties ||", "||\'\"OverrideClusterName\" : \"King_Cluster_SingleNode\"\''))
                                        .otherwise(expr('ExtendedProperties')))      
      .withColumn('ExtendedProperties', expr('"{"||ExtendedProperties ||"}"'))    
)
display(df)

# COMMAND ----------

def ConfigureManifest(dataFrameConfig, whereClause=None):
    # ------------- CONSTRUCT QUERY ----------------- #
    df = dataFrameConfig.where(whereClause)
    # ------------- DISPLAY ----------------- #
#     ShowQuery(df)

    # ------------- SAVE ----------------- #
    AddIngestion(df)
    
    # ------------- ShowConfig ----------------- #
    ShowConfig()

for system_code in ['ppmref','ppmdata']:
    SYSTEM_CODE = system_code
    ConfigureManifest(df, whereClause=f"SystemCode = '{SYSTEM_CODE}'")

#ADD BUSINESS KEY
ExecuteStatement("""
update dbo.extractLoadManifest set
businessKeyColumn = case sourceTableName
when '0INM_INITIATIVE_GUID_ATTR' then 'initiativeGuid'
when '0INM_INITIATIVE_GUID_TEXT' then 'initiativeGuid'
when '0PROJECT_ATTR' then 'projectIdDefinition'
when '0PROJECT_TEXT' then 'projectIdDefinition'
when '0RPM_BUCKET_GUID_ID_TEXT' then 'bucketGuid'
when '0RPM_BUCKET_GUID_ATTR' then 'bucketGuid'
when '0RPM_DECISION_GUID_ATTR' then 'decisionGuid'
when '0RPM_DECISION_GUID_ID_TEXT' then 'decisionGuid'
when '0RPM_DECISION_ID_TEXT' then 'decisionPointId,itemType'
when '0RPM_ITEM_TYPE_TEXT' then 'itemType'
when '0RPM_PORT_GUID_ATTR' then 'portGuid'
when '0RPM_PORT_GUID_ID_TEXT' then 'portGuid'
when '0WBS_ELEMT_ATTR' then 'projectDefinition,wbsElement'
when '0WBS_ELEMT_TEXT' then 'wbsElement'
when 'RPM_ITEM_D' then 'itemDetailGuid'
when 'IHPA' then 'objectNumber,partnerFunctionId,counterDigitId'
when 'RPSCO' then 'objectNumber,budgetLedger,valueType,objectIndicator,fiscalYear,valuecategory,budgetPlanning,planningVersion,analysisCategory,fund,transactionCurrency,periodBlock'
when 'RPM_RELATION_D' then 'guid'
when 'RPM_STATUS_T' then 'projectApprovalStatusId'
when 'CGPL_TEXT' then 'projectPlanningGuid'
when 'TCJ4T' then 'projectProfile'
when 'ZEPT_SUB_DELPART' then 'projectType,subDeliveryPartner'
when 'ZEPT_MPALIST' then 'majorProjectAssociationId'
when 'ZEPT_DEL_PARTNER' then 'deliveryPartnerKey,deliveryPartner'
when 'ZEPT_CIP_TYP' then 'cipAssetType'
when 'ZPS_PSF_COMMLOG' then 'projectObject,projectStatus,calendarYear,calendarMonth'
else businessKeyColumn
end
where systemCode in ('ppmref','ppmdata')
""")

# #Retain this until bxp landing folders are migrated to ppm
# ExecuteStatement("""
# update dbo.extractLoadManifest set
# SourceQuery = replace(SourceQuery, 'ppm', 'bxp')
# where systemCode in ('ppmref','ppmdata')
# and SourceTableName != '0RPM_DECISION_GUID_ID_TEXT'
# """) 

# COMMAND ----------

#Delete all cells below once this goes to Production

# COMMAND ----------

# %run /MDP-Framework/Common/common-spark

# COMMAND ----------

# df_c = spark.table("controldb.dbo_extractloadmanifest")
# display(df_c.where("SystemCode in ('ppmref','ppmdata')"))

# COMMAND ----------

# # for z in ["raw", "cleansed"]:
# for z in ["cleansed"]:
#     for t in df_c.where("SystemCode in ('ppmref','ppmdata')").collect():
#         tableFqn = f"{z}.{t.DestinationSchema}_{t.SourceTableName}"
#         print(tableFqn)
# #         CleanTable(tableFqn)

# COMMAND ----------

# CleanTable('cleaned.ppm_0RPM_DECISION_GUID_ID_TEXT')
