# Databricks notebook source
# MAGIC %run /MDP-Framework/ControlDB/common-controldb

# COMMAND ----------

SYSTEM_CODE = 'maximo'

# COMMAND ----------

tables = ['ADDRESS', 'ALNDOMAIN', 'ASSET', 'ASSETATTRIBUTE', 'ASSETMETER', 'ASSETSPEC', 'CLASSIFICATION', 'CLASSSTRUCTURE', 'CONTRACT', 'DOCLINKS', 'FAILURECODE', 'FAILUREREPORT', 'FINCNTRL', 'JOBLABOR', 'JOBPLAN', 'LABOR', 'LABTRANS', 'LOCANCESTOR', 'LOCATIONMETER', 'LOCATIONS', 'LOCATIONSPEC', 'LOCHIERARCHY', 'LOCOPER', 'LONGDESCRIPTION', 'MATUSETRANS', 'MAXINTMSGTRK', 'MULTIASSETLOCCI', 'PERSON', 'PERSONGROUP', 'PERSONGROUPTEAM', 'PHONE', 'PM', 'RELATEDRECORD', 'ROUTE_STOP', 'ROUTES', 'SERVRECTRANS', 'SR', 'SWCBUSSTATUS', 'SWCHIERARCHY', 'SWCLGA', 'SWCPROBLEMTYPE', 'SWCWOEXT', 'SYNONYMDOMAIN', 'TICKET', 'TOOLTRANS', 'WOACTIVITY', 'WOANCESTOR', 'WORKLOG', 'WORKORDER', 'WORKORDERSPEC', 'WOSTATUS']

# COMMAND ----------

base_query = """    WITH _Base AS (
      SELECT 'maximo' SourceSchema, 'oracle-load' SourceHandler, 'raw-load-delta' RawHandler, 'cleansed-load-delta' CleansedHandler, '{"GroupOrderBy" : "rowStamp Desc"}' ExtendedProperties, 'CAST(rowstamp as INT)' WatermarkColumn, 'daf-oracle-maximo-connectionstring' SourceKeyVaultSecret, '' SourceQuery, '' RawFileExtension
    )"""

# COMMAND ----------

for table in tables:
    base_query += (f"SELECT '{table}' SourceTableName, * FROM _Base \n UNION \n")
df = spark.sql(f"{base_query[:-5]}")    

# COMMAND ----------

display(df)

# COMMAND ----------

def ConfigureManifest(df):
    # ------------- CONSTRUCT QUERY ----------------- #
    
    # ------------- DISPLAY ----------------- #
    ShowQuery(df)

    # ------------- SAVE ----------------- #
    AddIngestion(df)
    
    #updating WatermarkColumn 
    ExecuteStatement("""
    update dbo.extractLoadManifest set
    businessKeyColumn = case sourceTableName
    when 'WORKORDER' then 'site,workOrder,rowStamp'
    when 'SWCPROBLEMTYPE' then 'problemType'
    when 'JOBPLAN' then 'jobPlan,organization,revision,site'
    when 'TICKET' then 'class,serviceRequest'
    when 'LOCOPER' then 'location,site'
    when 'RELATEDRECORD' then 'relatedRecordId,rowStamp'
    when 'SWCBUSSTATUS' then 'siteIdP1,recordClassP2,recordKeyP3'
    when 'ASSET' then 'asset,site'
    when 'JOBLABOR' then 'jobLabOrId'
    when 'ASSETMETER' then 'asset,linearSpecificationId,meter,site'
    when 'LABOR' then 'labor,organization'
    when 'SYNONYMDOMAIN' then 'domain,internalValue,organization,site,value'
    when 'ROUTE_STOP' then 'route,stopAddedToRoute,site'
    when 'TOOLTRANS' then 'tool,site,toolTransId'
    when 'ADDRESS' then 'addressCode,organization'
    when 'MAXINTMSGTRK' then 'messageId'
    when 'CLASSSTRUCTURE' then 'classStructure'
    when 'CONTRACT' then 'contract,organization,revision'
    when 'LOCATIONMETER' then 'location,meter,site'
    when 'PHONE' then 'person,phone,type'
    when 'SWCWOEXT' then 'swcwoextid,workorderid'
    when 'SERVRECTRANS' then 'service'
    when 'LOCHIERARCHY' then 'location,parent,site,system'
    when 'LOCATIONS' then 'location,site'
    when 'MATUSETRANS' then 'usageId,site'
    when 'FAILUREREPORT' then 'failureReportId'
    when 'LONGDESCRIPTION' then 'language,ldkey,ldownercol,ldownertable'
    when 'WORKLOG' then 'worklogId'
    when 'SR' then 'class,serviceRequest'
    when 'SWCHIERARCHY' then 'organisation,code,type'
    when 'WOACTIVITY' then 'site,activity,rowStamp'
    when 'FAILURECODE' then 'failureCode,organization'
    when 'FINCNTRL' then 'fCId,organization'
    when 'ASSETATTRIBUTE' then 'attribute,organization,site'
    when 'WOANCESTOR' then 'ancestor,site,workOrder'
    when 'SWCLGA' then 'lgaCode'
    when 'PERSON' then 'person'
    when 'LOCATIONSPEC' then 'attribute,location,section,site'
    when 'DOCLINKS' then 'docinfoId,documentFolder,ownerId,ownerTable'
    when 'ASSETSPEC' then 'attribute,asset,linearSpecificationId,section,site'
    when 'LOCANCESTOR' then 'searchLocationHierarchy,location,site,system'
    when 'LABTRANS' then 'labor,id,site'
    when 'PM' then 'pM,site,rowStamp'
    when 'ALNDOMAIN' then 'domain,organization,site,value'
    when 'WORKORDERSPEC' then 'attribute,section,site,workOrder'
    when 'MULTIASSETLOCCI' then 'multiId'
    when 'WOSTATUS' then 'wOStatusId'
    when 'ROUTES' then 'route,site'
    when 'CLASSIFICATION' then 'classification,organization,site'
    when 'PERSONGROUP' then 'personGroup,rowStamp'
    when 'PERSONGROUPTEAM' then 'personGroup,person,personGroupMember,useForOrganization,useForSite,rowStamp'
    when 'ALNDOMAIN' then 'domain,organization,site,value'
    else businessKeyColumn
    end
    where SystemCode = 'maximo'
    and SourceTableName in ('ADDRESS', 'ALNDOMAIN', 'ASSET', 'ASSETATTRIBUTE', 'ASSETMETER', 'ASSETSPEC', 'CLASSIFICATION', 'CLASSSTRUCTURE', 'CONTRACT', 'DOCLINKS', 'FAILURECODE', 'FAILUREREPORT', 'FINCNTRL', 'JOBLABOR', 'JOBPLAN', 'LABOR', 'LABTRANS', 'LOCANCESTOR', 'LOCATIONMETER', 'LOCATIONS', 'LOCATIONSPEC', 'LOCHIERARCHY', 'LOCOPER', 'LONGDESCRIPTION', 'MATUSETRANS', 'MAXINTMSGTRK', 'MULTIASSETLOCCI', 'PERSON', 'PERSONGROUP', 'PERSONGROUPTEAM', 'PHONE', 'PM', 'RELATEDRECORD', 'ROUTE_STOP', 'ROUTES', 'SERVRECTRANS', 'SR', 'SWCBUSSTATUS', 'SWCHIERARCHY', 'SWCLGA', 'SWCPROBLEMTYPE', 'SWCWOEXT', 'SYNONYMDOMAIN', 'TICKET', 'TOOLTRANS', 'WOACTIVITY', 'WOANCESTOR', 'WORKLOG', 'WORKORDER', 'WORKORDERSPEC', 'WOSTATUS')
    """)
    
    
    #updating ExtendedProperties 
    ExecuteStatement("""
    update dbo.extractLoadManifest set
    ExtendedProperties = NULL
    where systemCode in ('maximo')
    and SourceTableName in ('PERSONGROUP', 'PERSONGROUPTEAM', 'PM', 'RELATEDRECORD', 'WOACTIVITY', 'WORKORDER')
    """)

    # ------------- ShowConfig ----------------- #
    ShowConfig()
    

ConfigureManifest(df)       
