# Databricks notebook source
# DBTITLE 1,[Config] Connection Setup
storage_account_name = "saswcnonprod01landingtst"
storage_account_access_key = dbutils.secrets.get(scope="Test-Access",key="test-blob-key")
container_name = "archive"
file_location = "wasbs://archive@saswcnonprod01landingtst.blob.core.windows.net/archive/sapisu/0bpartner_attr/json/year=2021/month=08/day=05/0BPARTNER_ATTR_20210804141617.json"
file_type = "json"
print(storage_account_name)

# COMMAND ----------

spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
  storage_account_access_key)


# COMMAND ----------

# DBTITLE 1,[Source] Loading data into Dataframe
df = spark.read.format(file_type).option("inferSchema", "true").load(file_location)

# COMMAND ----------

# DBTITLE 1,[Source] Schema Check
df.printSchema()

# COMMAND ----------

# DBTITLE 0,[Result] Load Count Result into DataFrame
lakedf = spark.sql("select * from raw.sapisu_0bpartner_attr")

# COMMAND ----------

# DBTITLE 1,[Target] Schema Check
lakedf.printSchema()

# COMMAND ----------

# DBTITLE 1,[Source] Creating Temporary Table
df.createOrReplaceTempView("Source")

# COMMAND ----------

# DBTITLE 1,[Source] Displaying Records
# MAGIC %sql
# MAGIC select * from Source

# COMMAND ----------

# DBTITLE 1,[Target] Displaying Records
# MAGIC %sql
# MAGIC select * from raw.sapisu_0bpartner_attr

# COMMAND ----------

# DBTITLE 1,[Verification] Record Count Check
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from raw.sapisu_0bpartner_attr
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from Source

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Source and Target Data
# MAGIC %sql
# MAGIC select
# MAGIC ADDRCOMM                          
# MAGIC ,BIRTHDT                          
# MAGIC ,BIRTHPL                          
# MAGIC ,BPEXT                            
# MAGIC ,BPKIND                           
# MAGIC ,BU_GROUP                         
# MAGIC ,BU_LANGU                         
# MAGIC ,BU_SORT1                         
# MAGIC ,BU_SORT2                         
# MAGIC ,CHDAT                            
# MAGIC ,CHILDREN                         
# MAGIC ,CHTIM                            
# MAGIC ,CHUSR                            
# MAGIC ,CNDSC                            
# MAGIC ,CNTAX                            
# MAGIC ,CONTACT                          
# MAGIC ,CRDAT                            
# MAGIC ,CRTIM                            
# MAGIC ,CRUSR                            
# MAGIC ,DEATHDT                          
# MAGIC ,DI_OPERATION_TYPE                
# MAGIC ,DI_SEQUENCE_NUMBER               
# MAGIC ,EMPLO                            
# MAGIC ,EXTRACT_DATETIME                 
# MAGIC ,EXTRACT_RUN_ID                   
# MAGIC ,FOUND_DAT                        
# MAGIC ,IND_SECTOR                       
# MAGIC ,INITIALS                         
# MAGIC ,JOBGR                            
# MAGIC ,LANGU_CORR                       
# MAGIC ,LEGAL_ENTY                       
# MAGIC ,LEGAL_ORG                        
# MAGIC ,LIQUID_DAT                       
# MAGIC ,LOCATION_1                       
# MAGIC ,LOCATION_2                       
# MAGIC ,LOCATION_3                       
# MAGIC ,MARST                            
# MAGIC ,MC_NAME1                         
# MAGIC ,MC_NAME2                         
# MAGIC ,MEM_HOUSE                        
# MAGIC ,NAMCOUNTRY                       
# MAGIC ,NAME1_TEXT                       
# MAGIC ,NAMEFORMAT                       
# MAGIC ,NAMEMIDDLE                       
# MAGIC ,NAME_FIRST                       
# MAGIC ,NAME_GRP1                        
# MAGIC ,NAME_GRP2                        
# MAGIC ,NAME_LAST                        
# MAGIC ,NAME_LAST2                       
# MAGIC ,NAME_LST2                        
# MAGIC ,NAME_ORG1                        
# MAGIC ,NAME_ORG2                        
# MAGIC ,NAME_ORG3                        
# MAGIC ,NAME_ORG4                        
# MAGIC ,NATIO                            
# MAGIC ,NATPERS                          
# MAGIC ,NICKNAME                         
# MAGIC ,ODQ_CHANGEMODE                   
# MAGIC ,ODQ_ENTITYCNTR                   
# MAGIC ,PARTGRPTYP                       
# MAGIC ,PARTNER                          
# MAGIC ,PARTNER_GUID                     
# MAGIC ,PERNO                            
# MAGIC ,PERSNUMBER                       
# MAGIC ,PREFIX1                          
# MAGIC ,PREFIX2                          
# MAGIC ,PRINT_MODE                       
# MAGIC ,SOURCE                           
# MAGIC ,TD_SWITCH                        
# MAGIC ,TITLE                            
# MAGIC ,TITLE_ACA1                       
# MAGIC ,TITLE_ACA2                       
# MAGIC ,TITLE_LET                        
# MAGIC ,TITLE_ROYL                       
# MAGIC ,TYPE                             
# MAGIC ,VALID_FROM                       
# MAGIC ,VALID_TO                         
# MAGIC ,XBLCK                            
# MAGIC ,XDELE                            
# MAGIC ,XSEXF                            
# MAGIC ,XSEXM                            
# MAGIC ,XSEXU                            
# MAGIC ,XUBNAME                          
# MAGIC ,ZZAFLD00001Z                     
# MAGIC ,ZZBA_INDICATOR                   
# MAGIC ,ZZPAS_INDICATOR                  
# MAGIC ,ZZUSER                                                            
# MAGIC  from Source
# MAGIC except
# MAGIC select 
# MAGIC ADDRCOMM                          
# MAGIC ,BIRTHDT                          
# MAGIC ,BIRTHPL                          
# MAGIC ,BPEXT                            
# MAGIC ,BPKIND                           
# MAGIC ,BU_GROUP                         
# MAGIC ,BU_LANGU                         
# MAGIC ,BU_SORT1                         
# MAGIC ,BU_SORT2                         
# MAGIC ,CHDAT                            
# MAGIC ,CHILDREN                         
# MAGIC ,CHTIM                            
# MAGIC ,CHUSR                            
# MAGIC ,CNDSC                            
# MAGIC ,CNTAX                            
# MAGIC ,CONTACT                          
# MAGIC ,CRDAT                            
# MAGIC ,CRTIM                            
# MAGIC ,CRUSR                            
# MAGIC ,DEATHDT                          
# MAGIC ,DI_OPERATION_TYPE                
# MAGIC ,DI_SEQUENCE_NUMBER               
# MAGIC ,EMPLO                            
# MAGIC ,EXTRACT_DATETIME                 
# MAGIC ,EXTRACT_RUN_ID                   
# MAGIC ,FOUND_DAT                        
# MAGIC ,IND_SECTOR                       
# MAGIC ,INITIALS                         
# MAGIC ,JOBGR                            
# MAGIC ,LANGU_CORR                       
# MAGIC ,LEGAL_ENTY                       
# MAGIC ,LEGAL_ORG                        
# MAGIC ,LIQUID_DAT                       
# MAGIC ,LOCATION_1                       
# MAGIC ,LOCATION_2                       
# MAGIC ,LOCATION_3                       
# MAGIC ,MARST                            
# MAGIC ,MC_NAME1                         
# MAGIC ,MC_NAME2                         
# MAGIC ,MEM_HOUSE                        
# MAGIC ,NAMCOUNTRY                       
# MAGIC ,NAME1_TEXT                       
# MAGIC ,NAMEFORMAT                       
# MAGIC ,NAMEMIDDLE                       
# MAGIC ,NAME_FIRST                       
# MAGIC ,NAME_GRP1                        
# MAGIC ,NAME_GRP2                        
# MAGIC ,NAME_LAST                        
# MAGIC ,NAME_LAST2                       
# MAGIC ,NAME_LST2                        
# MAGIC ,NAME_ORG1                        
# MAGIC ,NAME_ORG2                        
# MAGIC ,NAME_ORG3                        
# MAGIC ,NAME_ORG4                        
# MAGIC ,NATIO                            
# MAGIC ,NATPERS                          
# MAGIC ,NICKNAME                         
# MAGIC ,ODQ_CHANGEMODE                   
# MAGIC ,ODQ_ENTITYCNTR                   
# MAGIC ,PARTGRPTYP                       
# MAGIC ,PARTNER                          
# MAGIC ,PARTNER_GUID                     
# MAGIC ,PERNO                            
# MAGIC ,PERSNUMBER                       
# MAGIC ,PREFIX1                          
# MAGIC ,PREFIX2                          
# MAGIC ,PRINT_MODE                       
# MAGIC ,SOURCE                           
# MAGIC ,TD_SWITCH                        
# MAGIC ,TITLE                            
# MAGIC ,TITLE_ACA1                       
# MAGIC ,TITLE_ACA2                       
# MAGIC ,TITLE_LET                        
# MAGIC ,TITLE_ROYL                       
# MAGIC ,TYPE                             
# MAGIC ,VALID_FROM                       
# MAGIC ,VALID_TO                         
# MAGIC ,XBLCK                            
# MAGIC ,XDELE                            
# MAGIC ,XSEXF                            
# MAGIC ,XSEXM                            
# MAGIC ,XSEXU                            
# MAGIC ,XUBNAME                          
# MAGIC ,ZZAFLD00001Z                     
# MAGIC ,ZZBA_INDICATOR                   
# MAGIC ,ZZPAS_INDICATOR                  
# MAGIC ,ZZUSER                                                 
# MAGIC from raw.sapisu_0bpartner_attr

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Target and Source Data
# MAGIC %sql
# MAGIC select 
# MAGIC ADDRCOMM                          
# MAGIC ,BIRTHDT                          
# MAGIC ,BIRTHPL                          
# MAGIC ,BPEXT                            
# MAGIC ,BPKIND                           
# MAGIC ,BU_GROUP                         
# MAGIC ,BU_LANGU                         
# MAGIC ,BU_SORT1                         
# MAGIC ,BU_SORT2                         
# MAGIC ,CHDAT                            
# MAGIC ,CHILDREN                         
# MAGIC ,CHTIM                            
# MAGIC ,CHUSR                            
# MAGIC ,CNDSC                            
# MAGIC ,CNTAX                            
# MAGIC ,CONTACT                          
# MAGIC ,CRDAT                            
# MAGIC ,CRTIM                            
# MAGIC ,CRUSR                            
# MAGIC ,DEATHDT                          
# MAGIC ,DI_OPERATION_TYPE                
# MAGIC ,DI_SEQUENCE_NUMBER               
# MAGIC ,EMPLO                            
# MAGIC ,EXTRACT_DATETIME                 
# MAGIC ,EXTRACT_RUN_ID                   
# MAGIC ,FOUND_DAT                        
# MAGIC ,IND_SECTOR                       
# MAGIC ,INITIALS                         
# MAGIC ,JOBGR                            
# MAGIC ,LANGU_CORR                       
# MAGIC ,LEGAL_ENTY                       
# MAGIC ,LEGAL_ORG                        
# MAGIC ,LIQUID_DAT                       
# MAGIC ,LOCATION_1                       
# MAGIC ,LOCATION_2                       
# MAGIC ,LOCATION_3                       
# MAGIC ,MARST                            
# MAGIC ,MC_NAME1                         
# MAGIC ,MC_NAME2                         
# MAGIC ,MEM_HOUSE                        
# MAGIC ,NAMCOUNTRY                       
# MAGIC ,NAME1_TEXT                       
# MAGIC ,NAMEFORMAT                       
# MAGIC ,NAMEMIDDLE                       
# MAGIC ,NAME_FIRST                       
# MAGIC ,NAME_GRP1                        
# MAGIC ,NAME_GRP2                        
# MAGIC ,NAME_LAST                        
# MAGIC ,NAME_LAST2                       
# MAGIC ,NAME_LST2                        
# MAGIC ,NAME_ORG1                        
# MAGIC ,NAME_ORG2                        
# MAGIC ,NAME_ORG3                        
# MAGIC ,NAME_ORG4                        
# MAGIC ,NATIO                            
# MAGIC ,NATPERS                          
# MAGIC ,NICKNAME                         
# MAGIC ,ODQ_CHANGEMODE                   
# MAGIC ,ODQ_ENTITYCNTR                   
# MAGIC ,PARTGRPTYP                       
# MAGIC ,PARTNER                          
# MAGIC ,PARTNER_GUID                     
# MAGIC ,PERNO                            
# MAGIC ,PERSNUMBER                       
# MAGIC ,PREFIX1                          
# MAGIC ,PREFIX2                          
# MAGIC ,PRINT_MODE                       
# MAGIC ,SOURCE                           
# MAGIC ,TD_SWITCH                        
# MAGIC ,TITLE                            
# MAGIC ,TITLE_ACA1                       
# MAGIC ,TITLE_ACA2                       
# MAGIC ,TITLE_LET                        
# MAGIC ,TITLE_ROYL                       
# MAGIC ,TYPE                             
# MAGIC ,VALID_FROM                       
# MAGIC ,VALID_TO                         
# MAGIC ,XBLCK                            
# MAGIC ,XDELE                            
# MAGIC ,XSEXF                            
# MAGIC ,XSEXM                            
# MAGIC ,XSEXU                            
# MAGIC ,XUBNAME                          
# MAGIC ,ZZAFLD00001Z                     
# MAGIC ,ZZBA_INDICATOR                   
# MAGIC ,ZZPAS_INDICATOR                  
# MAGIC ,ZZUSER                                                 
# MAGIC from raw.sapisu_0bpartner_attr
# MAGIC except
# MAGIC select
# MAGIC ADDRCOMM                          
# MAGIC ,BIRTHDT                          
# MAGIC ,BIRTHPL                          
# MAGIC ,BPEXT                            
# MAGIC ,BPKIND                           
# MAGIC ,BU_GROUP                         
# MAGIC ,BU_LANGU                         
# MAGIC ,BU_SORT1                         
# MAGIC ,BU_SORT2                         
# MAGIC ,CHDAT                            
# MAGIC ,CHILDREN                         
# MAGIC ,CHTIM                            
# MAGIC ,CHUSR                            
# MAGIC ,CNDSC                            
# MAGIC ,CNTAX                            
# MAGIC ,CONTACT                          
# MAGIC ,CRDAT                            
# MAGIC ,CRTIM                            
# MAGIC ,CRUSR                            
# MAGIC ,DEATHDT                          
# MAGIC ,DI_OPERATION_TYPE                
# MAGIC ,DI_SEQUENCE_NUMBER               
# MAGIC ,EMPLO                            
# MAGIC ,EXTRACT_DATETIME                 
# MAGIC ,EXTRACT_RUN_ID                   
# MAGIC ,FOUND_DAT                        
# MAGIC ,IND_SECTOR                       
# MAGIC ,INITIALS                         
# MAGIC ,JOBGR                            
# MAGIC ,LANGU_CORR                       
# MAGIC ,LEGAL_ENTY                       
# MAGIC ,LEGAL_ORG                        
# MAGIC ,LIQUID_DAT                       
# MAGIC ,LOCATION_1                       
# MAGIC ,LOCATION_2                       
# MAGIC ,LOCATION_3                       
# MAGIC ,MARST                            
# MAGIC ,MC_NAME1                         
# MAGIC ,MC_NAME2                         
# MAGIC ,MEM_HOUSE                        
# MAGIC ,NAMCOUNTRY                       
# MAGIC ,NAME1_TEXT                       
# MAGIC ,NAMEFORMAT                       
# MAGIC ,NAMEMIDDLE                       
# MAGIC ,NAME_FIRST                       
# MAGIC ,NAME_GRP1                        
# MAGIC ,NAME_GRP2                        
# MAGIC ,NAME_LAST                        
# MAGIC ,NAME_LAST2                       
# MAGIC ,NAME_LST2                        
# MAGIC ,NAME_ORG1                        
# MAGIC ,NAME_ORG2                        
# MAGIC ,NAME_ORG3                        
# MAGIC ,NAME_ORG4                        
# MAGIC ,NATIO                            
# MAGIC ,NATPERS                          
# MAGIC ,NICKNAME                         
# MAGIC ,ODQ_CHANGEMODE                   
# MAGIC ,ODQ_ENTITYCNTR                   
# MAGIC ,PARTGRPTYP                       
# MAGIC ,PARTNER                          
# MAGIC ,PARTNER_GUID                     
# MAGIC ,PERNO                            
# MAGIC ,PERSNUMBER                       
# MAGIC ,PREFIX1                          
# MAGIC ,PREFIX2                          
# MAGIC ,PRINT_MODE                       
# MAGIC ,SOURCE                           
# MAGIC ,TD_SWITCH                        
# MAGIC ,TITLE                            
# MAGIC ,TITLE_ACA1                       
# MAGIC ,TITLE_ACA2                       
# MAGIC ,TITLE_LET                        
# MAGIC ,TITLE_ROYL                       
# MAGIC ,TYPE                             
# MAGIC ,VALID_FROM                       
# MAGIC ,VALID_TO                         
# MAGIC ,XBLCK                            
# MAGIC ,XDELE                            
# MAGIC ,XSEXF                            
# MAGIC ,XSEXM                            
# MAGIC ,XSEXU                            
# MAGIC ,XUBNAME                          
# MAGIC ,ZZAFLD00001Z                     
# MAGIC ,ZZBA_INDICATOR                   
# MAGIC ,ZZPAS_INDICATOR                  
# MAGIC ,ZZUSER                                                            
# MAGIC  from Source
