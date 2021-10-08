# Databricks notebook source
# DBTITLE 1,[Config] Connection Setup
storage_account_name = "sadaftest01"
storage_account_access_key = dbutils.secrets.get(scope="Test-Access",key="test-datalake-key")
container_name = "raw"
file_location = "wasbs://raw@sadaftest01.blob.core.windows.net/isu/0bpartner_attr/json/year=2021/month=10/day=06/0BPARTNER_ATTR_2021-10-06_122010_303.json.gz"
file_type = "json"
print(storage_account_name)

# COMMAND ----------

spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
  storage_account_access_key)


# COMMAND ----------

# DBTITLE 1,[Source] Loading data into Dataframe
df = spark.read.format(file_type).option("inferSchema", "true").load(file_location)
#df2 = spark.read.format(file_type).option("inferSchema", "true").load(file_location2)

# COMMAND ----------

# DBTITLE 1,[Source] Schema Check - Refer to Raw2Cleansed Mapping
df.printSchema()
#df2.printSchema()

# COMMAND ----------

# DBTITLE 0,[Result] Load Count Result into DataFrame
lakedf = spark.sql("select * from raw.isu_0bpartner_attr")

# COMMAND ----------

display(lakedf)

# COMMAND ----------

# DBTITLE 1,[Target] Schema Check
lakedf.printSchema()

# COMMAND ----------

# DBTITLE 1,[Source] Creating Temporary Table
df.createOrReplaceTempView("Source")
#df2.createOrReplaceTempView("Source22")

# COMMAND ----------

df = spark.sql("select * from Source")
#df2 = spark.sql("select * from Source22")


# COMMAND ----------

lakedf.createOrReplaceTempView("Target")

# COMMAND ----------

# DBTITLE 1,[Validation] Source to Target
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
# MAGIC from raw.isu_0bpartner_attr where _FileDateTimeStamp = '20211005140203'

# COMMAND ----------

# DBTITLE 1,[Validation] Target to Source
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
# MAGIC from raw.isu_0bpartner_attr where _FileDateTimeStamp = '20211005140203'
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

# COMMAND ----------

# DBTITLE 1,[Verification] Count Checks
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from raw.isu_0bpartner_attr where _FileDateTimeStamp = '20211005140203'
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from (select
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
# MAGIC  from Source  )--and b.SPRAS ='E' 
