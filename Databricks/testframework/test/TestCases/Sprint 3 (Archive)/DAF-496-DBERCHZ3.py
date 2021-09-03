# Databricks notebook source
# DBTITLE 1,[Config] Connection Setup
storage_account_name = "saswcnonprod01landingtst"
storage_account_access_key = dbutils.secrets.get(scope="Test-Access",key="test-blob-key")
container_name = "archive"
file_location = "wasbs://archive@saswcnonprod01landingtst.blob.core.windows.net/archive/sapisu/dberchz3/json/year=2021/month=08/day=05/DBERCHZ3_20210805103817.json"
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
lakedf = spark.sql("select * from raw.sapisu_dberchz3")

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
# MAGIC select * from raw.sapisu_dberchz3

# COMMAND ----------

# DBTITLE 1,[Verification] Record Count Check
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from raw.sapisu_dberchz3
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from Source

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Source and Target Data
# MAGIC %sql
# MAGIC select
# MAGIC APERIODIC                    
# MAGIC ,AUFNR                        
# MAGIC ,BELNR                        
# MAGIC ,BELZEILE                     
# MAGIC ,BISZONE                      
# MAGIC ,BRUTTOZEILE                  
# MAGIC ,BUPLA                        
# MAGIC ,COLOGRP_INST_L               
# MAGIC ,ERMWSKZ                      
# MAGIC ,EXTRACT_DATETIME             
# MAGIC ,EXTRACT_RUN_ID               
# MAGIC ,GROSSGROUP                   
# MAGIC ,GSBER                        
# MAGIC ,KOSTL                        
# MAGIC ,LINE_CLASS                   
# MAGIC ,MANDT                        
# MAGIC ,MNGBASIS                     
# MAGIC ,MWSKZ                        
# MAGIC ,NETTOBTR                     
# MAGIC ,N_NETTOBTR_L                 
# MAGIC ,OPMULT                       
# MAGIC ,PAOBJNR                      
# MAGIC ,PAOBJNR_S                    
# MAGIC ,PRCTR                        
# MAGIC ,PREIADD                      
# MAGIC ,PREIFAKT                     
# MAGIC ,PREIGKL                      
# MAGIC ,PREIS                        
# MAGIC ,PREISART                     
# MAGIC ,PREISBTR                     
# MAGIC ,PREISTUF                     
# MAGIC ,PREISTYP                     
# MAGIC ,PREISZUS                     
# MAGIC ,PS_PSP_PNR                   
# MAGIC ,SEGMENT                      
# MAGIC ,TWAERS                       
# MAGIC ,TXDAT_KK                     
# MAGIC ,URPREIS                      
# MAGIC ,VONZONE                      
# MAGIC ,V_NETTOBTR_L                 
# MAGIC ,ZONENNR                      
# MAGIC  from Source
# MAGIC except
# MAGIC select 
# MAGIC APERIODIC                    
# MAGIC ,AUFNR                        
# MAGIC ,BELNR                        
# MAGIC ,BELZEILE                     
# MAGIC ,BISZONE                      
# MAGIC ,BRUTTOZEILE                  
# MAGIC ,BUPLA                        
# MAGIC ,COLOGRP_INST_L               
# MAGIC ,ERMWSKZ                      
# MAGIC ,EXTRACT_DATETIME             
# MAGIC ,EXTRACT_RUN_ID               
# MAGIC ,GROSSGROUP                   
# MAGIC ,GSBER                        
# MAGIC ,KOSTL                        
# MAGIC ,LINE_CLASS                   
# MAGIC ,MANDT                        
# MAGIC ,MNGBASIS                     
# MAGIC ,MWSKZ                        
# MAGIC ,NETTOBTR                     
# MAGIC ,N_NETTOBTR_L                 
# MAGIC ,OPMULT                       
# MAGIC ,PAOBJNR                      
# MAGIC ,PAOBJNR_S                    
# MAGIC ,PRCTR                        
# MAGIC ,PREIADD                      
# MAGIC ,PREIFAKT                     
# MAGIC ,PREIGKL                      
# MAGIC ,PREIS                        
# MAGIC ,PREISART                     
# MAGIC ,PREISBTR                     
# MAGIC ,PREISTUF                     
# MAGIC ,PREISTYP                     
# MAGIC ,PREISZUS                     
# MAGIC ,PS_PSP_PNR                   
# MAGIC ,SEGMENT                      
# MAGIC ,TWAERS                       
# MAGIC ,TXDAT_KK                     
# MAGIC ,URPREIS                      
# MAGIC ,VONZONE                      
# MAGIC ,V_NETTOBTR_L                 
# MAGIC ,ZONENNR                      
# MAGIC from raw.sapisu_dberchz3

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Target and Source Data
# MAGIC %sql
# MAGIC select 
# MAGIC APERIODIC                    
# MAGIC ,AUFNR                        
# MAGIC ,BELNR                        
# MAGIC ,BELZEILE                     
# MAGIC ,BISZONE                      
# MAGIC ,BRUTTOZEILE                  
# MAGIC ,BUPLA                        
# MAGIC ,COLOGRP_INST_L               
# MAGIC ,ERMWSKZ                      
# MAGIC ,EXTRACT_DATETIME             
# MAGIC ,EXTRACT_RUN_ID               
# MAGIC ,GROSSGROUP                   
# MAGIC ,GSBER                        
# MAGIC ,KOSTL                        
# MAGIC ,LINE_CLASS                   
# MAGIC ,MANDT                        
# MAGIC ,MNGBASIS                     
# MAGIC ,MWSKZ                        
# MAGIC ,NETTOBTR                     
# MAGIC ,N_NETTOBTR_L                 
# MAGIC ,OPMULT                       
# MAGIC ,PAOBJNR                      
# MAGIC ,PAOBJNR_S                    
# MAGIC ,PRCTR                        
# MAGIC ,PREIADD                      
# MAGIC ,PREIFAKT                     
# MAGIC ,PREIGKL                      
# MAGIC ,PREIS                        
# MAGIC ,PREISART                     
# MAGIC ,PREISBTR                     
# MAGIC ,PREISTUF                     
# MAGIC ,PREISTYP                     
# MAGIC ,PREISZUS                     
# MAGIC ,PS_PSP_PNR                   
# MAGIC ,SEGMENT                      
# MAGIC ,TWAERS                       
# MAGIC ,TXDAT_KK                     
# MAGIC ,URPREIS                      
# MAGIC ,VONZONE                      
# MAGIC ,V_NETTOBTR_L                 
# MAGIC ,ZONENNR                      
# MAGIC from raw.sapisu_dberchz3
# MAGIC except
# MAGIC select
# MAGIC APERIODIC                    
# MAGIC ,AUFNR                        
# MAGIC ,BELNR                        
# MAGIC ,BELZEILE                     
# MAGIC ,BISZONE                      
# MAGIC ,BRUTTOZEILE                  
# MAGIC ,BUPLA                        
# MAGIC ,COLOGRP_INST_L               
# MAGIC ,ERMWSKZ                      
# MAGIC ,EXTRACT_DATETIME             
# MAGIC ,EXTRACT_RUN_ID               
# MAGIC ,GROSSGROUP                   
# MAGIC ,GSBER                        
# MAGIC ,KOSTL                        
# MAGIC ,LINE_CLASS                   
# MAGIC ,MANDT                        
# MAGIC ,MNGBASIS                     
# MAGIC ,MWSKZ                        
# MAGIC ,NETTOBTR                     
# MAGIC ,N_NETTOBTR_L                 
# MAGIC ,OPMULT                       
# MAGIC ,PAOBJNR                      
# MAGIC ,PAOBJNR_S                    
# MAGIC ,PRCTR                        
# MAGIC ,PREIADD                      
# MAGIC ,PREIFAKT                     
# MAGIC ,PREIGKL                      
# MAGIC ,PREIS                        
# MAGIC ,PREISART                     
# MAGIC ,PREISBTR                     
# MAGIC ,PREISTUF                     
# MAGIC ,PREISTYP                     
# MAGIC ,PREISZUS                     
# MAGIC ,PS_PSP_PNR                   
# MAGIC ,SEGMENT                      
# MAGIC ,TWAERS                       
# MAGIC ,TXDAT_KK                     
# MAGIC ,URPREIS                      
# MAGIC ,VONZONE                      
# MAGIC ,V_NETTOBTR_L                 
# MAGIC ,ZONENNR                      
# MAGIC  from Source
