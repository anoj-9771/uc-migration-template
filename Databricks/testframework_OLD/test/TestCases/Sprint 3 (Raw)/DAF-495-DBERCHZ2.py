# Databricks notebook source
# DBTITLE 1,[Config] Connection Setup
storage_account_name = "saswcnonprod01landingtst"
storage_account_access_key = dbutils.secrets.get(scope="Test-Access",key="test-blob-key")
container_name = "archive"
file_location = "wasbs://archive@saswcnonprod01landingtst.blob.core.windows.net/archive/sapisu/dberchz2/json/year=2021/month=08/day=05/DBERCHZ2_20210805103817.json"
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
lakedf = spark.sql("select * from raw.sapisu_dberchz2")

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

# MAGIC %sql
# MAGIC select * from raw.sapisu_dberchz2

# COMMAND ----------

# DBTITLE 1,[Verification] Record Count Check
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from raw.sapisu_dberchz2
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from Source

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Source and Target Data
# MAGIC %sql
# MAGIC select
# MAGIC ABLBELNR
# MAGIC ,ABLESGR
# MAGIC ,ABLESGRV
# MAGIC ,ABLHINW
# MAGIC ,ADATMAX                        
# MAGIC ,ATIM                           
# MAGIC ,ATIMMAX                        
# MAGIC ,ATIMVA                         
# MAGIC ,BEGPROG                        
# MAGIC ,BELNR                          
# MAGIC ,BELZEILE                       
# MAGIC ,COLOGRP_INST_L                 
# MAGIC ,ENDEPROG                       
# MAGIC ,EQUNR                          
# MAGIC ,EXTPKZ                         
# MAGIC ,EXTRACT_DATETIME               
# MAGIC ,EXTRACT_RUN_ID                 
# MAGIC ,GERAET                         
# MAGIC ,INDEXNR                        
# MAGIC ,ISTABLART                      
# MAGIC ,ISTABLARTVA                    
# MAGIC ,LOGIKNR                        
# MAGIC ,LOGIKZW                        
# MAGIC ,MANDT                          
# MAGIC ,MATNR                          
# MAGIC ,MRCONNECT                      
# MAGIC ,N_ZWSTAND                      
# MAGIC ,N_ZWSTDIFF                     
# MAGIC ,N_ZWSTNDAB                     
# MAGIC ,N_ZWSTVOR                      
# MAGIC ,QDPROC                         
# MAGIC ,REGRELSORT                     
# MAGIC ,THGDATUM                       
# MAGIC ,V_ZWSTAND                      
# MAGIC ,V_ZWSTDIFF                     
# MAGIC ,V_ZWSTNDAB                     
# MAGIC ,V_ZWSTVOR                      
# MAGIC ,ZUORDDAT                       
# MAGIC ,ZWNUMMER
# MAGIC  from Source
# MAGIC except
# MAGIC select 
# MAGIC ABLBELNR
# MAGIC ,ABLESGR
# MAGIC ,ABLESGRV
# MAGIC ,ABLHINW
# MAGIC ,ADATMAX                        
# MAGIC ,ATIM                           
# MAGIC ,ATIMMAX                        
# MAGIC ,ATIMVA                         
# MAGIC ,BEGPROG                        
# MAGIC ,BELNR                          
# MAGIC ,BELZEILE                       
# MAGIC ,COLOGRP_INST_L                 
# MAGIC ,ENDEPROG                       
# MAGIC ,EQUNR                          
# MAGIC ,EXTPKZ                         
# MAGIC ,EXTRACT_DATETIME               
# MAGIC ,EXTRACT_RUN_ID                 
# MAGIC ,GERAET                         
# MAGIC ,INDEXNR                        
# MAGIC ,ISTABLART                      
# MAGIC ,ISTABLARTVA                    
# MAGIC ,LOGIKNR                        
# MAGIC ,LOGIKZW                        
# MAGIC ,MANDT                          
# MAGIC ,MATNR                          
# MAGIC ,MRCONNECT                      
# MAGIC ,N_ZWSTAND                      
# MAGIC ,N_ZWSTDIFF                     
# MAGIC ,N_ZWSTNDAB                     
# MAGIC ,N_ZWSTVOR                      
# MAGIC ,QDPROC                         
# MAGIC ,REGRELSORT                     
# MAGIC ,THGDATUM                       
# MAGIC ,V_ZWSTAND                      
# MAGIC ,V_ZWSTDIFF                     
# MAGIC ,V_ZWSTNDAB                     
# MAGIC ,V_ZWSTVOR                      
# MAGIC ,ZUORDDAT                       
# MAGIC ,ZWNUMMER
# MAGIC from raw.sapisu_dberchz2

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Target and Source Data
# MAGIC %sql
# MAGIC select 
# MAGIC ABLBELNR
# MAGIC ,ABLESGR
# MAGIC ,ABLESGRV
# MAGIC ,ABLHINW
# MAGIC ,ADATMAX                        
# MAGIC ,ATIM                           
# MAGIC ,ATIMMAX                        
# MAGIC ,ATIMVA                         
# MAGIC ,BEGPROG                        
# MAGIC ,BELNR                          
# MAGIC ,BELZEILE                       
# MAGIC ,COLOGRP_INST_L                 
# MAGIC ,ENDEPROG                       
# MAGIC ,EQUNR                          
# MAGIC ,EXTPKZ                         
# MAGIC ,EXTRACT_DATETIME               
# MAGIC ,EXTRACT_RUN_ID                 
# MAGIC ,GERAET                         
# MAGIC ,INDEXNR                        
# MAGIC ,ISTABLART                      
# MAGIC ,ISTABLARTVA                    
# MAGIC ,LOGIKNR                        
# MAGIC ,LOGIKZW                        
# MAGIC ,MANDT                          
# MAGIC ,MATNR                          
# MAGIC ,MRCONNECT                      
# MAGIC ,N_ZWSTAND                      
# MAGIC ,N_ZWSTDIFF                     
# MAGIC ,N_ZWSTNDAB                     
# MAGIC ,N_ZWSTVOR                      
# MAGIC ,QDPROC                         
# MAGIC ,REGRELSORT                     
# MAGIC ,THGDATUM                       
# MAGIC ,V_ZWSTAND                      
# MAGIC ,V_ZWSTDIFF                     
# MAGIC ,V_ZWSTNDAB                     
# MAGIC ,V_ZWSTVOR                      
# MAGIC ,ZUORDDAT                       
# MAGIC ,ZWNUMMER
# MAGIC from raw.sapisu_dberchz2
# MAGIC 
# MAGIC EXCEPT
# MAGIC 
# MAGIC select
# MAGIC ABLBELNR
# MAGIC ,ABLESGR
# MAGIC ,ABLESGRV
# MAGIC ,ABLHINW
# MAGIC ,ADATMAX                        
# MAGIC ,ATIM                           
# MAGIC ,ATIMMAX                        
# MAGIC ,ATIMVA                         
# MAGIC ,BEGPROG                        
# MAGIC ,BELNR                          
# MAGIC ,BELZEILE                       
# MAGIC ,COLOGRP_INST_L                 
# MAGIC ,ENDEPROG                       
# MAGIC ,EQUNR                          
# MAGIC ,EXTPKZ                         
# MAGIC ,EXTRACT_DATETIME               
# MAGIC ,EXTRACT_RUN_ID                 
# MAGIC ,GERAET                         
# MAGIC ,INDEXNR                        
# MAGIC ,ISTABLART                      
# MAGIC ,ISTABLARTVA                    
# MAGIC ,LOGIKNR                        
# MAGIC ,LOGIKZW                        
# MAGIC ,MANDT                          
# MAGIC ,MATNR                          
# MAGIC ,MRCONNECT                      
# MAGIC ,N_ZWSTAND                      
# MAGIC ,N_ZWSTDIFF                     
# MAGIC ,N_ZWSTNDAB                     
# MAGIC ,N_ZWSTVOR                      
# MAGIC ,QDPROC                         
# MAGIC ,REGRELSORT                     
# MAGIC ,THGDATUM                       
# MAGIC ,V_ZWSTAND                      
# MAGIC ,V_ZWSTDIFF                     
# MAGIC ,V_ZWSTNDAB                     
# MAGIC ,V_ZWSTVOR                      
# MAGIC ,ZUORDDAT                       
# MAGIC ,ZWNUMMER
# MAGIC  from Source
