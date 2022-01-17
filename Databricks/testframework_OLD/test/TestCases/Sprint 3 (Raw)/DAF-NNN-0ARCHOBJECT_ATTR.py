# Databricks notebook source
# DBTITLE 1,[Config] Connection Setup
storage_account_name = "saswcnonprod01landingtst"
storage_account_access_key = dbutils.secrets.get(scope="Test-Access",key="test-blob-key")
container_name = "archive"
file_location = "wasbs://archive@saswcnonprod01landingtst.blob.core.windows.net/archive/sapisu/0archobject_attr/json/year=2021/month=08/day=05/0ARCHOBJECT_ATTR_20210805103817.json"
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

# DBTITLE 1,[Source] Creating Temporary Table
df.createOrReplaceTempView("Source")

# COMMAND ----------

# DBTITLE 1,[Source] Displaying Records
# MAGIC %sql
# MAGIC select * from Source

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from raw.sapisu_0archobject_attr

# COMMAND ----------

# DBTITLE 0,[Result] Load Count Result into DataFrame
lakedf = spark.sql("select * from raw.sapisu_0archobject_attr")

# COMMAND ----------

# DBTITLE 1,[Target] Schema Check
lakedf.printSchema()

# COMMAND ----------

lakedf.createOrReplaceTempView("Target")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from raw.sapisu_0archobject_attr
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from Source

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Source and Target Data
# MAGIC %sql
# MAGIC select 
# MAGIC   * from Source
# MAGIC except
# MAGIC select  
# MAGIC   AOFUNCTION ,
# MAGIC   AOID ,
# MAGIC   AONR ,
# MAGIC   AOTYPE ,
# MAGIC   AUTHGRP ,
# MAGIC   BWSTRES1 ,
# MAGIC   BWSTRES2 ,
# MAGIC   BWSTREU1 ,
# MAGIC   BWSTREU2 ,
# MAGIC   BWSTREU3 ,
# MAGIC   BWSTREU4 ,
# MAGIC   BWSTREU5 ,
# MAGIC   CITY1 ,
# MAGIC   CITY2 ,
# MAGIC   COUNTRY ,
# MAGIC   EXTRACT_DATETIME ,
# MAGIC   EXTRACT_RUN_ID ,
# MAGIC   HOUSE_NUM1 ,
# MAGIC   HOUSE_NUM2 ,
# MAGIC   INTRENO ,
# MAGIC   MSEHI ,
# MAGIC   OBJNR ,
# MAGIC   ODQ_CHANGEMODE ,
# MAGIC   ODQ_ENTITYCNTR ,
# MAGIC   PARENTAOID ,
# MAGIC   PARENTAOTYPE ,
# MAGIC   POST_CODE1 ,
# MAGIC   POST_CODE2 ,
# MAGIC   PO_BOX ,
# MAGIC   REGION ,
# MAGIC   RESPONSIBLE ,
# MAGIC   SINSTBEZ ,
# MAGIC   SLAGEWE ,
# MAGIC   SOBJLAGE ,
# MAGIC   STREET ,
# MAGIC   SVERKEHR ,
# MAGIC   USAGECOMMON ,
# MAGIC   VALIDFROM ,
# MAGIC   VALIDTO ,
# MAGIC   ZCD_ADDR_LOT_NO ,
# MAGIC   ZCD_BAND ,
# MAGIC   ZCD_BLUESCOPE_FLAG ,
# MAGIC   ZCD_CAD_ID ,
# MAGIC   ZCD_CANCELLATION_DATE ,
# MAGIC   ZCD_CANC_REASON ,
# MAGIC   ZCD_CASENO_FLAG ,
# MAGIC   ZCD_COMMENTS ,
# MAGIC   ZCD_CONCESSION_FLAG ,
# MAGIC   ZCD_HYDRA_AREA_FLAG ,
# MAGIC   ZCD_HYDRA_AREA_UNIT ,
# MAGIC   ZCD_HYDRA_CALC_AREA ,
# MAGIC   ZCD_IND_COMMUNITY_TITLE ,
# MAGIC   ZCD_IND_MLIM ,
# MAGIC   ZCD_IND_SOPA ,
# MAGIC   ZCD_IND_TWA ,
# MAGIC   ZCD_IND_WICA ,
# MAGIC   ZCD_INF_PROP_TYPE ,
# MAGIC   ZCD_IS_PARENT ,
# MAGIC   ZCD_LOT_TYPE ,
# MAGIC   ZCD_NO_OF_FLATS ,
# MAGIC   ZCD_OVERRIDE_AREA,
# MAGIC   ZCD_OVERRIDE_AREA_UNIT ,
# MAGIC   ZCD_PARENT_PROP ,
# MAGIC   ZCD_PLAN_NUMBER ,
# MAGIC   ZCD_PLAN_TYPE ,
# MAGIC   ZCD_PROCESS_TYPE ,
# MAGIC   ZCD_PROPERTY_INFO ,
# MAGIC   ZCD_PROPERTY_NO ,
# MAGIC   ZCD_PROP_CN_FLAG ,
# MAGIC   ZCD_PROP_CR_DATE ,
# MAGIC   ZCD_PROP_LOT_NO ,
# MAGIC   ZCD_REQUEST_NO ,
# MAGIC   ZCD_SECTION_NUMBER ,
# MAGIC   ZCD_STORM_WATER_ASSESS ,
# MAGIC   ZCD_SUP_PROP_TYPE ,
# MAGIC   ZCD_UNIT_ENTITLEMENT 
# MAGIC  from raw.sapisu_0archobject_attr

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Target and Source Data
# MAGIC %sql
# MAGIC select  
# MAGIC   AOFUNCTION ,
# MAGIC   AOID ,
# MAGIC   AONR ,
# MAGIC   AOTYPE ,
# MAGIC   AUTHGRP ,
# MAGIC   BWSTRES1 ,
# MAGIC   BWSTRES2 ,
# MAGIC   BWSTREU1 ,
# MAGIC   BWSTREU2 ,
# MAGIC   BWSTREU3 ,
# MAGIC   BWSTREU4 ,
# MAGIC   BWSTREU5 ,
# MAGIC   CITY1 ,
# MAGIC   CITY2 ,
# MAGIC   COUNTRY ,
# MAGIC   EXTRACT_DATETIME ,
# MAGIC   EXTRACT_RUN_ID ,
# MAGIC   HOUSE_NUM1 ,
# MAGIC   HOUSE_NUM2 ,
# MAGIC   INTRENO ,
# MAGIC   MSEHI ,
# MAGIC   OBJNR ,
# MAGIC   ODQ_CHANGEMODE ,
# MAGIC   ODQ_ENTITYCNTR ,
# MAGIC   PARENTAOID ,
# MAGIC   PARENTAOTYPE ,
# MAGIC   POST_CODE1 ,
# MAGIC   POST_CODE2 ,
# MAGIC   PO_BOX ,
# MAGIC   REGION ,
# MAGIC   RESPONSIBLE ,
# MAGIC   SINSTBEZ ,
# MAGIC   SLAGEWE ,
# MAGIC   SOBJLAGE ,
# MAGIC   STREET ,
# MAGIC   SVERKEHR ,
# MAGIC   USAGECOMMON ,
# MAGIC   VALIDFROM ,
# MAGIC   VALIDTO ,
# MAGIC   ZCD_ADDR_LOT_NO ,
# MAGIC   ZCD_BAND ,
# MAGIC   ZCD_BLUESCOPE_FLAG ,
# MAGIC   ZCD_CAD_ID ,
# MAGIC   ZCD_CANCELLATION_DATE ,
# MAGIC   ZCD_CANC_REASON ,
# MAGIC   ZCD_CASENO_FLAG ,
# MAGIC   ZCD_COMMENTS ,
# MAGIC   ZCD_CONCESSION_FLAG ,
# MAGIC   ZCD_HYDRA_AREA_FLAG ,
# MAGIC   ZCD_HYDRA_AREA_UNIT ,
# MAGIC   ZCD_HYDRA_CALC_AREA ,
# MAGIC   ZCD_IND_COMMUNITY_TITLE ,
# MAGIC   ZCD_IND_MLIM ,
# MAGIC   ZCD_IND_SOPA ,
# MAGIC   ZCD_IND_TWA ,
# MAGIC   ZCD_IND_WICA ,
# MAGIC   ZCD_INF_PROP_TYPE ,
# MAGIC   ZCD_IS_PARENT ,
# MAGIC   ZCD_LOT_TYPE ,
# MAGIC   ZCD_NO_OF_FLATS ,
# MAGIC   ZCD_OVERRIDE_AREA,
# MAGIC   ZCD_OVERRIDE_AREA_UNIT ,
# MAGIC   ZCD_PARENT_PROP ,
# MAGIC   ZCD_PLAN_NUMBER ,
# MAGIC   ZCD_PLAN_TYPE ,
# MAGIC   ZCD_PROCESS_TYPE ,
# MAGIC   ZCD_PROPERTY_INFO ,
# MAGIC   ZCD_PROPERTY_NO ,
# MAGIC   ZCD_PROP_CN_FLAG ,
# MAGIC   ZCD_PROP_CR_DATE ,
# MAGIC   ZCD_PROP_LOT_NO ,
# MAGIC   ZCD_REQUEST_NO ,
# MAGIC   ZCD_SECTION_NUMBER ,
# MAGIC   ZCD_STORM_WATER_ASSESS ,
# MAGIC   ZCD_SUP_PROP_TYPE ,
# MAGIC   ZCD_UNIT_ENTITLEMENT 
# MAGIC from raw.sapisu_0archobject_attr
# MAGIC  except
# MAGIC  select 
# MAGIC   * from Source
