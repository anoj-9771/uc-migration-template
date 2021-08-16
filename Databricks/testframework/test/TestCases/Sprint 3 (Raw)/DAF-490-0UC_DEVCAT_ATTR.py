# Databricks notebook source
# DBTITLE 1,[Config] Connection Setup
storage_account_name = "saswcnonprod01landingtst"
storage_account_access_key = dbutils.secrets.get(scope="Test-Access",key="test-blob-key")
container_name = "archive"
file_location = "wasbs://archive@saswcnonprod01landingtst.blob.core.windows.net/archive/sapisu/0uc_devcat_attr/json/year=2021/month=08/day=05/0UC_DEVCAT_ATTR_20210804141617.json"
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

# DBTITLE 1,[Target] Displaying Records
# MAGIC %sql
# MAGIC select * from raw.sapisu_0uc_devcat_attr

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Source and Target Data
# MAGIC %sql
# MAGIC select * from Source
# MAGIC except
# MAGIC select ABMEIH ,
# MAGIC   ABM_TXT ,
# MAGIC   AEDAT ,
# MAGIC   AENAM ,
# MAGIC   AMCG_DGRP ,
# MAGIC   AMS_DGRP ,
# MAGIC   BAUFORM ,
# MAGIC   BAUKLAS ,
# MAGIC   BAUTXT ,
# MAGIC   BGLKZ ,
# MAGIC   BGLNETZ ,
# MAGIC   BREITEBEH ,
# MAGIC   DVGWNUM ,
# MAGIC   EAGRUPPE ,
# MAGIC   EIGENTUM ,
# MAGIC   EIGENTUM_TXT ,
# MAGIC   EXTRACT_DATETIME ,
# MAGIC   EXTRACT_RUN_ID ,
# MAGIC   FUNKLAS ,
# MAGIC   GEWEIH ,
# MAGIC   GEW_TXT ,
# MAGIC   GEW_ZUL ,
# MAGIC   GRPMATNR ,
# MAGIC   G_INFOSATZ ,
# MAGIC   HOEHEBEH ,
# MAGIC   KENNZTYP ,
# MAGIC   KENNZTYP_TXT ,
# MAGIC   KOMBINAT ,
# MAGIC   LADEVOL ,
# MAGIC   LADEZEIT ,
# MAGIC   LADV_EIH ,
# MAGIC   LADV_TXT ,
# MAGIC   LADZ_EIH ,
# MAGIC   LADZ_TXT ,
# MAGIC   LOSSDTGROUP ,
# MAGIC   MANDT ,
# MAGIC   MATNR ,
# MAGIC   MESSART ,
# MAGIC   MSG_DGRP_ID ,
# MAGIC   NENNBEL ,
# MAGIC   NOTIF_CODE ,
# MAGIC   ODQ_CHANGEMODE ,
# MAGIC   ODQ_ENTITYCNTR ,
# MAGIC   ORDER_CODE ,
# MAGIC   PPM_METER ,
# MAGIC   PREISKLA ,
# MAGIC   PRIMWNR1 ,
# MAGIC   PRIMWNR2 ,
# MAGIC   PRODUCT_AREA ,
# MAGIC   PTBNUM ,
# MAGIC   P_VOLTAGE ,
# MAGIC   RATING ,
# MAGIC   SEKWNR1 ,
# MAGIC   SEKWNR2 ,
# MAGIC   SMART_METER ,
# MAGIC   SPARTE ,
# MAGIC   STELLPLATZ ,
# MAGIC   S_VOLTAGE ,
# MAGIC   TIEFEBEH ,
# MAGIC   UEBERVER ,
# MAGIC   VLKZBAU ,
# MAGIC   VLZEITN ,
# MAGIC   VLZEITT ,
# MAGIC   VLZEITTI ,
# MAGIC   WGRUPPE ,
# MAGIC   ZSPANNP ,
# MAGIC   ZSPANNS ,
# MAGIC   ZSTROMP ,
# MAGIC   ZSTROMS ,
# MAGIC   ZWGRUPPE 
# MAGIC  from raw.sapisu_0uc_devcat_attr

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Target and Source Data
# MAGIC %sql
# MAGIC Select ABMEIH ,
# MAGIC   ABM_TXT ,
# MAGIC   AEDAT ,
# MAGIC   AENAM ,
# MAGIC   AMCG_DGRP ,
# MAGIC   AMS_DGRP ,
# MAGIC   BAUFORM ,
# MAGIC   BAUKLAS ,
# MAGIC   BAUTXT ,
# MAGIC   BGLKZ ,
# MAGIC   BGLNETZ ,
# MAGIC   BREITEBEH ,
# MAGIC   DVGWNUM ,
# MAGIC   EAGRUPPE ,
# MAGIC   EIGENTUM ,
# MAGIC   EIGENTUM_TXT ,
# MAGIC   EXTRACT_DATETIME ,
# MAGIC   EXTRACT_RUN_ID ,
# MAGIC   FUNKLAS ,
# MAGIC   GEWEIH ,
# MAGIC   GEW_TXT ,
# MAGIC   GEW_ZUL ,
# MAGIC   GRPMATNR ,
# MAGIC   G_INFOSATZ ,
# MAGIC   HOEHEBEH ,
# MAGIC   KENNZTYP ,
# MAGIC   KENNZTYP_TXT ,
# MAGIC   KOMBINAT ,
# MAGIC   LADEVOL ,
# MAGIC   LADEZEIT ,
# MAGIC   LADV_EIH ,
# MAGIC   LADV_TXT ,
# MAGIC   LADZ_EIH ,
# MAGIC   LADZ_TXT ,
# MAGIC   LOSSDTGROUP ,
# MAGIC   MANDT ,
# MAGIC   MATNR ,
# MAGIC   MESSART ,
# MAGIC   MSG_DGRP_ID ,
# MAGIC   NENNBEL ,
# MAGIC   NOTIF_CODE ,
# MAGIC   ODQ_CHANGEMODE ,
# MAGIC   ODQ_ENTITYCNTR ,
# MAGIC   ORDER_CODE ,
# MAGIC   PPM_METER ,
# MAGIC   PREISKLA ,
# MAGIC   PRIMWNR1 ,
# MAGIC   PRIMWNR2 ,
# MAGIC   PRODUCT_AREA ,
# MAGIC   PTBNUM ,
# MAGIC   P_VOLTAGE ,
# MAGIC   RATING ,
# MAGIC   SEKWNR1 ,
# MAGIC   SEKWNR2 ,
# MAGIC   SMART_METER ,
# MAGIC   SPARTE ,
# MAGIC   STELLPLATZ ,
# MAGIC   S_VOLTAGE ,
# MAGIC   TIEFEBEH ,
# MAGIC   UEBERVER ,
# MAGIC   VLKZBAU ,
# MAGIC   VLZEITN ,
# MAGIC   VLZEITT ,
# MAGIC   VLZEITTI ,
# MAGIC   WGRUPPE ,
# MAGIC   ZSPANNP ,
# MAGIC   ZSPANNS ,
# MAGIC   ZSTROMP ,
# MAGIC   ZSTROMS ,
# MAGIC   ZWGRUPPE 
# MAGIC  from raw.sapisu_0uc_devcat_attr 
# MAGIC  except
# MAGIC  select * from Source

# COMMAND ----------

# DBTITLE 0,[Result] Load Count Result into DataFrame
lakedf = spark.sql("select * from raw.sapisu_0uc_devcat_attr")

# COMMAND ----------

# DBTITLE 1,[Target] Schema Check
lakedf.printSchema()

# COMMAND ----------

lakedf.createOrReplaceTempView("Target")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from Target
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from Source
