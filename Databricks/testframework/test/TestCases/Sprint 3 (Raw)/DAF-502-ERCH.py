# Databricks notebook source
# DBTITLE 1,[Config] Connection Setup
storage_account_name = "saswcnonprod01landingtst"
storage_account_access_key = dbutils.secrets.get(scope="Test-Access",key="test-blob-key")
container_name = "archive"
file_location = "wasbs://archive@saswcnonprod01landingtst.blob.core.windows.net/archive/sapisu/erch/json/year=2021/month=08/day=05/ERCH_20210805103817.json"
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
# MAGIC select * from raw.sapisu_erch

# COMMAND ----------

# DBTITLE 0,[Result] Load Count Result into DataFrame
lakedf = spark.sql("select * from raw.sapisu_erch")

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

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Source and Target Data
# MAGIC %sql
# MAGIC select 
# MAGIC   * from Source
# MAGIC except
# MAGIC select  
# MAGIC   ABLEINH ,
# MAGIC   ABPOPBEL ,
# MAGIC   ABRDATS ,
# MAGIC   ABRDATSU ,
# MAGIC   ABRVORG ,
# MAGIC   ABRVORG2 ,
# MAGIC   ABRVORGU ,
# MAGIC   ABSCHLPAN ,
# MAGIC   ABWVK ,
# MAGIC   ACTPERIOD ,
# MAGIC   ACTPERORG ,
# MAGIC   ADATSOLL ,
# MAGIC   AEDAT ,
# MAGIC   AENAM ,
# MAGIC   ASBACH_REL ,
# MAGIC   BACKBI ,
# MAGIC   BASDYPER ,
# MAGIC   BCREASON ,
# MAGIC   BEGABRPE ,
# MAGIC   BEGEND ,
# MAGIC   BEGNACH ,
# MAGIC   BEGRU ,
# MAGIC   BELEGART ,
# MAGIC   BELEGDAT ,
# MAGIC   BELNR ,
# MAGIC   BELNRALT ,
# MAGIC   BERGRUND ,
# MAGIC   BILLINGRUNNO ,
# MAGIC   BILLING_PERIOD ,
# MAGIC   BP_BILL ,
# MAGIC   BUKRS ,
# MAGIC   COLOGRP_INST ,
# MAGIC   COLOGRP_INST_L ,
# MAGIC   CORRECTION_DATE ,
# MAGIC   DAUBUCH ,
# MAGIC   ENDABRPE ,
# MAGIC   ENDOFBB ,
# MAGIC   ENDOFPEB ,
# MAGIC   ENDPRIO ,
# MAGIC   ERCHC_V ,
# MAGIC   ERCHE_V ,
# MAGIC   ERCHO_V ,
# MAGIC   ERCHP_V ,
# MAGIC   ERCHR_V ,
# MAGIC   ERCHT_V ,
# MAGIC   ERCHU_V ,
# MAGIC   ERCHV_V ,
# MAGIC   ERCHZ_V ,
# MAGIC   ERDAT ,
# MAGIC   ERNAM ,
# MAGIC   EROETIM ,
# MAGIC   ESTINBILL ,
# MAGIC   ESTINBILLU ,
# MAGIC   ESTINBILL_SAV ,
# MAGIC   ESTINBILL_USAV ,
# MAGIC   EXBILLDOCNO ,
# MAGIC   EXTRACT_DATETIME ,
# MAGIC   EXTRACT_RUN_ID ,
# MAGIC   EZAWE ,
# MAGIC   FDGRP ,
# MAGIC   FORMULAR ,
# MAGIC   GPARTNER ,
# MAGIC   HVORG ,
# MAGIC   INSTGRTYPE ,
# MAGIC   INSTROLE ,
# MAGIC   KOFIZ ,
# MAGIC   KONZVER ,
# MAGIC   KTOKLASSE ,
# MAGIC   LOEVM ,
# MAGIC   MAINDOCNO ,
# MAGIC   MANBILLREL ,
# MAGIC   MANDT ,
# MAGIC   MDUSREQUESTID ,
# MAGIC   MEM_BUDAT ,
# MAGIC   MEM_OPBEL ,
# MAGIC   NBILLREL ,
# MAGIC   NINVOICE ,
# MAGIC   NOCANC ,
# MAGIC   NUMPERBB ,
# MAGIC   NUMPERPEB ,
# MAGIC   N_INVSEP ,
# MAGIC   ORIGDOC ,
# MAGIC   OSB_GROUP ,
# MAGIC   PERENDBI ,
# MAGIC   PORTION ,
# MAGIC   PTERMTDAT ,
# MAGIC   SC_BELNR_H ,
# MAGIC   SC_BELNR_N ,
# MAGIC   SIMRUNID ,
# MAGIC   SIMULATION ,
# MAGIC   SPARTE ,
# MAGIC   STORNODAT ,
# MAGIC   TOBRELEASD ,
# MAGIC   TXJCD ,
# MAGIC   VERTRAG ,
# MAGIC   VKONT ,
# MAGIC   ZBIZCAT ,
# MAGIC   ZDUE_DATE ,
# MAGIC   ZSTO_BELNR ,
# MAGIC   ZSTO_OPBEL ,
# MAGIC   ZUORDDAA 
# MAGIC  from Target

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Target and Source Data
# MAGIC %sql
# MAGIC select  
# MAGIC   ABLEINH ,
# MAGIC   ABPOPBEL ,
# MAGIC   ABRDATS ,
# MAGIC   ABRDATSU ,
# MAGIC   ABRVORG ,
# MAGIC   ABRVORG2 ,
# MAGIC   ABRVORGU ,
# MAGIC   ABSCHLPAN ,
# MAGIC   ABWVK ,
# MAGIC   ACTPERIOD ,
# MAGIC   ACTPERORG ,
# MAGIC   ADATSOLL ,
# MAGIC   AEDAT ,
# MAGIC   AENAM ,
# MAGIC   ASBACH_REL ,
# MAGIC   BACKBI ,
# MAGIC   BASDYPER ,
# MAGIC   BCREASON ,
# MAGIC   BEGABRPE ,
# MAGIC   BEGEND ,
# MAGIC   BEGNACH ,
# MAGIC   BEGRU ,
# MAGIC   BELEGART ,
# MAGIC   BELEGDAT ,
# MAGIC   BELNR ,
# MAGIC   BELNRALT ,
# MAGIC   BERGRUND ,
# MAGIC   BILLINGRUNNO ,
# MAGIC   BILLING_PERIOD ,
# MAGIC   BP_BILL ,
# MAGIC   BUKRS ,
# MAGIC   COLOGRP_INST ,
# MAGIC   COLOGRP_INST_L ,
# MAGIC   CORRECTION_DATE ,
# MAGIC   DAUBUCH ,
# MAGIC   ENDABRPE ,
# MAGIC   ENDOFBB ,
# MAGIC   ENDOFPEB ,
# MAGIC   ENDPRIO ,
# MAGIC   ERCHC_V ,
# MAGIC   ERCHE_V ,
# MAGIC   ERCHO_V ,
# MAGIC   ERCHP_V ,
# MAGIC   ERCHR_V ,
# MAGIC   ERCHT_V ,
# MAGIC   ERCHU_V ,
# MAGIC   ERCHV_V ,
# MAGIC   ERCHZ_V ,
# MAGIC   ERDAT ,
# MAGIC   ERNAM ,
# MAGIC   EROETIM ,
# MAGIC   ESTINBILL ,
# MAGIC   ESTINBILLU ,
# MAGIC   ESTINBILL_SAV ,
# MAGIC   ESTINBILL_USAV ,
# MAGIC   EXBILLDOCNO ,
# MAGIC   EXTRACT_DATETIME ,
# MAGIC   EXTRACT_RUN_ID ,
# MAGIC   EZAWE ,
# MAGIC   FDGRP ,
# MAGIC   FORMULAR ,
# MAGIC   GPARTNER ,
# MAGIC   HVORG ,
# MAGIC   INSTGRTYPE ,
# MAGIC   INSTROLE ,
# MAGIC   KOFIZ ,
# MAGIC   KONZVER ,
# MAGIC   KTOKLASSE ,
# MAGIC   LOEVM ,
# MAGIC   MAINDOCNO ,
# MAGIC   MANBILLREL ,
# MAGIC   MANDT ,
# MAGIC   MDUSREQUESTID ,
# MAGIC   MEM_BUDAT ,
# MAGIC   MEM_OPBEL ,
# MAGIC   NBILLREL ,
# MAGIC   NINVOICE ,
# MAGIC   NOCANC ,
# MAGIC   NUMPERBB ,
# MAGIC   NUMPERPEB ,
# MAGIC   N_INVSEP ,
# MAGIC   ORIGDOC ,
# MAGIC   OSB_GROUP ,
# MAGIC   PERENDBI ,
# MAGIC   PORTION ,
# MAGIC   PTERMTDAT ,
# MAGIC   SC_BELNR_H ,
# MAGIC   SC_BELNR_N ,
# MAGIC   SIMRUNID ,
# MAGIC   SIMULATION ,
# MAGIC   SPARTE ,
# MAGIC   STORNODAT ,
# MAGIC   TOBRELEASD ,
# MAGIC   TXJCD ,
# MAGIC   VERTRAG ,
# MAGIC   VKONT ,
# MAGIC   ZBIZCAT ,
# MAGIC   ZDUE_DATE ,
# MAGIC   ZSTO_BELNR ,
# MAGIC   ZSTO_OPBEL ,
# MAGIC   ZUORDDAA 
# MAGIC  from Target
# MAGIC  except
# MAGIC  select 
# MAGIC   * from Source
