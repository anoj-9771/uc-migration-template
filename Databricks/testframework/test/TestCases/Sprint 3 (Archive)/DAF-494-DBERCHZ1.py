# Databricks notebook source
# DBTITLE 1,[Config] Connection Setup
storage_account_name = "saswcnonprod01landingtst"
storage_account_access_key = dbutils.secrets.get(scope="Test-Access",key="test-blob-key")
container_name = "archive"
file_location = "wasbs://archive@saswcnonprod01landingtst.blob.core.windows.net/archive/sapisu/dberchz1/json/year=2021/month=08/day=05/DBERCHZ1_20210805103817.json"
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
# MAGIC select * from raw.sapisu_dberchz1

# COMMAND ----------

# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from raw.sapisu_dberchz1
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from Source

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Source and Target Data
# MAGIC %sql
# MAGIC select  AB ,
# MAGIC   ABGGRND1 ,
# MAGIC   ABGGRND10 ,
# MAGIC   ABGGRND2 ,
# MAGIC   ABGGRND3 ,
# MAGIC   ABGGRND4 ,
# MAGIC   ABGGRND5 ,
# MAGIC   ABGGRND6 ,
# MAGIC   ABGGRND7 ,
# MAGIC   ABGGRND8 ,
# MAGIC   ABGGRND9 ,
# MAGIC   ABSLKZ ,
# MAGIC   AKLASSE ,
# MAGIC   AKTIV ,
# MAGIC   ARTMENGE ,
# MAGIC   AUS01 ,
# MAGIC   AUS02 ,
# MAGIC   BACKCANC ,
# MAGIC   BACKCANC01 ,
# MAGIC   BACKCANC02 ,
# MAGIC   BACKCANC03 ,
# MAGIC   BACKCANC04 ,
# MAGIC   BACKCANC05 ,
# MAGIC   BACKDOCLINE ,
# MAGIC   BACKDOCNO ,
# MAGIC   BACKEXEC ,
# MAGIC   BELNR ,
# MAGIC   BELZART ,
# MAGIC   BELZEILE ,
# MAGIC   BETRSTREL ,
# MAGIC   BIS ,
# MAGIC   BRANCHE ,
# MAGIC   BUCHREL ,
# MAGIC   COLOGRP_INST_L ,
# MAGIC   CSNO ,
# MAGIC   DATUM1 ,
# MAGIC   DATUM2 ,
# MAGIC   DIFFKZ ,
# MAGIC   DRCKSTUF ,
# MAGIC   DYNCANC ,
# MAGIC   DYNCANC01 ,
# MAGIC   DYNCANC02 ,
# MAGIC   DYNCANC03 ,
# MAGIC   DYNCANC04 ,
# MAGIC   DYNCANC05 ,
# MAGIC   DYNEXEC ,
# MAGIC   EIN01 ,
# MAGIC   EIN02 ,
# MAGIC   EIN03 ,
# MAGIC   EIN04 ,
# MAGIC   ERCHV_ANCHOR ,
# MAGIC   EXTRACT_DATETIME ,
# MAGIC   EXTRACT_RUN_ID ,
# MAGIC   FRAN_TYPE ,
# MAGIC   GEGEN_TVORG ,
# MAGIC   GEWKEY ,
# MAGIC   IURU_COT ,
# MAGIC   IURU_CTT ,
# MAGIC   IURU_FIS ,
# MAGIC   IURU_FOR ,
# MAGIC   IURU_IND ,
# MAGIC   IURU_MIN ,
# MAGIC   IURU_MWSKZ ,
# MAGIC   IURU_RAO ,
# MAGIC   IURU_VOL ,
# MAGIC   IUSK_MWSKZ_OLD ,
# MAGIC   KONDIGR ,
# MAGIC   KONZIGR ,
# MAGIC   KONZVER ,
# MAGIC   LINESORT ,
# MAGIC   LRATESTEP ,
# MAGIC   MANDT ,
# MAGIC   MASS1 ,
# MAGIC   MASS2 ,
# MAGIC   MASS3 ,
# MAGIC   MASS4 ,
# MAGIC   MASSBILL ,
# MAGIC   MENGESTREL ,
# MAGIC   N_ABRMENGE ,
# MAGIC   N_ZAHL1 ,
# MAGIC   N_ZAHL2 ,
# MAGIC   N_ZAHL3 ,
# MAGIC   N_ZAHL4 ,
# MAGIC   N_ZEITANT ,
# MAGIC   OPLFDNR ,
# MAGIC   OUCONTRACT ,
# MAGIC   PEB ,
# MAGIC   PERTYP ,
# MAGIC   PRINTREL ,
# MAGIC   PROGRAMM ,
# MAGIC   RABZUS ,
# MAGIC   SAISON ,
# MAGIC   SCHEMANR ,
# MAGIC   SNO ,
# MAGIC   STAFO ,
# MAGIC   STATTART ,
# MAGIC   STGRAMT ,
# MAGIC   STGRQNT ,
# MAGIC   STTARIF ,
# MAGIC   TARIFNR ,
# MAGIC   TARIFTYP ,
# MAGIC   TAX_TVORG ,
# MAGIC   TCDENOMTOR ,
# MAGIC   TCNUMTOR ,
# MAGIC   TEMP_AREA ,
# MAGIC   TIMBASIS ,
# MAGIC   TIMECONTRL ,
# MAGIC   TIMTYP ,
# MAGIC   TIMTYPQUOT ,
# MAGIC   TIMTYPZA ,
# MAGIC   TVORG ,
# MAGIC   V_ABRMENGE ,
# MAGIC   V_ZAHL1 ,
# MAGIC   V_ZAHL2 ,
# MAGIC   V_ZAHL3 ,
# MAGIC   V_ZAHL4 ,
# MAGIC   V_ZEITANT ,
# MAGIC   WDHFAKT 
# MAGIC  from Source
# MAGIC except
# MAGIC select 
# MAGIC  AB ,
# MAGIC   ABGGRND1 ,
# MAGIC   ABGGRND10 ,
# MAGIC   ABGGRND2 ,
# MAGIC   ABGGRND3 ,
# MAGIC   ABGGRND4 ,
# MAGIC   ABGGRND5 ,
# MAGIC   ABGGRND6 ,
# MAGIC   ABGGRND7 ,
# MAGIC   ABGGRND8 ,
# MAGIC   ABGGRND9 ,
# MAGIC   ABSLKZ ,
# MAGIC   AKLASSE ,
# MAGIC   AKTIV ,
# MAGIC   ARTMENGE ,
# MAGIC   AUS01 ,
# MAGIC   AUS02 ,
# MAGIC   BACKCANC ,
# MAGIC   BACKCANC01 ,
# MAGIC   BACKCANC02 ,
# MAGIC   BACKCANC03 ,
# MAGIC   BACKCANC04 ,
# MAGIC   BACKCANC05 ,
# MAGIC   BACKDOCLINE ,
# MAGIC   BACKDOCNO ,
# MAGIC   BACKEXEC ,
# MAGIC   BELNR ,
# MAGIC   BELZART ,
# MAGIC   BELZEILE ,
# MAGIC   BETRSTREL ,
# MAGIC   BIS ,
# MAGIC   BRANCHE ,
# MAGIC   BUCHREL ,
# MAGIC   COLOGRP_INST_L ,
# MAGIC   CSNO ,
# MAGIC   DATUM1 ,
# MAGIC   DATUM2 ,
# MAGIC   DIFFKZ ,
# MAGIC   DRCKSTUF ,
# MAGIC   DYNCANC ,
# MAGIC   DYNCANC01 ,
# MAGIC   DYNCANC02 ,
# MAGIC   DYNCANC03 ,
# MAGIC   DYNCANC04 ,
# MAGIC   DYNCANC05 ,
# MAGIC   DYNEXEC ,
# MAGIC   EIN01 ,
# MAGIC   EIN02 ,
# MAGIC   EIN03 ,
# MAGIC   EIN04 ,
# MAGIC   ERCHV_ANCHOR ,
# MAGIC   EXTRACT_DATETIME ,
# MAGIC   EXTRACT_RUN_ID ,
# MAGIC   FRAN_TYPE ,
# MAGIC   GEGEN_TVORG ,
# MAGIC   GEWKEY ,
# MAGIC   IURU_COT ,
# MAGIC   IURU_CTT ,
# MAGIC   IURU_FIS ,
# MAGIC   IURU_FOR ,
# MAGIC   IURU_IND ,
# MAGIC   IURU_MIN ,
# MAGIC   IURU_MWSKZ ,
# MAGIC   IURU_RAO ,
# MAGIC   IURU_VOL ,
# MAGIC   IUSK_MWSKZ_OLD ,
# MAGIC   KONDIGR ,
# MAGIC   KONZIGR ,
# MAGIC   KONZVER ,
# MAGIC   LINESORT ,
# MAGIC   LRATESTEP ,
# MAGIC   MANDT ,
# MAGIC   MASS1 ,
# MAGIC   MASS2 ,
# MAGIC   MASS3 ,
# MAGIC   MASS4 ,
# MAGIC   MASSBILL ,
# MAGIC   MENGESTREL ,
# MAGIC   N_ABRMENGE ,
# MAGIC   N_ZAHL1 ,
# MAGIC   N_ZAHL2 ,
# MAGIC   N_ZAHL3 ,
# MAGIC   N_ZAHL4 ,
# MAGIC   N_ZEITANT ,
# MAGIC   OPLFDNR ,
# MAGIC   OUCONTRACT ,
# MAGIC   PEB ,
# MAGIC   PERTYP ,
# MAGIC   PRINTREL ,
# MAGIC   PROGRAMM ,
# MAGIC   RABZUS ,
# MAGIC   SAISON ,
# MAGIC   SCHEMANR ,
# MAGIC   SNO ,
# MAGIC   STAFO ,
# MAGIC   STATTART ,
# MAGIC   STGRAMT ,
# MAGIC   STGRQNT ,
# MAGIC   STTARIF ,
# MAGIC   TARIFNR ,
# MAGIC   TARIFTYP ,
# MAGIC   TAX_TVORG ,
# MAGIC   TCDENOMTOR ,
# MAGIC   TCNUMTOR ,
# MAGIC   TEMP_AREA ,
# MAGIC   TIMBASIS ,
# MAGIC   TIMECONTRL ,
# MAGIC   TIMTYP ,
# MAGIC   TIMTYPQUOT ,
# MAGIC   TIMTYPZA ,
# MAGIC   TVORG ,
# MAGIC   V_ABRMENGE ,
# MAGIC   V_ZAHL1 ,
# MAGIC   V_ZAHL2 ,
# MAGIC   V_ZAHL3 ,
# MAGIC   V_ZAHL4 ,
# MAGIC   V_ZEITANT ,
# MAGIC   WDHFAKT 
# MAGIC from raw.sapisu_dberchz1

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Target and Source Data
# MAGIC %sql
# MAGIC select 
# MAGIC  AB ,
# MAGIC   ABGGRND1 ,
# MAGIC   ABGGRND10 ,
# MAGIC   ABGGRND2 ,
# MAGIC   ABGGRND3 ,
# MAGIC   ABGGRND4 ,
# MAGIC   ABGGRND5 ,
# MAGIC   ABGGRND6 ,
# MAGIC   ABGGRND7 ,
# MAGIC   ABGGRND8 ,
# MAGIC   ABGGRND9 ,
# MAGIC   ABSLKZ ,
# MAGIC   AKLASSE ,
# MAGIC   AKTIV ,
# MAGIC   ARTMENGE ,
# MAGIC   AUS01 ,
# MAGIC   AUS02 ,
# MAGIC   BACKCANC ,
# MAGIC   BACKCANC01 ,
# MAGIC   BACKCANC02 ,
# MAGIC   BACKCANC03 ,
# MAGIC   BACKCANC04 ,
# MAGIC   BACKCANC05 ,
# MAGIC   BACKDOCLINE ,
# MAGIC   BACKDOCNO ,
# MAGIC   BACKEXEC ,
# MAGIC   BELNR ,
# MAGIC   BELZART ,
# MAGIC   BELZEILE ,
# MAGIC   BETRSTREL ,
# MAGIC   BIS ,
# MAGIC   BRANCHE ,
# MAGIC   BUCHREL ,
# MAGIC   COLOGRP_INST_L ,
# MAGIC   CSNO ,
# MAGIC   DATUM1 ,
# MAGIC   DATUM2 ,
# MAGIC   DIFFKZ ,
# MAGIC   DRCKSTUF ,
# MAGIC   DYNCANC ,
# MAGIC   DYNCANC01 ,
# MAGIC   DYNCANC02 ,
# MAGIC   DYNCANC03 ,
# MAGIC   DYNCANC04 ,
# MAGIC   DYNCANC05 ,
# MAGIC   DYNEXEC ,
# MAGIC   EIN01 ,
# MAGIC   EIN02 ,
# MAGIC   EIN03 ,
# MAGIC   EIN04 ,
# MAGIC   ERCHV_ANCHOR ,
# MAGIC   EXTRACT_DATETIME ,
# MAGIC   EXTRACT_RUN_ID ,
# MAGIC   FRAN_TYPE ,
# MAGIC   GEGEN_TVORG ,
# MAGIC   GEWKEY ,
# MAGIC   IURU_COT ,
# MAGIC   IURU_CTT ,
# MAGIC   IURU_FIS ,
# MAGIC   IURU_FOR ,
# MAGIC   IURU_IND ,
# MAGIC   IURU_MIN ,
# MAGIC   IURU_MWSKZ ,
# MAGIC   IURU_RAO ,
# MAGIC   IURU_VOL ,
# MAGIC   IUSK_MWSKZ_OLD ,
# MAGIC   KONDIGR ,
# MAGIC   KONZIGR ,
# MAGIC   KONZVER ,
# MAGIC   LINESORT ,
# MAGIC   LRATESTEP ,
# MAGIC   MANDT ,
# MAGIC   MASS1 ,
# MAGIC   MASS2 ,
# MAGIC   MASS3 ,
# MAGIC   MASS4 ,
# MAGIC   MASSBILL ,
# MAGIC   MENGESTREL ,
# MAGIC   N_ABRMENGE ,
# MAGIC   N_ZAHL1 ,
# MAGIC   N_ZAHL2 ,
# MAGIC   N_ZAHL3 ,
# MAGIC   N_ZAHL4 ,
# MAGIC   N_ZEITANT ,
# MAGIC   OPLFDNR ,
# MAGIC   OUCONTRACT ,
# MAGIC   PEB ,
# MAGIC   PERTYP ,
# MAGIC   PRINTREL ,
# MAGIC   PROGRAMM ,
# MAGIC   RABZUS ,
# MAGIC   SAISON ,
# MAGIC   SCHEMANR ,
# MAGIC   SNO ,
# MAGIC   STAFO ,
# MAGIC   STATTART ,
# MAGIC   STGRAMT ,
# MAGIC   STGRQNT ,
# MAGIC   STTARIF ,
# MAGIC   TARIFNR ,
# MAGIC   TARIFTYP ,
# MAGIC   TAX_TVORG ,
# MAGIC   TCDENOMTOR ,
# MAGIC   TCNUMTOR ,
# MAGIC   TEMP_AREA ,
# MAGIC   TIMBASIS ,
# MAGIC   TIMECONTRL ,
# MAGIC   TIMTYP ,
# MAGIC   TIMTYPQUOT ,
# MAGIC   TIMTYPZA ,
# MAGIC   TVORG ,
# MAGIC   V_ABRMENGE ,
# MAGIC   V_ZAHL1 ,
# MAGIC   V_ZAHL2 ,
# MAGIC   V_ZAHL3 ,
# MAGIC   V_ZAHL4 ,
# MAGIC   V_ZEITANT ,
# MAGIC   WDHFAKT 
# MAGIC  from raw.sapisu_dberchz1
# MAGIC except
# MAGIC select 
# MAGIC  AB ,
# MAGIC   ABGGRND1 ,
# MAGIC   ABGGRND10 ,
# MAGIC   ABGGRND2 ,
# MAGIC   ABGGRND3 ,
# MAGIC   ABGGRND4 ,
# MAGIC   ABGGRND5 ,
# MAGIC   ABGGRND6 ,
# MAGIC   ABGGRND7 ,
# MAGIC   ABGGRND8 ,
# MAGIC   ABGGRND9 ,
# MAGIC   ABSLKZ ,
# MAGIC   AKLASSE ,
# MAGIC   AKTIV ,
# MAGIC   ARTMENGE ,
# MAGIC   AUS01 ,
# MAGIC   AUS02 ,
# MAGIC   BACKCANC ,
# MAGIC   BACKCANC01 ,
# MAGIC   BACKCANC02 ,
# MAGIC   BACKCANC03 ,
# MAGIC   BACKCANC04 ,
# MAGIC   BACKCANC05 ,
# MAGIC   BACKDOCLINE ,
# MAGIC   BACKDOCNO ,
# MAGIC   BACKEXEC ,
# MAGIC   BELNR ,
# MAGIC   BELZART ,
# MAGIC   BELZEILE ,
# MAGIC   BETRSTREL ,
# MAGIC   BIS ,
# MAGIC   BRANCHE ,
# MAGIC   BUCHREL ,
# MAGIC   COLOGRP_INST_L ,
# MAGIC   CSNO ,
# MAGIC   DATUM1 ,
# MAGIC   DATUM2 ,
# MAGIC   DIFFKZ ,
# MAGIC   DRCKSTUF ,
# MAGIC   DYNCANC ,
# MAGIC   DYNCANC01 ,
# MAGIC   DYNCANC02 ,
# MAGIC   DYNCANC03 ,
# MAGIC   DYNCANC04 ,
# MAGIC   DYNCANC05 ,
# MAGIC   DYNEXEC ,
# MAGIC   EIN01 ,
# MAGIC   EIN02 ,
# MAGIC   EIN03 ,
# MAGIC   EIN04 ,
# MAGIC   ERCHV_ANCHOR ,
# MAGIC   EXTRACT_DATETIME ,
# MAGIC   EXTRACT_RUN_ID ,
# MAGIC   FRAN_TYPE ,
# MAGIC   GEGEN_TVORG ,
# MAGIC   GEWKEY ,
# MAGIC   IURU_COT ,
# MAGIC   IURU_CTT ,
# MAGIC   IURU_FIS ,
# MAGIC   IURU_FOR ,
# MAGIC   IURU_IND ,
# MAGIC   IURU_MIN ,
# MAGIC   IURU_MWSKZ ,
# MAGIC   IURU_RAO ,
# MAGIC   IURU_VOL ,
# MAGIC   IUSK_MWSKZ_OLD ,
# MAGIC   KONDIGR ,
# MAGIC   KONZIGR ,
# MAGIC   KONZVER ,
# MAGIC   LINESORT ,
# MAGIC   LRATESTEP ,
# MAGIC   MANDT ,
# MAGIC   MASS1 ,
# MAGIC   MASS2 ,
# MAGIC   MASS3 ,
# MAGIC   MASS4 ,
# MAGIC   MASSBILL ,
# MAGIC   MENGESTREL ,
# MAGIC   N_ABRMENGE ,
# MAGIC   N_ZAHL1 ,
# MAGIC   N_ZAHL2 ,
# MAGIC   N_ZAHL3 ,
# MAGIC   N_ZAHL4 ,
# MAGIC   N_ZEITANT ,
# MAGIC   OPLFDNR ,
# MAGIC   OUCONTRACT ,
# MAGIC   PEB ,
# MAGIC   PERTYP ,
# MAGIC   PRINTREL ,
# MAGIC   PROGRAMM ,
# MAGIC   RABZUS ,
# MAGIC   SAISON ,
# MAGIC   SCHEMANR ,
# MAGIC   SNO ,
# MAGIC   STAFO ,
# MAGIC   STATTART ,
# MAGIC   STGRAMT ,
# MAGIC   STGRQNT ,
# MAGIC   STTARIF ,
# MAGIC   TARIFNR ,
# MAGIC   TARIFTYP ,
# MAGIC   TAX_TVORG ,
# MAGIC   TCDENOMTOR ,
# MAGIC   TCNUMTOR ,
# MAGIC   TEMP_AREA ,
# MAGIC   TIMBASIS ,
# MAGIC   TIMECONTRL ,
# MAGIC   TIMTYP ,
# MAGIC   TIMTYPQUOT ,
# MAGIC   TIMTYPZA ,
# MAGIC   TVORG ,
# MAGIC   V_ABRMENGE ,
# MAGIC   V_ZAHL1 ,
# MAGIC   V_ZAHL2 ,
# MAGIC   V_ZAHL3 ,
# MAGIC   V_ZAHL4 ,
# MAGIC   V_ZEITANT ,
# MAGIC   WDHFAKT 
# MAGIC  from Source

# COMMAND ----------

# DBTITLE 0,[Result] Load Count Result into DataFrame
lakedf = spark.sql("select * from raw.sapisu_dberchz1")

# COMMAND ----------

# DBTITLE 1,[Target] Schema Check
lakedf.printSchema()
