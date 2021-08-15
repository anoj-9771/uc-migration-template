# Databricks notebook source
# DBTITLE 1,[Source] Raw Table
# MAGIC %sql
# MAGIC select * from raw.sap_0uc_devcat_attr

# COMMAND ----------

# DBTITLE 1,[Target] Cleansed Table
# MAGIC %sql
# MAGIC select * from cleansedtable

# COMMAND ----------

# DBTITLE 1,[Source] Test Code
# MAGIC %sql
# MAGIC select * from (
# MAGIC SELECT
# MAGIC MATNR	as	materialNumber
# MAGIC ,KOMBINAT	as	deviceCategoryCombination
# MAGIC ,FUNKLAS	as	functionClassCode
# MAGIC ,fk.FUNKTXT	as	functionClass
# MAGIC ,BAUKLAS	as	constructionClassCode
# MAGIC ,bk.BAUKLTXT	as	constructionClass
# MAGIC ,BAUFORM	as	deviceCategoryDescription
# MAGIC ,BAUTXT	as	deviceCategoryName
# MAGIC ,PREISKLA	as	Price_Class_NM
# MAGIC ,PTBNUM	as	ptiNumber
# MAGIC ,DVGWNUM	as	ggwaNumber
# MAGIC ,BGLKZ	as	certificationRequirementType
# MAGIC ,VLZEITT	as	calibrationValidityYears
# MAGIC ,VLZEITTI	as	internalCertificationYears
# MAGIC ,VLZEITN	as	VLZEITN
# MAGIC ,VLKZBAU	as	VLKZBAU
# MAGIC ,ZWGRUPPE	as	registerGroupCode
# MAGIC ,EAGRUPPE	as	Input_Output_Group_CD
# MAGIC ,MESSART	as	Measurement_Type_CD
# MAGIC ,b.EZWG_INFO	as	registerGroup
# MAGIC ,UEBERVER	as	transformationRatio
# MAGIC ,BGLNETZ	as	Install_Device_Certify_IND
# MAGIC ,WGRUPPE	as	Winding_Group_CD
# MAGIC ,PRIMWNR1	as	Active_Primary_Winding_NUM
# MAGIC ,SEKWNR1	as	Active_Secondary_Winding_NUM
# MAGIC ,PRIMWNR2	as	PRIMWNR2
# MAGIC ,SEKWNR2	as	SEKWNR2
# MAGIC ,AENAM	as	changedBy
# MAGIC ,AEDAT	as	lastChangedDate
# MAGIC ,ZSPANNS	as	ZSPANNS
# MAGIC ,ZSTROMS	as	ZSTROMS
# MAGIC ,ZSPANNP	as	ZSPANNP
# MAGIC ,ZSTROMP	as	ZSTROMP
# MAGIC ,GRPMATNR	as	GRPMATNR
# MAGIC ,ORDER_CODE	as	ORDER_CODE
# MAGIC ,SPARTE	as	division
# MAGIC ,NENNBEL	as	nominalLoad
# MAGIC ,KENNZTYP	as	Vehicle_Category_CD
# MAGIC ,KENNZTYP_TXT	as	KENNZTYP_TXT
# MAGIC ,STELLPLATZ	as	containerSpaceCount
# MAGIC ,HOEHEBEH	as	containerCategoryHeight
# MAGIC ,BREITEBEH	as	containerCategoryWidth
# MAGIC ,TIEFEBEH	as	containerCategoryDepth
# MAGIC ,ABMEIH	as	ABMEIH
# MAGIC ,LADEZEIT	as	LADEZEIT
# MAGIC ,LADZ_EIH	as	LADZ_EIH
# MAGIC ,LADEVOL	as	LADEVOL
# MAGIC ,LADV_EIH	as	LADV_EIH
# MAGIC ,GEW_ZUL	as	GEW_ZUL
# MAGIC ,GEWEIH	as	GEWEIH
# MAGIC ,EIGENTUM	as	EIGENTUM
# MAGIC ,EIGENTUM_TXT	as	EIGENTUM_TXT
# MAGIC ,LADV_TXT	as	LADV_TXT
# MAGIC ,LADZ_TXT	as	LADZ_TXT
# MAGIC ,GEW_TXT	as	GEW_TXT
# MAGIC ,ABM_TXT	as	ABM_TXT
# MAGIC ,PRODUCT_AREA	as	PRODUCT_AREA
# MAGIC ,NOTIF_CODE	as	NOTIF_CODE
# MAGIC ,G_INFOSATZ	as	G_INFOSATZ
# MAGIC ,PPM_METER	as	PPM_METER
# MAGIC ,row_number() over (partition by MATNR order by MATNR desc) as rn
# MAGIC from raw.sap_0uc_devcat_attr d
# MAGIC left join CUSTOMER_BILLING.S4H_0UC_FUNKLAS_TEXT fk 
# MAGIC on fk.FUNKLAS = d.FUNKLAS
# MAGIC left join CUSTOMER_BILLING.S4H_0UC_BAUKLAS_TEXT bk 
# MAGIC on bk.BAUKLAS = d.BAUKLAS and SPRAS ='E'
# MAGIC left join 0UC_REGGRP_TEXT b
# MAGIC on d.ZWGRUPPE = b.ZWGRUPPE)a
# MAGIC where a.rn=1

# COMMAND ----------

# DBTITLE 1,[Verify] Count of Source
# MAGIC %sql
# MAGIC select count(*) from (
# MAGIC select * from (
# MAGIC SELECT
# MAGIC MATNR	as	materialNumber
# MAGIC ,KOMBINAT	as	deviceCategoryCombination
# MAGIC ,FUNKLAS	as	functionClassCode
# MAGIC ,fk.FUNKTXT	as	functionClass
# MAGIC ,BAUKLAS	as	constructionClassCode
# MAGIC ,bk.BAUKLTXT	as	constructionClass
# MAGIC ,BAUFORM	as	deviceCategoryDescription
# MAGIC ,BAUTXT	as	deviceCategoryName
# MAGIC ,PREISKLA	as	Price_Class_NM
# MAGIC ,PTBNUM	as	ptiNumber
# MAGIC ,DVGWNUM	as	ggwaNumber
# MAGIC ,BGLKZ	as	certificationRequirementType
# MAGIC ,VLZEITT	as	calibrationValidityYears
# MAGIC ,VLZEITTI	as	internalCertificationYears
# MAGIC ,VLZEITN	as	VLZEITN
# MAGIC ,VLKZBAU	as	VLKZBAU
# MAGIC ,ZWGRUPPE	as	registerGroupCode
# MAGIC ,EAGRUPPE	as	Input_Output_Group_CD
# MAGIC ,MESSART	as	Measurement_Type_CD
# MAGIC ,b.EZWG_INFO	as	registerGroup
# MAGIC ,UEBERVER	as	transformationRatio
# MAGIC ,BGLNETZ	as	Install_Device_Certify_IND
# MAGIC ,WGRUPPE	as	Winding_Group_CD
# MAGIC ,PRIMWNR1	as	Active_Primary_Winding_NUM
# MAGIC ,SEKWNR1	as	Active_Secondary_Winding_NUM
# MAGIC ,PRIMWNR2	as	PRIMWNR2
# MAGIC ,SEKWNR2	as	SEKWNR2
# MAGIC ,AENAM	as	changedBy
# MAGIC ,AEDAT	as	lastChangedDate
# MAGIC ,ZSPANNS	as	ZSPANNS
# MAGIC ,ZSTROMS	as	ZSTROMS
# MAGIC ,ZSPANNP	as	ZSPANNP
# MAGIC ,ZSTROMP	as	ZSTROMP
# MAGIC ,GRPMATNR	as	GRPMATNR
# MAGIC ,ORDER_CODE	as	ORDER_CODE
# MAGIC ,SPARTE	as	division
# MAGIC ,NENNBEL	as	nominalLoad
# MAGIC ,KENNZTYP	as	Vehicle_Category_CD
# MAGIC ,KENNZTYP_TXT	as	KENNZTYP_TXT
# MAGIC ,STELLPLATZ	as	containerSpaceCount
# MAGIC ,HOEHEBEH	as	containerCategoryHeight
# MAGIC ,BREITEBEH	as	containerCategoryWidth
# MAGIC ,TIEFEBEH	as	containerCategoryDepth
# MAGIC ,ABMEIH	as	ABMEIH
# MAGIC ,LADEZEIT	as	LADEZEIT
# MAGIC ,LADZ_EIH	as	LADZ_EIH
# MAGIC ,LADEVOL	as	LADEVOL
# MAGIC ,LADV_EIH	as	LADV_EIH
# MAGIC ,GEW_ZUL	as	GEW_ZUL
# MAGIC ,GEWEIH	as	GEWEIH
# MAGIC ,EIGENTUM	as	EIGENTUM
# MAGIC ,EIGENTUM_TXT	as	EIGENTUM_TXT
# MAGIC ,LADV_TXT	as	LADV_TXT
# MAGIC ,LADZ_TXT	as	LADZ_TXT
# MAGIC ,GEW_TXT	as	GEW_TXT
# MAGIC ,ABM_TXT	as	ABM_TXT
# MAGIC ,PRODUCT_AREA	as	PRODUCT_AREA
# MAGIC ,NOTIF_CODE	as	NOTIF_CODE
# MAGIC ,G_INFOSATZ	as	G_INFOSATZ
# MAGIC ,PPM_METER	as	PPM_METER
# MAGIC ,row_number() over (partition by MATNR order by MATNR desc) as rn
# MAGIC from raw.sap_0uc_devcat_attr d
# MAGIC left join CUSTOMER_BILLING.S4H_0UC_FUNKLAS_TEXT fk 
# MAGIC on fk.FUNKLAS = d.FUNKLAS
# MAGIC left join CUSTOMER_BILLING.S4H_0UC_BAUKLAS_TEXT bk 
# MAGIC on bk.BAUKLAS = d.BAUKLAS and SPRAS ='E'
# MAGIC left join 0UC_REGGRP_TEXT b
# MAGIC on d.ZWGRUPPE = b.ZWGRUPPE)a
# MAGIC where a.rn=1
# MAGIC )

# COMMAND ----------

# DBTITLE 1,[Verify] Count of Target
select count (*) from targettable

# COMMAND ----------

# DBTITLE 1,[Verify] Duplicate Checks (Partition)
select *,
row_number() over (partition by materialNumber order by materialNumber desc) rn
from cleansed.t_sapisu_0uc_devcat_attr
where materialNumber = 'M_D_ANY_UNK_GEN'

# COMMAND ----------

# DBTITLE 1,[Verify] Duplicate Checks (Having)
# MAGIC %sql
# MAGIC SELECT materialNumber, COUNT (*) as count
# MAGIC FROM cleansed.t_sapisu_0uc_devcat_attr
# MAGIC GROUP BY materialNumber
# MAGIC HAVING COUNT (*) > 1

# COMMAND ----------

# DBTITLE 1,[Verify] Source to Target Comparison
# MAGIC %sql
# MAGIC select * from (
# MAGIC SELECT
# MAGIC MATNR	as	materialNumber
# MAGIC ,KOMBINAT	as	deviceCategoryCombination
# MAGIC ,FUNKLAS	as	functionClassCode
# MAGIC ,fk.FUNKTXT	as	functionClass
# MAGIC ,BAUKLAS	as	constructionClassCode
# MAGIC ,bk.BAUKLTXT	as	constructionClass
# MAGIC ,BAUFORM	as	deviceCategoryDescription
# MAGIC ,BAUTXT	as	deviceCategoryName
# MAGIC ,PREISKLA	as	Price_Class_NM
# MAGIC ,PTBNUM	as	ptiNumber
# MAGIC ,DVGWNUM	as	ggwaNumber
# MAGIC ,BGLKZ	as	certificationRequirementType
# MAGIC ,VLZEITT	as	calibrationValidityYears
# MAGIC ,VLZEITTI	as	internalCertificationYears
# MAGIC ,VLZEITN	as	VLZEITN
# MAGIC ,VLKZBAU	as	VLKZBAU
# MAGIC ,ZWGRUPPE	as	registerGroupCode
# MAGIC ,EAGRUPPE	as	Input_Output_Group_CD
# MAGIC ,MESSART	as	Measurement_Type_CD
# MAGIC ,b.EZWG_INFO	as	registerGroup
# MAGIC ,UEBERVER	as	transformationRatio
# MAGIC ,BGLNETZ	as	Install_Device_Certify_IND
# MAGIC ,WGRUPPE	as	Winding_Group_CD
# MAGIC ,PRIMWNR1	as	Active_Primary_Winding_NUM
# MAGIC ,SEKWNR1	as	Active_Secondary_Winding_NUM
# MAGIC ,PRIMWNR2	as	PRIMWNR2
# MAGIC ,SEKWNR2	as	SEKWNR2
# MAGIC ,AENAM	as	changedBy
# MAGIC ,AEDAT	as	lastChangedDate
# MAGIC ,ZSPANNS	as	ZSPANNS
# MAGIC ,ZSTROMS	as	ZSTROMS
# MAGIC ,ZSPANNP	as	ZSPANNP
# MAGIC ,ZSTROMP	as	ZSTROMP
# MAGIC ,GRPMATNR	as	GRPMATNR
# MAGIC ,ORDER_CODE	as	ORDER_CODE
# MAGIC ,SPARTE	as	division
# MAGIC ,NENNBEL	as	nominalLoad
# MAGIC ,KENNZTYP	as	Vehicle_Category_CD
# MAGIC ,KENNZTYP_TXT	as	KENNZTYP_TXT
# MAGIC ,STELLPLATZ	as	containerSpaceCount
# MAGIC ,HOEHEBEH	as	containerCategoryHeight
# MAGIC ,BREITEBEH	as	containerCategoryWidth
# MAGIC ,TIEFEBEH	as	containerCategoryDepth
# MAGIC ,ABMEIH	as	ABMEIH
# MAGIC ,LADEZEIT	as	LADEZEIT
# MAGIC ,LADZ_EIH	as	LADZ_EIH
# MAGIC ,LADEVOL	as	LADEVOL
# MAGIC ,LADV_EIH	as	LADV_EIH
# MAGIC ,GEW_ZUL	as	GEW_ZUL
# MAGIC ,GEWEIH	as	GEWEIH
# MAGIC ,EIGENTUM	as	EIGENTUM
# MAGIC ,EIGENTUM_TXT	as	EIGENTUM_TXT
# MAGIC ,LADV_TXT	as	LADV_TXT
# MAGIC ,LADZ_TXT	as	LADZ_TXT
# MAGIC ,GEW_TXT	as	GEW_TXT
# MAGIC ,ABM_TXT	as	ABM_TXT
# MAGIC ,PRODUCT_AREA	as	PRODUCT_AREA
# MAGIC ,NOTIF_CODE	as	NOTIF_CODE
# MAGIC ,G_INFOSATZ	as	G_INFOSATZ
# MAGIC ,PPM_METER	as	PPM_METER
# MAGIC ,row_number() over (partition by MATNR order by MATNR desc) as rn
# MAGIC from raw.sap_0uc_devcat_attr d
# MAGIC left join CUSTOMER_BILLING.S4H_0UC_FUNKLAS_TEXT fk 
# MAGIC on fk.FUNKLAS = d.FUNKLAS
# MAGIC left join CUSTOMER_BILLING.S4H_0UC_BAUKLAS_TEXT bk 
# MAGIC on bk.BAUKLAS = d.BAUKLAS and SPRAS ='E'
# MAGIC left join 0UC_REGGRP_TEXT b
# MAGIC on d.ZWGRUPPE = b.ZWGRUPPE)a
# MAGIC where a.rn=1
# MAGIC 
# MAGIC EXCEPT
# MAGIC 
# MAGIC Select *, '1' as rn
# MAGIC from cleansedtable

# COMMAND ----------

# DBTITLE 1,[Verify] Target to Source Comparison
# MAGIC %sql
# MAGIC Select *, '1' as rn
# MAGIC from cleansedtable
# MAGIC 
# MAGIC except
# MAGIC 
# MAGIC select * from (
# MAGIC SELECT
# MAGIC MATNR	as	materialNumber
# MAGIC ,KOMBINAT	as	deviceCategoryCombination
# MAGIC ,FUNKLAS	as	functionClassCode
# MAGIC ,fk.FUNKTXT	as	functionClass
# MAGIC ,BAUKLAS	as	constructionClassCode
# MAGIC ,bk.BAUKLTXT	as	constructionClass
# MAGIC ,BAUFORM	as	deviceCategoryDescription
# MAGIC ,BAUTXT	as	deviceCategoryName
# MAGIC ,PREISKLA	as	Price_Class_NM
# MAGIC ,PTBNUM	as	ptiNumber
# MAGIC ,DVGWNUM	as	ggwaNumber
# MAGIC ,BGLKZ	as	certificationRequirementType
# MAGIC ,VLZEITT	as	calibrationValidityYears
# MAGIC ,VLZEITTI	as	internalCertificationYears
# MAGIC ,VLZEITN	as	VLZEITN
# MAGIC ,VLKZBAU	as	VLKZBAU
# MAGIC ,ZWGRUPPE	as	registerGroupCode
# MAGIC ,EAGRUPPE	as	Input_Output_Group_CD
# MAGIC ,MESSART	as	Measurement_Type_CD
# MAGIC ,b.EZWG_INFO	as	registerGroup
# MAGIC ,UEBERVER	as	transformationRatio
# MAGIC ,BGLNETZ	as	Install_Device_Certify_IND
# MAGIC ,WGRUPPE	as	Winding_Group_CD
# MAGIC ,PRIMWNR1	as	Active_Primary_Winding_NUM
# MAGIC ,SEKWNR1	as	Active_Secondary_Winding_NUM
# MAGIC ,PRIMWNR2	as	PRIMWNR2
# MAGIC ,SEKWNR2	as	SEKWNR2
# MAGIC ,AENAM	as	changedBy
# MAGIC ,AEDAT	as	lastChangedDate
# MAGIC ,ZSPANNS	as	ZSPANNS
# MAGIC ,ZSTROMS	as	ZSTROMS
# MAGIC ,ZSPANNP	as	ZSPANNP
# MAGIC ,ZSTROMP	as	ZSTROMP
# MAGIC ,GRPMATNR	as	GRPMATNR
# MAGIC ,ORDER_CODE	as	ORDER_CODE
# MAGIC ,SPARTE	as	division
# MAGIC ,NENNBEL	as	nominalLoad
# MAGIC ,KENNZTYP	as	Vehicle_Category_CD
# MAGIC ,KENNZTYP_TXT	as	KENNZTYP_TXT
# MAGIC ,STELLPLATZ	as	containerSpaceCount
# MAGIC ,HOEHEBEH	as	containerCategoryHeight
# MAGIC ,BREITEBEH	as	containerCategoryWidth
# MAGIC ,TIEFEBEH	as	containerCategoryDepth
# MAGIC ,ABMEIH	as	ABMEIH
# MAGIC ,LADEZEIT	as	LADEZEIT
# MAGIC ,LADZ_EIH	as	LADZ_EIH
# MAGIC ,LADEVOL	as	LADEVOL
# MAGIC ,LADV_EIH	as	LADV_EIH
# MAGIC ,GEW_ZUL	as	GEW_ZUL
# MAGIC ,GEWEIH	as	GEWEIH
# MAGIC ,EIGENTUM	as	EIGENTUM
# MAGIC ,EIGENTUM_TXT	as	EIGENTUM_TXT
# MAGIC ,LADV_TXT	as	LADV_TXT
# MAGIC ,LADZ_TXT	as	LADZ_TXT
# MAGIC ,GEW_TXT	as	GEW_TXT
# MAGIC ,ABM_TXT	as	ABM_TXT
# MAGIC ,PRODUCT_AREA	as	PRODUCT_AREA
# MAGIC ,NOTIF_CODE	as	NOTIF_CODE
# MAGIC ,G_INFOSATZ	as	G_INFOSATZ
# MAGIC ,PPM_METER	as	PPM_METER
# MAGIC ,row_number() over (partition by MATNR order by MATNR desc) as rn
# MAGIC from raw.sap_0uc_devcat_attr d
# MAGIC left join CUSTOMER_BILLING.S4H_0UC_FUNKLAS_TEXT fk 
# MAGIC on fk.FUNKLAS = d.FUNKLAS
# MAGIC left join CUSTOMER_BILLING.S4H_0UC_BAUKLAS_TEXT bk 
# MAGIC on bk.BAUKLAS = d.BAUKLAS and SPRAS ='E'
# MAGIC left join 0UC_REGGRP_TEXT b
# MAGIC on d.ZWGRUPPE = b.ZWGRUPPE)a
# MAGIC where a.rn=1

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
# MAGIC select * from raw.sap_0uc_devcat_attr

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
# MAGIC  from raw.sap_0uc_devcat_attr

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
# MAGIC  from raw.sap_0uc_devcat_attr 
# MAGIC  except
# MAGIC  select * from Source

# COMMAND ----------

# DBTITLE 0,[Result] Load Count Result into DataFrame
lakedf = spark.sql("select * from raw.sap_0uc_devcat_attr")

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

# DBTITLE 1,[Verification] Compare Target and Source Data (Template)
# MAGIC %sql
# MAGIC select * from Source
# MAGIC except
# MAGIC select * from Target
