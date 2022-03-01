# Databricks notebook source
#config parameters
source = 'ISU' #either CRM or ISU
table = '0UC_REGIST_ATTR'

environment = 'test'
storage_account_name = "sablobdaftest01"
storage_account_access_key = dbutils.secrets.get(scope="TestScope",key="test-sablob-key")
containerName = "archive"


# COMMAND ----------

spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")

# COMMAND ----------

# MAGIC %run ../../includes/tableEvaluation

# COMMAND ----------

lakedf = spark.sql("select * from cleansed.${vars.table}")

# COMMAND ----------

lakedf.printSchema()

# COMMAND ----------

# DBTITLE 1,[Source] with mapping
# MAGIC %sql
# MAGIC select
# MAGIC equipmentNumber
# MAGIC ,registerNumber
# MAGIC ,validToDate
# MAGIC ,validFromDate
# MAGIC ,logicalRegisterNumber
# MAGIC ,divisionCategoryCode
# MAGIC ,divisionCategory
# MAGIC ,registerIdCode
# MAGIC ,registerId
# MAGIC ,registerTypeCode
# MAGIC ,registerType
# MAGIC ,registerCategoryCode
# MAGIC ,registerCategory
# MAGIC ,reactiveApparentOrActiveRegister
# MAGIC ,unitOfMeasurementMeterReading
# MAGIC ,doNotReadIndicator
# MAGIC ,altitudeCorrectionPressure
# MAGIC ,setGasLawDeviationFactor
# MAGIC ,actualGasLawDeviationFactor
# MAGIC ,gasCorrectionPressure
# MAGIC ,intervalLengthId
# MAGIC ,deletedIndicator
# MAGIC from
# MAGIC (select
# MAGIC EQUNR as equipmentNumber
# MAGIC ,ZWNUMMER as registerNumber
# MAGIC ,BIS as validToDate
# MAGIC ,AB as validFromDate
# MAGIC ,LOGIKZW as logicalRegisterNumber
# MAGIC ,SPARTYP as divisionCategoryCode
# MAGIC ,b.sectorCategory as divisionCategory
# MAGIC ,ZWKENN as registerIdCode
# MAGIC ,c.registerId as registerId
# MAGIC ,ZWART as registerTypeCode
# MAGIC ,d.registerType as registerType
# MAGIC ,ZWTYP as registerCategoryCode
# MAGIC ,f.domainValueText as registerCategory
# MAGIC ,BLIWIRK as reactiveApparentOrActiveRegister
# MAGIC ,MASSREAD as unitOfMeasurementMeterReading
# MAGIC ,NABLESEN as doNotReadIndicator
# MAGIC ,HOEKORR as altitudeCorrectionPressure
# MAGIC ,KZAHLE as setGasLawDeviationFactor
# MAGIC ,KZAHLT as actualGasLawDeviationFactor
# MAGIC ,CRGPRESS as gasCorrectionPressure
# MAGIC ,INTSIZEID as intervalLengthId
# MAGIC ,LOEVM as deletedIndicator
# MAGIC ,row_number() over (partition by EQUNR,ZWNUMMER,BIS order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table}
# MAGIC left join cleansed.isu_0UCDIVISCAT_TEXT b
# MAGIC on SPARTYP = b.sectorCategoryCode  
# MAGIC left join  cleansed.isu_TE065T c
# MAGIC on SPARTYP = c.divisionCategoryCode and ZWKENN = c.registerIdCode 
# MAGIC left join  cleansed.isu_TE523T d
# MAGIC on ZWART = d.registerTypeCode 
# MAGIC left join  cleansed.isu_DD07T f
# MAGIC on ZWTYP = f.domainValueKey and f.domainName ='E_ZWTYP'
# MAGIC --and f.DDLANGUAGE ='E' 
# MAGIC )a where  a.rn = 1

# COMMAND ----------

# DBTITLE 1,[Verification] Count Check
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from cleansed.${vars.table}
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from (select
# MAGIC equipmentNumber
# MAGIC ,registerNumber
# MAGIC ,validToDate
# MAGIC ,validFromDate
# MAGIC ,logicalRegisterNumber
# MAGIC ,divisionCategoryCode
# MAGIC ,divisionCategory
# MAGIC ,registerIdCode
# MAGIC ,registerId
# MAGIC ,registerTypeCode
# MAGIC ,registerType
# MAGIC ,registerCategoryCode
# MAGIC ,registerCategory
# MAGIC ,reactiveApparentOrActiveRegister
# MAGIC ,unitOfMeasurementMeterReading
# MAGIC ,doNotReadIndicator
# MAGIC ,altitudeCorrectionPressure
# MAGIC ,setGasLawDeviationFactor
# MAGIC ,actualGasLawDeviationFactor
# MAGIC ,gasCorrectionPressure
# MAGIC ,intervalLengthId
# MAGIC ,deletedIndicator
# MAGIC from
# MAGIC (select
# MAGIC EQUNR as equipmentNumber
# MAGIC ,ZWNUMMER as registerNumber
# MAGIC ,BIS as validToDate
# MAGIC ,AB as validFromDate
# MAGIC ,LOGIKZW as logicalRegisterNumber
# MAGIC ,SPARTYP as divisionCategoryCode
# MAGIC ,b.sectorCategory as divisionCategory
# MAGIC ,ZWKENN as registerIdCode
# MAGIC ,c.registerId as registerId
# MAGIC ,ZWART as registerTypeCode
# MAGIC ,d.registerType as registerType
# MAGIC ,ZWTYP as registerCategoryCode
# MAGIC ,f.domainValueText as registerCategory
# MAGIC ,BLIWIRK as reactiveApparentOrActiveRegister
# MAGIC ,MASSREAD as unitOfMeasurementMeterReading
# MAGIC ,NABLESEN as doNotReadIndicator
# MAGIC ,HOEKORR as altitudeCorrectionPressure
# MAGIC ,KZAHLE as setGasLawDeviationFactor
# MAGIC ,KZAHLT as actualGasLawDeviationFactor
# MAGIC ,CRGPRESS as gasCorrectionPressure
# MAGIC ,INTSIZEID as intervalLengthId
# MAGIC ,LOEVM as deletedIndicator
# MAGIC ,row_number() over (partition by EQUNR,ZWNUMMER,BIS order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table}
# MAGIC left join cleansed.isu_0UCDIVISCAT_TEXT b
# MAGIC on SPARTYP = b.sectorCategoryCode  
# MAGIC left join  cleansed.isu_TE065T c
# MAGIC on SPARTYP = c.divisionCategoryCode and ZWKENN = c.registerIdCode 
# MAGIC left join  cleansed.isu_TE523T d
# MAGIC on ZWART = d.registerTypeCode 
# MAGIC left join  cleansed.isu_DD07T f
# MAGIC on ZWTYP = f.domainValueKey and f.domainName ='E_ZWTYP'
# MAGIC --and f.DDLANGUAGE ='E' 
# MAGIC )a where  a.rn = 1)

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT equipmentNumber,registerNumber,validToDate
# MAGIC , COUNT (*) as count
# MAGIC FROM cleansed.${vars.table}
# MAGIC GROUP BY equipmentNumber,registerNumber,validToDate
# MAGIC HAVING COUNT (*) > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT * FROM (
# MAGIC SELECT
# MAGIC *,
# MAGIC row_number() OVER(PARTITION BY equipmentNumber,registerNumber,validToDate  order by equipmentNumber,registerNumber,validToDate) as rn
# MAGIC FROM  cleansed.${vars.table}
# MAGIC )a where a.rn > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Source and Target Data
# MAGIC %sql
# MAGIC select
# MAGIC equipmentNumber
# MAGIC ,registerNumber
# MAGIC ,validToDate
# MAGIC ,validFromDate
# MAGIC ,logicalRegisterNumber
# MAGIC ,divisionCategoryCode
# MAGIC ,divisionCategory
# MAGIC ,registerIdCode
# MAGIC ,registerId
# MAGIC ,registerTypeCode
# MAGIC ,registerType
# MAGIC ,registerCategoryCode
# MAGIC ,registerCategory
# MAGIC ,reactiveApparentOrActiveRegister
# MAGIC ,unitOfMeasurementMeterReading
# MAGIC ,doNotReadIndicator
# MAGIC ,altitudeCorrectionPressure
# MAGIC ,setGasLawDeviationFactor
# MAGIC ,actualGasLawDeviationFactor
# MAGIC ,gasCorrectionPressure
# MAGIC ,intervalLengthId
# MAGIC ,deletedIndicator
# MAGIC from
# MAGIC (select
# MAGIC EQUNR as equipmentNumber
# MAGIC ,ZWNUMMER as registerNumber
# MAGIC ,BIS as validToDate
# MAGIC ,AB as validFromDate
# MAGIC ,LOGIKZW as logicalRegisterNumber
# MAGIC ,SPARTYP as divisionCategoryCode
# MAGIC ,b.sectorCategory as divisionCategory
# MAGIC ,ZWKENN as registerIdCode
# MAGIC ,c.registerId as registerId
# MAGIC ,ZWART as registerTypeCode
# MAGIC ,d.registerType as registerType
# MAGIC ,ZWTYP as registerCategoryCode
# MAGIC ,f.domainValueText as registerCategory
# MAGIC ,BLIWIRK as reactiveApparentOrActiveRegister
# MAGIC ,MASSREAD as unitOfMeasurementMeterReading
# MAGIC ,NABLESEN as doNotReadIndicator
# MAGIC ,HOEKORR as altitudeCorrectionPressure
# MAGIC ,KZAHLE as setGasLawDeviationFactor
# MAGIC ,KZAHLT as actualGasLawDeviationFactor
# MAGIC ,CRGPRESS as gasCorrectionPressure
# MAGIC ,INTSIZEID as intervalLengthId
# MAGIC ,LOEVM as deletedIndicator
# MAGIC ,row_number() over (partition by EQUNR,ZWNUMMER,BIS order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table}
# MAGIC left join cleansed.isu_0UCDIVISCAT_TEXT b
# MAGIC on SPARTYP = b.sectorCategoryCode  
# MAGIC left join  cleansed.isu_TE065T c
# MAGIC on SPARTYP = c.divisionCategoryCode and ZWKENN = c.registerIdCode 
# MAGIC left join  cleansed.isu_TE523T d
# MAGIC on ZWART = d.registerTypeCode 
# MAGIC left join  cleansed.isu_DD07T f
# MAGIC on ZWTYP = f.domainValueKey and f.domainName ='E_ZWTYP'
# MAGIC --and f.DDLANGUAGE ='E' 
# MAGIC )a where  a.rn = 1
# MAGIC except
# MAGIC select
# MAGIC equipmentNumber
# MAGIC ,registerNumber
# MAGIC ,validToDate
# MAGIC ,validFromDate
# MAGIC ,logicalRegisterNumber
# MAGIC ,divisionCategoryCode
# MAGIC ,divisionCategory
# MAGIC ,registerIdCode
# MAGIC ,registerId
# MAGIC ,registerTypeCode
# MAGIC ,registerType
# MAGIC ,registerCategoryCode
# MAGIC ,registerCategory
# MAGIC ,reactiveApparentOrActiveRegister
# MAGIC ,unitOfMeasurementMeterReading
# MAGIC ,doNotReadIndicator
# MAGIC ,altitudeCorrectionPressure
# MAGIC ,setGasLawDeviationFactor
# MAGIC ,actualGasLawDeviationFactor
# MAGIC ,gasCorrectionPressure
# MAGIC ,intervalLengthId
# MAGIC ,deletedIndicator
# MAGIC from
# MAGIC cleansed.${vars.table}

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Target and Source Data
# MAGIC %sql
# MAGIC select
# MAGIC equipmentNumber
# MAGIC ,registerNumber
# MAGIC ,validToDate
# MAGIC ,validFromDate
# MAGIC ,logicalRegisterNumber
# MAGIC ,divisionCategoryCode
# MAGIC ,divisionCategory
# MAGIC ,registerIdCode
# MAGIC ,registerId
# MAGIC ,registerTypeCode
# MAGIC ,registerType
# MAGIC ,registerCategoryCode
# MAGIC ,registerCategory
# MAGIC ,reactiveApparentOrActiveRegister
# MAGIC ,unitOfMeasurementMeterReading
# MAGIC ,doNotReadIndicator
# MAGIC ,altitudeCorrectionPressure
# MAGIC ,setGasLawDeviationFactor
# MAGIC ,actualGasLawDeviationFactor
# MAGIC ,gasCorrectionPressure
# MAGIC ,intervalLengthId
# MAGIC ,deletedIndicator
# MAGIC from
# MAGIC cleansed.${vars.table}
# MAGIC except
# MAGIC select
# MAGIC equipmentNumber
# MAGIC ,registerNumber
# MAGIC ,validToDate
# MAGIC ,validFromDate
# MAGIC ,logicalRegisterNumber
# MAGIC ,divisionCategoryCode
# MAGIC ,divisionCategory
# MAGIC ,registerIdCode
# MAGIC ,registerId
# MAGIC ,registerTypeCode
# MAGIC ,registerType
# MAGIC ,registerCategoryCode
# MAGIC ,registerCategory
# MAGIC ,reactiveApparentOrActiveRegister
# MAGIC ,unitOfMeasurementMeterReading
# MAGIC ,doNotReadIndicator
# MAGIC ,altitudeCorrectionPressure
# MAGIC ,setGasLawDeviationFactor
# MAGIC ,actualGasLawDeviationFactor
# MAGIC ,gasCorrectionPressure
# MAGIC ,intervalLengthId
# MAGIC ,deletedIndicator
# MAGIC from
# MAGIC (select
# MAGIC EQUNR as equipmentNumber
# MAGIC ,ZWNUMMER as registerNumber
# MAGIC ,BIS as validToDate
# MAGIC ,AB as validFromDate
# MAGIC ,LOGIKZW as logicalRegisterNumber
# MAGIC ,SPARTYP as divisionCategoryCode
# MAGIC ,b.sectorCategory as divisionCategory
# MAGIC ,ZWKENN as registerIdCode
# MAGIC ,c.registerId as registerId
# MAGIC ,ZWART as registerTypeCode
# MAGIC ,d.registerType as registerType
# MAGIC ,ZWTYP as registerCategoryCode
# MAGIC ,f.domainValueText as registerCategory
# MAGIC ,BLIWIRK as reactiveApparentOrActiveRegister
# MAGIC ,MASSREAD as unitOfMeasurementMeterReading
# MAGIC ,NABLESEN as doNotReadIndicator
# MAGIC ,HOEKORR as altitudeCorrectionPressure
# MAGIC ,KZAHLE as setGasLawDeviationFactor
# MAGIC ,KZAHLT as actualGasLawDeviationFactor
# MAGIC ,CRGPRESS as gasCorrectionPressure
# MAGIC ,INTSIZEID as intervalLengthId
# MAGIC ,LOEVM as deletedIndicator
# MAGIC ,row_number() over (partition by EQUNR,ZWNUMMER,BIS order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table}
# MAGIC left join cleansed.isu_0UCDIVISCAT_TEXT b
# MAGIC on SPARTYP = b.sectorCategoryCode  
# MAGIC left join  cleansed.isu_TE065T c
# MAGIC on SPARTYP = c.divisionCategoryCode and ZWKENN = c.registerIdCode 
# MAGIC left join  cleansed.isu_TE523T d
# MAGIC on ZWART = d.registerTypeCode 
# MAGIC left join  cleansed.isu_DD07T f
# MAGIC on ZWTYP = f.domainValueKey and f.domainName ='E_ZWTYP'
# MAGIC --and f.DDLANGUAGE ='E' 
# MAGIC )a where  a.rn = 1
