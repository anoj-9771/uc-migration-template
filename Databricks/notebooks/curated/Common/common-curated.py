# Databricks notebook source
#################
#Master Notebook
#Add comments
###########

# COMMAND ----------

# MAGIC %run ./includes/util-common

# COMMAND ----------

# MAGIC %run ./functions/common-functions-dimensions

# COMMAND ----------

# MAGIC %run ./functions/common-functions-facts

# COMMAND ----------

# DBTITLE 1,Parameters
#Set Parameters
dbutils.widgets.removeAll()

dbutils.widgets.text("Start_Date","")
dbutils.widgets.text("End_Date","")

#Get Parameters
start_date = dbutils.widgets.get("Start_Date")
end_date = dbutils.widgets.get("End_Date")

params = {"start_date": start_date, "end_date": end_date}

#DEFAULT IF ITS BLANK
start_date = "2000-01-01" if not start_date else start_date
end_date = "9999-12-31" if not end_date else end_date

#Print Date Range
print(f"Start_Date = {start_date}| End_Date = {end_date}")

# COMMAND ----------

# DBTITLE 1,Spark Config
# When set to true Spark SQL will automatically select a compression codec for each column based on statistics of the data.
spark.conf.set("spark.sql.inMemoryColumnarStorage.compressed",True)

# Limit of total size of serialized results of all partitions for each Spark action (e.g. collect) in bytes. Should be at least 1M, or 0 for unlimited. Jobs will be aborted if the total size is above this limit. Having a high limit may cause out-of-memory errors in driver (depends on spark.driver.memory and memory overhead of objects in JVM). Setting a proper limit can protect the driver from out-of-memory errors.
#spark.conf.set("spark.driver.maxResultSize",0)

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

# COMMAND ----------

# DBTITLE 1,All DataFrames
#Common TABLES
#DimProperty
accessZ309TpropertyDf = DeltaTableAsCurrent("cleansed.t_access_z309_tproperty")
accessZ309TpropertyDf = accessZ309TpropertyDf.withColumn("sourceSystemCode", lit("Access"))
sapisu0ucConbjAttr2Df = DeltaTableAsCurrent("cleansed.t_sapisu_0uc_connobj_attr_2")
sapisu0ucConbjAttr2Df = sapisu0ucConbjAttr2Df.withColumn("sourceSystemCode", lit("SAP"))
sapisuVibdaoDf = DeltaTableAsCurrent("cleansed.t_sapisu_vibdao")

#Load other Dim tables here below


# COMMAND ----------

# #Remove - For testing
# # from pyspark.sql import functions as F
# # accessZ309TpropertyDf1 = accessZ309TpropertyDf.withColumn("sourceSystemCode", lit("Access")).drop()
# # # df = accessZ309TpropertyDf1.select("propertyNumber","sourceSystemCode","propertyTypeEffectiveFrom","propertyType","superiorPropertyType","propertyArea","propertyAreaTypeCode","LGA")

# # df = accessZ309TpropertyDf1.selectExpr("propertyNumber","sourceSystemCode","propertyTypeEffectiveFrom","propertyType","superiorPropertyType","LGA","CASE WHEN propertyAreaTypeCode == 'H' THEN  propertyArea * 10000 ELSE propertyArea END AS propertyArea")
# # df = df.withColumnRenamed("propertyTypeEffectiveFrom", "propertyStartDate")
# # # accessZ309TpropertyDf1 = accessZ309TpropertyDf1.withColumn(col("propertyArea"),when(accessZ309TpropertyDf1.propertyAreaTypeCode == 'H', accessZ309TpropertyDf1.propertyArea * 10000).otherwise(col("propertyArea")))

# # # df = accessZ309TpropertyDf1.selectExpr("propertyNumber","sourceSystemCode","propertyTypeEffectiveFrom","propertyType","superiorPropertyType","propertyAreaTypeCode","LGA","CASE WHEN propertyAreaTypeCode == 'H' THEN  propertyArea * 10000 ELSE propertyArea END AS propertyArea").show()

# # #df = df.withColumn(df[propertyArea], when(df[propertyAreaTypeCode]=="H",df[propertyArea] * 10000).otherwise(df[propertyArea]))

# # sapisu0ucConbjAttr2Df1 = sapisu0ucConbjAttr2Df.selectExpr("propertyNumber","sourceSystemCode", \
# #                                                            "inferiorPropertyType","superiorPropertyType","architecturalObjectInternalId","LGA")
# # sapisu0ucConbjAttr2Df1 = sapisu0ucConbjAttr2Df1.withColumn("propertyStartDate", lit("9999-12-31")).withColumn("propertyEndDate", lit("9999-12-31"))
# # sapisu0ucConbjAttr2Df1 = sapisu0ucConbjAttr2Df1.withColumnRenamed("inferiorPropertyType", "PropertyType")

# accessZ309TpropertyDf = accessZ309TpropertyDf.selectExpr("propertyNumber","sourceSystemCode","propertyTypeEffectiveFrom", \
#                                                            "propertyType","superiorPropertyType","LGA", \
#                                                            "CASE WHEN propertyAreaTypeCode == 'H' THEN  propertyArea * 10000 \
#                                                            ELSE propertyArea END AS propertyArea")
# accessZ309TpropertyDf = accessZ309TpropertyDf.withColumnRenamed("propertyTypeEffectiveFrom", "propertyStartDate")
# accessZ309TpropertyDf = accessZ309TpropertyDf.withColumn("propertyEndDate", lit("9999-12-31"))
# accessZ309TpropertyDf = accessZ309TpropertyDf.select("propertyNumber","sourceSystemCode","propertyStartDate","propertyEndDate", \
#                                               "propertyType","superiorPropertyType","propertyArea","LGA")

# sapisu0ucConbjAttr2Df = sapisu0ucConbjAttr2Df.selectExpr("propertyNumber","sourceSystemCode","inferiorPropertyType","superiorPropertyType", \
#                                                          "architecturalObjectInternalId","validFromDate","LGA")
# sapisu0ucConbjAttr2Df = sapisu0ucConbjAttr2Df.withColumn("propertyEndDate", lit("9999-12-31"))
# sapisu0ucConbjAttr2Df = sapisu0ucConbjAttr2Df.withColumnRenamed("inferiorPropertyType", "PropertyType")\
#                                               .withColumnRenamed("validFromDate", "propertyStartDate")

# # sapisuVibdaoDf = sapisuVibdaoDf.selectExpr("architecturalObjectInternalId","validFromDate","validToDate", \
# #                                           "CASE WHEN hydraAreaUnit == 'HAR' THEN  hydraCalculatedArea * 10000 \
# #                                                 WHEN hydraAreaUnit == 'M2' THEN  hydraCalculatedArea \
# #                                                 ELSE null END AS propertyArea")

# sapisuVibdaoDf = sapisuVibdaoDf.selectExpr("architecturalObjectInternalId", \
#                                           "CASE WHEN hydraAreaUnit == 'HAR' THEN  hydraCalculatedArea * 10000 \
#                                                 WHEN hydraAreaUnit == 'M2' THEN  hydraCalculatedArea \
#                                                 ELSE null END AS propertyArea")

# df = sapisu0ucConbjAttr2Df.join(sapisuVibdaoDf, sapisu0ucConbjAttr2Df.architecturalObjectInternalId == sapisuVibdaoDf.architecturalObjectInternalId, how="inner")\
#                           .drop(sapisuVibdaoDf.architecturalObjectInternalId).drop(sapisu0ucConbjAttr2Df.architecturalObjectInternalId)
# df = df.select("propertyNumber","sourceSystemCode","propertyStartDate","propertyEndDate", \
#                                               "propertyType","superiorPropertyType","propertyArea","LGA")

# df = accessZ309TpropertyDf.union(df) 
# # this is working fine


  
# display(df)

# COMMAND ----------

# df = df.selectExpr( \
# 	 "propertyNumber  as propertyId" \
#     ,"sourceSystemCode" \
#     ,"propertyStartDate" \
#     ,"propertyEndDate" \
#     ,"sourceSystemCode as propertyType" \
#     ,"superiorPropertyType" \
#     ,"CAST(propertyArea AS DECIMAL(18,6)) as propertyArea" \
#     ,"LGA" \
#   )
# display(df)

# COMMAND ----------

def TemplateEtl(df : object, entity, businessKey, AddSK = True):
  rawEntity = entity
  entity = GeneralToPascalCase(rawEntity)
  LogEtl(f"Starting {entity}.")
  
  v_ISU_SQL_SCHEMA = "COMMON"
  v_ISU_CURATED_DATABASE = "COMMON"
  v_ISU_DATALAKE_FOLDER = "COMMON"
  
  #CIMMergeSCD(df, rawEntity, businessKey) if appendMode==False else CIMAppend(df, rawEntity, businessKey)
  DeltaSaveDataFrameToDeltaTable(
df, rawEntity, ADS_DATALAKE_ZONE_CURATED, v_COMMON_CURATED_DATABASE, v_COMMON_DATALAKE_FOLDER, ADS_WRITE_MODE_MERGE, track_changes = True, is_delta_extract = False, business_key = businessKey, AddSKColumn = AddSK, delta_column = "", start_counter = "0", end_counter = "0")

#   delta_table = f"{v_ISU_SQL_SCHEMA}.{rawEntity}"
#   DeltaSyncToSQLEDW(delta_table, v_COMMON_SQL_SCHEMA, entity, businessKey, delta_column = "", start_counter = "0", data_load_mode = ADS_WRITE_MODE_MERGE, track_changes = True, is_delta_extract = False, schema_file_url = "", additional_property = "")
    
  LogEtl(f"Finished {entity}.")

# COMMAND ----------

def Property():
  TemplateEtl(df=GetCommonProperty(accessZ309TpropertyDf,sapisu0ucConbjAttr2Df,sapisuVibdaoDf), 
             entity="Property", 
             businessKey="propertyNumber,sourceSystemCode,propertyStartDate"
            )
# def AwardRuleset():
#   TemplateEtl(df=GetCIMAwardRuleset(ebsOneebsEbs0165AwardRuleSetDf), 
#              entity="AwardRuleset",
#              businessKey="AwardRulesetId"
#             )


# COMMAND ----------

# def UnitOfferings():
#   TemplateEtl(df=GetCIMUnitOfferings(ebsOneebsEbs0165UnitInstanceOccurrencesDf,start_date,end_date),
#   entity="UnitOfferings",
#   businessKey="UnitOfferingsKey",
#   AddSK=False)


# COMMAND ----------

# def CourseOfferings():
#   TemplateEtl(df=GetCIMCourseOfferings(ebsOneebsEbs0165UnitInstanceOccurrencesDf,start_date,end_date),
#   entity="CourseOfferings",
#   businessKey="CourseOfferingsKey",
#   AddSK=False)

# COMMAND ----------

# #TODO: PLACE THIS INTO A DEVOPS SCRIPT FOR CI/CD
# def DatabaseChanges():
#   #CREATE stage AND common DATABASES IS NOT PRESENT
#   spark.sql("CREATE DATABASE IF NOT EXISTS stage")
#   spark.sql("CREATE DATABASE IF NOT EXISTS common")  

#   #PRADA-1655 - ADD NEW LocationOrganisationCode COLUMN
#   #try:
#    # spark.sql("ALTER TABLE cim.location ADD COLUMNS ( LocationOrganisationCode string )")
#  # except:
#   #  e=False

# COMMAND ----------

LoadDimensions = True
LoadFacts = False

# COMMAND ----------

# DBTITLE 1,Main - ETL
def Main():
 # DatabaseChanges()
  #==============
  # DIMENSIONS
  #==============
  
  if LoadDimensions:
    LogEtl("Start Dimensions")
    Property()
    #AwardRuleset()
    
    LogEtl("End Dimensions")

  #==============
  # FACTS
  #==============
  if LoadFacts:
    LogEtl("Start Facts")
    #UnitOfferings()
    #CourseOfferings()
  
    LogEtl("End Facts")
    
  

# COMMAND ----------

Main()
