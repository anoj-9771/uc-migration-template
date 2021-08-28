# Databricks notebook source
##################################################################
#Master Notebook
#1.Include all util user function for the notebook
#2.Include all dimension user function for the notebook
#3.Include all fact related user function for the notebook
#4.Define and get Widgets/Parameters
#5.Spark Config
#6.Function: Load data into Curated delta table
#7.Function: Load Dimensions
#8.Function: Create stage and curated database if not exist
#9.Flag Dimension/Fact load
#10.Function: Main - ETL
#11.Call Main function
#12.Exit Notebook
##################################################################

# COMMAND ----------

# DBTITLE 1,1. Include all util user functions for this notebook
# MAGIC %run ./includes/util-common

# COMMAND ----------

# DBTITLE 1,2. Include all dimension related user function for the notebook
# MAGIC %run ./functions/common-functions-dimensions

# COMMAND ----------

# DBTITLE 1,3. Include all fact related user function for the notebook
# MAGIC %run ./functions/common-functions-facts

# COMMAND ----------

# DBTITLE 1,4. Define and get Widgets/Parameters
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

# DBTITLE 1,5. Spark Config
# When set to true Spark SQL will automatically select a compression codec for each column based on statistics of the data.
spark.conf.set("spark.sql.inMemoryColumnarStorage.compressed",True)

# Limit of total size of serialized results of all partitions for each Spark action (e.g. collect) in bytes. Should be at least 1M, or 0 for unlimited. Jobs will be aborted if the total size is above this limit. Having a high limit may cause out-of-memory errors in driver (depends on spark.driver.memory and memory overhead of objects in JVM). Setting a proper limit can protect the driver from out-of-memory errors.
#spark.conf.set("spark.driver.maxResultSize",0)

#Configures the maximum size in bytes for a table that will be broadcast to all worker nodes when performing a join. By setting this value to -1 broadcasting can be disabled.
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

# COMMAND ----------

# DBTITLE 1,Test - Remove it
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

# spark.sql("DROP TABLE curated.dimproperty")
# spark.sql("DROP TABLE stage.dimproperty")

# df = spark.sql("SELECT \
#                     propertyId, \
#                     _RecordStart, \
#                     CAST(ROW_NUMBER() OVER (ORDER BY 1) AS bigint) + 0 AS dimPropertySK \
#                   FROM curated.dimProperty \
#                   WHERE dimPropertySK IS NULL") 
               
# # if df.count() > df.dropDuplicates(['propertyId']).count():
# #     raise ValueError('Data has duplicates')

# df \
#     .groupby(['propertyId']) \
#     .count() \
#     .where('count > 1') \
#     .sort('count', ascending=True) \
#     .show()

# # sapisu0ucConbjAttr2Df = sapisu0ucConbjAttr2Df.dropDuplicates()
# # sapisu0ucConbjAttr2Df \
# #     .groupby(['propertyNumber']) \
# #     .count() \
# #     .where('count > 1') \
# #     .sort('count', ascending=True) \
# #     .show()

# # display(sapisu0ucConbjAttr2Df.filter("PropertyNumber == 6206050"))

# df = spark.sql("select * from cleansed.stg_sapisu_0uc_connobj_attr_2 where haus = 5471789")
# df = spark.sql("select * from curated.dimProperty where propertyId = 5471789")
# display(df)
  
# df = spark.sql("select *, coalesce(lead(validFromDate) over (partition by propertyNumber order by validFromDate)-1, to_date('9999-12-31', 'yyyy-mm-dd'))  as propertyEndDate from cleansed.t_sapisu_0uc_connobj_attr_2 where _RecordCurrent = 1 and _RecordDeleted = 0")

# df = spark.sql("select *, coalesce(lead(propertyTypeEffectiveFrom) over (partition by propertyNumber order by propertyTypeEffectiveFrom)-1, to_date('9999-12-31', 'yyyy-mm-dd'))  as propertyEndDate from cleansed.t_access_z309_tproperty where _RecordCurrent = 1 and _RecordDeleted = 0")

# df = spark.sql("select * from cleansed.stg_sapisu_0uc_connobj_attr_2 where haus = 6206050")
# df = spark.sql("select * from cleansed.t_sapisu_0uc_connobj_attr_2 where propertyNumber = 6206050")

# display(df)

# df = spark.sql("WITH UpdateSK \
# AS (SELECT \
#   propertyId, \
#   sourceSystemCode, \
#   propertyStartDate, \
#   _RecordStart, \
#   CAST(ROW_NUMBER() OVER (ORDER BY 1) AS bigint) + 0 AS DimPropertySK \
# FROM curated.DimProperty \
# WHERE DimPropertySK IS NULL) \
# MERGE INTO curated.DimProperty TGT USING UpdateSK SRC ON SRC.propertyId = TGT.propertyId AND SRC.sourceSystemCode = TGT.sourceSystemCode AND SRC.propertyStartDate = TGT.propertyStartDate AND SRC._RecordStart = TGT._RecordStart WHEN MATCHED THEN UPDATE SET DimPropertySK = SRC.DimPropertySK")

# df = spark.sql("SELECT \
#   propertyId, \
#   sourceSystemCode, \
#   propertyStartDate, \
#   _RecordStart, \
#   CAST(ROW_NUMBER() OVER (ORDER BY 1) AS bigint) + 0 AS DimPropertySK \
# FROM curated.DimProperty \
# WHERE DimPropertySK IS NULL \
# ")

# if df.count() > df.dropDuplicates(['propertyId','sourceSystemCode', 'propertyStartDate']).count():
#     raise ValueError('Data has duplicates')
    
# df \
#     .groupby(['propertyId','sourceSystemCode', 'propertyStartDate']) \
#     .count() \
#     .where('count > 1') \
#     .sort('count', ascending=True) \
#     .show()

# df = spark.sql("select * from curated.dimProperty where propertyId = 6206051")

# display(df)

# accessZ309TpropertyDf = spark.sql("select propertyNumber, 'Access' as sourceSystemCode, propertyTypeEffectiveFrom as propertyStartDate, \
#                                             coalesce(lead(propertyTypeEffectiveFrom) over (partition by propertyNumber order by propertyTypeEffectiveFrom)-1, \
#                                             to_date('9999-12-31', 'yyyy-mm-dd'))  as propertyEndDate, \
#                                             propertyType, superiorPropertyType, LGA, \
#                                             CASE WHEN propertyAreaTypeCode == 'H' THEN  propertyArea * 10000 \
#                                             ELSE propertyArea END AS propertyArea \
#                                      from cleansed.t_access_z309_tproperty \
#                                      where _RecordCurrent = 1 and _RecordDeleted = 0")
# #   accessZ309TpropertyDf = accessZ309TpropertyDf.withColumn("sourceSystemCode", lit("Access"))
# #   accessZ309TpropertyDf = accessZ309TpropertyDf.dropDuplicates() #Please remove once upstream data is fixed
# #   accessZ309TpropertyDf = accessZ309TpropertyDf.selectExpr("propertyNumber","sourceSystemCode","propertyTypeEffectiveFrom", \
# #                                                              "propertyEndDate","propertyType","superiorPropertyType","LGA", \
# #                                                              "CASE WHEN propertyAreaTypeCode == 'H' THEN  propertyArea * 10000 \
# #                                                              ELSE propertyArea END AS propertyArea")
# #   accessZ309TpropertyDf = accessZ309TpropertyDf.withColumnRenamed("propertyTypeEffectiveFrom", "propertyStartDate")
# accessZ309TpropertyDf = accessZ309TpropertyDf.select("propertyNumber","sourceSystemCode","propertyStartDate","propertyEndDate", \
#                                                        "propertyType","superiorPropertyType","propertyArea","LGA")
# display(accessZ309TpropertyDf)

#   sapisu0ucConbjAttr2Df = spark.sql("select propertyNumber, 'SAP' as sourceSystemCode,inferiorPropertyType as PropertyType, superiorPropertyType, \
#                                             architecturalObjectInternalId, validFromDate as propertyStartDate, LGA,\
#                                             coalesce(lead(validFromDate) over (partition by propertyNumber order by validFromDate)-1, \
#                                             to_date('9999-12-31', 'yyyy-mm-dd'))  as propertyEndDate \
#                                      from cleansed.t_sapisu_0uc_connobj_attr_2 \
#                                      where _RecordCurrent = 1 and _RecordDeleted = 0")
# #   sapisu0ucConbjAttr2Df = sapisu0ucConbjAttr2Df.withColumn("sourceSystemCode", lit("SAP"))
# #   sapisu0ucConbjAttr2Df = sapisu0ucConbjAttr2Df.dropDuplicates() #Please remove once upstream data is fixed
# #   sapisu0ucConbjAttr2Df = sapisu0ucConbjAttr2Df.selectExpr("propertyNumber","sourceSystemCode","inferiorPropertyType","superiorPropertyType", \
# #                                                            "architecturalObjectInternalId","validFromDate","propertyEndDate","LGA")
# #   sapisu0ucConbjAttr2Df = sapisu0ucConbjAttr2Df.withColumnRenamed("inferiorPropertyType", "PropertyType")\
# #                                                 .withColumnRenamed("validFromDate", "propertyStartDate")
# display(sapisu0ucConbjAttr2Df)

# sapisuVibdaoDf = spark.sql("select architecturalObjectInternalId, \
#                                    CASE WHEN hydraAreaUnit == 'HAR' THEN  hydraCalculatedArea * 10000 \
#                                         WHEN hydraAreaUnit == 'M2' THEN  hydraCalculatedArea \
#                                         ELSE null END AS propertyArea \
#                             from cleansed.t_sapisu_vibdao \
#                             where _RecordCurrent = 1 and _RecordDeleted = 0")
# sapisuVibdaoDf = sapisuVibdaoDf.dropDuplicates() #Please remove once upstream data is fixed
# #   sapisuVibdaoDf = sapisuVibdaoDf.selectExpr("architecturalObjectInternalId", \
# #                                             "CASE WHEN hydraAreaUnit == 'HAR' THEN  hydraCalculatedArea * 10000 \
# #                                                   WHEN hydraAreaUnit == 'M2' THEN  hydraCalculatedArea \
# #                                                   ELSE null END AS propertyArea") 
# display(sapisuVibdaoDf)

# df = spark.sql("select * from curated.dimproperty") #2357919
# display(df)

# COMMAND ----------

# DBTITLE 1,6. Function: Load data into Curated delta table
def TemplateEtl(df : object, entity, businessKey, AddSK = True):
  rawEntity = entity
  entity = GeneralToPascalCase(rawEntity)
  LogEtl(f"Starting {entity}.")
  
  v_COMMON_SQL_SCHEMA = "COMMON"
  v_COMMON_CURATED_DATABASE = "curated"
  v_COMMON_DATALAKE_FOLDER = "curated"
  
  DeltaSaveDataFrameToDeltaTable(df, 
                                 rawEntity, 
                                 ADS_DATALAKE_ZONE_CURATED, 
                                 v_COMMON_CURATED_DATABASE, 
                                 v_COMMON_DATALAKE_FOLDER, 
                                 ADS_WRITE_MODE_MERGE, 
                                 track_changes = True, 
                                 is_delta_extract = False, 
                                 business_key = businessKey, 
                                 AddSKColumn = AddSK, 
                                 delta_column = "", 
                                 start_counter = "0", 
                                 end_counter = "0")
    
  LogEtl(f"Finished {entity}.")

# COMMAND ----------

# DBTITLE 1, 7. Function: Load Dimensions
#Call Property function to load DimProperty
def Property():
  TemplateEtl(df=GetCommonProperty(), 
             entity="DimProperty", 
             businessKey="propertyId,sourceSystemCode,propertyEndDate",
             AddSK=True
            )

def billingDocumentSapisu():
  TemplateEtl(df=getCommonBillingDocumentSapisu(), 
             entity="dimBillingDocument", 
             businessKey="billingDocumentNumber,sourceSystemCode",
             AddSK=True
            )

def billedWaterConsumption():
  TemplateEtl(df=getBilledWaterConsumption(), 
             entity="factBilledWaterConsumption", 
             businessKey="sourceSystemCode,dimBillingDocumentSK,dimPropertySK,dimMeterSK,billingPeriodStartDateSK",
             AddSK=True
            )

def billedWaterConsumptionDaily():
  TemplateEtl(df=getBilledWaterConsumptionDaily(), 
             entity="factDailyApportionedConsumption", 
             businessKey="sourceSystemCode,consumptionDateSK,dimBillingDocumentSK,dimPropertySK,dimMeterSK",
             AddSK=True
            )

# Add New Dim here
# def Dim2_Example():
#   TemplateEtl(df=GetDim2Example(), 
#              entity="Dim2Example",
#              businessKey="col1",
#              AddSK=True
#             )


# COMMAND ----------

# DBTITLE 1,8. Function: Create stage and curated database if not exist
def DatabaseChanges():
  #CREATE stage AND curated DATABASES IS NOT PRESENT
  spark.sql("CREATE DATABASE IF NOT EXISTS stage")
  spark.sql("CREATE DATABASE IF NOT EXISTS curated")  


# COMMAND ----------

# DBTITLE 1,9. Flag Dimension/Fact load
LoadDimensions = False
LoadFacts = True

# COMMAND ----------

# DBTITLE 1,10. Function: Main - ETL
def Main():
  DatabaseChanges()
  #==============
  # DIMENSIONS
  #==============
  
  if LoadDimensions:
    LogEtl("Start Dimensions")
    #Property()
    billingDocumentSapisu()
    #Add new Dim here()
    
    LogEtl("End Dimensions")

  #==============
  # FACTS
  #==============
  if LoadFacts:
    LogEtl("Start Facts")
    #billedWaterConsumption()
    billedWaterConsumptionDaily()
    #fact2()
  
    LogEtl("End Facts")   
  

# COMMAND ----------

# DBTITLE 1,11. Call Main function
Main()

# COMMAND ----------

# DBTITLE 1,12. Exit Notebook
dbutils.notebook.exit("1")
