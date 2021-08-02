# Databricks notebook source
###########################################################################################################################
# Function: GetCommonProperty
#  GETS Property DIMENSION 
# Parameters: 
#  accessZ309TpropertyDf = t_access_z309_tproperty
#  sapisu0ucConbjAttr2Df = t_sapisu_0uc_connobj_attr_2
#  sapisuVibdaoDf = t_sapisu_vibdao
# Returns:
#  Dataframe of transformed Property
#############################################################################################################################
# Method
# 1.Create Function
# 2.ALIAS TABLES
# 3.JOIN TABLES
# 4.UNION TABLES
# 5.SELECT / TRANSFORM
#############################################################################################################################
#Create Function - Getting dataframe as parameter  
def GetCommonProperty(accessZ309TpropertyDf, sapisu0ucConbjAttr2Df, sapisuVibdaoDf):
  spark.udf.register("TidyCase", GeneralToTidyCase)  
  
  #ALIAS TABLES

  accessZ309TpropertyDf = accessZ309TpropertyDf.selectExpr("propertyNumber","sourceSystemCode","propertyTypeEffectiveFrom", \
                                                             "propertyType","superiorPropertyType","LGA", \
                                                             "CASE WHEN propertyAreaTypeCode == 'H' THEN  propertyArea * 10000 \
                                                             ELSE propertyArea END AS propertyArea")
  accessZ309TpropertyDf = accessZ309TpropertyDf.withColumnRenamed("propertyTypeEffectiveFrom", "propertyStartDate")
  accessZ309TpropertyDf = accessZ309TpropertyDf.withColumn("propertyEndDate", lit("9999-12-31"))
  accessZ309TpropertyDf = accessZ309TpropertyDf.select("propertyNumber","sourceSystemCode","propertyStartDate","propertyEndDate", \
                                                "propertyType","superiorPropertyType","propertyArea","LGA")

  sapisu0ucConbjAttr2Df = sapisu0ucConbjAttr2Df.selectExpr("propertyNumber","sourceSystemCode","inferiorPropertyType","superiorPropertyType", \
                                                           "architecturalObjectInternalId","validFromDate","LGA")
  sapisu0ucConbjAttr2Df = sapisu0ucConbjAttr2Df.withColumn("propertyEndDate", lit("9999-12-31"))
  sapisu0ucConbjAttr2Df = sapisu0ucConbjAttr2Df.withColumnRenamed("inferiorPropertyType", "PropertyType")\
                                                .withColumnRenamed("validFromDate", "propertyStartDate")

  # sapisuVibdaoDf = sapisuVibdaoDf.selectExpr("architecturalObjectInternalId","validFromDate","validToDate", \
  #                                           "CASE WHEN hydraAreaUnit == 'HAR' THEN  hydraCalculatedArea * 10000 \
  #                                                 WHEN hydraAreaUnit == 'M2' THEN  hydraCalculatedArea \
  #                                                 ELSE null END AS propertyArea")

  sapisuVibdaoDf = sapisuVibdaoDf.selectExpr("architecturalObjectInternalId", \
                                            "CASE WHEN hydraAreaUnit == 'HAR' THEN  hydraCalculatedArea * 10000 \
                                                  WHEN hydraAreaUnit == 'M2' THEN  hydraCalculatedArea \
                                                  ELSE null END AS propertyArea")   

  #JOIN TABLES
  
  df = sapisu0ucConbjAttr2Df.join(sapisuVibdaoDf, sapisu0ucConbjAttr2Df.architecturalObjectInternalId == sapisuVibdaoDf.architecturalObjectInternalId, how="inner")\
                            .drop(sapisuVibdaoDf.architecturalObjectInternalId).drop(sapisu0ucConbjAttr2Df.architecturalObjectInternalId)
  df = df.select("propertyNumber","sourceSystemCode","propertyStartDate","propertyEndDate", \
                                                "propertyType","superiorPropertyType","propertyArea","LGA")
  
  #UNION TABLES
  df = accessZ309TpropertyDf.union(df)
  
  #SELECT / TRANSFORM
  df = df.selectExpr( \
	 "propertyNumber  as propertyId" \
    ,"sourceSystemCode" \
    ,"propertyStartDate" \
    ,"propertyEndDate" \
    ,"sourceSystemCode as propertyType" \
    ,"superiorPropertyType" \
    ,"CAST(propertyArea AS DECIMAL(18,6)) as propertyArea" \
    ,"LGA" \
  )

  return df
