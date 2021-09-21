# Databricks notebook source
###########################################################################################################################
# Function: GetCommonProperty
#  GETS Property DIMENSION 
# Returns:
#  Dataframe of transformed Property
#############################################################################################################################
# Method
# 1.Create Function
# 2.Load Cleansed layer table data into dataframe and transform
# 3.JOIN TABLES
# 4.UNION TABLES
# 5.SELECT / TRANSFORM
#############################################################################################################################
#1.Create Function
def GetCommonProperty():
  
  spark.udf.register("TidyCase", GeneralToTidyCase)  
  
  #DimProperty
  #2.Load Cleansed layer table data into dataframe
  accessZ309TpropertyDf = spark.sql("select propertyNumber, 'Access' as sourceSystemCode, propertyTypeEffectiveFrom as propertyStartDate, \
                                            coalesce(lead(propertyTypeEffectiveFrom) over (partition by propertyNumber order by propertyTypeEffectiveFrom)-1, \
                                            to_date('9999-12-31', 'yyyy-mm-dd'))  as propertyEndDate, \
                                            propertyType, superiorPropertyType, LGA, \
                                            CASE WHEN propertyAreaTypeCode == 'H' THEN  propertyArea * 10000 \
                                            ELSE propertyArea END AS propertyArea \
                                     from cleansed.t_access_z309_tproperty \
                                     where _RecordCurrent = 1 and _RecordDeleted = 0")

  sapisu0ucConbjAttr2Df = spark.sql("select propertyNumber, 'SAPISU' as sourceSystemCode,inferiorPropertyType as PropertyType, superiorPropertyType, \
                                            architecturalObjectInternalId, validFromDate as propertyStartDate, LGA,\
                                            coalesce(lead(validFromDate) over (partition by propertyNumber order by validFromDate)-1, \
                                            to_date('9999-12-31', 'yyyy-mm-dd'))  as propertyEndDate \
                                     from cleansed.t_sapisu_0uc_connobj_attr_2 \
                                     where _RecordCurrent = 1 and _RecordDeleted = 0")

  sapisuVibdaoDf = spark.sql("select architecturalObjectInternalId, \
                                   CASE WHEN hydraAreaUnit == 'HAR' THEN  hydraCalculatedArea * 10000 \
                                        WHEN hydraAreaUnit == 'M2' THEN  hydraCalculatedArea \
                                        ELSE null END AS propertyArea \
                            from cleansed.t_sapisu_vibdao \
                            where _RecordCurrent = 1 and _RecordDeleted = 0")
  dummyDimRecDf = spark.createDataFrame([("-1", "SAPISU", "9999-12-31"), ("-1", "Access", "9999-12-31")], ["propertyNumber", "sourceSystemCode", "propertyEndDate"])
  
  #3.JOIN TABLES  
  df = sapisu0ucConbjAttr2Df.join(sapisuVibdaoDf, sapisu0ucConbjAttr2Df.architecturalObjectInternalId == sapisuVibdaoDf.architecturalObjectInternalId, how="inner")\
                            .drop(sapisuVibdaoDf.architecturalObjectInternalId).drop(sapisu0ucConbjAttr2Df.architecturalObjectInternalId)
  df = df.select("propertyNumber","sourceSystemCode","propertyStartDate","propertyEndDate", \
                                                "propertyType","superiorPropertyType","propertyArea","LGA")
  
  #4.UNION TABLES
  df = accessZ309TpropertyDf.union(df)
  df = df.unionByName(dummyDimRecDf, allowMissingColumns = True)
  
  #5.SELECT / TRANSFORM
  df = df.selectExpr( \
	 "propertyNumber as propertyId" \
    ,"sourceSystemCode" \
    ,"propertyStartDate" \
    ,"propertyEndDate" \
    ,"propertyType" \
    ,"superiorPropertyType" \
    ,"CAST(propertyArea AS DECIMAL(18,6)) as areaSize" \
    ,"LGA" \
  )

  return df
