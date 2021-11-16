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
  accessZ309TpropertyDf = accessZ309TpropertyDf.dropDuplicates() #Please remove once upstream data is fixed

  isu0ucConbjAttr2Df = spark.sql("select propertyNumber, 'SAP' as sourceSystemCode,inferiorPropertyType as PropertyType, superiorPropertyType, \
                                            architecturalObjectInternalId, validFromDate as propertyStartDate, LGA,\
                                            coalesce(lead(validFromDate) over (partition by propertyNumber order by validFromDate)-1, \
                                            to_date('9999-12-31', 'yyyy-mm-dd'))  as propertyEndDate \
                                     from cleansed.t_isu_0uc_connobj_attr_2 \
                                     where _RecordCurrent = 1 and _RecordDeleted = 0")
  isu0ucConbjAttr2Df = isu0ucConbjAttr2Df.dropDuplicates() #Please remove once upstream data is fixed

  isuVibdaoDf = spark.sql("select architecturalObjectInternalId, \
                                   CASE WHEN hydraAreaUnit == 'HAR' THEN  hydraCalculatedArea * 10000 \
                                        WHEN hydraAreaUnit == 'M2' THEN  hydraCalculatedArea \
                                        ELSE null END AS propertyArea \
                            from cleansed.t_isu_vibdao \
                            where _RecordCurrent = 1 and _RecordDeleted = 0")
  isuVibdaoDf = isuVibdaoDf.dropDuplicates() #Please remove once upstream data is fixed
  
  #3.JOIN TABLES  
  df = isu0ucConbjAttr2Df.join(isuVibdaoDf, isu0ucConbjAttr2Df.architecturalObjectInternalId == isuVibdaoDf.architecturalObjectInternalId, how="inner")\
                            .drop(isuVibdaoDf.architecturalObjectInternalId).drop(isu0ucConbjAttr2Df.architecturalObjectInternalId)
  df = df.select("propertyNumber","sourceSystemCode","propertyStartDate","propertyEndDate", \
                                                "propertyType","superiorPropertyType","propertyArea","LGA")
  
  #4.UNION TABLES
  df = accessZ309TpropertyDf.union(df)
  
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
