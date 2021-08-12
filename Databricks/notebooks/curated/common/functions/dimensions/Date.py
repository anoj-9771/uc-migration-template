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
  dateDf = spark.sql("SELECT   \
                                   sysdate AS calendarDate, \
                                    DATEPART(MM, Getdate()) ")
  accessZ309TpropertyDf = accessZ309TpropertyDf.dropDuplicates() #Please remove once upstream data is fixed

  sapisu0ucConbjAttr2Df = spark.sql("select propertyNumber, 'SAP' as sourceSystemCode,inferiorPropertyType as PropertyType, superiorPropertyType, \
                                            architecturalObjectInternalId, validFromDate as propertyStartDate, LGA,\
                                            coalesce(lead(validFromDate) over (partition by propertyNumber order by validFromDate)-1, \
                                            to_date('9999-12-31', 'yyyy-mm-dd'))  as propertyEndDate \
                                     from cleansed.t_sapisu_0uc_connobj_attr_2 \
                                     where _RecordCurrent = 1 and _RecordDeleted = 0")
  sapisu0ucConbjAttr2Df = sapisu0ucConbjAttr2Df.dropDuplicates() #Please remove once upstream data is fixed

  sapisuVibdaoDf = spark.sql("select architecturalObjectInternalId, \
                                   CASE WHEN hydraAreaUnit == 'HAR' THEN  hydraCalculatedArea * 10000 \
                                        WHEN hydraAreaUnit == 'M2' THEN  hydraCalculatedArea \
                                        ELSE null END AS propertyArea \
                            from cleansed.t_sapisu_vibdao \
                            where _RecordCurrent = 1 and _RecordDeleted = 0")
  sapisuVibdaoDf = sapisuVibdaoDf.dropDuplicates() #Please remove once upstream data is fixed
  
  #3.JOIN TABLES  
  df = sapisu0ucConbjAttr2Df.join(sapisuVibdaoDf, sapisu0ucConbjAttr2Df.architecturalObjectInternalId == sapisuVibdaoDf.architecturalObjectInternalId, how="inner")\
                            .drop(sapisuVibdaoDf.architecturalObjectInternalId).drop(sapisu0ucConbjAttr2Df.architecturalObjectInternalId)
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

# COMMAND ----------

  dateDf = spark.sql("SELECT   \
                                  current_date ()  AS calendarDate \
                                  date_format(date @CurrentDate, "L") AS dayName")
  display(dateDf)
