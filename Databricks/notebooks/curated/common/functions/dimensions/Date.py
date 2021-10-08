# Databricks notebook source
###########################################################################################################################
# Function: GetCommonDate
#  GETS Date DIMENSION 
# Returns:
#  Dataframe of transformed dates
#############################################################################################################################
# Method
# 1.Create Function
# 2.Load Cleansed layer table data into dataframe and transform
# 3.JOIN TABLES
# 4.UNION TABLES
# 5.SELECT / TRANSFORM
#############################################################################################################################
#1.Create Function
def GetCommonDate():
  
  #spark.udf.register("TidyCase", GeneralToTidyCase)  
  
  #DimProperty
  #2.Load Cleansed layer table data into dataframe
  dateDf = spark.sql(f"SELECT  * \
                                   from {ADS_DATABASE_CLEANSED}.isu_scal_tt_date")
 
  #3.JOIN TABLES  
  
  
  #4.UNION TABLES

  #5.SELECT / TRANSFORM
  df = dateDf.selectExpr( \
                          "calendarDate" \
                          ,"dayOfWeek" \
                          ,"date_format(calendarDate, 'EEEE') as dayName" \
                          ,"dayOfMonth" \
                          ,"dayOfYear" \
                          ,"monthOfYear" \
                          ,"date_format(calendarDate, 'MMMM') as monthName" \
                          ,"quarterOfYear" \
                          ,"halfOfYear" \
                          ,"monthStartDate" \
                          ,"monthEndDate" \
                          ,"cast(cast(calendarYear as string)||'-01-01' as date) as yearStartDate" \
                          ,"cast(cast(calendarYear as string)||'-12-31' as date) as yearEndDate" \
                          ,"cast(case when date_format(calendarDate,'M') > 6 then date_format(calendarDate,'yyyy') + 1 else date_format(calendarDate,'yyyy') end as int) as financialYear" \
                          ,"cast(cast(cast(case when date_format(calendarDate,'M') > 6 then date_format(calendarDate,'yyyy') else date_format(calendarDate,'yyyy') - 1 end as int) as string)||'-07-01' as date)  as financialYearStartDate" \
                          ,"cast(cast(cast(case when date_format(calendarDate,'M') > 6 then date_format(calendarDate,'yyyy') + 1 else date_format(calendarDate,'yyyy') end as int) as string)||'-06-30' as date)  as financialYearEndDate" \
                          ,"cast(case when date_format(calendarDate,'M') > 6 then date_format(calendarDate,'M') - 6 else date_format(calendarDate,'M') + 6 end as int)  as monthOfFinancialYear" \
                          ,"case when quarter(calendarDate) < 3 then quarter(calendarDate) + 2 else quarter(calendarDate) - 2 end as quarterOfFinacialYear" \
                          ,"case when quarter(calendarDate) < 3 then 2 else 1 end as halfOfFinacialYear" \
                       )
  #6.Apply schema definition
  newSchema = StructType([
                            StructField("calendarDate", DateType(), False),
                            StructField("dayOfWeek", IntegerType(), False),
                            StructField("dayName", StringType(), False),
                            StructField("dayOfMonth", IntegerType(), False),
                            StructField("dayOfYear", IntegerType(), False),
                            StructField("monthOfYear", IntegerType(), False),
                            StructField("monthName", StringType(), False),
                            StructField("quarterOfYear", IntegerType(), False),
                            StructField("halfOfYear", IntegerType(), False),
                            StructField("monthStartDate", DateType(), False),
                            StructField("monthEndDate", DateType(), False),
                            StructField("yearStartDate", DateType(), False),
                            StructField("yearEndDate", DateType(), False),
                            StructField("financialYear", StringType(), False),
                            StructField("financialYearStartDate", DateType(), False),
                            StructField("financialYearEndDate", DateType(), True),
                            StructField("monthOfFinancialYear", IntegerType(), False),
                            StructField("quarterOfFinancialYear", IntegerType(), False),
                            StructField("halfOfFinancialYear", IntegerType(), False)
                      ])
  
  df = spark.createDataFrame(df.rdd, schema=newSchema)
  return df

