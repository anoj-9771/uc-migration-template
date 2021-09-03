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
def GetCommonDate():
  
  spark.udf.register("TidyCase", GeneralToTidyCase)  
  
  #DimProperty
  #2.Load Cleansed layer table data into dataframe
  dateDf = spark.sql("SELECT  * \
                                   from cleansed.t_sapisu_scal_tt_date")
 
  #3.JOIN TABLES  
  
  
  #4.UNION TABLES
 
  
  #5.SELECT / TRANSFORM
  df = dateDf.selectExpr( \
                          "calendarDate" \
                          ,"monthOfYear" \
                          ,"monthName" \
                          ,"dayOfMonth" \
                          ,"dayName" \
                          ,"quarterOfYear" \
                          ,"monthStartDate" \
                          ,"monthEndDate" \
                          ,"yearStartDate" \
                          ,"yearEndDate" \
                          ,"financialYear" \
                          ,"financialYearStartDate" \
                          ,"financialYearEndDate" \
                          ,"monthOfFinancialYear" \
                          ,"quarterOfFinancialYear" \
                       )

  return df

