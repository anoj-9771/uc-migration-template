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
#  dateDf = dateDf.dropDuplicates() #Please remove once upstream data is fixed

 
  #3.JOIN TABLES  
  
  
  #4.UNION TABLES
 
  
  #5.SELECT / TRANSFORM
  df = dateDf.selectExpr( \
	 "calendarDate" \
    ,"CALENDARYEAR as calendarYear" \
    ,"monthOfYear" \
    ,"quarterOfYear" \
    ,"monthStartDate" \
    ,"monthEndDate" \
  )

  return df


# COMMAND ----------



# COMMAND ----------

df = spark.sql("Select calendardate, count(1) from  cleansed.t_sapisu_scal_tt_date group by calendardate having count(1) > 1 order by 1")
display(df)

# COMMAND ----------

  df2 = spark.sql("SELECT  * \
                                   from cleansed.t_sapisu_scal_tt_date where calendardate ='1903-01-30'")
  
print(df2.count())
df2 = df2.dropDuplicates() #Please remove once upstream data is fixed
print(df2.count())
# df2 = spark.sql("Select calendardate, count(1) from  cleansed.t_sapisu_scal_tt_date group by calendardate having count(1) > 1")

display(df2)
