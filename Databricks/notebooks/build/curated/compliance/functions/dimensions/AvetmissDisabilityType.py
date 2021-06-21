# Databricks notebook source
from pyspark.sql.functions import *
from datetime import datetime
from pyspark.sql.window import *
from pyspark.sql.types import *

# COMMAND ----------

def GetAvetmissDisabilityType(dfDisabilities, dfCurrentReportingPeriod, startDate, endDate):
  # ##########################################################################################################################  
  # Function: getDisabilityType
  # Returns a dataframe that for the Disability Type
  # Parameters:
  # dfDisabilities = Input DataFrame - EBS Disabilities 
  # dfCurrentReportingPeriod = Input DataFrame - current reporting period 
  # Returns:
  # A dataframe that has all attributes for the Disability Type Table
  # ############################################################################################################################
  #Current reporting calendar year and calender year end date
  dfCurrentCalendarYear = dfCurrentReportingPeriod.filter(expr("left(ReportingYear, 1) = 'C'")).selectExpr("ReportingYearNo as ReportingYear", "ReportingYearEndDate")

  #Year Start and Year End Columns
  dfDisabilitiesV = dfDisabilities.withColumn("Start_Date_Year", year("START_DATE")).withColumn("End_Date_Year", coalesce(year("END_DATE"), lit("2999")))
  
  #Join Student Status Log
  df = dfCurrentCalendarYear.join(dfDisabilitiesV, ((dfCurrentCalendarYear.ReportingYear >= dfDisabilitiesV.Start_Date_Year) & (dfCurrentCalendarYear.ReportingYear <= dfDisabilitiesV.End_Date_Year)), how="inner")

  #Filter on Reporting Period
  df = df.filter((col("END_DATE") >= col("ReportingYearEndDate")) | (col("END_DATE").isNull()))
  
  #Fiter Reporting Year to At Least This year
  df = df.filter(col("ReportingYear") <= year(current_date()))
  
  #Group By
  df = df.groupBy("PER_PERSON_CODE", "ReportingYear")
  
  #Aggregations
  df = df.agg(max(when(col("DISABILITY_TYPE") == 11, lit(1)).otherwise(lit(0))).alias("DISAB_HEARING"),
            max(when(col("DISABILITY_TYPE") == 12, lit(1)).otherwise(lit(0))).alias("DISAB_PHYSICAL"),
            max(when(col("DISABILITY_TYPE") == 13, lit(1)).otherwise(lit(0))).alias("DISAB_INTELLECTUAL"),
            max(when(col("DISABILITY_TYPE") == 14, lit(1)).otherwise(lit(0))).alias("DISAB_LEARNING"),
            max(when(col("DISABILITY_TYPE") == 15, lit(1)).otherwise(lit(0))).alias("DISAB_MENTAL_ILLNESS"),
            max(when(col("DISABILITY_TYPE") == 16, lit(1)).otherwise(lit(0))).alias("DISAB_ACQUIRED_BRAIN_IMPAIR"),
            max(when(col("DISABILITY_TYPE") == 17, lit(1)).otherwise(lit(0))).alias("DISAB_VISUAL"),
            max(when(col("DISABILITY_TYPE") == 18, lit(1)).otherwise(lit(0))).alias("DISAB_MEDICAL_CONDITION"),
            max(when(col("DISABILITY_TYPE") == 19, lit(1)).otherwise(lit(0))).alias("DISAB_OTHER"),
            max(when(col("DISABILITY_TYPE") == 99, lit(1)).otherwise(lit(0))).alias("DISAB_NOT_SPECIFIED")
           )

  #Disability Count
  df = df.withColumn("Count_Disabilities", (df.DISAB_HEARING + df.DISAB_PHYSICAL + df.DISAB_INTELLECTUAL + df.DISAB_LEARNING + df.DISAB_MENTAL_ILLNESS + df.DISAB_ACQUIRED_BRAIN_IMPAIR + df.DISAB_VISUAL + df.DISAB_MEDICAL_CONDITION + df.DISAB_OTHER))

  #Disability Type
  df = df.withColumn("DisabilityType", \
                     when(col("Count_Disabilities") > 1, "Multiple")\
                    .when(col("DISAB_HEARING") == 1, "Hearing")
                    .when(col("DISAB_PHYSICAL") == 1, "Physical")
                    .when(col("DISAB_INTELLECTUAL") == 1, "Intellectual")
                    .when(col("DISAB_LEARNING")== 1, "Learning")
                    .when(col("DISAB_MENTAL_ILLNESS") == 1, "Mental Illness")
                    .when(col("DISAB_ACQUIRED_BRAIN_IMPAIR") == 1, "Acquired Brain Impairment")
                    .when(col("DISAB_VISUAL") == 1, "Visual")
                    .when(col("DISAB_MEDICAL_CONDITION") == 1, "Medical Condition")
                    .when(col("DISAB_OTHER") == 1, "Other")
                    .when(col("DISAB_NOT_SPECIFIED") == 1, "Not Known").otherwise(None))
  #Select Columns #PRADA-1275
  df = df.selectExpr("CAST(PER_PERSON_CODE AS BIGINT) as PersonCode", "ReportingYear", "DisabilityType")
  
  return df
