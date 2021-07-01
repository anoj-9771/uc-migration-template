# Databricks notebook source
from pyspark.sql.functions import *
from datetime import datetime
from pyspark.sql.window import *
from pyspark.sql.types import *
import pyspark.sql.functions as f

# COMMAND ----------

def GetNAT030Avetmiss(dfNAT120Avetmiss, dfNAT130Avetmiss, dfAvetmissCourse, dfQualificationType):
  ###########################################################################################################################
  # Function: GetNAT030Avetmiss
  # Parameters: 
  # dfNAT120Avetmiss = NAT Avetmiss 120
  # dfNAT130Avetmiss = NAT Avetmiss 130
  # dfAvetmissCourse = Avetmiss Course
  # dfQualificationType = Reference Qualification Type
  # Returns:
  #  Dataframe for all NAT 030 Avetmiss
  ############################################################################################################################
  
  #Alias
  print("Starting")
  #dfNAT120AvetmissV=dfNAT120Avetmiss.select(*(col(x).alias("Avt120_"+ x) for x in dfNAT120Avetmiss.columns))
  dftmp=dfNAT120Avetmiss.select("PROGRAM_ID").distinct()
  dfNAT120AvetmissV=dftmp.select(*(col(x).alias("Avt120_"+ x) for x in dftmp.columns))
  #dfNAT130AvetmissV=dfNAT130Avetmiss.select(*(col(x).alias("Avt130_"+ x) for x in dfNAT130Avetmiss.columns))
  dftmp=dfNAT130Avetmiss.select("Program_Id").distinct()
  dfNAT130AvetmissV=dftmp.select(*(col(x).alias("Avt130_"+ x) for x in dftmp.columns))
  
  #Joins
  df = dfAvetmissCourse.join(dfQualificationType, dfAvetmissCourse.QualificationTypeCode == dfQualificationType.QualificationTypeID, how="left")
  df = df.join(dfNAT120AvetmissV, df.AvetmissCourseCode == dfNAT120AvetmissV.Avt120_PROGRAM_ID, how="left")
  df = df.join(dfNAT130AvetmissV, df.AvetmissCourseCode == dfNAT130AvetmissV.Avt130_Program_Id, how="left")
  
  #Filter
  df = df.filter(col("Avt120_PROGRAM_ID").isNotNull() | col("Avt130_Program_Id").isNotNull())

  #Calculated Column
  df = df.withColumn("Program_Recognition_Identifier", \
                     when(col("CourseCategory")=="AC", "12")\
                     .when(col("CourseCategory")=="HEC", "15")\
                     .when(col("CourseCategory")=="NNRC", "14")\
                     .when(col("CourseCategory")=="RTOSS", "16")\
                     .when(col("CourseCategory")=="TPQ", "11")\
                     .when(col("CourseCategory")=="TPSS", "13")\
                     .when((col("CourseCategory")=="TPC") & (col("AvetmissCourseCode").isNotNull()), "12")\
                     .when((col("CourseCategory")=="TPC") & (col("AvetmissCourseCode").isNull()), "14").otherwise("14")
                    )
  #PRADA-1611
  df = df.withColumn("CourseName", NatSpecialCharacterReplace(col("CourseName")))
  #Pad
  df = df.withColumn("Program_Identifier", rpad(coalesce(col("AvetmissCourseCode"),lit('')), 15, ' ')) \
        .withColumn("Program_Name",rpad(coalesce(col("CourseName"),lit('')), 100,' ')) \
        .withColumn("Nominal_Hours", lpad(coalesce((col("NominalHours").cast(IntegerType())), lit('')), 4, "0")) \
        .withColumn("Program_Recognition_Identifier", rpad(coalesce(col("Program_Recognition_Identifier"),lit('')), 2, ' ')) \
        .withColumn("Program_Level_Of_Education_Identifier", rpad(coalesce(col("AvetmissQualificationID"),lit('')), 3, ' ')) \
        .withColumn("Program_Field_Of_Education_Identifier", rpad(coalesce(col("FieldOfEducationID"),lit('')), 4, ' ')) \
        .withColumn("ANZSCO_Identifier", rpad(coalesce(col("OccupationCode"),lit('')), 6, ' ')) \
        .withColumn("ISVETFEEHelp", rpad(lit('Y'), 1, ' ')) \
  
  #PRADA-1611, PRADA-1670
  df = df.withColumn("RANK", expr("RANK() OVER (PARTITION BY Program_Identifier ORDER BY CourseStatus, Nominal_Hours DESC, CourseCode DESC)")).where(expr("RANK=1"))
  #Output Columns
  df = df.selectExpr("Program_Identifier"
                     ,"Program_Name"
                     ,"Nominal_Hours"
                     ,"Program_Recognition_Identifier"
                     ,"Program_Level_Of_Education_Identifier"
                     ,"Program_Field_Of_Education_Identifier"
                     ,"ANZSCO_Identifier"
                     ,"ISVETFEEHelp"
                     
                    )
  
  df = df.withColumn("Output", (concat(*df.columns))).dropDuplicates()
  print("Ending")
    
  return df
