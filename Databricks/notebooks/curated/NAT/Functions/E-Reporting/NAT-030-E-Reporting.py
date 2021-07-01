# Databricks notebook source
#PRADA-1599, PRADA-1657
def AlignCourse_Nat30_Nat30A(dataFrame):
  return dataFrame.where(expr("COALESCE(OccupationCode, '') != ''")) \
    .withColumn("RANK", expr("RANK() OVER (PARTITION BY AvetmissCourseCode ORDER BY CASE WHEN CourseStatus = 'DRAFT' THEN 99 ELSE 1 END, CourseCode DESC)")) \
    .where(expr("RANK = 1"))

# COMMAND ----------

def GetNAT030ERpt(dfAvetmissCourse, rtoCode, dfNAT120, dfNAT130):
  ###########################################################################################################################
  # Function: GetNAT030ERpt
  # Parameters: 
  # dfAvetmissCourse = Avetmiss Course
  # dfNAT120 = EReporting NAT120 file
  # dfNAT130 = EReporting NAT130 file
  # rtoCode = RTO Code
  # Returns:
  #  Dataframe for all NAT 030 ERPT
  #############################################################################################################################
  dfNAT120 = dfNAT120.selectExpr("COURSE_CODE").distinct()
  dfNAT130 = dfNAT130.selectExpr("COURSE_CODE").distinct()
  dfPrograms = dfNAT120.union(dfNAT130).distinct()

  df=dfPrograms.join(dfAvetmissCourse,dfPrograms.COURSE_CODE==dfAvetmissCourse.AvetmissCourseCode,how="inner")
  df=df.withColumn("PROGRAM_IDENTIFIER", coalesce(col("COURSE_CODE"),lit('')))
  df=df.withColumn("PROGRAM_NAME", coalesce(col("CourseName"),lit('')))
  df=df.withColumn("NOMINAL_HOURS", coalesce(col("NominalHours"),lit('')).cast(DecimalType(4,0)))
  df=AlignCourse_Nat30_Nat30A(df)
  df=df.withColumn("Output", \
                    concat(rpad(coalesce(col("PROGRAM_IDENTIFIER"),lit(' ')),10, ' '), \
                           rpad(coalesce(col("PROGRAM_NAME"),lit(' ')),100,' '), \
                           lpad(coalesce(col("NOMINAL_HOURS"),lit(' ')),4,'0'), \
                           rpad(lit(' '),16,' ')
                          )
                   )
  df=df.selectExpr("PROGRAM_IDENTIFIER",
                   "PROGRAM_NAME",
                   "NOMINAL_HOURS"
                   ,"Output"
                  ).dropDuplicates()
  df=df.sort("PROGRAM_IDENTIFIER")
  #PRADA-1600
  df=df.withColumn("Output", regexp_replace(col("Output"), "–", "-"))
  return df

# COMMAND ----------

