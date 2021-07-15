# Databricks notebook source
def GetNAT030AERpt(dfAvetmissCourse, rtoCode, dfNAT120, dfNAT130, dfQualificationType):
  ###########################################################################################################################
  # Function: GetNAT030AERpt
  # Parameters: 
  # dfAvetmissCourse = Avetmiss Course
  # rtoCode = RTO Code
  # dfNAT120 = EReporting NAT120 file
  # dfNAT130 = EReporting NAT130 file
  # dfQualificationType = Field of Education level reference data
  # Returns:
  #  Dataframe for all NAT 030 ERPT
  #############################################################################################################################
  dfNAT120 = dfNAT120.selectExpr("COURSE_CODE").distinct()
  dfNAT130 = dfNAT130.selectExpr("COURSE_CODE").distinct()
  dfPrograms = dfNAT120.union(dfNAT130).distinct()           
  
  #Joins
  df=dfPrograms.join(dfAvetmissCourse,dfPrograms.COURSE_CODE==dfAvetmissCourse.AvetmissCourseCode,how="inner")
  df=df.join(dfQualificationType,df.QualificationTypeCode==dfQualificationType.QualificationTypeID,how="left")
  
  #Fields
  df=df.withColumn("PROGRAM_IDENTIFIER",col("COURSE_CODE"))
  df=df.withColumn("PROGRAM_NAME",col("CourseName"))
  df=df.withColumn("NOMINAL_HOURS",col("NominalHours").cast(DecimalType(4,0)))
  df=df.withColumn("PROGRAM_RECOGNITION_IDENTIFIER",\
                   when(col("CourseCategory") == 'AC' , '12')\
                   .when(col("CourseCategory") == 'HEC' , '15')\
                   .when(col("CourseCategory") == 'NNRC' , '14' )\
                   .when(col("CourseCategory") == 'RTOSS' , '16' )\
                   .when(col("CourseCategory") == 'TPQ' , '11' )\
                   .when(col("CourseCategory") == 'TPSS' , '13' )\
                   .when((col("CourseCategory") == 'TPC') & (~col("AvetmissCourseCode").isNull()) , '12')\
                   .when((col("CourseCategory") == 'TPC') & (col("AvetmissCourseCode").isNull()) , '14')\
                   .otherwise('14')
                  )
  df=df.withColumn("PROGRAM_LEVEL_OF_EDUCATION_IDENTIFIER",\
                   when(substring(col("CourseCode"),4,6)=='-19361','611')\
                   .otherwise(col("AvetmissQualificationID"))
                  )
  df=df.withColumn("PROGRAM_FIELD_OF_EDUCATION_IDENTIFIER",col("FieldOfEducationId"))
  df=df.withColumn("ANZSCO_IDENTIFIER",col("OccupationCode"))
  df=df.withColumn("VET_FLAG", lit("Y"))
  df=AlignCourse_Nat30_Nat30A(df)
  df=df.withColumn("Output", \
                    concat(rpad(coalesce(col("PROGRAM_IDENTIFIER"),lit(' ')),10, ' '), \
                           rpad(coalesce(col("PROGRAM_NAME"),lit(' ')),100,' '), \
                           lpad(coalesce(col("NOMINAL_HOURS"),lit(' ')),4,'0'), \
                           rpad(coalesce(col("PROGRAM_RECOGNITION_IDENTIFIER"),lit(' ')),2,' '), \
                           rpad(coalesce(col("PROGRAM_LEVEL_OF_EDUCATION_IDENTIFIER"),lit(' ')),3,' '), \
                           rpad(coalesce(col("PROGRAM_FIELD_OF_EDUCATION_IDENTIFIER"),lit(' ')),4,' '), \
                           rpad(coalesce(col("ANZSCO_IDENTIFIER"),lit(' ')),6,' '), \
                           rpad(coalesce(col("VET_FLAG"),lit(' ')),1,' ')
                          )
                   )

  df=df.selectExpr("PROGRAM_IDENTIFIER",
                   "PROGRAM_NAME",
                   "NOMINAL_HOURS",
                   "PROGRAM_RECOGNITION_IDENTIFIER",
                   "PROGRAM_LEVEL_OF_EDUCATION_IDENTIFIER",
                   "PROGRAM_FIELD_OF_EDUCATION_IDENTIFIER",
                   "ANZSCO_IDENTIFIER",
                   "VET_FLAG"
                   ,"Output"
                  ).dropDuplicates()
  df=df.sort("PROGRAM_IDENTIFIER")
  #PRADA-1600
  df=df.withColumn("Output", regexp_replace(col("Output"), "–", "-"))

  return df
