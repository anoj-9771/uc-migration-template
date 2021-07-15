# Databricks notebook source
def GetNAT060ERpt(dfNAT120ERpt, dfAvetmissUnit):
  dfNAT120=dfNAT120ERpt.selectExpr("SUBJECT_ID").distinct()
  dfUnit=GeneralAliasDataFrameColumns(dfAvetmissUnit,"U_")
  dfUnit=dfUnit.withColumn("U_SUBJECT_ID", coalesce(col("U_NationalCourseCode"), col("U_UnitCode")))
  df=dfNAT120.join(dfUnit,dfNAT120.SUBJECT_ID==dfUnit.U_SUBJECT_ID)
  #PRADA-1627
  df=df.withColumn("Rank", expr("RANK() OVER(PARTITION BY U_SUBJECT_ID ORDER BY cast(U_NominalHours as int) DESC, COALESCE(U_FieldOfEducationId, '') DESC)")).where(col("Rank") == 1)
  df=df.selectExpr("coalesce(U_SUBJECT_ID,' ') as SUBJECT_IDENTIFIER"
                   ,"coalesce(U_UnitName,' ') as SUBJECT_NAME"
                   ,"coalesce(U_FieldOfEducationId,' ') as SUBJECT_FIELD_OF_ED"
                   ,"coalesce(Left(U_IsVocational,1),' ') as VET_FLAG"
                   ,"coalesce(cast(U_NominalHours as int), 0) as NOMINAL_HOURS"
                  ).distinct()
  df=df.withColumn("output", 
                      concat 
                      ( 
                         rpad(df.SUBJECT_IDENTIFIER,12,' ') 
                        ,rpad(df.SUBJECT_NAME,100,' ') 
                        ,rpad(df.SUBJECT_FIELD_OF_ED,6,' ') 
                        ,rpad(df.VET_FLAG,1,' ') 
                        ,lpad(df.NOMINAL_HOURS,4,'0') 
                      ) 
                   ).dropDuplicates()
  
  return df.sort("SUBJECT_IDENTIFIER")

# COMMAND ----------


