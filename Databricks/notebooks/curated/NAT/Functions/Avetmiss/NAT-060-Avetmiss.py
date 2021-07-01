# Databricks notebook source
from pyspark.sql.functions import *
from datetime import datetime
from pyspark.sql.window import *
from pyspark.sql.types import *

# COMMAND ----------

def GetNAT060Avetmiss(dfNAT120Avetmiss,dfAvetmissUnit):

  dfNAT120=dfNAT120Avetmiss.selectExpr("SUBJECT_ID").distinct()
  
  dfUnit=GeneralAliasDataFrameColumns(dfAvetmissUnit,"U_")
  dfUnit=dfUnit.withColumn("U_SUBJECT_ID",coalesce(col("U_NationalCourseCode"),col("U_UnitCode")))
  
  df=dfNAT120.join(dfUnit,dfNAT120.SUBJECT_ID==dfUnit.U_SUBJECT_ID)
  
  df=df.selectExpr("CASE \
                      WHEN U_UnitCategory IN ('TPU') THEN 'C' \
                      ELSE 'M' \
                    END AS SUBJECT_FLAG"
                   ,"coalesce(U_SUBJECT_ID,' ') as SUBJECT_IDENTIFIER"
                   ,"coalesce(U_UnitName,' ') as SUBJECT_NAME"
                   ,"coalesce(U_FieldOfEducationId,' ') as SUBJECT_FIELD_OF_ED"
                   ,"coalesce(Left(U_IsVocational,1),' ') as VET_FLAG"
                   ,"coalesce(cast(U_NominalHours as int), 0) as NOMINAL_HOURS"
                  ).distinct()
  #PRADA-1612
  df=df.withColumn("SUBJECT_NAME", NatSpecialCharacterReplace(col("SUBJECT_NAME")))
  df=df.withColumn("Output", 
                      concat 
                      ( 
                         rpad(df.SUBJECT_FLAG,1,' ') 
                        ,rpad(df.SUBJECT_IDENTIFIER,12,' ') 
                        ,rpad(df.SUBJECT_NAME,100,' ')
                        ,rpad(df.SUBJECT_FIELD_OF_ED,6,' ')
                        ,rpad(df.VET_FLAG,1,' ')
                        ,lpad(df.NOMINAL_HOURS,4,'0')
                      ) 
                   ).dropDuplicates()
  
  return df
