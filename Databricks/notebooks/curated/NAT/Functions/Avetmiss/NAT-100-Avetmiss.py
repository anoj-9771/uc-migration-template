# Databricks notebook source
from pyspark.sql.functions import *
from datetime import datetime
from pyspark.sql.window import *
from pyspark.sql.types import *
from functools import reduce
from pyspark.sql import DataFrame

# COMMAND ----------

def GetNAT100Avetmiss(dfAvetmissStudent, dfNAT120, dfNAT130):
  ###########################################################################################################################
  # Function: GetNAT100Avetmiss
  # Parameters: 
  # dfAvetmissStudent = Avetmiss Student
  # dfNAT120 = NAT120 Avtemiss Dataframe
  # dfNAT130 = NAT130 Avtemiss Dataframe
  # Returns:
  #  Dataframe for all NAT 100 ERPT
  #############################################################################################################################

  dfNAT120 = dfNAT120.selectExpr("CLIENT_ID").distinct()
  dfNAT130 = dfNAT130.selectExpr("CLIENT_ID").distinct()
  dfPriorEducation = dfNAT120.union(dfNAT130).distinct()
   
  #Joins
  df=dfPriorEducation.join(dfAvetmissStudent,dfPriorEducation.CLIENT_ID==dfAvetmissStudent.RegistrationNo,how="inner")
  
  #Filters
  df1=df.filter(col("QualificationDegree")=='Y').selectExpr("CLIENT_ID","'008' AS PRIOR_ED_ACHIEVEMENT_ID")
  df2=df.filter(col("QualificationAdvancedDiploma")=='Y').selectExpr("CLIENT_ID","'410' AS PRIOR_ED_ACHIEVEMENT_ID")
  df3=df.filter(col("QualificationAssociateDiploma")=='Y').selectExpr("CLIENT_ID","'420' AS PRIOR_ED_ACHIEVEMENT_ID")
  df4=df.filter(col("QualificationAdvancedCertificate")=='Y').selectExpr("CLIENT_ID","'511' AS PRIOR_ED_ACHIEVEMENT_ID")
  df5=df.filter(col("QualificationTradeCertificate")=='Y').selectExpr("CLIENT_ID","'514' AS PRIOR_ED_ACHIEVEMENT_ID")
  df6=df.filter(col("QualificationCertificateII")=='Y').selectExpr("CLIENT_ID","'521' AS PRIOR_ED_ACHIEVEMENT_ID")
  df7=df.filter(col("QualificationCertificateI")=='Y').selectExpr("CLIENT_ID","'524' AS PRIOR_ED_ACHIEVEMENT_ID")
  df8=df.filter(col("QualificationOtherCertificate")=='Y').selectExpr("CLIENT_ID","'990' AS PRIOR_ED_ACHIEVEMENT_ID")
  
  dfs=[df1, df2, df3, df4, df5, df6, df7, df8]
  df=reduce(DataFrame.union,dfs)
  
  #Pad
  df=df.withColumn("Output", \
                    concat(rpad(col("CLIENT_ID"),10, ' '), \
                           rpad(col("PRIOR_ED_ACHIEVEMENT_ID"),3,'0')
                          )
                   )
  #Select
  df=df.selectExpr("CLIENT_ID",
                   "PRIOR_ED_ACHIEVEMENT_ID",
                   "Output"
                  ).dropDuplicates()
  return df
