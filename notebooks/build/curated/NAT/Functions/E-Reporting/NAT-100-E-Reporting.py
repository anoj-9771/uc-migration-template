# Databricks notebook source
from functools import reduce
from pyspark.sql import DataFrame

# COMMAND ----------

def GetNAT100ERpt(dfAvetmissStudent, rtoCode, dfBase):
  ###########################################################################################################################
  # Function: GetNAT100ERpt
  # Parameters: 
  # dfAvetmissStudent = Avetmiss Student
  # rtoCode = RTO Code
  # dfBase = EReporting NAT BASE file
  # Returns:
  #  Dataframe for all NAT 030 ERPT
  #############################################################################################################################
  dfBase=dfBase.withColumnRenamed("REGISTRATION_NO","CLIENT_ID")
#   dfNAT120 = dfBase.selectExpr("CLIENT_ID").distinct()
#   dfNAT130 = dfNAT130.selectExpr("CLIENT_ID").distinct()
#   dfPriorEducation = dfNAT120.union(dfNAT130).distinct()
  dfPriorEducation = dfBase.selectExpr("CLIENT_ID").distinct()
  #Joins
  df=dfPriorEducation.join(dfAvetmissStudent,dfPriorEducation.CLIENT_ID==dfAvetmissStudent.RegistrationNo,how="inner")
  
  df1=df.filter(col("QualificationDegree")=='Y').selectExpr("CLIENT_ID","'008' AS PRIOR_ED_ACHIEVEMENT_ID")
  df2=df.filter(col("QualificationAdvancedDiploma")=='Y').selectExpr("CLIENT_ID","'410' AS PRIOR_ED_ACHIEVEMENT_ID")
  df3=df.filter(col("QualificationAssociateDiploma")=='Y').selectExpr("CLIENT_ID","'420' AS PRIOR_ED_ACHIEVEMENT_ID")
  df4=df.filter(col("QualificationAdvancedCertificate")=='Y').selectExpr("CLIENT_ID","'511' AS PRIOR_ED_ACHIEVEMENT_ID")
  df5=df.filter(col("QualificationTradeCertificate")=='Y').selectExpr("CLIENT_ID","'514' AS PRIOR_ED_ACHIEVEMENT_ID")
  df6=df.filter(col("QualificationCertificateII")=='Y').selectExpr("CLIENT_ID","'521' AS PRIOR_ED_ACHIEVEMENT_ID")
  df7=df.filter(col("QualificationCertificateI")=='Y').selectExpr("CLIENT_ID","'524' AS PRIOR_ED_ACHIEVEMENT_ID")
  df8=df.filter(col("QualificationOtherCertificate")=='Y').selectExpr("CLIENT_ID","'990' AS PRIOR_ED_ACHIEVEMENT_ID")
  
  #Usage of reduce - https://walkenho.github.io/merging-multiple-dataframes-in-pyspark/
  dfs=[df1, df2, df3, df4, df5, df6, df7, df8]
  df=reduce(DataFrame.union,dfs)
  df=df.withColumn("PRIOR_ED_ACHIEVEMENT_ID",col("PRIOR_ED_ACHIEVEMENT_ID").cast(DecimalType(3,0)))
  
  #Pad
  df=df.withColumn("Output", \
                    concat(rpad(col("CLIENT_ID"),10, ' '), \
                           lpad(col("PRIOR_ED_ACHIEVEMENT_ID"),3,'0')
                          )
                   )
  df=df.sort("CLIENT_ID")
  #Select
  df=df.selectExpr("CLIENT_ID",
                   "PRIOR_ED_ACHIEVEMENT_ID",
                   "Output"
                  ).dropDuplicates()
  return df