# Databricks notebook source
###########################################################################################################################
# Function: GetNAT090ERpt
# Parameters: 
#  dfBase = NAT E-Reporting 120 base file which has course enrolments only
#  dfAvetmissStudent = Avetmiss Student
#  dfStudentStatusLog = Avetmiss StudentStatusLog
#  dfDisabilities = EBS Disabilities
#  params = Master Notebook Parameters
# Returns:
#  Dataframe for all NAT 090 ERpt
############################################################################################################################
def GetNAT090ERpt(dfBase, dfAvetmissStudent, dfStudentStatusLog, dfDisabilities, params):
  #INHERIT FROM eNAT080 #PRADA-1234
  df = GetNAT080ERpt(dfBase, dfAvetmissStudent, dfStudentStatusLog, dfDisabilities, params).where("DISABILITY_FLG = 'Y'")
  
  #SELECT #PRADA-1689
  df = df.selectExpr("CLIENT_IDENTIFER Client_ID" \
                 ,"COALESCE(CASE WHEN DISABILITY_TYPE='20' THEN '19' ELSE DISABILITY_TYPE END, '99') Disability_ID" \
  )

  #OUTPUT
  df = df.withColumn("Output", concat( \
    rpad(col("Client_ID"), 10,' ') 
    ,rpad(col("Disability_ID"), 2,' ') 
  )).distinct()
  df = df.sort("Client_ID")
  return df

# COMMAND ----------


