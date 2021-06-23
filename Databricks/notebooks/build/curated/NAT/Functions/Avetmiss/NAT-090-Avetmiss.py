# Databricks notebook source
def GetNAT090Avetmiss(dfNAT120Avetmiss, dfNAT130Avetmiss, dfAvetmissStudent, dfDisabilities, params):
  ###########################################################################################################################
  # Function: GetNAT090Avetmiss
  # Parameters: 
  # dfNAT120Avetmiss = NAT Avetmiss 120
  # dfNAT130Avetmiss = NAT Avetmiss 130
  # dfAvetmissStudent = NAT Student
  # dfDisabilities = EBS Disabilities
  # params = Master Notebook Parameters
  # Returns:
  #  Dataframe for all NAT 090 Avetmiss
  ############################################################################################################################
  NAT_Dates = spark.read.json(sc.parallelize([params]))
  CollectionStart = params["CollectionStartAvt"]
  CollectionStartYear = (datetime.strptime(CollectionStart, '%Y-%m-%d')).year
  CollectionEnd = params["CollectionEndAvt"]
  CutOffDate = params["CutOffDateAvt"]
  CollectionEndYear = (datetime.strptime(CollectionEnd, '%Y-%m-%d')).year
  CollectionEndYearMinusOneYr = ((datetime.strptime(CutOffDate, '%Y-%m-%d')).year)-1
  CollectionEndYearMinusTwoYr = ((datetime.strptime(CutOffDate, '%Y-%m-%d')).year)-2
  CutOffDateAdd12Months = (datetime.strptime(CutOffDate, '%Y-%m-%d')) + relativedelta(months=+12)

  #Client_ID list
  dfClientID  = dfNAT120Avetmiss.select("CLIENT_ID").union(dfNAT130Avetmiss.select("CLIENT_ID")).distinct()
  #Disability list
  dfDisabilities=GeneralAliasDataFrameColumns(dfDisabilities,"DT_")
  
  #PRADA-1669
  df = dfClientID.join(dfAvetmissStudent, (coalesce(dfAvetmissStudent.RegistrationNo, dfAvetmissStudent.PersonCode) == dfClientID.CLIENT_ID), how="inner").filter(col("CLIENT_ID").isNotNull())
  df = df.join(dfDisabilities, (df.PersonCode == dfDisabilities.DT_PER_PERSON_CODE) & (lit(CollectionEnd).between(dfDisabilities.DT_START_DATE, dfDisabilities.DT_END_DATE) | dfDisabilities.DT_END_DATE.isNull()), how="inner")

  #Rpad
  df = df.withColumn("Client_ID", rpad((coalesce(df.RegistrationNo, df.PersonCode)),10,' ')) \
          .withColumn("Disability_ID", rpad(coalesce(when(col("DT_DISABILITY_TYPE")=="20","19") \
                                        .otherwise(col("DT_DISABILITY_TYPE")), lit("99")), 2, ' '))
  #Select
  df = df.select("Client_ID", "Disability_ID")

  df = df.withColumn("Output", (concat(*df.columns))).dropDuplicates()
  
  return df
