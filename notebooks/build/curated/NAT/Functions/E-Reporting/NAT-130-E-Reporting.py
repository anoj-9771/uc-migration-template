# Databricks notebook source
def GetNAT130ERpt(dfNATBase, rtoCode, params):
  ###########################################################################################################################
  # Function: GetNAT130ERpt
  # Input Parameters: 
  # dfNAT120ERpt = NAT 120 E-Reporting Dataframe
  # dfAvetmissCourseEnrolment = Avetmiss Course Enrolment
  # dfAvetmissStudent = Avetmiss Student Dimension
  # dfAwards = EBS Awards
  # dfConfigurableStatuses = EBS Configurable Statuses
  # dfAvetmissLocation = Avetmiss Location
  # rtoCode = RTO Code - To Return Institute
  # params = Master Notebook Parameters
  # Returns:
  # Dataframe for all NAT 130 E-Reporting
  # Course Enrolment Data
  #############################################################################################################################
  #Params
  NAT_Dates = spark.read.json(sc.parallelize([params]))
  CollectionStart = params["CollectionStartERpt"]
  CollectionStartYear = (datetime.strptime(CollectionStart, '%Y-%m-%d')).year
  CollectionEnd = params["CollectionEndERpt"]
  CollectionEndYear = (datetime.strptime(CollectionEnd, '%Y-%m-%d')).year
  ReportingYear = params["ReportingYearERpt"]
  
  #prefix Columns
  dfBase=GeneralAliasDataFrameColumns(dfNATBase,"BS_")
  if rtoCode is None:
    rtoCode=90003
  else :
    dfBase=dfBase.filter(col("BS_RTOCode")==rtoCode)
  
  #PRADA-1725
  dfBase = dfBase.where("PROGRESS_CODE NOT LIKE '%WD%'")
  
  dfAWD=GeneralAliasDataFrameColumns(spark.table("trusted.OneEBS_EBS_0165_AWARDS").filter(col("_RecordCurrent") == 1).filter(col("_RecordDeleted") == 0).filter(col("CODE")=='WITHDRWAN') ,"AWD_")
  dfAPD=GeneralAliasDataFrameColumns(spark.table("trusted.OneEBS_EBS_0165_AWARDS_PRINTED_DETAILS").filter(col("_RecordCurrent") == 1).filter(col("_RecordDeleted") == 0),"APD_")
  dfCS=GeneralAliasDataFrameColumns(spark.table("trusted.OneEBS_EBS_0165_CONFIGURABLE_STATUSES") \
                                    .filter(col("_RecordCurrent") == 1) \
                                    .filter(col("_RecordDeleted") == 0) \
                                    .filter(col("LEARNER_VIEWABLE")=='Y') \
                                    .filter(col("ACTIVE")=='Y') \
                                    .filter(col("STATUS_TYPE")=='AWARD') \
                                    ,"CS_")
  
  df=dfBase.join(dfAPD,dfAPD.APD_ATTAINMENT_CODE==dfBase.BS_ATTAINMENT_CODE,how="left")
  df=df.join(dfCS,dfCS.CS_ID==df.BS_CONFIGURABLE_STATUS_ID,how="left")
  df=df.join(dfAWD,dfAWD.AWD_ID==df.BS_AWARD_ID,how="leftanti")
  
  #PRADA-1631
  dfNAT120ERptAll = GetNAT120ERpt(dfNATBase,paramsERpt,None).select("CLIENT_IDENTIFER").distinct()
  df=df.join(dfNAT120ERptAll, expr("CLIENT_IDENTIFER = COALESCE(COALESCE(BS_REGISTRATION_NO, BS_PERSON_CODE))"))
  
  #WHERE Clause
  df=df.filter(year(col("BS_DATE_AWARDED")).between(CollectionStartYear, CollectionEndYear)).filter(col("BS_UI_TYPE")=='C')
  #PRADA-1630
  dfPUL=GeneralAliasDataFrameColumns(spark.table("trusted.OneEBS_EBS_0165_PEOPLE_UNIT_LINKS").filter(col("_RecordCurrent") == 1).filter(col("_RecordDeleted") == 0),"PUL_")
  df.join(dfPUL, expr("BS_REGISTRATION_NO == PUL_PARENT_ID"))
  #Select Columns
  df=df.withColumn("ISSUED_FLAG",when(~(isnull(col("APD_TESTAMUR_DATE_LAST_PRINTED"))),'Y') \
                   .otherwise('Y'))
  df=df.withColumn("DATE_PROGRAM_COMPLETED",date_format(col("BS_DATE_AWARDED"), "ddMMyyyy"))
  df=df.withColumn("Output",concat( \
                                   rpad(lit(rtoCode),10,' '), \
                                   rpad(coalesce(col("BS_COURSE_CODE"), lit('')),10,' '), \
                                   rpad((coalesce(col("CLIENT_IDENTIFER"))),10,' '), \
                                   rpad(coalesce(col("DATE_PROGRAM_COMPLETED"),lit('')),8, ' '), \
                                   rpad(coalesce(col("ISSUED_FLAG"),lit('')), 1, ' ')
                                  )) 
  df=df.sort("BS_COURSE_CODE")
  df=df.selectExpr("BS_TRAINING_ORGANISATION_ID TRAINING_ORGANISATION_ID"
               ,"BS_COURSE_CODE COURSE_CODE"
               ,"CLIENT_IDENTIFER CLIENT_ID"
               ,"DATE_PROGRAM_COMPLETED"
               ,"ISSUED_FLAG"
               ,"Output").dropDuplicates()
  
  return df
