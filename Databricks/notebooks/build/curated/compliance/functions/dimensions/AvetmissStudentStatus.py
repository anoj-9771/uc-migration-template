# Databricks notebook source
def GetAvetmissStudentStatus(dfStudentStatusLog, dfStudentStatus, dfCurrentReportingPeriod, startDate, endDate):
  # ##########################################################################################################################  
  # Function: getStudentStatus
  # Returns a dataframe that for the Student Status Table
  # Parameters:
  # dfStudentStatusLog = Input DataFrame - Student Status Log 
  # dfStudentStatus = Input DataFrame - Student Status Reference 
  # dfDate = Input DataFrame - Curated Date
  # Returns:
  # A dataframe that has all attributes for the Student Status Table
  # ############################################################################################################################
  #Current reporting calendar year and calender year end date
  dfCurrentCalendarYear = dfCurrentReportingPeriod.filter(expr("left(ReportingYear, 1) = 'C'")).selectExpr("ReportingYearNo as ReportingYear", "ReportingYearEndDate")

  #Year Start and Year End Columns
  dfStudentStatusLogV = dfStudentStatusLog.withColumn("Start_Date_Year", year("START_DATE")).withColumn("End_Date_Year", coalesce(year("END_DATE"), lit("2999")))

  #Join Student Status Log
  df = dfCurrentCalendarYear.join(dfStudentStatusLogV, ((dfCurrentCalendarYear.ReportingYear >= dfStudentStatusLogV.Start_Date_Year) & (dfCurrentCalendarYear.ReportingYear <= dfStudentStatusLogV.End_Date_Year)), how="inner")

  #Filter on Reporting Period
  df = df.filter((col("END_DATE") >= col("ReportingYearEndDate")) | (col("END_DATE").isNull()))

  #Join Student Status
  df = df.join(dfStudentStatus, df.STUDENT_STATUS_CODE == dfStudentStatus.StudentStatusCode, how="left")

  #Fiter Reporting Year to At Least This year
  df = df.filter(col("ReportingYear") <= year(current_date()))

  #Agg and Sort
  df = df.groupBy("PERSON_CODE", "ReportingYear").agg(concat_ws(",",sort_array(collect_list("STUDENT_STATUS_CODE"))).alias("StudentStatusCode"))
  
  #Final Select Columns #PRADA-1275
  df = df.selectExpr("CAST(PERSON_CODE AS BIGINT) as PersonCode", "ReportingYear", "StudentStatusCode")
  
  return df
