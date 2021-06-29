# Databricks notebook source
###########################################################################################################################
# Function: GetAvetmissReportingPeriod
#  GETS THE REPORTING PERIOD
# Parameters: 
#  reportingPeriodReferenceDf = reference.reporting_period Dataframe
# Returns:
#  NULL
#############################################################################################################################
def GetAvetmissReportingPeriod(reportingPeriodReferenceDf):
  return reportingPeriodReferenceDf.selectExpr(
  "CAST(ReportingPeriodKey AS INT) ReportingPeriodKey" \
  ,"CAST(TO_DATE(CAST(UNIX_TIMESTAMP(ReportingDate, 'dd/MM/yyyy') AS TIMESTAMP)) AS DATE) ReportingDate" \
  ,"CAST(ReportingWeek AS INT) ReportingWeek /*02M-EDWDM-631*/" \
  ,"CAST(ReportingMonthNo AS INT) ReportingMonthNo /*03M-EDWDM-632*/" \
  ,"CAST(ReportingMonthName AS VARCHAR(10)) ReportingMonthName /*04M-EDWDM-633*/" \
  ,"CAST(ReportingQuarter AS INT) ReportingQuarter /*05M-EDWDM-634*/" \
  ,"CAST(ReportingSemester AS INT) ReportingSemester /*06M-EDWDM-635*/" \
  ,"CAST(ReportingYear AS VARCHAR(10)) ReportingYear /*07M-EDWDM-636*/" \
  ,"CAST(TO_DATE(CAST(UNIX_TIMESTAMP(ReportingDatePreviousYear, 'dd/MM/yyyy') AS TIMESTAMP)) AS DATE) ReportingDatePreviousYear /*09M-EDWDM-638*/" \
  ,"CAST(TO_DATE(CAST(UNIX_TIMESTAMP(ReportingDateNextYear, 'dd/MM/yyyy') AS TIMESTAMP)) AS DATE) ReportingDateNextYear /*10M-EDWDM-639*/" \
  ,"CAST(ReportingYearNo AS INT) ReportingYearNo" \
  ,"CAST(MonthNameFY AS VARCHAR(20)) MonthNameFY" \
  ,"CAST(SpecialReportingYear AS INT) SpecialReportingYear" \
  ,"CAST(ReportingPeriodKeyLastYear AS INT) ReportingPeriodKeyLastYear"
  ,"CAST(TO_DATE(CAST(UNIX_TIMESTAMP(ReportingYearStartDate, 'dd/MM/yyyy') AS TIMESTAMP)) AS DATE) ReportingYearStartDate" \
  ,"CAST(TO_DATE(CAST(UNIX_TIMESTAMP(ReportingYearEndDate, 'dd/MM/yyyy') AS TIMESTAMP)) AS DATE) ReportingYearEndDate" \
  )

# COMMAND ----------

