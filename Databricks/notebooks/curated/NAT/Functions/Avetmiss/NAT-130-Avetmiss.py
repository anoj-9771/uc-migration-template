# Databricks notebook source
from pyspark.sql.functions import *
from datetime import datetime
from pyspark.sql.window import *
from pyspark.sql.types import *
from dateutil.relativedelta import relativedelta

# COMMAND ----------

def GetNAT130Avetmiss(dfAvetmissCourseEnrolment, dfAwards, dfConfigurableStatuses, params):
  ###########################################################################################################################
  # Function: GetNAT130Avetmiss
  # Parameters: 
  # dfAvetmissCourseEnrolment = Avetmiss Course Enrolment
  # dfAwards = EBS Awards
  # dfConfigurableStatuses = EBS Configurable Statuses
  # params = Master Notebook Parameters
  # Returns:
  #  Dataframe for all NAT 130 Avetmiss
  ############################################################################################################################
 #Params
  NAT_Dates = spark.read.json(sc.parallelize([params]))
  CollectionStart = params["CollectionStartAvt"]
  CollectionStartYear = (datetime.strptime(CollectionStart, '%Y-%m-%d')).year
  CollectionEnd = params["CollectionEndAvt"]
  CutOffDate = params["CutOffDateAvt"]
  CollectionEndYear = (datetime.strptime(CollectionEnd, '%Y-%m-%d')).year
  CollectionEndYearMinusOneYr = ((datetime.strptime(CutOffDate, '%Y-%m-%d')).year)-1
  CollectionEndYearMinusTwoYr = ((datetime.strptime(CutOffDate, '%Y-%m-%d')).year)-2
  CutOffDateAdd12Months = (datetime.strptime(CutOffDate, '%Y-%m-%d')) + relativedelta(months=+12)
  ReportingYear = params["ReportingYearAvt"]
  
  lastReportingDate = dfAvetmissCourseEnrolment.filter(col("ReportingYear")==lit(ReportingYear)).agg(max("ReportingDate")).collect()[0][0]

  #Join
  df = dfAvetmissCourseEnrolment.filter((col("ReportingYear")==lit(ReportingYear)) & (col("ReportingDate")==lastReportingDate))

  #Filter Withdrawn Awards
  df = df.join(dfAwards.filter(col("Code")=="WITHDRAWN"), df.AwardID == dfAwards.ID , how="left").filter(col("ID").isNull())

  #Filter Configurable Status
  df = df.join(dfConfigurableStatuses.filter((col("learner_viewable")=="Y") & (col("active")=="Y") & (col("status_type")=="AWARD") & (col("STATUS_CODE")!="Pending")), df.ConfigurableStatusID == dfConfigurableStatuses.ID , how="inner")

  #Filter Dates
  df = df.filter(
    ((year(col("CourseAwardDate")) == CollectionEndYear) & (year(col("CourseEnrolmentEndDate")) >= CollectionEndYear)) 
    | ((col("CourseAwardDate") > CutOffDate) & (year(col("CourseEnrolmentEndDate")).isin([CollectionEndYearMinusOneYr, CollectionEndYearMinusTwoYr])))
    | ((year(col("CourseAwardDate")) == CollectionEndYear) & (col("CourseEnrolmentEndDate") <= CollectionEnd) & (col("CourseAwardDate") >= CutOffDate))
    | (((year(col("DateConferred")) == CollectionEndYear) | (col("CourseAwardDate") >= CutOffDateAdd12Months)) & (year(col("CourseAwardDate")) >= CollectionEndYear-10))
  )

  #PRADA-1731
  df = df.where("CourseProgressStatus NOT IN ('W')")

  #PRADA-1624, PRADA-1733
  df = df.withColumn("Training_Ord_Id", lit("90003")) \
        .withColumn("Program_Id", col("AvetmissCourseCode")) \
        .withColumn("Client_ID", df.AvetmissClientId) \
        .withColumn("Year_Program_Comp", year(col("CourseAwardDate"))) \
        .withColumn("ISSUED_FLG", col("IssuedFlag")) \
        .withColumn("Award_Id", col("AwardID").cast('integer')) \
        .withColumn("Date_Awarded", date_format(col("CourseAwardDate"), "ddMMyyyy")) \
        .withColumn("Course_Start_Date", date_format(col("CourseEnrolmentStartDate"), "ddMMyyyy")) \
        .withColumn("Course_End_Date", date_format(col("CourseEnrolmentEndDate"), "ddMMyyyy")) \
        .withColumn("Progress_Code", col("CourseProgressCode")) \
        .withColumn("Progress_Status", col("CourseProgressStatus")) \
        .withColumn("Progress_Reason", col("CourseProgressReason")) \
        .withColumn("NZ_Funding", col("CourseFundingSourceCode")) \
        .withColumn("Parchment_Issue_Date", date_format(col("ParchmentIssueDate"), "ddMMyyyy")) \
        .withColumn("Parchment_Number", lit('')) \
        .withColumn("Date_Conferred", date_format(col("DateConferred"), "ddMMyyyy"))
  df = df.withColumn("Output", concat( \
        rpad(lit("90003"),10, ' '), \
        rpad(coalesce(col("AvetmissCourseCode"),lit('')), 15,' '), \
        rpad((coalesce(df.AvetmissClientId)),10,' '), \
        rpad(coalesce(year(col("CourseAwardDate")),lit('')),4, ' '), \
        rpad(coalesce(col("IssuedFlag"),lit('')), 1, ' '), \
        rpad(coalesce(col("AwardID").cast('integer'),lit('')), 15, ' '), \
        rpad(coalesce(date_format(col("CourseAwardDate"), "ddMMyyyy"),lit('')), 10, ' '), \
        rpad(coalesce(date_format(col("CourseEnrolmentStartDate"), "ddMMyyyy"),lit('')),10, ' '), \
        rpad(coalesce(date_format(col("CourseEnrolmentEndDate"), "ddMMyyyy"),lit('')),10, ' '), \
        rpad(coalesce(col("CourseProgressCode"),lit('')),20, ' '), \
        rpad(coalesce(col("CourseProgressStatus"),lit('')),4, ' '), \
        rpad(coalesce(col("CourseProgressReason"),lit('')),30, ' '), \
        rpad(coalesce(col("CourseFundingSourceCode"),lit('')),10, ' '), \
        rpad(coalesce(date_format(col("ParchmentIssueDate"), "ddMMyyyy"),lit('')),10, ' '), \
        rpad(lit(''),25, ' '), \
        rpad(coalesce(date_format(col("DateConferred"), "ddMMyyyy"),lit('')),10, ' ') \
      ))

  df = df.selectExpr(
    "Training_Ord_Id",
    "Program_Id",
    "Client_ID"
    ,"Year_Program_Comp"
    ,"IssuedFlag"
    ,"Award_Id"
    ,"Date_Awarded"
    ,"Course_Start_Date"
    ,"Course_End_Date"
    ,"Progress_Code"
    ,"Progress_Status"
    ,"Progress_Reason"
    ,"NZ_Funding"
    ,"Parchment_Issue_Date"
    ,"Parchment_Number"
    ,"Date_Conferred"
    ,"Output"
  )

  return df

# COMMAND ----------


