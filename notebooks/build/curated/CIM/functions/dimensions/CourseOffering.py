# Databricks notebook source
############################################################################################################################  
# Function: GetCIMCourseOffering
#  Returns a dataframe that for the Course Offering Dimension
# Parameters:
#  dfUIO = Input DataFrame - Unit Intance Occurrences 
# Returns:
# A dataframe that has all attributes for CIM Unit Offering
##############################################################################################################################
def GetCIMCourseOffering(dfUIO, startDate, endDate):
  #Alias Unit Offering Instances with PreFix
  df = GeneralAliasDataFrameColumns(dfUIO.where("CALOCC_CALENDAR_TYPE_CODE = 'COURSE'"), "UIO_")
  
  
  #Join Unit Instance Occurrences and Master Reference Data
  

  #INCREMENTALS
  df = df.where(expr(f"UIO__DLTrustedZoneTimeStamp BETWEEN '{startDate}' AND '{endDate}'"))
  
  #Output Columns #PRADA-1275
  df = df.selectExpr(
      "CAST(UIO_UIO_ID AS BIGINT) as CourseOfferingID"
    , "UIO_COURSE_OCCURRENCE_CODE as CourseOfferingCode"
    , "UIO_CALOCC_OCCURRENCE_CODE as CourseCaloccCode"
    , "UIO_FES_UINS_INSTANCE_CODE as CourseCode"
    , "UIO_MAXIMUM_HOURS as NominalHours"
    , "UIO_FES_POSSIBLE_HOURS as DeliveryHours"
    , "UIO_SP_VISIBLE as PublishToWebsites"
    , "UIO_SP_CAN_APPLY as OnlineApplications"
    , "UIO_CAN_REGISTER as OnlineRegistration"
    , "UIO_SP_CAN_ENROL as OnlineEnrolments"
    , "UIO_CAN_APPLY as CampusApplications"
    , "UIO_CAN_ENROL as CampusEnrolments"
    , "UIO_IS_FLEXIBLE_DELIVERY as AnytimeEnrolment"
    , "UIO_FES_USER_16 as Duration"
    , "UIO_OFFERING_TYPE as CourseOfferingType"
    , "UIO_FES_USER_14 as WebStartDate"
    , "UIO_FES_USER_9 as WebEndDate"
    , "UIO_STATUS AS Status"
    , "UIO_FES_START_DATE as CourseOfferingStartDate"
    , "UIO_FES_END_DATE as CourseOfferingEnddate"
    ,"ROW_NUMBER() OVER(PARTITION BY UIO_UIO_ID ORDER BY UIO__transaction_date desc) AS CourseOfferingRank"
  ).dropDuplicates()
  
  df = df.where(expr("CourseOfferingRank == 1")).drop("CourseOfferingRank") \
  .orderBy("CourseOfferingID")
  
  return df

# COMMAND ----------


