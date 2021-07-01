# Databricks notebook source
###########################################################################################################################  
# Function: GetAvetmissCourseOffering
#  Returns a dataframe that for the Course Offering Dimension
# Parameters:
#  dfUIO = Input DataFrame - Unit Instance Occurrences 
#  dfUnitInstances = Input Dataframe Unit Instances
#  dfDM = Reference Delivery Mode Table
#  dfAM = Reference Attendance Mode Table
#  dfLoc = Reference Location Table
#  dfOrgUnits = Input DataFrame - Organisational Units
#  dfFundingSource = Input Dataframe - Funding Source
#  dfOfferingStatus = Input Dataframe - Offering Status
# Returns:
#  A dataframe that has all attributes for Prada Course Offering
#############################################################################################################################
def GetAvetmissCourseOffering(dfUIO, dfUnitInstances, dfDM, dfAM, dfLoc, dfOrgUnits, dfFundingSource, dfOfferingStatus, startDate, endDate):
  
  #Alias Unit Offering Instances with PreFix
  df = dfUIO.select(*(col(x).alias("UIO_"+ x) for x in dfUIO.columns))
  
  #Join Unit Instance Occurrences and Unit Instances
  df = df.join(dfUnitInstances,df.UIO_FES_UINS_INSTANCE_CODE ==  dfUnitInstances.FES_UNIT_INSTANCE_CODE, how="left").filter(col("UI_LEVEL") == "COURSE")
  
  #Join df with Delivery Mode
  df = df.join(dfDM,df.UIO_FES_MOA_CODE ==  dfDM.DeliveryModeCode, how="left")

  #Join df with Attendeance Mode
  df = df.join(dfAM,df.UIO_FES_USER_8 ==  dfAM.AttendanceModeCode, how="left")

  #Join df with Location
  df = df.join(dfLoc,df.UIO_SLOC_LOCATION_CODE ==  dfLoc.LocationCode, how="left")
  
  #Organisational Units Filter - TS and FA
  dfOrgUnitsTS = dfOrgUnits.selectExpr("FES_FULL_NAME as TS_FES_FULL_NAME", "ORGANISATION_CODE as TS_ORGANISATION_CODE", "_DLTrustedZoneTimeStamp TS_DLTrustedZoneTimeStamp")#.filter(col("ORGANISATION_TYPE") == 'TS')
  dfOrgUnitsFA = dfOrgUnits.selectExpr("FES_FULL_NAME as FA_FES_FULL_NAME", "ORGANISATION_CODE as FA_ORGANISATION_CODE", "_DLTrustedZoneTimeStamp FA_DLTrustedZoneTimeStamp")#.filter(col("ORGANISATION_TYPE") == 'FA')
  
  #Join Organisational Units
  df = df.join(dfOrgUnitsTS, df.UIO_OWNING_ORGANISATION == dfOrgUnitsTS.TS_ORGANISATION_CODE, how="left")
  df = df.join(dfOrgUnitsFA, df.UIO_OFFERING_ORGANISATION == dfOrgUnitsFA.FA_ORGANISATION_CODE, how="left")
  
  #Funding Source
  df = df.join(dfFundingSource, df.UIO_NZ_FUNDING == dfFundingSource.FundingSourceCode, how="left")
  
  #Offering Status
  df = df.join(dfOfferingStatus, df.UIO_OFFERING_STATUS == dfOfferingStatus.OfferingStatusCode , how="left")
  
  #Calculated Columns
  df = df.withColumn("AvetmissDeliveryModeID", when(col("UIO_FES_MOA_CODE").isin(['CLAS','SELF','FLEX']),10)\
                     .when(col("UIO_FES_MOA_CODE").isin(['ELEC','ONLN','BLND']), 20)\
                     .when(col("UIO_FES_MOA_CODE").isin(['ONJB','ONJD','SIMU']), 30)\
                     .when(col("UIO_FES_MOA_CODE").isin(['DIST','FLXO','MIXD','OTHR']), 40)\
                     .otherwise(lit(None)))\
          .withColumn("AvetmissDeliveryModeName", when(col("UIO_FES_MOA_CODE").isin(['CLAS','SELF','FLEX']),"Classroom based")\
                     .when(col("UIO_FES_MOA_CODE").isin(['ELEC','ONLN','BLND']), 'Electronic based')\
                     .when(col("UIO_FES_MOA_CODE").isin(['ONJB','ONJD','SIMU']), 'Employment based')\
                     .when(col("UIO_FES_MOA_CODE").isin(['DIST','FLXO','MIXD','OTHR']), 'Other delivery')\
                     .otherwise(lit(None)))\
          .withColumn("TOLinOfferingcode", when(col("UIO_FES_USER_1").like("%TOL %"),"Tol Offering Code"))

  #INCREMENTALS
  df = df.where(expr(f"UIO__DLTrustedZoneTimeStamp BETWEEN '{startDate}' AND '{endDate}' \
        OR trusted.oneebs_ebs_0165_unit_instances._DLTrustedZoneTimeStamp BETWEEN '{startDate}' AND '{endDate}' \
        OR TS_DLTrustedZoneTimeStamp BETWEEN '{startDate}' AND '{endDate}' \
        OR FA_DLTrustedZoneTimeStamp BETWEEN '{startDate}' AND '{endDate}' \
        OR reference.delivery_mode._DLTrustedZoneTimeStamp BETWEEN '{startDate}' AND '{endDate}' \
        OR reference.attendance_mode._DLTrustedZoneTimeStamp BETWEEN '{startDate}' AND '{endDate}' \
        OR reference.funding_source._DLTrustedZoneTimeStamp BETWEEN '{startDate}' AND '{endDate}' \
        OR reference.offering_status._DLTrustedZoneTimeStamp BETWEEN '{startDate}' AND '{endDate}' \
        OR compliance.avetmiss_location._DLCuratedZoneTimeStamp BETWEEN '{startDate}' AND '{endDate}' \
      "))

  #Output Columns
  df = df.selectExpr(
    "UIO_UIO_ID as CourseOfferingID"
  , "UIO_COURSE_OCCURRENCE_CODE as CourseOfferingCode"
  , "UIO_CALOCC_OCCURRENCE_CODE as CourseCaloccCode"
  , "UIO_FES_USER_8 as AttendanceModeCode"
  , "AttendanceModeDescription"
  , "UIO_SLOC_LOCATION_CODE as OfferingEnrolmentLocation"
  , "UIO_FES_USER_1 as OfferingCode"
  , "UIO_DELIVERY_LOCATION as OfferingDeliveryLocation"
  , "UIO_FES_MOA_CODE as OfferingDeliveryMode"
  , "DeliveryModeName as OfferingDeliveryModeName"
  , "case \
       when upper(DeliveryModeDescription) = 'UNIT - CLASS SELF-PACED' then 'Unit - Class Self-Paced' \
       else replace(initcap(replace(DeliveryModeDescription, '/', '   ')), '   ', '/') \
     end as OfferingDeliveryModeDescription"
  , "AvetmissDeliveryModeID"
  , "AvetmissDeliveryModeName"
  , "UIO_FES_START_DATE as OfferingStartDate"
  , "UIO_FES_END_DATE as OfferingEnddate"
  , "UIO_STATUS Status" 
  , "OfferingStatusDescription" 
  , "UIO_FES_UINS_INSTANCE_CODE as CourseCode"
  , "TOLinOfferingcode"
  , "UIO_OWNING_ORGANISATION as TeachingSectionCode"
  ,"CASE \
      WHEN SUBSTRING(TS_FES_FULL_NAME, 4, 1) = '-' THEN SUBSTRING(TS_FES_FULL_NAME, 5, CHAR_LENGTH(TS_FES_FULL_NAME) - 4) \
      ELSE TS_FES_FULL_NAME \
    END AS TeachingSectionDescription"
  , "UIO_OFFERING_ORGANISATION as Faculty"
  , "FA_FES_FULL_NAME as FacultyDescription"
  , "UIO_MAXIMUM_HOURS as NominalHours"
  , "UIO_FES_POSSIBLE_HOURS as DeliveryHours"
  , "UIO_sp_visible as PublishToWebsites"
  , "UIO_sp_can_apply as OnlineApplications"
  , "UIO_can_register as OnlineRegistration"
  , "UIO_sp_can_enrol as OnlineEnrolments"
  , "UIO_fes_user_14 as WebStartDate"
  , "UIO_fes_user_9 as WebEndDate"
  , "UIO_can_apply as CampusApplications"
  , "UIO_can_enrol as CampusEnrolments"
  , "UIO_is_flexible_delivery as AnytimeEnrolment"
  , "UIO_created_date as CreatedDate"
  , "UIO_NZ_FUNDING as CourseFundingSourceCode"
  , "FundingSourceDescription as CourseFundingSourceDescription"
  , "UIO_FES_USER_16 as Duration"  
  , "UIO_OFFERING_TYPE as OfferingType"  
  ).dropDuplicates()
  
  return df

# COMMAND ----------


