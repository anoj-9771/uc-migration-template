# Databricks notebook source
from pyspark.sql.functions import *
from datetime import datetime
from pyspark.sql.window import *
from pyspark.sql.types import *

# COMMAND ----------

############################################################################################################################  
# Function: GetCIMCourseOfferings
#  Returns a dataframe that for the Unit Offering Fact
# Parameters:
#  dfUIO = Input DataFrame - Unit Intance Occurrences 
# Returns:
# A dataframe that has all attributes for CIM Course Offerings
##############################################################################################################################
def GetCIMCourseOfferings(dfUIO, startDate, endDate):

  dfCO = DeltaTableAsCurrent("cim.CourseOffering")
  dfDM = DeltaTableAsCurrent("reference.delivery_mode")
  dfAM = DeltaTableAsCurrent("reference.attendance_mode")
  dfTS = DeltaTableAsCurrent("cim.TeachingSection")
  dfEL = DeltaTableAsCurrent("cim.Location")
  dfFS = DeltaTableAsCurrent("reference.funding_source")
  dfOSP = DeltaTableAsCurrent("cim.OfferingSkillsPoint")
  
  df = GeneralAliasDataFrameColumns(dfUIO.where("CALOCC_CALENDAR_TYPE_CODE = 'COURSE'"), "UIO_")
  dfCO = GeneralAliasDataFrameColumns(dfCO, "CO_")
  dfDM = GeneralAliasDataFrameColumns(dfDM, "DM_")
  dfAM = GeneralAliasDataFrameColumns(dfAM, "AM_")
  dfTS = GeneralAliasDataFrameColumns(dfTS, "TS_")
  dfEL = GeneralAliasDataFrameColumns(dfEL, "EL_")
  dfFS = GeneralAliasDataFrameColumns(dfFS, "FS_")
  dfOSP = GeneralAliasDataFrameColumns(dfOSP, "OSP_")
  
  #Join Location
  df = df.join(dfCO, expr("UIO_UIO_ID = CO_CourseOfferingID"), how="inner")
  df = df.join(dfDM, expr("UIO_FES_MOA_CODE = DM_DeliveryModeCode"), how="left")
  df = df.join(dfAM, expr("UIO_FES_USER_8 = AM_AttendanceModeCode"), how="left")
  df = df.join(dfTS, expr("UIO_OWNING_ORGANISATION = TS_TeachingSectionCode"), how="left")
  df = df.join(dfEL, expr("UIO_SLOC_LOCATION_CODE = EL_LocationCode"), how="left")
  df = df.join(dfFS, expr("UIO_NZ_FUNDING = FS_FundingSourceCode"), how="left")
  df = df.join(dfOSP, expr("UIO_OFFERING_ORGANISATION = OSP_OfferingSkillsPointCostCentreCode"), how="left")

  #INCREMENTALS
  #df = df.where(expr(f"UIO__DLTrustedZoneTimeStamp BETWEEN '{startDate}' AND '{endDate}' \
  #      OR CO__DLCuratedZoneTimeStamp BETWEEN '{startDate}' AND '{endDate}' \
  #      OR DM__DLCuratedZoneTimeStamp BETWEEN '{startDate}' AND '{endDate}' \
  #      OR AM__DLCuratedZoneTimeStamp BETWEEN '{startDate}' AND '{endDate}' \
  #      OR TS__DLCuratedZoneTimeStamp BETWEEN '{startDate}' AND '{endDate}' \
  #      OR EL__DLCuratedZoneTimeStamp BETWEEN '{startDate}' AND '{endDate}' \
  #      OR FS__DLCuratedZoneTimeStamp BETWEEN '{startDate}' AND '{endDate}' \
  #      OR OSP__DLCuratedZoneTimeStamp BETWEEN '{startDate}' AND '{endDate}' \
  #    "))
  
  df = df.selectExpr(
      "CO_CourseOfferingSK as CourseOfferingSK"
    , "DM_DeliveryModeCode as DeliveryModeCode"
    , "AM_AttendanceModeCode as AttendanceModeCode"
    , "TS_TeachingSectionSK as TeachingSectionSK"
    , "EL_LocationSK as EnrolmentLocationCode"
    , "FS_FundingSourceCode as FundingSourceCode"
    , "OSP_OfferingSkillsPointSK as OfferingSkillsPointSK" #Getting lots of NULLs here
  ).dropDuplicates()
  
  df = df.na.fill('')
  
  df = df.withColumn("CourseOfferingsKey", concat_ws('|', df.CourseOfferingSK, df.DeliveryModeCode, df.AttendanceModeCode, df.TeachingSectionSK, df.EnrolmentLocationCode, df.FundingSourceCode, df.OfferingSkillsPointSK))
  
  return df
