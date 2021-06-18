# Databricks notebook source
from pyspark.sql.functions import *
from datetime import datetime
from pyspark.sql.window import *
from pyspark.sql.types import *

# COMMAND ----------

############################################################################################################################  
# Function: GetCIMUnitOfferings
#  Returns a dataframe that for the Unit Offering Fact
# Parameters:
#  dfUIO = Input DataFrame - Unit Intance Occurrences 
# Returns:
# A dataframe that has all attributes for CIM Unit Offerings
##############################################################################################################################
def GetCIMUnitOfferings(dfUIO, startDate, endDate):

  dfUO = DeltaTableAsCurrent("cim.UnitOffering")
  dfDM = DeltaTableAsCurrent("reference.delivery_mode")
  dfAM = DeltaTableAsCurrent("reference.attendance_mode")
  dfTS = DeltaTableAsCurrent("cim.TeachingSection")
  dfEL = DeltaTableAsCurrent("cim.Location")
  dfOSP = DeltaTableAsCurrent("cim.OfferingSkillsPoint")
  
  df = GeneralAliasDataFrameColumns(dfUIO.where("CALOCC_CALENDAR_TYPE_CODE = 'UNIT'"), "UIO_")
  dfUO = GeneralAliasDataFrameColumns(dfUO, "UO_")
  dfDM = GeneralAliasDataFrameColumns(dfDM, "DM_")
  dfAM = GeneralAliasDataFrameColumns(dfAM, "AM_")
  dfTS = GeneralAliasDataFrameColumns(dfTS, "TS_")
  dfEL = GeneralAliasDataFrameColumns(dfEL, "EL_")
  dfOSP = GeneralAliasDataFrameColumns(dfOSP, "OSP_")
  
  #Join Location
  df = df.join(dfUO, expr("UIO_UIO_ID = UO_UnitOfferingID"), how="inner")
  df = df.join(dfDM, expr("UIO_FES_MOA_CODE = DM_DeliveryModeCode"), how="left")
  df = df.join(dfAM, expr("UIO_FES_USER_8 = AM_AttendanceModeCode"), how="left")
  df = df.join(dfTS, expr("UIO_OWNING_ORGANISATION = TS_TeachingSectionCode"), how="left")
  df = df.join(dfEL, expr("UIO_SLOC_LOCATION_CODE = EL_LocationCode"), how="left")
  df = df.join(dfOSP, expr("UIO_OFFERING_ORGANISATION = OSP_OfferingSkillsPointCostCentreCode"), how="left")

  #INCREMENTALS
  #df = df.where(expr(f"UIO__DLTrustedZoneTimeStamp BETWEEN '{startDate}' AND '{endDate}' \
  #      OR UO__DLCuratedZoneTimeStamp BETWEEN '{startDate}' AND '{endDate}' \
  #      OR DM__DLCuratedZoneTimeStamp BETWEEN '{startDate}' AND '{endDate}' \
  #      OR AM__DLCuratedZoneTimeStamp BETWEEN '{startDate}' AND '{endDate}' \
  #      OR TS__DLCuratedZoneTimeStamp BETWEEN '{startDate}' AND '{endDate}' \
  #      OR EL__DLCuratedZoneTimeStamp BETWEEN '{startDate}' AND '{endDate}' \
  #      OR OSP__DLCuratedZoneTimeStamp BETWEEN '{startDate}' AND '{endDate}' \
  #    "))
  
  df = df.selectExpr(
      "UO_UnitOfferingSK as UnitOfferingSK"
    , "DM_DeliveryModeCode as DeliveryModeCode"
    , "AM_AttendanceModeCode as AttendanceModeCode"
    , "TS_TeachingSectionSK as TeachingSectionSK"
    , "EL_LocationSK as EnrolmentLocationSK"
    , "OSP_OfferingSkillsPointSK as OfferingSkillsPointSK"
  ).dropDuplicates()
  
  df = df.na.fill('')
  
  df = df.withColumn("UnitOfferingsKey", concat_ws('|', df.UnitOfferingSK, df.DeliveryModeCode, df.AttendanceModeCode, df.TeachingSectionSK, df.EnrolmentLocationSK, df.OfferingSkillsPointSK))
  
  
  return df
