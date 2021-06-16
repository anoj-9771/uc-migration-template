# Databricks notebook source
############################################################################################################################  
# Function: GetAvetmissUnitOffering
#  Returns a dataframe that for the Unit Offering Dimension
# Parameters:
#  dfUIO = Input DataFrame - Unit Links Occurrences 
#  dfUIL = Input DataFrame - Unit Links
#  dfUnitInstances = Input Dataframe Unit Instances
#  dfDM = Reference Delivery Mode Table
#  dfAM = Reference Attendance Mode Table
#  dfLoc = Reference Location Table
#  refDeliveryModeAvetmissDeliveryModeDf = Join Delivery Mode Avetmiss Delivery Mode
# Returns:
# A dataframe that has all attributes for Prada Unit Offering
##############################################################################################################################
def GetAvetmissUnitOffering(dfUIO, dfUnitInstances, dfDM, dfAM, dfLoc, dfOrgUnits, refDeliveryModeAvetmissDeliveryModeDf, startDate, endDate):
  #Alias Unit Offering Instances with PreFix
  df = dfUIO.select(*(col(x).alias("UIO_"+ x) for x in dfUIO.columns))
  
  #Join Unit Instance Occurrences and Unit Instances
  df = df.join(dfUnitInstances,df.UIO_FES_UINS_INSTANCE_CODE ==  dfUnitInstances.FES_UNIT_INSTANCE_CODE, how="left").filter(col("UI_LEVEL") == "UNIT")
  
  #Join Location
  df = df.join(dfLoc, df.UIO_SLOC_LOCATION_CODE == dfLoc.LocationCode, how="left")

  #Organisational Units Filter - TS and FA
  dfOrgUnitsTS = dfOrgUnits.selectExpr("FES_FULL_NAME as TS_FES_FULL_NAME", "ORGANISATION_CODE as TS_ORGANISATION_CODE", "_DLTrustedZoneTimeStamp TS__DLTrustedZoneTimeStamp").filter(col("ORGANISATION_TYPE") == 'TS')
  dfOrgUnitsFA = dfOrgUnits.selectExpr("FES_FULL_NAME as FA_FES_FULL_NAME", "ORGANISATION_CODE as FA_ORGANISATION_CODE", "_DLTrustedZoneTimeStamp FA__DLTrustedZoneTimeStamp").filter(col("ORGANISATION_TYPE") == 'FA')

  #Join Delivery Mode
  df = df.join(dfDM, df.UIO_FES_MOA_CODE == dfDM.DeliveryModeCode, how="left")
  
  #Join Attendance Mode
  df = df.join(dfAM, df.UIO_FES_USER_8 == dfAM.AttendanceModeCode, how="left")
  
  #Join Organisation Units
  df = df.join(dfOrgUnits, df.UIO_OWNING_ORGANISATION == dfOrgUnits.ORGANISATION_CODE, how="left")
  df = df.join(dfOrgUnitsTS, df.UIO_OWNING_ORGANISATION == dfOrgUnitsTS.TS_ORGANISATION_CODE, how="left")
  df = df.join(dfOrgUnitsFA, df.UIO_OFFERING_ORGANISATION == dfOrgUnitsFA.FA_ORGANISATION_CODE, how="left")
  
  #Join Delivery Mode Avetmiss Delivery Mode
  df = df.join(refDeliveryModeAvetmissDeliveryModeDf, df.UIO_FES_MOA_CODE == refDeliveryModeAvetmissDeliveryModeDf.DeliveryModeCode, how="left" )

  #INCREMENTALS
  df = df.where(expr(f"UIO__DLTrustedZoneTimeStamp BETWEEN '{startDate}' AND '{endDate}' \
        OR TS__DLTrustedZoneTimeStamp BETWEEN '{startDate}' AND '{endDate}' \
        OR FA__DLTrustedZoneTimeStamp BETWEEN '{startDate}' AND '{endDate}' \
        OR trusted.oneebs_ebs_0165_unit_instances._DLTrustedZoneTimeStamp BETWEEN '{startDate}' AND '{endDate}' \
        OR trusted.oneebs_ebs_0165_organisation_units._DLTrustedZoneTimeStamp BETWEEN '{startDate}' AND '{endDate}' \
        OR reference.delivery_mode._DLTrustedZoneTimeStamp BETWEEN '{startDate}' AND '{endDate}' \
        OR reference.attendance_mode._DLTrustedZoneTimeStamp BETWEEN '{startDate}' AND '{endDate}' \
        OR reference.delivery_mode_avetmiss_delivery_mode._DLTrustedZoneTimeStamp BETWEEN '{startDate}' AND '{endDate}' \
        OR compliance.avetmiss_location._DLCuratedZoneTimeStamp BETWEEN '{startDate}' AND '{endDate}' \
      "))
  
  #Output Columns #PRADA-1275
  df = df.selectExpr(
      "CAST(UIO_UIO_ID AS BIGINT) as UnitOfferingID"
    , "UIO_COURSE_OCCURRENCE_CODE as UnitOfferingCode"
    , "UIO_FES_USER_8 as AttendanceCode"
    , "AttendanceModeName"
    , "AttendanceModeDescription"
    , "UIO_CALOCC_OCCURRENCE_CODE as CaloccOccurrenceCode"
    , "UIO_MAXIMUM_HOURS as MaximumHours"
    , "UIO_FES_USER_1 as OfferingCode"
    , "FES_FULL_NAME AS OfferingCostCentre"
    , "UIO_OWNING_ORGANISATION AS OfferingCostCentreCode"
    , "UIO_DELIVERY_LOCATION AS OfferingDeliveryLocation"
    , "UIO_FES_MOA_CODE AS OfferingDeliveryMode"
    , "DeliveryModeName"
    , "DeliveryModeDescription"
    , "AvetmissDeliveryModeID"
    , "LocationDescription AS OfferingEnrolmentLocation"
    , "UIO_NZ_FUNDING AS OfferingFundingSource"
    , "UIO_OFFERING_STATUS AS OfferingStatus"
    , "UIO_OFFERING_TYPE AS OfferingType"
    , "UIO_FES_START_DATE as OfferingStartDate"
    , "UIO_FES_END_DATE as OfferingEnddate"
    , "AvetmissDeliveryModeCode"
    , "UIO_OWNING_ORGANISATION as TeachingSectionCode /*PRADA-1606*/"
    , "CASE \
        WHEN SUBSTRING(TS_FES_FULL_NAME, 4, 1) = '-' THEN SUBSTRING(TS_FES_FULL_NAME, 5, CHAR_LENGTH(TS_FES_FULL_NAME) - 4) \
        ELSE TS_FES_FULL_NAME \
      END AS TeachingSectionDescription /*PRADA-1606*/"
    , "UIO_OFFERING_ORGANISATION as Faculty"
    , "FA_FES_FULL_NAME as FacultyDescription"
    , "UIO_FES_POSSIBLE_HOURS as DeliveryHours"
    , "UIO_PREDOMINANT_DELIVERY_MODE as PredominantDeliveryMode"
    , "FES_UNIT_INSTANCE_CODE as UnitCode"
  ).dropDuplicates()
  
  return df

# COMMAND ----------


