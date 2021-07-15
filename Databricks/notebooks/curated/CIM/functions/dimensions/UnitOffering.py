# Databricks notebook source
############################################################################################################################  
# Function: GetCIMUnitOffering
#  Returns a dataframe that for the Unit Offering Dimension
# Parameters:
#  dfUIO = Input DataFrame - Unit Intance Occurrences 
#  dfUIOLinks = Input DataFrame - UIO Links
#  dfMDR = Input Dataframe Reference Master Data Reference
# Returns:
# A dataframe that has all attributes for CIM Unit Offering
##############################################################################################################################
def GetCIMUnitOffering(dfUIO, dfMDR, dfUIOLinks, startDate, endDate):

  df = GeneralAliasDataFrameColumns(dfUIO.where("CALOCC_CALENDAR_TYPE_CODE = 'UNIT'"), "UIO_")
  MDRdf = GeneralAliasDataFrameColumns(dfMDR.where("Ref_7 ='OFFERING_STATUS' AND TABLE_NAME = 'VERIFIERS' AND _RecordCurrent = 1"), "MDR_")
  UIOLdf = GeneralAliasDataFrameColumns(dfUIOLinks, "UIOL_")
  COdf = GeneralAliasDataFrameColumns(dfUIO.where("CALOCC_CALENDAR_TYPE_CODE = 'COURSE'"), "CO_")
  
  
  #Join Unit Instance Occurrences and Master Reference Data
  df = df.join(MDRdf, expr("MDR_ref_1 = UIO_OFFERING_STATUS"), how="left")
  df = df.join(UIOLdf, expr("UIOL_UIO_ID_TO = UIO_UIO_ID"), how="left")
  df = df.join(COdf, expr("CO_UIO_ID = UIOL_UIO_ID_FROM"), how="left")
  

  #INCREMENTALS
  df = df.where(expr(f"UIO__DLTrustedZoneTimeStamp BETWEEN '{startDate}' AND '{endDate}' \
        OR UIOL__DLTrustedZoneTimeStamp BETWEEN '{startDate}' AND '{endDate}' \
        OR CO__DLTrustedZoneTimeStamp BETWEEN '{startDate}' AND '{endDate}' \
      "))
  
  #Output Columns #PRADA-1275
  df = df.selectExpr(
      "CAST(UIO_UIO_ID AS BIGINT) as UnitOfferingID"
    , "UIO_COURSE_OCCURRENCE_CODE as UnitOfferingCode"
    , "UIO_CALOCC_OCCURRENCE_CODE as UnitCaloccCode"
    , "UIO_FES_UINS_INSTANCE_CODE as UnitCode"
    , "CAST(CO_UIO_ID AS BIGINT) as CourseOfferingID"
    , "UIO_MAXIMUM_HOURS as NominalHours"
    , "UIO_FES_POSSIBLE_HOURS as DeliveryHours"
    , "UIO_PREDOMINANT_DELIVERY_MODE as PredominantDeliveryMode"
    , "UIO_OFFERING_STATUS AS UnitOfferingStatusCode"
    , "MDR_Ref_3 as UnitOfferingStatusDescription"
    , "UIO_OFFERING_TYPE AS UnitOfferingType"
    , "UIO_FES_START_DATE as UnitOfferingStartDate"
    , "UIO_FES_END_DATE as UnitOfferingEnddate"
    ,"ROW_NUMBER() OVER(PARTITION BY UIO_UIO_ID ORDER BY UIO__transaction_date desc) AS UnitOfferingRank"
  ).dropDuplicates()
  
  df = df.where(expr("UnitOfferingRank == 1")).drop("UnitOfferingRank") \
  .orderBy("UnitOfferingID")
  
  return df

# COMMAND ----------


