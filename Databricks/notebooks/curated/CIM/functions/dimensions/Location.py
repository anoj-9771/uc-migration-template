# Databricks notebook source
###########################################################################################################################
# Function: GetCIMLocation
#  GETS LOCATION DIMENSION 
# Parameters: 
#  locationDf = OneEBS_EBS_0165_locations
#  orgUnitDf = OneEBS_EBS_0165_organisation_units
#  orgUnitLinksDf = OneEBS_EBS_0165_org_unit_links
#  addressDf = OneEBS_EBS_0165_addresses
#  instituteDf = reference.institute
# Returns:
#  Dataframe of transformed Location
#############################################################################################################################

def GetCIMLocation(LocationDf, AddressDf, OrgUnitDf, OrgUnitLinkDf, InstituteDf):
  spark.udf.register("TidyCase", GeneralToTidyCase)
  #ALIAS TABLES
  df = GeneralAliasDataFrameColumns(LocationDf, "L_")
  dfAR = GeneralAliasDataFrameColumns(AddressDf.where("ADDRESS_TYPE = 'RES'"), "AR_")
  dfAP = GeneralAliasDataFrameColumns(AddressDf.where("ADDRESS_TYPE = 'POS'"), "AP_")
  dfAI = GeneralAliasDataFrameColumns(AddressDf.where("ADDRESS_TYPE = 'RES'"), "AI_")
  dfAIP = GeneralAliasDataFrameColumns(AddressDf.where("ADDRESS_TYPE = 'POS'"), "AIP_")
  dfOUP = GeneralAliasDataFrameColumns(OrgUnitDf, "OUP_")
  dfOUC = GeneralAliasDataFrameColumns(OrgUnitDf.where("ORGANISATION_TYPE = 'CO'"), "OUC_")
  dfOUPR = GeneralAliasDataFrameColumns(OrgUnitDf.where("ORGANISATION_TYPE = 'REG'"), "OUPR_")
  dfOUL = GeneralAliasDataFrameColumns(OrgUnitLinkDf, "OUL_")
  dfOULR = GeneralAliasDataFrameColumns(OrgUnitLinkDf, "OULR_")
  dfRI = GeneralAliasDataFrameColumns(InstituteDf, "RI_")

  #JOIN TABLES
  df = df.join(dfOUC, expr("OUC_ORGANISATION_CODE = L_ORGANISATION_CODE"), how="left")
  df = df.join(dfOUL, expr("OUL_SECONDARY_ORGANISATION = OUC_ORGANISATION_CODE"), how="left")
  df = df.join(dfOUP, expr("OUP_ORGANISATION_CODE = OUL_PRIMARY_ORGANISATION"), how="left")
  df = df.join(dfOULR, expr("OULR_SECONDARY_ORGANISATION = OUP_ORGANISATION_CODE"), how="left")
  df = df.join(dfOUPR, expr("OUPR_ORGANISATION_CODE = OULR_PRIMARY_ORGANISATION"), how="left")
  df = df.join(dfAR, expr("AR_OWNER_REF = L_ORGANISATION_CODE"), how="left")
  df = df.join(dfAP, expr("AP_OWNER_REF = L_ORGANISATION_CODE"), how="left")
  df = df.join(dfAI, expr("AI_OWNER_REF = OUP_ORGANISATION_CODE"), how="left")
  df = df.join(dfAIP, expr("AIP_OWNER_REF = OUP_ORGANISATION_CODE"), how="left")
  df = df.join(dfRI, expr("RIGHT(OUP_ORGANISATION_CODE, 3) = RI_InstituteID"), how="left")
  
  
  #SELECT / TRANSFORM
  df = df.selectExpr(
	 "L_SITE_CODE as LocationId"
    ,"L_LOCATION_CODE as LocationCode"
    ,"L_ORGANISATION_CODE as LocationOrganisationCode"
	,"L_FES_SHORT_DESCRIPTION as LocationName"
	,"L_FES_LONG_DESCRIPTION as LocationDescription"
	,"L_ADDRESS_LINE_1 AddressLine1"
    ,"L_ADDRESS_LINE_2 AddressLine2"
    ,"L_TOWN as Suburb"
    ,"L_REGION as AusState"
    ,"L_UK_POST_CODE_PT1 as Postcode"
    ,"L_FES_ACTIVE as ActiveFlag"
    ,"COALESCE(OUP_ORGANISATION_CODE, 'N/A') as InstituteID"
    ,"COALESCE(RI_InstituteCode, 'N/A') InstituteCode"
    ,"COALESCE(REPLACE(OUP_FES_FULL_NAME, ' Institute',''), 'Inactive') as InstituteName"
    ,"COALESCE(OUP_FES_FULL_NAME, 'Inactive') as InstituteDescription"
    ,"RI_RTOCode as RTOCode"
    ,"COALESCE(AI_ADDRESS_LINE_1, AIP_ADDRESS_LINE_1) InstituteAddressLine1"
    ,"CASE WHEN AI_ADDRESS_LINE_1 IS NOT NULL THEN AI_ADDRESS_LINE_2 ELSE AIP_ADDRESS_LINE_2 END InstituteAddressLine2"
    ,"CASE WHEN AI_ADDRESS_LINE_1 IS NOT NULL THEN AI_TOWN ELSE AIP_TOWN END InstituteSuburb"
    ,"CASE WHEN AI_ADDRESS_LINE_1 IS NOT NULL THEN AI_REGION ELSE AIP_REGION END InstituteAusState"
    ,"CASE WHEN AI_ADDRESS_LINE_1 IS NOT NULL THEN AI_UK_POST_CODE_PT1 ELSE AIP_UK_POST_CODE_PT1 END InstitutePostcode"
    ,"COALESCE(OUPR_ORGANISATION_CODE, 'N/A') as RegionID"
    ,"COALESCE(OUPR_FES_FULL_NAME, 'Inactive') as RegionName"
    ,"ROW_NUMBER() OVER( PARTITION BY L_LOCATION_CODE ORDER BY AR_START_DATE desc) AS AddressRank"
  )
  
  df = df.withColumn("InstituteName", expr("TidyCase(LOWER(InstituteName))")) \
  .withColumn("InstituteDescription", expr("TidyCase(LOWER(InstituteDescription))")) \
  .where(expr("AddressRank == 1")).drop("AddressRank") \
  .orderBy("LocationCode")
  
  return df

# COMMAND ----------


